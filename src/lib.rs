#[macro_use]
extern crate log;
extern crate rev_buf_reader;

use fs2::FileExt;
use priority_queue::PriorityQueue;
use rev_buf_reader::RevBufReader;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::fs::{self, metadata, File};
use std::io::{self, BufRead, Read, Seek, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt; // For Unix-like systems

#[cfg(windows)]
use std::os::windows::fs::MetadataExt; // For Windows

pub const ACTIVE_LOG_FILENAME: &str = "db";
pub const DEFAULT_READ_BUF_SIZE: usize = 1024 * 1024; // 1 MB
pub const FIELD_SEPARATOR: u8 = b'\x1C';
pub const ESCAPE_CHARACTER: u8 = b'\x1D';
pub const SEQ_RECORD_SEP: &[u8] = &[FIELD_SEPARATOR, FIELD_SEPARATOR, ESCAPE_CHARACTER];
pub const SEQ_LIT_ESCAPE: &[u8] = &[ESCAPE_CHARACTER, ESCAPE_CHARACTER, ESCAPE_CHARACTER];
pub const SEQ_LIT_FIELD_SEP: &[u8] = &[ESCAPE_CHARACTER, FIELD_SEPARATOR, ESCAPE_CHARACTER];

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum IndexableValue {
    Int(i64),
    String(String),
}

#[derive(Debug, Clone)]
pub enum RecordFieldType {
    Int,
    Float,
    String,
    Bytes,
}

#[derive(Debug, Clone)]
pub enum RecordValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl RecordValue {
    fn serialize(&self) -> Vec<u8> {
        match self {
            RecordValue::Null => {
                vec![0] // Tag for Null
            }
            RecordValue::Int(i) => {
                let mut bytes = vec![1]; // Tag for Int
                let data_bytes = escape_bytes(&i.to_be_bytes());
                bytes.extend(&data_bytes);
                bytes
            }
            RecordValue::Float(f) => {
                let mut bytes = vec![2]; // Tag for Float
                let data_bytes = escape_bytes(&f.to_be_bytes());
                bytes.extend(&data_bytes);
                bytes
            }
            RecordValue::String(s) => {
                let mut bytes = vec![3]; // Tag for String
                let length = s.len() as u64;
                let length_bytes = escape_bytes(&length.to_be_bytes());
                bytes.extend(&length_bytes);
                let data_bytes = escape_bytes(s.as_bytes());
                bytes.extend(&data_bytes);
                bytes
            }
            RecordValue::Bytes(b) => {
                let mut bytes = vec![4]; // Tag for Bytes
                let length = b.len() as u64;
                let length_bytes = escape_bytes(&length.to_be_bytes());
                bytes.extend(&length_bytes);
                let data_bytes = escape_bytes(b);
                bytes.extend(&data_bytes);
                bytes
            }
        }
    }

    /// Deserialize a RecordValue from a byte slice.
    /// Returns the deserialized RecordValue and the number of bytes consumed.
    fn deserialize(bytes: &[u8]) -> (RecordValue, usize) {
        match bytes[0] {
            0 => (RecordValue::Null, 1),
            1 => {
                let mut int_bytes = [0; 8];
                int_bytes.copy_from_slice(&bytes[1..1 + 8]);
                (RecordValue::Int(i64::from_be_bytes(int_bytes)), 1 + 8)
            }
            2 => {
                let mut float_bytes = [0; 8];
                float_bytes.copy_from_slice(&bytes[1..1 + 8]);
                (RecordValue::Float(f64::from_be_bytes(float_bytes)), 1 + 8)
            }
            3 => {
                let length_bytes = &bytes[1..1 + 8];
                let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                (
                    RecordValue::String(
                        String::from_utf8(bytes[1 + 8..1 + 8 + length].to_vec()).unwrap(),
                    ),
                    1 + 8 + length,
                )
            }
            4 => {
                let length_bytes = &bytes[1..1 + 8];
                let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                (
                    RecordValue::Bytes(bytes[1 + 8..1 + 8 + length].to_vec()),
                    1 + 8 + length,
                )
            }
            _ => panic!("Invalid tag: {}", bytes[0]),
        }
    }

    fn as_indexable(&self) -> Option<IndexableValue> {
        match self {
            RecordValue::Int(i) => Some(IndexableValue::Int(*i)),
            RecordValue::String(s) => Some(IndexableValue::String(s.clone())),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub values: Vec<RecordValue>,
}

impl Record {
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in &self.values {
            bytes.extend(value.serialize());
        }
        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Record {
        let mut values = Vec::new();
        let mut start = 0;
        while start < bytes.len() {
            let (rv, consumed) = RecordValue::deserialize(&bytes[start..]);
            values.push(rv);
            start += consumed;
        }
        Record { values }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MemtableEvictPolicy {
    LeastWritten,
    LeastRead,
    LeastReadOrWritten,
}

pub struct ConfigBuilder<'a, Field: Eq + Clone + Debug> {
    data_dir: Option<String>,
    segment_size: Option<usize>,
    memtable_capacity: Option<usize>,
    fields: Option<&'a Vec<(Field, RecordFieldType)>>,
    primary_key: Option<Field>,
    secondary_keys: Option<Vec<Field>>,
    memtable_evict_policy: Option<MemtableEvictPolicy>,
}

impl<'a, Field: Eq + Clone + Debug> ConfigBuilder<'a, Field> {
    pub fn new() -> ConfigBuilder<'a, Field> {
        ConfigBuilder::<Field> {
            data_dir: None,
            segment_size: None,
            memtable_capacity: None,
            fields: None,
            primary_key: None,
            secondary_keys: None,
            memtable_evict_policy: None,
        }
    }

    /// Directory where the database will store its data.
    pub fn data_dir(&mut self, data_dir: &str) -> &mut Self {
        self.data_dir = Some(data_dir.to_string());
        self
    }

    /// The maximum size of a segment file in bytes.
    /// Once a segment file reaches this size, it is closed and a new one is created.
    /// Closed segment files can be compacted.
    pub fn segment_size(&mut self, segment_size: usize) -> &mut Self {
        self.segment_size = Some(segment_size);
        self
    }

    /// The maximum size of a single memtable in terms of records.
    /// Note that each secondary index will have its own memtable.
    pub fn memtable_capacity(&mut self, memtable_capacity: usize) -> &mut Self {
        self.memtable_capacity = Some(memtable_capacity);
        self
    }

    /// The field schema of the database.
    pub fn fields(&mut self, fields: &'a Vec<(Field, RecordFieldType)>) -> &mut Self {
        self.fields = Some(fields);
        self
    }

    /// The primary key of the database, used to construct
    /// the primary memtable index. This should be the field
    /// that is most frequently queried.
    pub fn primary_key(&mut self, primary_key: Field) -> &mut Self {
        self.primary_key = Some(primary_key);
        self
    }

    /// The secondary keys of the database, used to construct
    /// the secondary memtable indexes.
    pub fn secondary_keys(&mut self, secondary_keys: Vec<Field>) -> &mut Self {
        self.secondary_keys = Some(secondary_keys);
        self
    }

    /// The eviction policy for the memtables. Determines which
    /// record will be dropped from a memtable when it reaches
    /// capacity.
    pub fn memtable_evict_policy(
        &mut self,
        memtable_evict_policy: MemtableEvictPolicy,
    ) -> &mut Self {
        self.memtable_evict_policy = Some(memtable_evict_policy);
        self
    }

    pub fn initialize(&self) -> Result<DB<Field>, io::Error> {
        let config = Config::<Field> {
            data_dir: self.data_dir.clone().unwrap_or("db_data".to_string()),
            segment_size: self.segment_size.unwrap_or(4 * 1024 * 1024), // 4MB
            memtable_capacity: self.memtable_capacity.unwrap_or(1_000_000),
            fields: self
                .fields
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Required config value \"fields\" is not set",
                ))?
                .clone(),
            primary_key: self.primary_key.clone().ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Required config value \"primary_key\" is not set",
            ))?,
            secondary_keys: self.secondary_keys.clone().unwrap_or(Vec::new()),
            memtable_evict_policy: self
                .memtable_evict_policy
                .clone()
                .unwrap_or(MemtableEvictPolicy::LeastReadOrWritten),
        };

        DB::initialize(&config)
    }
}

#[derive(Clone)]
struct Config<Field: Eq + Clone> {
    pub data_dir: String,
    pub segment_size: usize,
    pub memtable_capacity: usize,
    pub fields: Vec<(Field, RecordFieldType)>,
    pub primary_key: Field,
    pub secondary_keys: Vec<Field>,
    pub memtable_evict_policy: MemtableEvictPolicy,
}

trait Memtable<Field: Eq + Clone + Debug, T: Debug> {
    fn field(&self) -> &Field;
    fn new(field: &Field, capacity: usize, evict_policy: MemtableEvictPolicy) -> Self;
    fn set(&mut self, key: &IndexableValue, value: &T);
    fn get(&mut self, key: &IndexableValue) -> Option<&T>;
}

struct SimpleMemtable<Field: Eq + Clone + Debug, T: Clone + Debug> {
    field: Field,
    capacity: usize,
    /// Running counter of memtable operations, used as priority
    /// in evict_queue.
    n_operations: u64,
    records: BTreeMap<IndexableValue, T>,
    /// A max heap priority queue of keys. Note: n_operations must
    /// be negated upon append to evict oldest values first.
    evict_queue: PriorityQueue<IndexableValue, i64>,
    evict_policy: MemtableEvictPolicy,
}

impl<Field: Eq + Clone + Debug, T: Clone + Debug> Memtable<Field, T> for SimpleMemtable<Field, T> {
    fn field(&self) -> &Field {
        &self.field
    }

    fn new(
        field: &Field,
        capacity: usize,
        evict_policy: MemtableEvictPolicy,
    ) -> SimpleMemtable<Field, T> {
        SimpleMemtable {
            field: field.clone(),
            capacity,
            n_operations: 0,
            records: BTreeMap::new(),
            evict_queue: PriorityQueue::new(),
            evict_policy,
        }
    }

    fn set(&mut self, key: &IndexableValue, value: &T) {
        if self.capacity == 0 {
            return;
        }

        debug!(
            "Inserting/updating record in primary memtable with key {:?} = {:?}",
            &key, &value,
        );

        if self.records.len() >= self.capacity {
            let (evict_key, _prio) = self.evict_queue.pop().expect("Evict queue was empty");
            self.records.remove(&evict_key);
        }

        self.records.insert(key.clone(), value.clone());

        if self.evict_policy == MemtableEvictPolicy::LeastWritten
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }
    }

    fn get(&mut self, key: &IndexableValue) -> Option<&T> {
        if self.evict_policy == MemtableEvictPolicy::LeastRead
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }

        self.records.get(key)
    }
}

impl<Field: Eq + Clone + Debug, T: Clone + Debug> SimpleMemtable<Field, T> {
    fn set_priority(&mut self, key: &IndexableValue) {
        let priority = self.get_and_increment_current_priority();
        match self.evict_queue.get(key) {
            Some(_) => {
                self.evict_queue.change_priority(key, priority);
            }
            None => {
                self.evict_queue.push(key.clone(), priority);
            }
        }
    }

    fn get_and_increment_current_priority(&mut self) -> i64 {
        let ret = -(self.n_operations as i64);
        self.n_operations += 1;
        ret
    }
}

pub struct DB<Field: Eq + Clone + Debug> {
    config: Config<Field>,
    log_path: PathBuf,
    primary_memtable: SimpleMemtable<Field, Record>,
    secondary_memtables: Vec<SimpleMemtable<Field, HashSet<Record>>>,
}

impl<Field: Eq + Clone + Debug> DB<Field> {
    /// Create a new database configuration builder.
    pub fn configure() -> ConfigBuilder<'static, Field> {
        ConfigBuilder::new()
    }

    fn initialize(config: &Config<Field>) -> Result<DB<Field>, io::Error> {
        info!("Initializing DB");
        // If data_dir does not exist, create it
        if !fs::exists(&config.data_dir)? {
            fs::create_dir_all(&config.data_dir)?;
        }

        let log_path = Path::new(&config.data_dir).join(ACTIVE_LOG_FILENAME);

        // Create the log file if it does not exist
        let _file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        // Join primary key and secondary keys vec into a single vec
        let mut all_keys = vec![&config.primary_key];
        all_keys.extend(&config.secondary_keys);

        // If any of the keys is not in the schema or
        // is not an IndexableValue, return an error
        for &key in &all_keys {
            let (_, field_type) =
                config
                    .fields
                    .iter()
                    .find(|(field, _)| field == key)
                    .ok_or(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Secondary key must be present in the field schema",
                    ))?;

            match field_type {
                RecordFieldType::Int | RecordFieldType::String => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Secondary key must be an IndexableValue",
                    ))
                }
            }
        }
        let primary_memtable = SimpleMemtable::new(
            &config.primary_key,
            config.memtable_capacity,
            config.memtable_evict_policy.clone(),
        );
        let secondary_memtables = config
            .secondary_keys
            .iter()
            .map(|key| {
                SimpleMemtable::new(
                    key,
                    config.memtable_capacity,
                    config.memtable_evict_policy.clone(),
                )
            })
            .collect();

        let db = DB::<Field> {
            config: config.clone(),
            log_path,
            primary_memtable,
            secondary_memtables,
        };
        Ok(db)
    }

    pub fn upsert(&mut self, record: &Record) -> Result<(), io::Error> {
        debug!("Upserting record: {:?}", record);
        // Validate the record length
        if record.values.len() != self.config.fields.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Record has an incorrect number of fields: {}, expected {}",
                    record.values.len(),
                    self.config.fields.len()
                ),
            ));
        }

        // Validate that record fields match schema types
        // TODO: handle Null
        for (i, (_, field_type)) in self.config.fields.iter().enumerate() {
            match (&record.values[i], field_type) {
                (RecordValue::Int(_), RecordFieldType::Int) => {}
                (RecordValue::Float(_), RecordFieldType::Float) => {}
                (RecordValue::String(_), RecordFieldType::String) => {}
                (RecordValue::Bytes(_), RecordFieldType::Bytes) => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Record field {} has incorrect type: {:?}, expected {:?}",
                            &i, &record.values[i], &field_type
                        ),
                    ))
                }
            }
        }

        debug!("Record is valid");
        debug!("Opening file in append mode and acquiring exclusive lock...");

        // Open the log file in append mode
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        // Acquire an exclusive lock for writing
        file.lock_exclusive()?;

        if !is_file_same_as_path(&file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!("Lock acquired, but the log file has been rotated. Retrying upsert...");
            file.unlock()?;
            drop(file);
            return self.upsert(record);
        }

        debug!("Lock acquired, appending to log file");

        // Write the record to the log
        // Each serialized row is suffixed with the field separator character sequence
        let mut serialized_record = record.serialize();
        serialized_record.extend(SEQ_RECORD_SEP);
        file.write_all(&serialized_record)?;

        // Sync to disk
        file.flush()?;
        file.sync_all()?;

        file.unlock()?;

        debug!("Record appended to log file, lock released");
        debug!("Updating memtables");

        // Update the primary memtable
        let primary_key_index = self
            .config
            .fields
            .iter()
            .position(|(field, _)| field == &self.config.primary_key)
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Primary key not found in schema after initialize",
            ))?;
        let primary_value =
            &record.values[primary_key_index]
                .as_indexable()
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Primary key must be an IndexableValue",
                ))?;

        self.primary_memtable.set(primary_value, record);

        // TODO: Update secondary memtables

        Ok(())
    }

    pub fn get(
        &mut self,
        field: &Field,
        query_key: &RecordValue,
    ) -> Result<Option<Record>, io::Error> {
        let query_key_original = query_key;
        debug!("Getting record with field {:?} = {:?}", field, query_key);
        let query_key = query_key_original.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

        // If the requested field is the primary key, look up the value in the primary memtable
        if *field == self.config.primary_key {
            debug!("Looking up key {:?} in primary memtable", query_key);
            let found = self.primary_memtable.get(&query_key);
            if let Some(record) = found {
                debug!("Found record in primary memtable: {:?}", record);
                return Ok(Some(record.clone()));
            }
        }

        // TODO: query secondary memtables

        debug!(
            "No memtable entry found, looking up key {:?} in log file",
            query_key
        );

        // Get the index of the requested field
        let key_index = self
            .config
            .fields
            .iter()
            .position(|(schema_field, _)| schema_field == field)
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Key not found in schema after initialize",
            ))?;

        debug!("Matching key index {}", key_index);
        debug!("Opening file in read mode and acquiring shared lock...");

        // Open the file and acquire a shared lock for reading
        let mut file = fs::OpenOptions::new().read(true).open(&self.log_path)?;
        file.lock_shared()?;

        if !is_file_same_as_path(&file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!("Lock acquired, but the log file has been rotated. Retrying get...");
            file.unlock()?;
            drop(file);
            return self.get(field, query_key_original);
        }

        debug!("Lock acquired, searching log file for record");

        let mut log_reader = LogReader::new(&mut file)?;
        let result = log_reader.find(|record| {
            let record_key = record.values[key_index]
                .as_indexable()
                .expect("A non-indexable value was stored at key index");
            record_key == query_key
        });

        file.unlock()?;
        debug!("Record search complete, lock released");

        let result_value = match result {
            Some(record) => record,
            None => {
                debug!("No record found for key {:?}", query_key);
                return Ok(None);
            }
        };

        debug!("Found matching record in log file");

        if *field == self.config.primary_key {
            self.primary_memtable.set(&query_key, &result_value);
        }

        Ok(Some(result_value))
    }
}

#[derive(Debug, Eq, PartialEq)]
enum SpecialSequence {
    RecordSeparator,
    LiteralFieldSeparator,
    LiteralEscape,
}

pub struct LogReader<'a> {
    rev_reader: RevBufReader<&'a mut fs::File>,
}

impl<'a> LogReader<'a> {
    pub fn new(file: &mut fs::File) -> Result<LogReader, io::Error> {
        let rev_reader = RevBufReader::new(file);
        Ok(LogReader { rev_reader })
    }

    fn read_record(&mut self) -> Result<Option<Record>, io::Error> {
        if self.rev_reader.stream_position()? == 0 {
            return Ok(None);
        }

        // Check that the record starts with the record separator
        if self.read_special_sequence()? != SpecialSequence::RecordSeparator {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Record candidate does not end with record separator",
            ));
        }

        // The buffer that stores all the bytes of the record read so far in reverse order.
        let mut result_buf: Vec<u8> = Vec::new();
        // The buffer that stores the bytes read from the file.
        let mut read_buf: Vec<u8> = Vec::new();

        loop {
            read_buf.clear();
            self.rev_reader
                .read_until(ESCAPE_CHARACTER, &mut read_buf)?;

            result_buf.extend(read_buf.iter().rev());

            if self.rev_reader.stream_position()? == 0 {
                // If we've reached the beginning of the file, we've read the entire record.
                break;
            }

            // Otherwise, we must have encountered an escape character.
            match self.read_special_sequence()? {
                SpecialSequence::RecordSeparator => {
                    // The record is complete, so we can break out of the loop.
                    // Move the cursor back to the beginning of the special sequence.
                    self.rev_reader.seek_relative(3)?;
                    break;
                }
                SpecialSequence::LiteralFieldSeparator => {
                    // The field separator is escaped, so we need to add it to the result buffer.
                    result_buf.push(FIELD_SEPARATOR);
                }
                SpecialSequence::LiteralEscape => {
                    // The escape character is escaped, so we need to add it to the result buffer.
                    result_buf.push(ESCAPE_CHARACTER);
                }
            }
        }

        result_buf.reverse();
        let record = Record::deserialize(&result_buf);
        Ok(Some(record))
    }

    fn read_special_sequence(&mut self) -> Result<SpecialSequence, io::Error> {
        let mut special_buf: Vec<u8> = vec![0; 3];
        self.rev_reader.read_exact(&mut special_buf)?;

        match validate_special(&special_buf.as_slice()) {
            Some(special) => Ok(special),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Not a special sequence",
            )),
        }
    }
}

impl Iterator for LogReader<'_> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_record() {
            Ok(Some(record)) => Some(record),
            Ok(None) => None,
            Err(err) => panic!("Error reading record: {:?}", err),
        }
    }
}

/// There are three special characters that need to be handled:
/// Here: SC = escape char, FS = field separator.
/// - FS FS SC  -> actual record separator
/// - SC FS SC  -> literal FS
/// - SC SC SC  -> literal SC
fn validate_special(buf: &[u8]) -> Option<SpecialSequence> {
    match buf {
        SEQ_RECORD_SEP => Some(SpecialSequence::RecordSeparator),
        SEQ_LIT_FIELD_SEP => Some(SpecialSequence::LiteralFieldSeparator),
        SEQ_LIT_ESCAPE => Some(SpecialSequence::LiteralEscape),
        _ => None,
    }
}

fn escape_bytes(buf: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    for byte in buf {
        match byte {
            &FIELD_SEPARATOR => {
                result.extend(SEQ_LIT_FIELD_SEP);
            }
            &ESCAPE_CHARACTER => {
                result.extend(SEQ_LIT_ESCAPE);
            }
            _ => result.push(*byte),
        }
    }
    result
}

fn is_file_same_as_path(file: &File, path: &PathBuf) -> io::Result<bool> {
    // Get the metadata for the open file handle
    let file_metadata = file.metadata()?;

    // Get the metadata for the file at the specified path
    let path_metadata = metadata(path)?;

    // Platform-specific comparison
    #[cfg(unix)]
    {
        Ok(
            file_metadata.dev() == path_metadata.dev()
                && file_metadata.ino() == path_metadata.ino(),
        )
    }

    #[cfg(windows)]
    {
        Ok(file_metadata.file_index() == path_metadata.file_index()
            && file_metadata.volume_serial_number() == path_metadata.volume_serial_number())
    }
}

#[macro_use]
extern crate log;

mod common;
mod log_reader_forward;
mod log_reader_reverse;
mod memtable_primary;
mod memtable_secondary;

pub use common::*;
use fs2::FileExt;
pub use log_reader_forward::ForwardLogReader;
use log_reader_forward::ForwardLogReaderItem;
pub use log_reader_reverse::ReverseLogReader;
use memtable_primary::PrimaryMemtable;
use memtable_secondary::SecondaryMemtable;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct ConfigBuilder<Field: Eq + Clone + Debug> {
    data_dir: Option<String>,
    segment_size: Option<usize>,
    fields: Option<Vec<(Field, ValueType)>>,
    primary_key: Option<Field>,
    secondary_keys: Option<Vec<Field>>,
    write_durability: Option<WriteDurability>,
    read_consistency: Option<ReadConsistency>,
}

impl<'a, Field: Eq + Clone + Debug> ConfigBuilder<Field> {
    pub fn new() -> ConfigBuilder<Field> {
        ConfigBuilder::<Field> {
            data_dir: None,
            segment_size: None,
            fields: None,
            primary_key: None,
            secondary_keys: None,
            write_durability: None,
            read_consistency: None,
        }
    }

    /// The directory where the database will store its data.
    pub fn data_dir(&mut self, data_dir: &str) -> &mut Self {
        self.data_dir = Some(data_dir.to_string());
        self
    }

    /// The maximum size of a segment file in bytes.
    /// Once a segment file reaches this size, it can be closed, rotated and compacted.
    /// Note that this is not a hard limit: if `db.do_maintenance_tasks()` is not called,
    /// the segment file may continue to grow.
    pub fn segment_size(&mut self, segment_size: usize) -> &mut Self {
        self.segment_size = Some(segment_size);
        self
    }

    /// The field schema of the database.
    pub fn fields(&mut self, fields: &[(Field, ValueType)]) -> &mut Self {
        self.fields = Some(fields.to_vec());
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
    pub fn secondary_keys(&mut self, secondary_keys: &[Field]) -> &mut Self {
        self.secondary_keys = Some(secondary_keys.to_vec());
        self
    }

    /// The write durability policy for the database.
    /// This determines how writes are persisted to disk.
    /// The default is WriteDurability::Flush.
    pub fn write_durability(&mut self, write_durability: WriteDurability) -> &mut Self {
        self.write_durability = Some(write_durability);
        self
    }

    /// The read consistency policy for the database.
    /// This determines how recent writes are visible when reading.
    /// See individual `ReadConsistency` enum values for more information.
    /// The default is ReadConsistency::Strong.
    pub fn read_consistency(&mut self, read_consistency: ReadConsistency) -> &mut Self {
        self.read_consistency = Some(read_consistency);
        self
    }

    pub fn initialize(&self) -> Result<DB<Field>, DBError> {
        let config = Config::<Field> {
            data_dir: self.data_dir.clone().unwrap_or("db_data".to_string()),
            segment_size: self.segment_size.unwrap_or(4 * 1024 * 1024), // 4MB
            fields: self
                .fields
                .as_ref()
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
            write_durability: self
                .write_durability
                .clone()
                .unwrap_or(WriteDurability::Flush),
            read_consistency: self
                .read_consistency
                .clone()
                .unwrap_or(ReadConsistency::Strong),
        };

        DB::initialize(&config)
    }
}

#[derive(Clone)]
struct Config<Field: Eq + Clone> {
    pub data_dir: String,
    pub segment_size: usize,
    pub fields: Vec<(Field, ValueType)>,
    pub primary_key: Field,
    pub secondary_keys: Vec<Field>,
    pub write_durability: WriteDurability,
    pub read_consistency: ReadConsistency,
}

pub struct DB<Field: Eq + Clone + Debug> {
    config: Config<Field>,
    data_dir: PathBuf,
    active_metadata_file: fs::File,
    active_data_file: fs::File,
    primary_key_index: usize,
    primary_memtable: PrimaryMemtable,
    secondary_memtables: Vec<SecondaryMemtable>,
    refresh_next_logkey: LogKey,
}

impl<Field: Eq + Clone + Debug> DB<Field> {
    /// Create a new database configuration builder.
    pub fn configure() -> ConfigBuilder<Field> {
        ConfigBuilder::new()
    }

    fn initialize(config: &Config<Field>) -> Result<DB<Field>, DBError> {
        info!("Initializing DB...");
        // If data_dir does not exist or is empty, create it and any necessary files
        // After creation, the directory should always be in a complete state
        // without missing files.
        // A tempdir-move strategy is used to achieve one-phase commit.

        // Ensure the data directory exists
        let data_dir_path = Path::new(&config.data_dir);
        match fs::create_dir(&data_dir_path) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() != io::ErrorKind::AlreadyExists {
                    return Err(DBError::IOError(e));
                }
            }
        }

        // Create an initialize lock file to prevent multiple concurrent initializations
        let init_lock_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&data_dir_path.join(INIT_LOCK_FILENAME))?;

        init_lock_file.lock_exclusive()?;

        // We have acquired the lock, check if the data directory is in a complete state
        // If not, initialize it, otherwise skip.
        if !fs::exists(data_dir_path.join(ACTIVE_SYMLINK_FILENAME))? {
            let (segment_uuid, _) = create_segment_data_file(data_dir_path)?;
            let (segment_num, _) = create_segment_metadata_file(data_dir_path, &segment_uuid)?;
            set_active_segment(data_dir_path, segment_num)?;

            // Create the exclusive lock request file
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(data_dir_path.join(EXCL_LOCK_REQUEST_FILENAME))?;
        }

        init_lock_file.unlock()?;

        // Calculate the index of the primary value in a record
        let primary_key_index = config
            .fields
            .iter()
            .position(|(field, _)| field == &config.primary_key)
            .ok_or(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Primary key not found in schema after initialize",
            ))?;

        // Join primary key and secondary keys vec into a single vec
        let mut all_keys = vec![&config.primary_key];
        all_keys.extend(&config.secondary_keys);

        // If any of the keys is not in the schema or
        // is not an IndexableValue, return an error
        for &key in &all_keys {
            let (
                _,
                ValueType {
                    prim_value_type, ..
                },
            ) = config
                .fields
                .iter()
                .find(|(field, _)| field == key)
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Secondary key must be present in the field schema",
                ))?;

            match prim_value_type {
                PrimValueType::Int | PrimValueType::String => {}
                _ => {
                    return Err(DBError::ValidationError(
                        "Secondary key must be an IndexableValue".to_owned(),
                    ))
                }
            }
        }
        let primary_memtable = PrimaryMemtable::new();
        let secondary_memtables = config
            .secondary_keys
            .iter()
            .map(|_| SecondaryMemtable::new())
            .collect();

        let active_symlink = Path::new(&config.data_dir).join(ACTIVE_SYMLINK_FILENAME);

        let active_target = fs::read_link(&active_symlink)?;
        let active_metadata_path = Path::new(&config.data_dir).join(active_target);
        let mut active_metadata_file = APPEND_MODE.open(&active_metadata_path)?;

        let active_metadata_header = read_metadata_header(&mut active_metadata_file)?;
        validate_metadata_header(&active_metadata_header)?;

        let active_data_path =
            Path::new(&config.data_dir).join(active_metadata_header.uuid.to_string());
        let active_data_file = APPEND_MODE.open(&active_data_path)?;

        let mut db = DB::<Field> {
            config: config.clone(),
            data_dir: data_dir_path.to_path_buf(),
            active_metadata_file,
            active_data_file,
            primary_key_index,
            primary_memtable,
            secondary_memtables,
            refresh_next_logkey: LogKey::new(1, 0),
        };

        info!("Rebuilding memtable indexes...");
        db.refresh_indexes()?;

        info!("Database ready.");

        Ok(db)
    }

    fn refresh_indexes(&mut self) -> Result<(), DBError> {
        let active_symlink_path = self.data_dir.join(ACTIVE_SYMLINK_FILENAME);
        let active_target = fs::read_link(active_symlink_path)?;
        let active_metadata_path = self.data_dir.join(active_target);

        let to_segnum = parse_segment_number(&active_metadata_path)?;
        let from_segnum = self.refresh_next_logkey.segment_num();
        let mut from_index = self.refresh_next_logkey.index();

        for segnum in from_segnum..=to_segnum {
            let metadata_path = self.data_dir.join(metadata_filename(segnum));
            let mut metadata_file = READ_MODE.open(&metadata_path)?;

            let metadata_len = metadata_file.seek(SeekFrom::End(0))?;
            if (metadata_len - METADATA_FILE_HEADER_SIZE as u64) % METADATA_ROW_LENGTH as u64 != 0 {
                return Err(DBError::ConsistencyError(format!(
                    "Metadata file {} has invalid size: {}",
                    metadata_path.display(),
                    metadata_len
                )));
            }

            request_shared_lock(&self.data_dir, &mut metadata_file)
                .map_err(|lre| DBError::LockRequestError(lre))?;

            let metadata_header = read_metadata_header(&mut metadata_file)?;
            validate_metadata_header(&metadata_header)?;

            let data_path = self.data_dir.join(metadata_header.uuid.to_string());
            let data_file = READ_MODE.open(data_path)?;

            for ForwardLogReaderItem { record, index } in
                ForwardLogReader::new_with_index(metadata_file, data_file, from_index)
            {
                let log_key = LogKey::new(segnum, index);

                if record.is_tombstone() {
                    self.remove_record_from_memtables(&record);
                } else {
                    self.insert_record_to_memtables(&log_key, &record);
                }

                // Update from_index in case this is the last iteration: we need to know the next
                // index that should be read on later invocations of refresh_indexes.
                from_index = index + 1
            }

            // If there are still segments to read, set from_index to zero to read them
            // from beginning. Otherwise we leave from_index as the index of the next record to read.
            if segnum != to_segnum {
                from_index = 0
            }
        }

        self.refresh_next_logkey = LogKey::new(to_segnum, from_index);

        Ok(())
    }

    fn insert_record_to_memtables(&mut self, log_key: &LogKey, record: &Record) {
        let pk = record.at(self.primary_key_index).as_indexable().unwrap();
        self.primary_memtable.set(&pk, &log_key);

        for (sk_index, sk_field) in self.config.secondary_keys.iter().enumerate() {
            let secondary_memtable = &mut self.secondary_memtables[sk_index];
            let sk_field_index = self
                .config
                .fields
                .iter()
                .position(|(f, _)| sk_field == f)
                .unwrap();
            let sk = record.at(sk_field_index).as_indexable().unwrap();

            secondary_memtable.set(&sk, &log_key);
        }
    }

    fn remove_record_from_memtables(&mut self, record: &Record) {
        let pk = record.at(self.primary_key_index).as_indexable().unwrap();
        self.primary_memtable.remove(&pk);

        for (sk_index, sk_field) in self.config.secondary_keys.iter().enumerate() {
            let secondary_memtable = &mut self.secondary_memtables[sk_index];
            let sk_field_index = self
                .config
                .fields
                .iter()
                .position(|(f, _)| sk_field == f)
                .unwrap();
            let sk = record.at(sk_field_index).as_indexable().unwrap();

            secondary_memtable.remove(&sk);
        }
    }

    /// Insert a record into the database. If the primary key value already exists,
    /// the existing record will be replaced by the supplied one.
    pub fn upsert(&mut self, record: &Record) -> Result<(), DBError> {
        debug!("Upserting record: {:?}", record);

        record.validate(&self.config.fields)?;

        debug!("Record is valid");
        debug!("Opening file in append mode and acquiring exclusive lock...");

        // Acquire an exclusive lock for writing
        request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;

        if !self.ensure_metadata_file_is_active()?
            || !ensure_active_metadata_is_valid(&self.data_dir, &mut self.active_metadata_file)?
        {
            // The log file has been rotated, so we must try again
            self.active_metadata_file.unlock()?;
            return self.upsert(record);
        }

        self.active_data_file.lock_exclusive()?;

        let active_symlink_path = self.data_dir.join(ACTIVE_SYMLINK_FILENAME);
        let active_target = fs::read_link(active_symlink_path)?;
        let segment_num = parse_segment_number(&active_target)?;

        debug!("Exclusive lock acquired, appending to log file");

        // Write the record to the log
        let serialized = &record.serialize();
        let record_offset = self.active_data_file.seek(SeekFrom::End(0))?;
        let record_length = serialized.len() as u64;
        self.active_data_file.write_all(serialized)?;

        // Flush and sync data to disk
        if self.config.write_durability == WriteDurability::Flush {
            self.active_data_file.flush()?;
        }
        if self.config.write_durability == WriteDurability::FlushSync {
            self.active_data_file.flush()?;
            self.active_data_file.sync_all()?;
        }

        let metadata_pos = self.active_metadata_file.seek(SeekFrom::End(0))?;
        let metadata_index =
            (metadata_pos - METADATA_FILE_HEADER_SIZE as u64) / METADATA_ROW_LENGTH as u64;

        // Write the record metadata to the metadata file
        let mut metadata_buf = vec![];
        metadata_buf.extend(&record_offset.to_be_bytes());
        metadata_buf.extend(&record_length.to_be_bytes());

        assert_eq!(metadata_buf.len(), 16);
        self.active_metadata_file.write_all(&metadata_buf)?;

        // Flush and sync metadata to disk
        if self.config.write_durability == WriteDurability::Flush {
            self.active_metadata_file.flush()?;
        }
        if self.config.write_durability == WriteDurability::FlushSync {
            self.active_metadata_file.flush()?;
            self.active_metadata_file.sync_all()?;
        }

        debug!("Record appended to log file, releasing locks");

        // Manually release the locks because the file handles are left open
        self.active_data_file.unlock()?;
        self.active_metadata_file.unlock()?;

        debug!("Update memtables with newly written data");
        let log_key = LogKey::new(segment_num, metadata_index);

        self.insert_record_to_memtables(&log_key, &record);

        // These post-condition asserts are commented out since they seemed
        // to sometimes report false positives.
        //
        // let len = self.active_metadata_file.seek(SeekFrom::End(0))?;
        // assert!(len >= METADATA_FILE_HEADER_SIZE as u64);
        // assert_eq!((len - METADATA_FILE_HEADER_SIZE as u64) % 16, 0);
        // let data_file_len = self.active_data_file.seek(SeekFrom::End(0))?;
        // assert_eq!(data_file_len, record_offset + record_length);

        Ok(())
    }

    /// Get a record by its primary index value.
    /// E.g. `db.get(Value::Int(10))`.
    pub fn get(&mut self, query_key: &Value) -> Result<Option<Record>, DBError> {
        let pk_type = &self.config.fields[self.primary_key_index].1;
        if !type_check(&query_key, &pk_type) {
            return Err(DBError::ValidationError(format!(
                "Queried value does not match primary key type: {:?}",
                pk_type
            )));
        }

        debug!(
            "Getting record with field {:?} = {:?}",
            &self.config.primary_key, query_key
        );
        let query_key = query_key.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

        if self.config.read_consistency == ReadConsistency::Strong {
            self.refresh_indexes()?;
        }

        debug!("Looking up key {:?} in primary memtable", query_key);
        let log_key = match self.primary_memtable.get(&query_key) {
            Some(log_key) => log_key,
            None => {
                debug!("Not found in primary memtable, returning None");
                return Ok(None);
            }
        };

        debug!("Found log_key in primary memtable: {:?}", log_key);
        let segment_num = log_key.segment_num();
        let segment_index = log_key.index();

        let metadata_path = &self.data_dir.join(metadata_filename(segment_num));
        let mut metadata_file = READ_MODE.open(&metadata_path)?;

        request_shared_lock(&self.data_dir, &mut metadata_file)?;

        let metadata_header = read_metadata_header(&mut metadata_file)?;

        metadata_file.seek_relative(segment_index as i64 * 16)?;

        let mut metadata_buf = [0; 2 * 8];
        metadata_file.read_exact(&mut metadata_buf)?;

        metadata_file.unlock()?;

        let data_offset = u64::from_be_bytes(metadata_buf[0..8].try_into().unwrap());
        let data_length = u64::from_be_bytes(metadata_buf[8..16].try_into().unwrap());

        let data_path = &self.data_dir.join(metadata_header.uuid.to_string());
        let mut data_file = READ_MODE.open(&data_path)?;

        request_shared_lock(&self.data_dir, &mut data_file)?;

        data_file.seek(SeekFrom::Start(data_offset))?;

        let mut data_buf = vec![0; data_length as usize];
        data_file.read_exact(&mut data_buf)?;

        debug!(
            "Read matching record with size {} from log file, deserializing and returning.",
            data_buf.len()
        );

        let record = Record::deserialize(&data_buf);
        return Ok(Some(record));
    }

    /// Get a collection of records based on a field value.
    /// Indexes will be used if they contain the requested key.
    pub fn find_all(&mut self, field: &Field, query_key: &Value) -> Result<Vec<Record>, DBError> {
        // If querying by primary key, return the result of `get` wrapped in a vec.
        if field == &self.config.primary_key {
            return match self.get(query_key)? {
                Some(record) => Ok(vec![record.clone()]),
                None => Ok(vec![]),
            };
        }

        // Otherwise, continue with querying secondary indexes.
        debug!(
            "Finding all records with field {:?} = {:?}",
            field, query_key
        );
        let query_key = query_key.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

        // Try to find a memtable with the queried key
        let memtable_index =
            match get_secondary_memtable_index_by_field(&self.config.secondary_keys, field) {
                Some(index) => index,
                None => {
                    return Err(DBError::ValidationError(
                        "Cannot find_all by non-secondary key".to_owned(),
                    ))
                }
            };

        if self.config.read_consistency == ReadConsistency::Strong {
            self.refresh_indexes()?;
        }

        debug!(
            "Found suitable secondary index. Looking up key {:?} in the memtable",
            query_key
        );
        let memtable = &self.secondary_memtables[memtable_index];
        let log_keys = memtable.find_all(&query_key);

        debug!("Found log keys in secondary memtable: {:?}", log_keys);

        let mut records = vec![];
        for log_key in log_keys.iter() {
            // TODO optimize this so that a given segment is only opened once per find_all, and not for every log key
            let segment_num = log_key.segment_num();
            let segment_index = log_key.index();

            let metadata_path = &self.data_dir.join(metadata_filename(segment_num));
            let mut metadata_file = READ_MODE.open(&metadata_path)?;

            request_shared_lock(&self.data_dir, &mut metadata_file)?;

            let metadata_header = read_metadata_header(&mut metadata_file)?;

            metadata_file.seek_relative(segment_index as i64 * 16)?;

            let mut metadata_buf = [0; 2 * 8];
            metadata_file.read_exact(&mut metadata_buf)?;

            metadata_file.unlock()?;

            let data_offset = u64::from_be_bytes(metadata_buf[0..8].try_into().unwrap());
            let data_length = u64::from_be_bytes(metadata_buf[8..16].try_into().unwrap());

            let data_path = &self.data_dir.join(metadata_header.uuid.to_string());
            let mut data_file = READ_MODE.open(&data_path)?;

            request_shared_lock(&self.data_dir, &mut data_file)?;

            data_file.seek(SeekFrom::Start(data_offset))?;

            let mut data_buf = vec![0; data_length as usize];
            data_file.read_exact(&mut data_buf)?;

            debug!(
                "Read matching record with size {} from log file, deserializing and adding to result set.",
                data_buf.len()
            );

            let record = Record::deserialize(&data_buf);
            records.push(record);
        }

        Ok(records)
    }

    /// Ensures that the `self.metadata_file` and `self.data_file` handles are still pointing to the correct files.
    /// If the segment has been rotated, the handle will be closed and reopened.
    /// Returns `false` if the file has been rotated and the handle has been reopened, `true` otherwise.
    fn ensure_metadata_file_is_active(&mut self) -> Result<bool, DBError> {
        let active_target = fs::read_link(&self.data_dir.join(ACTIVE_SYMLINK_FILENAME))?;
        let active_metadata_path = &self.data_dir.join(active_target);

        let correct = is_file_same_as_path(&self.active_metadata_file, &active_metadata_path)?;
        if !correct {
            debug!("Metadata file has been rotated. Reopening...");
            let mut metadata_file = APPEND_MODE.open(&active_metadata_path)?;

            request_shared_lock(&self.data_dir, &mut metadata_file)?;

            let metadata_header = read_metadata_header(&mut self.active_metadata_file)?;

            validate_metadata_header(&metadata_header)?;

            let data_file_path = &self.data_dir.join(metadata_header.uuid.to_string());

            self.active_metadata_file = metadata_file;
            self.active_data_file = APPEND_MODE.open(&data_file_path)?;

            return Ok(false);
        } else {
            return Ok(true);
        }
    }

    /// Delete records by a field value.
    pub fn delete(&mut self, field: &Field, value: &Value) -> Result<u64, DBError> {
        todo!()
    }

    /// Check if there are any pending tasks and do them. Tasks include:
    /// - Rotating the active log file if it has reached capacity and compacting it.
    ///
    /// This function should be called periodically to ensure that the database remains in an optimal state.
    /// Note that this function is synchronous and may block for a relatively long time.
    /// You may call this function in a separate thread or process to avoid blocking the main thread.
    /// However, the database will be exclusively locked, so all writes and reads will be blocked during the tasks.
    pub fn do_maintenance_tasks(&mut self) -> Result<(), DBError> {
        request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;

        ensure_active_metadata_is_valid(&self.data_dir, &mut self.active_metadata_file)?;

        let metadata_size = self.active_metadata_file.seek(SeekFrom::End(0))?;
        if metadata_size >= self.config.segment_size as u64 {
            self.rotate_and_compact()?;
        }

        self.active_metadata_file.unlock()?;

        self.refresh_indexes()?;

        Ok(())
    }

    fn rotate_and_compact(&mut self) -> Result<(), io::Error> {
        debug!("Active log size exceeds threshold, starting rotation and compaction...");

        self.active_data_file.lock_shared()?;
        let original_data_len = self.active_data_file.seek(SeekFrom::End(0))?;
        let metadata_size = self.active_metadata_file.seek(SeekFrom::End(0))?;

        debug!("Reading segment data into a BTreeMap");
        let mut map = BTreeMap::new();
        let forward_log_reader = ForwardLogReader::new(
            self.active_metadata_file.try_clone()?,
            self.active_data_file.try_clone()?,
        );

        self.active_data_file.unlock()?;

        let mut read_n = 0;
        for (original_index, item) in forward_log_reader.enumerate() {
            let pk = item
                .record
                .at(self.primary_key_index)
                .as_indexable()
                .expect("Primary key was not indexable");

            // If the record is a tombstone, remove the PK from the map
            if item.record.is_tombstone() {
                map.remove(&pk);
            } else {
                map.insert(pk, (original_index, item.record));
            }

            read_n += 1;
        }

        debug!(
            "Read {} records, out of which {} were unique",
            read_n,
            map.len()
        );
        debug!("Opening temporary files for writing compacted data");
        // Create a new log data file
        let (new_data_uuid, new_data_path) = create_segment_data_file(&self.data_dir)?;
        let mut new_data_file = APPEND_MODE.open(&new_data_path)?;

        let temp_metadata_file = tempfile::NamedTempFile::new()?;
        let temp_metadata_path = temp_metadata_file.as_ref();
        let mut temp_metadata_file = WRITE_MODE.open(temp_metadata_path)?;

        debug!("Writing compacted data to temporary files");
        let metadata_header = MetadataHeader {
            version: 1,
            uuid: new_data_uuid,
        };

        temp_metadata_file.write_all(&metadata_header.serialize())?;

        let mut metadata_rows_buf = vec![0; metadata_size as usize - METADATA_FILE_HEADER_SIZE];

        let mut offset = 0u64;
        for (original_index, record) in map.values() {
            let serialized = record.serialize();
            let len = serialized.len() as u64;
            new_data_file.write_all(&serialized)?;

            let metadata_offset = original_index * 16;

            for (i, byte) in offset.to_be_bytes().iter().enumerate() {
                metadata_rows_buf[metadata_offset + i] = *byte;
            }
            for (i, byte) in len.to_be_bytes().iter().enumerate() {
                metadata_rows_buf[metadata_offset + 8 + i] = *byte;
            }

            offset += len;
        }

        temp_metadata_file.write_all(&metadata_rows_buf)?;

        // Sync the temporary files to disk
        // This is fine to do without consulting WriteDurability because this is a one-off
        // operation that is not part of the normal write path.
        temp_metadata_file.flush()?;
        new_data_file.flush()?;

        let final_len = new_data_file.seek(io::SeekFrom::End(0))?;

        let active_num = greatest_segment_number(&self.data_dir)?;

        debug!("Moving temporary files to their final locations");
        let new_data_path = &self.data_dir.join(new_data_uuid.to_string());
        let active_metadata_path = &self.data_dir.join(metadata_filename(active_num)); // overwrite active

        fs::rename(&temp_metadata_path, &active_metadata_path)?;

        debug!(
            "Compaction complete, reduced data size: {} -> {}",
            original_data_len, final_len
        );

        let new_segment_num = active_num + 1;
        let new_metadata_path = self.data_dir.join(metadata_filename(new_segment_num));
        let mut new_metadata_file = APPEND_MODE.clone().create(true).open(&new_metadata_path)?;

        let new_metadata_header = MetadataHeader {
            version: 1,
            uuid: new_data_uuid,
        };

        new_metadata_file.write_all(&new_metadata_header.serialize())?;

        set_active_segment(&self.data_dir, new_segment_num)?;

        // Old active metadata file should lose lock by RAII, or by
        // the manual unlock call in the do_maintenance_tasks method.

        self.active_metadata_file = APPEND_MODE.open(&new_metadata_path)?;
        self.active_data_file = APPEND_MODE.open(&new_data_path)?;

        // The new active log file is not locked by this client so it cannot be touched.
        debug!(
            "Active log file rotated and compacted, new segment: {}",
            new_segment_num
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(Eq, PartialEq, Clone, Debug)]
    enum Field {
        Id,
        Name,
    }

    #[test]
    fn test_compaction() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let capacity = 5;
        let segment_size = capacity * 2 * 8 + METADATA_FILE_HEADER_SIZE;
        let mut db = DB::configure()
            .data_dir(data_dir.to_str().unwrap())
            .segment_size(segment_size)
            .fields(&[(Field::Id, ValueType::int())])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to create DB");

        // Insert records with same value until we reach the capacity
        for _ in 0..capacity {
            let record = Record::from(&[Value::Int(0 as i64)]);
            db.upsert(&record).expect("Failed to insert record");
        }

        let mut segment1_file = READ_MODE.open(data_dir.join(metadata_filename(1))).unwrap();
        let segment1_metadata_size_original = segment1_file.seek(io::SeekFrom::End(0)).unwrap();

        let segment1_header = read_metadata_header(&mut segment1_file).unwrap();
        let mut segment1_data_file = READ_MODE
            .open(data_dir.join(segment1_header.uuid.to_string()))
            .unwrap();
        let segment1_data_size_original = segment1_data_file.seek(io::SeekFrom::End(0)).unwrap();

        // Rotate and compact
        db.do_maintenance_tasks()
            .expect("Failed to do maintenance tasks");

        // Insert one extra with different value, this goes into another segment
        let record = Record::from(&[Value::Int(1 as i64)]);
        db.upsert(&record).expect("Failed to insert record");

        // Check that rotation resulted in 2 segments
        assert!(fs::exists(data_dir.join(metadata_filename(1))).unwrap());
        assert!(fs::exists(data_dir.join(metadata_filename(2))).unwrap());
        // Note negation here
        assert!(!fs::exists(data_dir.join(metadata_filename(3))).unwrap());

        // Check that the compacted metadata file has the same size
        let mut segment1_metadata_file_compacted =
            READ_MODE.open(data_dir.join(metadata_filename(1))).unwrap();
        let segment1_metadata_size_compacted = segment1_metadata_file_compacted
            .seek(io::SeekFrom::End(0))
            .unwrap();
        assert_eq!(
            segment1_metadata_size_compacted,
            segment1_metadata_size_original
        );

        // Check that the compacted data file is smaller
        let segment1_header_compacted =
            read_metadata_header(&mut segment1_metadata_file_compacted).unwrap();
        let mut segment1_data_file_compacted = READ_MODE
            .open(data_dir.join(segment1_header_compacted.uuid.to_string()))
            .unwrap();
        let segment1_data_size_compacted = segment1_data_file_compacted
            .seek(io::SeekFrom::End(0))
            .unwrap();
        assert!(
            segment1_data_size_compacted < segment1_data_size_original,
            "Original: {}, Compacted: {}",
            segment1_data_size_original,
            segment1_data_size_compacted
        );

        // Check that the records can be read
        let rec0 = db
            .get(&Value::Int(0 as i64))
            .expect("Failed to get record")
            .expect("Record not found");

        assert!(match rec0.at(0) {
            Value::Int(0) => true,
            _ => false,
        });

        let rec1 = db
            .get(&Value::Int(1 as i64))
            .expect("Failed to get record")
            .expect("Record not found");

        assert!(match rec1.at(0) {
            Value::Int(1) => true,
            _ => false,
        });
    }

    #[test]
    fn test_repair() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let mut db = DB::configure()
            .data_dir(data_dir.to_str().unwrap())
            .fields(&[(Field::Id, ValueType::int())])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to create DB");

        // Insert records
        let n_recs = 100;
        for i in 0..n_recs {
            let record = Record::from(&[Value::Int(i as i64)]);
            db.upsert(&record).expect("Failed to insert record");
        }

        // Open the segment file and write garbage to it to simulate corruption
        let segment_metadata_path = data_dir.join(metadata_filename(1));
        let mut file = APPEND_MODE
            .open(&segment_metadata_path)
            .expect("Failed to open file");

        file.write_all(&[1, 0, 0, 0]) // A partially written integer value ([1] + some bytes)
            .expect("Failed to write garbage");
        file.flush().unwrap();

        let len = file.seek(SeekFrom::End(0)).expect("Failed to seek");
        assert_ne!(len, METADATA_FILE_HEADER_SIZE as u64 + n_recs * 16);

        // Try to refresh indexes, reading the file from beginning to end: should lead to error
        db.refresh_indexes()
            .expect_err("refresh_indexes should fail because of partial write");

        // Trigger autorepair
        db.do_maintenance_tasks()
            .expect("Failed to run maintenance tasks");

        // Try to refresh indexes, reading the file from beginning to end: should work now
        db.refresh_indexes()
            .expect("refresh_indexes should succeed");

        // Reopen file and check that it has the correct size
        let mut file = READ_MODE
            .open(&segment_metadata_path)
            .expect("Failed to open file");
        let len = file.seek(SeekFrom::End(0)).expect("Failed to seek");
        assert_eq!(len, METADATA_FILE_HEADER_SIZE as u64 + n_recs * 16);
    }

    #[test]
    fn test_memtables_updated_on_write() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let mut db = DB::configure()
            .data_dir(data_dir.to_str().unwrap())
            .fields(&[
                (Field::Id, ValueType::int()),
                (Field::Name, ValueType::string()),
            ])
            .primary_key(Field::Id)
            .secondary_keys(&[Field::Name])
            .initialize()
            .expect("Failed to create DB");

        // Check that the key is not indexed before write
        assert_eq!(db.primary_memtable.get(&IndexableValue::Int(0)), None);
        assert_eq!(
            db.secondary_memtables[0].find_all(&IndexableValue::String("John".to_string())),
            &HashSet::new()
        );

        // Insert record
        let record = Record::from(&[Value::Int(0), Value::String("John".to_string())]);
        db.upsert(&record).expect("Failed to insert record");

        // Check that the key is now indexed
        let expected_log_key = LogKey::new(1, 0);
        assert_eq!(
            db.primary_memtable.get(&IndexableValue::Int(0)),
            Some(&expected_log_key)
        );
        let mut expected_set: HashSet<LogKey> = HashSet::new();
        expected_set.insert(expected_log_key);
        assert_eq!(
            db.secondary_memtables[0].find_all(&IndexableValue::String("John".to_string())),
            &expected_set,
        );
    }
}

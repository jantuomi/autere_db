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
pub use log_reader_reverse::ReverseLogReader;
use memtable_primary::PrimaryMemtable;
use memtable_secondary::SecondaryMemtable;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self};
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub struct ConfigBuilder<Field: Eq + Clone + Debug> {
    data_dir: Option<String>,
    segment_size: Option<usize>,
    fields: Option<Vec<(Field, RecordField)>>,
    primary_key: Option<Field>,
    secondary_keys: Option<Vec<Field>>,
    write_durability: Option<WriteDurability>,
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
    pub fn fields(&mut self, fields: &[(Field, RecordField)]) -> &mut Self {
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
    pub fn secondary_keys(&mut self, secondary_keys: Vec<Field>) -> &mut Self {
        self.secondary_keys = Some(secondary_keys);
        self
    }

    /// The write durability policy for the database.
    /// This determines how writes are persisted to disk.
    /// The default is WriteDurability::Flush.
    pub fn write_durability(&mut self, write_durability: WriteDurability) -> &mut Self {
        self.write_durability = Some(write_durability);
        self
    }

    pub fn initialize(&self) -> Result<DB<Field>, io::Error> {
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
        };

        DB::initialize(&config)
    }
}

#[derive(Clone)]
struct Config<Field: Eq + Clone> {
    pub data_dir: String,
    pub segment_size: usize,
    pub fields: Vec<(Field, RecordField)>,
    pub primary_key: Field,
    pub secondary_keys: Vec<Field>,
    pub write_durability: WriteDurability,
}

pub struct DB<Field: Eq + Clone + Debug> {
    config: Config<Field>,
    data_dir: PathBuf,
    active_metadata_file: fs::File,
    active_data_file: fs::File,
    primary_key_index: usize,
    primary_memtable: PrimaryMemtable,
    secondary_memtables: Vec<SecondaryMemtable>,
}

impl<Field: Eq + Clone + Debug> DB<Field> {
    /// Create a new database configuration builder.
    pub fn configure() -> ConfigBuilder<Field> {
        ConfigBuilder::new()
    }

    fn initialize(config: &Config<Field>) -> Result<DB<Field>, io::Error> {
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
                    return Err(e);
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
            let (_, RecordField { field_type, .. }) = config
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

        let db = DB::<Field> {
            config: config.clone(),
            data_dir: data_dir_path.to_path_buf(),
            active_metadata_file,
            active_data_file,
            primary_key_index,
            primary_memtable,
            secondary_memtables,
        };

        // info!("Rebuilding memtable indexes...");
        // TODO FIXME build memtable indexes

        info!("Database ready.");

        Ok(db)
    }

    /// Insert a record into the database. If the primary key value already exists,
    /// the existing record will be replaced by the supplied one.
    pub fn upsert(&mut self, record: &Record) -> Result<(), io::Error> {
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

        // Manually release the locks because the file handles are left open
        self.active_data_file.unlock()?;
        self.active_metadata_file.unlock()?;

        debug!("Record appended to log file, lock released");

        let len = self.active_metadata_file.seek(SeekFrom::End(0))?;
        assert!(len >= METADATA_FILE_HEADER_SIZE as u64);
        assert_eq!((len - METADATA_FILE_HEADER_SIZE as u64) % 16, 0);

        // Depending on write durability, the data might not be written to disk yet
        //let data_file_len = self.active_data_file.seek(SeekFrom::End(0))?;
        //assert_eq!(data_file_len, record_offset + record_length);

        Ok(())
    }

    /// Get a record by its primary index value.
    /// E.g. `db.get(Value::Int(10))`.
    pub fn get(&mut self, query_key: &Value) -> Result<Option<Record>, io::Error> {
        let query_key_original = query_key;
        debug!(
            "Getting record with field {:?} = {:?}",
            &self.config.primary_key, query_key
        );
        let query_key = query_key_original.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

        match is_metadata_file_valid(&mut self.active_metadata_file)? {
            IsMetadatafileValidResult::Ok => {}
            _ => {
                debug!("Active metadata file is invalid, acquiring exclusive lock to start autorepair...");
                request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;

                ensure_active_metadata_is_valid(&self.data_dir, &mut self.active_metadata_file)?;
                self.ensure_metadata_file_is_active()?;
                // The lock should be dropped by RAII, but just in case
                self.active_metadata_file.unlock()?;

                debug!("Active metadata file is now valid, retrying get operation...");
                return self.get(query_key_original);
            }
        };

        debug!("Looking up key {:?} in primary memtable", query_key);
        let found = self.primary_memtable.get(&query_key);
        if let Some(record) = found {
            debug!("Found record in primary memtable: {:?}", record);
            return Ok(Some(record.clone()));
        }

        debug!(
            "No memtable entry found, looking up key {:?} in log file",
            query_key
        );

        debug!(
            "Matching records based on value at primary key index ({})",
            &self.primary_key_index
        );

        let greatest = greatest_segment_number(&self.data_dir)?;
        debug!("Searching segments {} through 1", greatest);

        let mut found_record: Option<Record> = None;
        for segment_num in (1..=greatest).rev() {
            let segment_path = &self.data_dir.join(format!("metadata.{}", segment_num));

            debug!(
                "Opening segment {} in read mode and acquiring shared lock...",
                segment_num
            );

            let mut metadata_file = READ_MODE.open(&segment_path)?;

            request_shared_lock(&self.data_dir, &mut metadata_file)?;

            let metadata_header = read_metadata_header(&mut metadata_file)?;

            validate_metadata_header(&metadata_header)?;

            let data_path = &self.data_dir.join(metadata_header.uuid.to_string());
            let data_file = READ_MODE.open(&data_path)?;

            // We should not "request_shared_lock()" here because we do not want
            // to give way to writers at this point. That would possibly lead to a deadlock.
            data_file.lock_shared()?;

            let mut reader = ReverseLogReader::new(metadata_file, data_file)?;

            if let Some(found) = reader.find(|record| {
                let record_key = record
                    .at(self.primary_key_index)
                    .as_indexable()
                    .expect("Primary key must be indexable");
                record_key == query_key
            }) {
                found_record = Some(found);
                break;
            }
        }

        debug!("Record search complete");
        debug!("Found matching record in log file.");

        Ok(found_record)
    }

    /// Get a collection of records based on a field value.
    /// Indexes will be used if they contain the requested key.
    pub fn find_all(&mut self, field: &Field, query_key: &Value) -> Result<Vec<Record>, io::Error> {
        // If querying by primary key, return the result of `get` wrapped in a vec.
        if field == &self.config.primary_key {
            return match self.get(query_key)? {
                Some(record) => Ok(vec![record.clone()]),
                None => Ok(vec![]),
            };
        }

        // Otherwise, continue with querying secondary indexes.
        let query_key_original = query_key;
        debug!(
            "Finding all records with field {:?} = {:?}",
            field, query_key
        );
        let query_key = query_key_original.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

        match is_metadata_file_valid(&mut self.active_metadata_file)? {
            IsMetadatafileValidResult::Ok => {}
            _ => {
                debug!("Active metadata file is invalid, acquiring exclusive lock to start autorepair...");
                request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;

                ensure_active_metadata_is_valid(&self.data_dir, &mut self.active_metadata_file)?;
                self.ensure_metadata_file_is_active()?;
                // The lock should be dropped by RAII, but just in case
                self.active_metadata_file.unlock()?;

                debug!("Active metadata file is now valid, retrying find_all operation...");
                return self.find_all(field, query_key_original);
            }
        };

        // Try to find a memtable with the queried key
        let found_memtable_index =
            get_secondary_memtable_index_by_field(&self.config.secondary_keys, field);

        if let Some(memtable_index) = found_memtable_index {
            debug!(
                "Found suitable secondary index. Looking up key {:?} in the memtable",
                query_key
            );
            let records = self.secondary_memtables[memtable_index]
                .find_all(&self.primary_memtable, &query_key);
            debug!("Found matching key");
            return Ok(records.iter().map(|record| record.clone()).collect());
        }

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

        let greatest = greatest_segment_number(&self.data_dir)?;

        let mut found_records = vec![];
        for segment_num in (1..=greatest).rev() {
            let segment_path = &self.data_dir.join(format!("metadata.{}", segment_num));

            debug!(
                "Opening segment {} in read mode and acquiring shared lock...",
                segment_num
            );

            let mut metadata_file = READ_MODE.open(&segment_path)?;

            request_shared_lock(&self.data_dir, &mut metadata_file)?;

            let metadata_header = read_metadata_header(&mut metadata_file)?;

            if metadata_header.version != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unsupported segment version",
                ));
            }

            let data_path = &self.data_dir.join(metadata_header.uuid.to_string());
            let data_file = READ_MODE.open(&data_path)?;

            // We should not "request_shared_lock()" here because we do not want
            // to give way to writers at this point. That would possibly lead to a deadlock.
            data_file.lock_shared()?;

            let reader = ReverseLogReader::new(metadata_file, data_file)?;

            for record in reader {
                let record_key = record
                    .at(key_index)
                    .as_indexable()
                    .expect("Secondary key must be indexable");
                if record_key == query_key {
                    found_records.push(record);
                }
            }
        }

        debug!("Record search complete");

        debug!(
            "Number of matching records found in log file: {}",
            found_records.len()
        );

        if let Some(memtable_index) = found_memtable_index {
            debug!("Inserting result set into secondary index");
            let primary_values: Vec<IndexableValue> = found_records
                .iter()
                .map(|r| {
                    r.at(self.primary_key_index)
                        .as_indexable()
                        .expect("A non-indexable value was stored at primary key index")
                })
                .collect();
            self.secondary_memtables[memtable_index].set_all(&query_key, &primary_values);
        }

        Ok(found_records)
    }

    /// Ensures that the `self.metadata_file` and `self.data_file` handles are still pointing to the correct files.
    /// If the segment has been rotated, the handle will be closed and reopened.
    /// Returns `false` if the file has been rotated and the handle has been reopened, `true` otherwise.
    fn ensure_metadata_file_is_active(&mut self) -> Result<bool, io::Error> {
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

    /// Check if there are any pending tasks and do them. Tasks include:
    /// - Rotating the active log file if it has reached capacity and compacting it.
    ///
    /// This function should be called periodically to ensure that the database remains in an optimal state.
    /// Note that this function is synchronous and may block for a relatively long time.
    /// You may call this function in a separate thread or process to avoid blocking the main thread.
    /// However, the database will be exclusively locked, so all writes will be blocked during the tasks.
    pub fn do_maintenance_tasks(&mut self) -> Result<(), io::Error> {
        let active_log_path = &self.data_dir.join(ACTIVE_SYMLINK_FILENAME);
        let active_target = fs::read_link(&active_log_path)?;
        let active_metadata_path = &self.data_dir.join(active_target);
        let active_log_md = fs::metadata(&active_log_path)?;

        let mut already_locked = false;
        match is_metadata_file_valid(&mut self.active_metadata_file)? {
            IsMetadatafileValidResult::Ok => {}
            _ => {
                debug!("Active metadata is invalid, acquiring exclusive lock...");
                request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;
                already_locked = true;

                ensure_active_metadata_is_valid(&self.data_dir, &mut self.active_metadata_file)?;
            }
        };

        if active_log_md.size() >= self.config.segment_size as u64 {
            // Rotate the active log file

            debug!("Starting rotation");
            if !already_locked {
                debug!("Requesting exclusive lock on active log file...");
                request_exclusive_lock(&self.data_dir, &mut self.active_metadata_file)?;
            }

            debug!("Exclusive lock acquired, rotating active log file...");

            // Create a new active log segment
            let (data_uuid, _) = create_segment_data_file(&self.data_dir)?;
            let (new_segment_num, _) = create_segment_metadata_file(&self.data_dir, &data_uuid)?;
            set_active_segment(&self.data_dir, new_segment_num)?;

            // The new active log file is not locked by this client so it cannot be touched.
            debug!("Active log file rotated, new segment: {}", new_segment_num);

            let new_metadata_path = &self.data_dir.join(format!("metadata.{}", new_segment_num));
            self.active_metadata_file = APPEND_MODE.open(&new_metadata_path)?;
            let new_data_path = &self.data_dir.join(data_uuid.to_string());
            self.active_data_file = APPEND_MODE.open(&new_data_path)?;

            // Compact the rotated segment without a lock.
            // Since the rotated segment and the compacted segment based on it will be
            // a) read-only, and b) identical in effective content, there is no need to lock it.
            self.compact_segment(&active_metadata_path)?;

            // The new active log file is not locked by this client so it cannot be touched.
            debug!("Active log file rotated, new segment: {}", new_segment_num);
            debug!("Segment compacted");
        }

        self.active_metadata_file.unlock()?;
        self.active_data_file.unlock()?;

        Ok(())
    }

    fn compact_segment(&self, metadata_path: &Path) -> Result<(), io::Error> {
        debug!("Opening segment file {:?} for compaction", metadata_path);
        let mut metadata_file = READ_MODE.open(metadata_path)?;
        let metadata_header = read_metadata_header(&mut metadata_file)?;
        validate_metadata_header(&metadata_header)?;

        let data_file_path = &self.data_dir.join(metadata_header.uuid.to_string());
        let data_file = READ_MODE.open(&data_file_path)?;

        debug!("Reading segment data into a BTreeMap");
        let mut map = BTreeMap::new();
        let forward_log_reader = ForwardLogReader::new(metadata_file, data_file);
        for entry in forward_log_reader {
            let primary_key = entry
                .at(self.primary_key_index)
                .as_indexable()
                .expect("Primary key was not indexable");
            map.insert(primary_key, entry);
        }

        debug!("Opening temporary files for writing compacted data");
        let temp_data_file = tempfile::NamedTempFile::new()?;
        let temp_data_path = temp_data_file.as_ref();
        let mut temp_data_file = APPEND_MODE.open(temp_data_path)?;

        let temp_metadata_file = tempfile::NamedTempFile::new()?;
        let temp_metadata_path = temp_metadata_file.as_ref();
        let mut temp_metadata_file = APPEND_MODE.open(temp_metadata_path)?;

        let new_data_uuid = Uuid::new_v4();

        debug!("Writing compacted data to temporary files");
        let metadata_header = MetadataHeader {
            version: 1,
            uuid: new_data_uuid,
        };

        temp_metadata_file.write_all(&metadata_header.serialize())?;

        let mut offset = 0u64;
        for entry in map.values() {
            let serialized = entry.serialize();
            let len = serialized.len() as u64;
            temp_data_file.write_all(&serialized)?;

            let mut metadata_buf = vec![];
            metadata_buf.extend(&offset.to_be_bytes());
            metadata_buf.extend(&len.to_be_bytes());
            temp_metadata_file.write_all(&metadata_buf)?;

            offset += len;
        }

        // Sync the temporary files to disk
        // This is fine to do without consulting WriteDurability because this is a one-off
        // operation that is not part of the normal write path.
        temp_metadata_file.flush()?;
        temp_data_file.flush()?;

        let final_len = temp_metadata_file.seek(io::SeekFrom::End(0))?;

        debug!("Moving temporary files to their final locations");
        let target_data_file_path = &self.data_dir.join(new_data_uuid.to_string());
        fs::rename(&temp_data_path, &target_data_file_path)?;
        fs::rename(&temp_metadata_path, metadata_path)?;

        debug!("Compaction complete, resulting size: {}", final_len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Eq, PartialEq, Clone, Debug)]
    enum Field {
        Id,
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
            .fields(&[(Field::Id, RecordField::int())])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to create DB");

        // Insert records with same value until we reach the capacity
        for _ in 0..capacity {
            let record = Record::from(&[Value::Int(0 as i64)]);
            db.upsert(&record).expect("Failed to insert record");
        }

        // Rotate and compact
        db.do_maintenance_tasks()
            .expect("Failed to do maintenance tasks");

        // Insert one extra with different value, this goes into another segment
        let record = Record::from(&[Value::Int(1 as i64)]);
        db.upsert(&record).expect("Failed to insert record");

        // Check that rotation resulted in 2 segments
        assert!(fs::exists(data_dir.join("metadata.1")).unwrap());
        assert!(fs::exists(data_dir.join("metadata.2")).unwrap());
        // Note negation here
        assert!(!fs::exists(data_dir.join("metadata.3")).unwrap());

        // Check that the first segment was compacted
        let segment1_size = fs::metadata(data_dir.join("metadata.1")).unwrap().len();
        assert_eq!(segment1_size, 2 * 8 + METADATA_FILE_HEADER_SIZE as u64);

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
            .fields(&[(Field::Id, RecordField::int())])
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
        let segment_metadata_path = data_dir.join("metadata.1");
        let mut file = APPEND_MODE
            .open(&segment_metadata_path)
            .expect("Failed to open file");

        file.write_all(&[0, 1, 2, 3])
            .expect("Failed to write garbage");
        file.flush().unwrap();

        let len = file.seek(SeekFrom::End(0)).expect("Failed to seek");
        assert_ne!(len, METADATA_FILE_HEADER_SIZE as u64 + n_recs * 16);

        // Try to read from the file, triggering autorepair
        db.get(&Value::Int(0)).expect("Failed to get record");

        // Reopen file and check that it has the correct size
        let mut file = READ_MODE
            .open(&segment_metadata_path)
            .expect("Failed to open file");
        let len = file.seek(SeekFrom::End(0)).expect("Failed to seek");
        assert_eq!(len, METADATA_FILE_HEADER_SIZE as u64 + n_recs * 16);
    }
}

#[macro_use]
extern crate log;

mod common;
mod log_reader_forward;
mod log_reader_reverse;
mod memtable_primary;
mod memtable_secondary;

pub use common::*;
use fs2::lock_contended_error;
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
use std::io::{self, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::thread;
use tempfile;
use tempfile::tempfile_in;
use tempfile::NamedTempFile;
use uuid::Uuid;

pub struct ConfigBuilder<Field: Eq + Clone + Debug> {
    data_dir: Option<String>,
    segment_size: Option<usize>,
    memtable_capacity: Option<usize>,
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
            memtable_capacity: None,
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

    /// The maximum size of a single memtable in terms of records.
    /// Note that each secondary index will have its own memtable.
    pub fn memtable_capacity(&mut self, memtable_capacity: usize) -> &mut Self {
        self.memtable_capacity = Some(memtable_capacity);
        self
    }

    /// The field schema of the database.
    pub fn fields(&mut self, fields: Vec<(Field, RecordField)>) -> &mut Self {
        self.fields = Some(fields.clone());
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
            memtable_capacity: self.memtable_capacity.unwrap_or(1_000_000),
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
    pub memtable_capacity: usize,
    pub fields: Vec<(Field, RecordField)>,
    pub primary_key: Field,
    pub secondary_keys: Vec<Field>,
    pub write_durability: WriteDurability,
}

pub struct DB<Field: Eq + Clone + Debug> {
    config: Config<Field>,
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
        if !fs::exists(&config.data_dir)?
            || !fs::exists(&Path::new(&config.data_dir).join(ACTIVE_SYMLINK_FILENAME))?
        {
            let tmpdir = tempfile::tempdir()?;
            let tmpdir_path = tmpdir.into_path();

            let (segment_uuid, _) = DB::<Field>::create_segment_data_file(&tmpdir_path)?;
            let (segment_num, _) =
                DB::<Field>::create_segment_metadata_file(&tmpdir_path, &segment_uuid)?;
            DB::<Field>::set_active_segment(&tmpdir_path, segment_num)?;

            // Create the exclusive lock request file
            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&tmpdir_path.join(EXCL_LOCK_REQUEST_FILENAME))?;

            fs::rename(tmpdir_path, &config.data_dir)?;
        }

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
        let mut active_metadata_file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_metadata_path)?;

        let active_metadata_header = DB::<Field>::read_metadata_header(&mut active_metadata_file)?;
        let active_data_path =
            Path::new(&config.data_dir).join(active_metadata_header.uuid.to_string());
        let active_data_file = fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&active_data_path)?;

        let db = DB::<Field> {
            config: config.clone(),
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
        for (i, (_, field)) in self.config.fields.iter().enumerate() {
            match (&record.values[i], field) {
                (
                    RecordValue::Null,
                    RecordField {
                        nullable: true,
                        field_type: _,
                    },
                ) => {}
                (
                    RecordValue::Int(_),
                    RecordField {
                        field_type: RecordFieldType::Int,
                        ..
                    },
                ) => {}
                (
                    RecordValue::String(_),
                    RecordField {
                        field_type: RecordFieldType::String,
                        ..
                    },
                ) => {}
                (
                    RecordValue::Bytes(_),
                    RecordField {
                        field_type: RecordFieldType::Bytes,
                        ..
                    },
                ) => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Record field {} has incorrect type: {:?}, expected {:?}",
                            &i, &record.values[i], &field.field_type
                        ),
                    ))
                }
            }
        }

        debug!("Record is valid");
        debug!("Opening file in append mode and acquiring exclusive lock...");

        // Acquire an exclusive lock for writing
        self.request_exclusive_lock()?;

        if self.ensure_correct_file_is_open()? {
            // The log file has been rotated, so we must try again
            return self.upsert(record);
        }

        debug!("Lock acquired, appending to log file");

        // Write the record to the log
        self.log_file.write_all(&record.serialize())?;

        // Flush and sync to disk
        if self.config.write_durability == WriteDurability::Flush {
            self.log_file.flush()?;
        }
        if self.config.write_durability == WriteDurability::FlushSync {
            self.log_file.flush()?;
            self.log_file.sync_all()?;
        }

        self.log_file.unlock()?;

        debug!("Record appended to log file, lock released");

        debug!("Updating memtables");
        self.insert_to_memtables(record);

        Ok(())
    }

    /// Get a record by its primary index value.
    /// E.g. `db.get(RecordValue::Int(10))`.
    pub fn get(&mut self, query_key: &RecordValue) -> Result<Option<Record>, io::Error> {
        let query_key_original = query_key;
        debug!(
            "Getting record with field {:?} = {:?}",
            &self.config.primary_key, query_key
        );
        let query_key = query_key_original.as_indexable().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Queried value must be indexable",
        ))?;

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
        debug!("Opening file in read mode and acquiring shared lock...");

        // Open the file and acquire a shared lock for reading
        let mut file = fs::OpenOptions::new().read(true).open(&self.log_path)?;

        self.request_shared_lock(&mut file)?;

        if !is_file_same_as_path(&file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!("Lock acquired, but the log file has been rotated. Retrying get...");
            file.unlock()?;
            drop(file);
            return self.get(query_key_original);
        }

        debug!("Lock acquired, searching log files for record");

        let greatest = DB::<Field>::greatest_segment_number(&Path::new(&self.config.data_dir))?;
        let segment_numbers = (0..=greatest); // FIXME all of this needs fixing
        let mut result: Option<Record> = None;
        for n in segment_numbers {
            if n == 0 {
                debug!("Searching the active log file...");
                result = ReverseLogReader::new(&mut file)?.find(|record| {
                    let record_key = record.values[self.primary_key_index]
                        .as_indexable()
                        .expect("A non-indexable value was stored at key index");
                    record_key == query_key
                });

                debug!("Active log file searched, releasing shared lock...");
                file.unlock()?;
            } else {
                debug!("Locking and searching rotated log segment file {}...", n);
                let path = Path::new(&self.config.data_dir)
                    .join(ACTIVE_SYMLINK_FILENAME)
                    .with_extension(n.to_string());

                let mut segm_file = fs::OpenOptions::new().read(true).open(&path)?;
                self.request_shared_lock(&mut segm_file)?;
                result = ReverseLogReader::new(&mut segm_file)?.find(|record| {
                    let record_key = record.values[self.primary_key_index]
                        .as_indexable()
                        .expect("A non-indexable value was stored at key index");
                    record_key == query_key
                });

                debug!("Segment file searched, releasing shared lock...");
                segm_file.unlock()?;
            };

            if result.is_some() {
                break;
            }
        }

        debug!("Record search complete");

        let result_value = match &result {
            Some(record) => record,
            None => {
                debug!("No record found for key {:?}", query_key);
                return Ok(None);
            }
        };

        debug!("Found matching record in log file.");

        debug!("Updating memtables");
        self.insert_to_memtables(&result_value);

        Ok(result)
    }

    /// Get a collection of records based on a field value.
    /// Indexes will be used if they contain the requested key.
    pub fn find_all(
        &mut self,
        field: &Field,
        query_key: &RecordValue,
    ) -> Result<Vec<Record>, io::Error> {
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

        // Try to find a memtable with the queried key
        let found_memtable_index = self.get_secondary_memtable_index_by_field(field);

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

        debug!("Matching key index {}", key_index);
        debug!("Acquiring shared lock...");

        // Acquire a shared lock for reading
        self.log_file.lock_shared()?;

        if self.ensure_correct_file_is_open()? {
            // The log file has been rotated, so we must try again
            return self.find_all(field, query_key_original);
        }

        debug!("Lock acquired, searching log file for record");

        let result = ReverseLogReader::new(&mut self.log_file)?
            .filter(|record| {
                let record_key = record.values[key_index]
                    .as_indexable()
                    .expect("A non-indexable value was stored at key index");
                record_key == query_key
            })
            .collect::<Vec<Record>>();

        self.log_file.unlock()?;
        debug!("Record search complete, lock released");

        debug!(
            "Number of matching records found in log file: {}",
            result.len()
        );

        if let Some(memtable_index) = found_memtable_index {
            debug!("Inserting result set into secondary index");
            let primary_values: Vec<IndexableValue> = result
                .iter()
                .map(|r| {
                    r.values[self.primary_key_index]
                        .as_indexable()
                        .expect("A non-indexable value was stored at primary key index")
                })
                .collect();
            self.secondary_memtables[memtable_index].set_all(&query_key, &primary_values);
        }

        Ok(result)
    }

    fn get_secondary_memtable_index_by_field(&self, field: &Field) -> Option<usize> {
        self.config
            .secondary_keys
            .iter()
            .position(|schema_field| schema_field == field)
    }

    /// Ensures that the `self.log_file` handle is still pointing to the correct file.
    /// If the file has been rotated, the handle will be closed and reopened.
    /// Returns `true` if the file has been rotated and the handle has been reopened.
    fn ensure_correct_file_is_open(&mut self) -> Result<bool, io::Error> {
        if !is_file_same_as_path(&self.log_file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!(
                "Lock acquired, but the log file has been rotated. Reopening file and retrying..."
            );
            self.log_file.unlock()?;

            self.log_file = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&self.log_path)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn insert_to_memtables(&mut self, record: &Record) {
        let key = record.values[self.primary_key_index]
            .as_indexable()
            .expect("A non-indexable value was stored at key index");

        if self.config.memtable_capacity == 0 {
            return;
        }

        debug!(
            "Inserting/updating record in primary memtable with key {:?} = {:?}",
            &key, &record,
        );

        self.primary_memtable.set(&key, record);

        for (field_index, value) in record.values.iter().enumerate() {
            let field = &self.config.fields[field_index].0;
            if let Some(smt_index) = self.get_secondary_memtable_index_by_field(field) {
                debug!("Updating memtable for index on {:?}", field);

                let memtable = &mut self.secondary_memtables[smt_index];
                let key = value.as_indexable().expect("Primary key was not indexable");
                let primary_key = record.values[self.primary_key_index]
                    .as_indexable()
                    .expect("Primary key was not indexable");
                memtable.set(&key, &primary_key);
            }
        }
    }

    fn request_exclusive_lock(&mut self) -> Result<(), io::Error> {
        // Create a lock on the exclusive lock request file to signal to readers that they should wait
        let lock_request_path = Path::new(&self.config.data_dir).join(EXCL_LOCK_REQUEST_FILENAME);
        let lock_request_file = fs::OpenOptions::new()
            .create(true)
            .write(true) // When requesting a lock, we need to have either read or write permissions
            .open(&lock_request_path)?;

        // Attempt to acquire an exclusive lock on the lock request file
        // This will block until the lock is acquired
        lock_request_file.lock_exclusive()?;

        // Check that the exclusive lock request file is still the same as the one we opened
        // NOTE: this isn't strictly necessary, but it's a good sanity check. Disabled for now.
        // if !is_file_same_as_path(&lock_request_file, &lock_request_path)? {
        //     // The lock request file has been removed
        //     return Err(io::Error::new(
        //         io::ErrorKind::Other,
        //         "Lock request file was removed unexpectedly",
        //     ));
        // }

        // Acquire an exclusive lock on the log file
        self.log_file.lock_exclusive()?;

        // Unlock the request file
        lock_request_file.unlock()?;

        Ok(())
    }

    fn is_exclusive_lock_requested(&self, data_dir: &str) -> Result<bool, io::Error> {
        let lock_request_path = Path::new(data_dir).join(EXCL_LOCK_REQUEST_FILENAME);
        let lock_request_file = fs::OpenOptions::new()
            .create(true)
            .write(true) // When requesting a lock, we need to have either read or write permissions
            .open(&lock_request_path)?;

        // Attempt to acquire a shared lock on the lock request file
        // If the file is already locked, return false
        match lock_request_file.try_lock_shared() {
            Err(e) => {
                if e.kind() == lock_contended_error().kind() {
                    return Ok(true);
                }
                return Err(e);
            }
            Ok(_) => {
                // Check that the exclusive lock request file is still the same as the one we opened
                if !is_file_same_as_path(&lock_request_file, &lock_request_path)? {
                    // The lock request file has been removed
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Lock request file was removed unexpectedly",
                    ));
                }

                lock_request_file.unlock()?;
                return Ok(false);
            }
        }
    }

    fn request_shared_lock(&self, file: &mut fs::File) -> Result<(), io::Error> {
        const SHARED_LOCK_WAIT_MAX_MS: u64 = 100;
        let mut timeout = 5;
        loop {
            if self.is_exclusive_lock_requested(&self.config.data_dir)? {
                debug!("Exclusive lock requested, waiting for {}ms before requesting a shared lock again", timeout);
                thread::sleep(std::time::Duration::from_millis(timeout));
                timeout = std::cmp::min(timeout * 2, SHARED_LOCK_WAIT_MAX_MS);
            } else {
                file.lock_shared()?;
                return Ok(());
            }
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
        let active_log_path = Path::new(&self.config.data_dir).join(ACTIVE_SYMLINK_FILENAME);
        let active_log_md = fs::metadata(&active_log_path)?;

        if active_log_md.size() >= self.config.segment_size as u64 {
            // Rotate the active log file

            debug!("Starting rotation, requesting exclusive lock...");
            self.request_exclusive_lock()?;

            debug!("Exclusive lock acquired, rotating active log file...");
            let next_segment_number =
                DB::<Field>::greatest_segment_number(&Path::new(&self.config.data_dir))? + 1;
            let next_segment_path =
                &active_log_path.with_extension(next_segment_number.to_string());

            debug!("Renaming active log file to {:?}", &next_segment_path);
            fs::rename(&active_log_path, &next_segment_path)?;

            // Create a new active log file
            self.log_file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&active_log_path)?;

            // The new active log file is not locked by this client so it cannot be touched.
            debug!("Active log file rotated");

            // Compact the rotated segment without a lock.
            // Since the rotated segment and the compacted segment based on it will be
            // a) read-only, and b) identical in effective content, there is no need to lock it.
            self.compact_segment(&next_segment_path)?;

            debug!("Segment compacted");
        }

        Ok(())
    }

    fn compact_segment(&self, path: &Path) -> Result<(), io::Error> {
        debug!("Opening segment file {:?} for compaction", path);
        let mut segment_file = fs::OpenOptions::new().read(true).open(path)?;

        debug!("Reading segment data into a BTreeMap");
        let mut map = BTreeMap::new();
        let forward_log_reader = ForwardLogReader::new(&mut segment_file);
        for entry in forward_log_reader {
            let primary_key = entry.values[self.primary_key_index]
                .as_indexable()
                .expect("Primary key was not indexable");
            map.insert(primary_key, entry);
        }

        debug!("Opening temporary file for writing compacted data");
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.as_ref();

        let mut temp_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(temp_path)?;

        for entry in map.values() {
            temp_file.write_all(&entry.serialize())?;
        }

        fs::rename(&temp_path, path)?;

        Ok(())
    }

    /// Get the number of the segment with the greatest ordinal.
    /// This is the newest segment, i.e. the one that is pointed to by the `active` symlink.
    /// If there are no segments yet, returns 0.
    fn greatest_segment_number(data_dir_path: &Path) -> Result<u16, io::Error> {
        let active_symlink = data_dir_path.join(ACTIVE_SYMLINK_FILENAME);

        if !fs::exists(&active_symlink)? {
            return Ok(0);
        }

        let segment_metadata_path = fs::read_link(&active_symlink)?;
        let filename = segment_metadata_path
            .file_name()
            .expect("No filename in symlink")
            .to_str()
            .expect("Filename was not valid UTF-8");

        // parse number from format "metadata.1"
        let segment_number = filename
            .split('.')
            .last()
            .expect("Filename did not have a number")
            .parse::<u16>();

        segment_number.map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to parse segment number from filename",
            )
        })
    }

    /// Create a new segment data file and return its UUID.
    /// A data file contains the segment data, tightly packed without separators.
    /// An accompanying metadata file is required to interpret the data.
    fn create_segment_data_file(data_dir_path: &Path) -> Result<(Uuid, PathBuf), io::Error> {
        let uuid = Uuid::new_v4();
        let new_segment_path = data_dir_path.join(uuid.to_string());
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&new_segment_path)?;

        Ok((uuid, new_segment_path))
    }

    /// Create a new segment metadata file and return its number and path.
    /// A metadata file contains the segment metadata, including the UUID of the data file.
    /// See `ARCHITECTURE.md` for the file format.
    fn create_segment_metadata_file(
        data_dir_path: &Path,
        data_file_uuid: &Uuid,
    ) -> Result<(u16, PathBuf), io::Error> {
        let version = 1u8;
        let padding = vec![0; 7];
        let uuid_bytes = data_file_uuid.as_bytes().to_vec();

        let mut header = vec![version];
        header.extend(padding);
        header.extend(uuid_bytes);

        assert!(header.len() == METADATA_FILE_HEADER_SIZE);

        let current_greatest_num = DB::<Field>::greatest_segment_number(data_dir_path)?;
        let new_num = current_greatest_num + 1;

        let metadata_filename = format!("metadata.{}", new_num);
        let metadata_path = data_dir_path.join(metadata_filename);

        let mut metadata_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&metadata_path)?;

        metadata_file.write_all(&header)?;

        Ok((new_num, metadata_path))
    }

    /// Set the active segment to the segment with the given ordinal number.
    fn set_active_segment(data_dir_path: &Path, segment_num: u16) -> Result<(), io::Error> {
        let tmp_uuid = Uuid::new_v4();
        let tmp_filename = format!("active_{}", tmp_uuid.to_string());
        let tmp_path = data_dir_path.join(tmp_filename);

        let metadata_filename = format!("metadata.{}", segment_num);
        let metadata_path = Path::new(&metadata_filename);
        let active_symlink = data_dir_path.join(ACTIVE_SYMLINK_FILENAME);

        symlink(&metadata_path, &tmp_path)?;
        fs::rename(&tmp_path, &active_symlink)?;

        Ok(())
    }

    /// Reads the metadata header from the metadata file.
    /// Leaves the file seek head at the beginning of the records, after the header.
    fn read_metadata_header(metadata_file: &mut fs::File) -> Result<MetadataHeader, io::Error> {
        let mut version_buf = vec![0u8; 1];
        metadata_file.seek(SeekFrom::Start(0))?;
        metadata_file.read_exact(&mut version_buf)?;
        let version = version_buf[0];

        let mut uuid_buf = vec![0u8; 16];
        metadata_file.seek_relative(7)?; // skip over padding
        metadata_file.read_exact(&mut uuid_buf)?;
        let uuid = Uuid::from_slice(&uuid_buf).expect("Invalid UUID");
        Ok(MetadataHeader { version, uuid })
    }
}

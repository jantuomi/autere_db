#[macro_use]
extern crate log;
extern crate rev_buf_reader;

mod common;
mod log_reader;
mod primary_memtable;
mod secondary_memtable;

pub use common::*;
use fs2::FileExt;
pub use log_reader::LogReader;
use primary_memtable::PrimaryMemtable;
use secondary_memtable::SecondaryMemtable;
use std::fmt::Debug;
use std::fs::{self};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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

pub struct DB<Field: Eq + Clone + Debug> {
    config: Config<Field>,
    log_path: PathBuf,
    primary_key_index: usize,
    primary_memtable: PrimaryMemtable,
    secondary_memtables: Vec<SecondaryMemtable<Field>>,
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
        let primary_memtable = PrimaryMemtable::new(
            config.memtable_capacity,
            config.memtable_evict_policy.clone(),
        );
        let secondary_memtables = config
            .secondary_keys
            .iter()
            .map(|key| {
                SecondaryMemtable::new(
                    key,
                    primary_key_index,
                    config.memtable_capacity,
                    config.memtable_evict_policy.clone(),
                )
            })
            .collect();

        let db = DB::<Field> {
            config: config.clone(),
            log_path,
            primary_key_index,
            primary_memtable,
            secondary_memtables,
        };
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
        debug!("Updating primary memtable");

        let primary_value =
            &record.values[self.primary_key_index]
                .as_indexable()
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Primary key must be an IndexableValue",
                ))?;

        self.primary_memtable.set(primary_value, record);

        self.secondary_memtables
            .iter_mut()
            .for_each(|secondary_memtable| {
                debug!(
                    "Updating memtable for index on {:?}",
                    &secondary_memtable.field
                );
                for (index, (schema_field, _)) in self.config.fields.iter().enumerate() {
                    if schema_field == &secondary_memtable.field {
                        let key = record.values[index]
                            .as_indexable()
                            .expect("Secondary index key was not indexable");
                        secondary_memtable.set(&key, record);
                    }
                }
            });

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
        file.lock_shared()?;

        if !is_file_same_as_path(&file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!("Lock acquired, but the log file has been rotated. Retrying get...");
            file.unlock()?;
            drop(file);
            return self.get(query_key_original);
        }

        debug!("Lock acquired, searching log file for record");

        let mut log_reader = LogReader::new(&mut file)?;
        let result = log_reader.find(|record| {
            let record_key = record.values[self.primary_key_index]
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

        debug!("Found matching record in log file. Storing result in primary memtable.");
        self.primary_memtable.set(&query_key, &result_value);

        Ok(Some(result_value))
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
        let found_memtable = self
            .secondary_memtables
            .iter_mut()
            .find(|mt| &mt.field == field);

        if let Some(memtable) = found_memtable {
            debug!(
                "Found suitable secondary index. Looking up key {:?} in the memtable",
                query_key
            );
            let records = memtable.find_all(&query_key);
            debug!("Found matching key");
            return Ok(records.iter().map(|&record| record.clone()).collect());
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
        debug!("Opening file in read mode and acquiring shared lock...");

        // Open the file and acquire a shared lock for reading
        let mut file = fs::OpenOptions::new().read(true).open(&self.log_path)?;
        file.lock_shared()?;

        if !is_file_same_as_path(&file, &self.log_path)? {
            // The log file has been rotated, so we must try again
            debug!("Lock acquired, but the log file has been rotated. Retrying find_all...");
            file.unlock()?;
            drop(file);
            return self.find_all(field, query_key_original);
        }

        debug!("Lock acquired, searching log file for record");

        let result = LogReader::new(&mut file)?
            .filter(|record| {
                let record_key = record.values[key_index]
                    .as_indexable()
                    .expect("A non-indexable value was stored at key index");
                record_key == query_key
            })
            .collect::<Vec<Record>>();

        file.unlock()?;
        debug!("Record search complete, lock released");

        debug!(
            "Number of matching records found in log file: {}",
            result.len()
        );

        if let Some(memtable) = found_memtable {
            debug!("Inserting result set into secondary index");
            memtable.set_all(&query_key, &result);
        }

        Ok(result)
    }
}

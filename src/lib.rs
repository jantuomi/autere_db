#[macro_use]
extern crate log;
pub mod log_db {
    use fs2::FileExt;
    use priority_queue::PriorityQueue;
    use std::collections::{BTreeMap, HashSet};
    use std::fs;
    use std::io;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    const ACTIVE_LOG_FILENAME: &str = "db";

    #[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
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
                    bytes.extend(&i.to_be_bytes());
                    bytes
                }
                RecordValue::Float(f) => {
                    let mut bytes = vec![2]; // Tag for Float
                    bytes.extend(&f.to_be_bytes());
                    bytes
                }
                RecordValue::String(s) => {
                    let mut bytes = vec![3]; // Tag for String
                    let length = s.len() as u64;
                    bytes.extend(&length.to_be_bytes());
                    bytes.extend(s.as_bytes());
                    bytes
                }
                RecordValue::Bytes(b) => {
                    let mut bytes = vec![3]; // Tag for Bytes
                    let length = b.len() as u64;
                    bytes.extend(&length.to_be_bytes());
                    bytes.extend(b);
                    bytes
                }
            }
        }

        fn deserialize(bytes: &[u8]) -> RecordValue {
            match bytes[0] {
                0 => RecordValue::Null,
                1 => {
                    let mut int_bytes = [0; 8];
                    int_bytes.copy_from_slice(&bytes[1..9]);
                    RecordValue::Int(i64::from_be_bytes(int_bytes))
                }
                2 => {
                    let mut float_bytes = [0; 8];
                    float_bytes.copy_from_slice(&bytes[1..9]);
                    RecordValue::Float(f64::from_be_bytes(float_bytes))
                }
                3 => {
                    let length_bytes = &bytes[1..9];
                    let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                    RecordValue::String(String::from_utf8(bytes[9..9 + length].to_vec()).unwrap())
                }
                4 => {
                    let length_bytes = &bytes[1..9];
                    let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                    RecordValue::Bytes(bytes[9..9 + length].to_vec())
                }
                _ => panic!("Invalid tag"),
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

    #[derive(Clone)]
    pub struct Config<Field: Eq + Clone> {
        /// Directory where the database will store its data.
        pub data_dir: String,
        /// The maximum size of a segment file in bytes.
        /// Once a segment file reaches this size, it is closed and a new one is created.
        /// Closed segment files can be compacted.
        pub segment_size: u64,
        /// The maximum size of a single memtable in bytes.
        /// Note that each secondary index will have its own memtable.
        pub memtable_size: u64,
        /// The field schema of the database.
        pub fields: Vec<(Field, RecordFieldType)>,
        /// The primary key of the database, used to construct
        /// the primary memtable index. This should be the field
        /// that is most frequently queried.
        pub primary_key: Field,
        /// The secondary keys of the database, used to construct
        /// the secondary memtable indexes.
        pub secondary_keys: Vec<Field>,
    }

    pub struct DB<Field: Eq + Clone> {
        config: Config<Field>,
        log_path: PathBuf,
        primary_memtable: BTreeMap<IndexableValue, Record>,
        secondary_memtables: Vec<BTreeMap<IndexableValue, HashSet<Record>>>,
    }

    impl<Field: Eq + Clone> DB<Field> {
        pub fn initialize(config: &Config<Field>) -> Result<DB<Field>, io::Error> {
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
            let primary_memtable = BTreeMap::<IndexableValue, Record>::new();
            let secondary_memtables = config
                .secondary_keys
                .iter()
                .map(|_| BTreeMap::<IndexableValue, HashSet<Record>>::new())
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
                                i, record.values[i], field_type
                            ),
                        ))
                    }
                }
            }

            // Open the log file in append mode
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_path)?;

            // Acquire an exclusive lock for writing
            file.lock_exclusive()?;

            // Write the record to the log
            // Each serialized row is suffixed with the field separator character
            let mut serialized = record
                .values
                .iter()
                .flat_map(|field| field.serialize())
                .collect::<Vec<u8>>();
            serialized.extend(vec![b'\x1C']);
            file.write_all(&serialized)?;

            // Sync to disk
            file.flush()?;
            file.sync_all()?;

            file.unlock()?;

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

            self.primary_memtable
                .insert(primary_value.clone(), record.clone());

            // TODO: Update secondary memtables

            Ok(())
        }

        pub fn get(&self, field: Field, key: RecordValue) -> Result<Option<Record>, io::Error> {
            // If the requested field is the primary key, look up the value in the primary memtable
            if field == self.config.primary_key {
                let primary_value = key.as_indexable().ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Primary key must be an IndexableValue",
                ))?;

                let found = self.primary_memtable.get(&primary_value);
                if let Some(record) = found {
                    return Ok(Some(record.clone()));
                }
            }

            // TODO: query secondary memtables

            // Get the index of the requested field
            let key_index = self
                .config
                .fields
                .iter()
                .position(|(schema_field, _)| schema_field == &field)
                .ok_or(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Key not found in schema after initialize",
                ))?;

            // Open the file and acquire a shared lock for reading
            let file = fs::OpenOptions::new().read(true).open(&self.log_path)?;
            file.lock_shared()?;

            // Do a log file scan from the bottom up to find the record
            // TODO

            file.unlock()?;

            Ok(None)
        }
    }
}

#[macro_use]
extern crate log;
pub mod log_db {
    use fs2::FileExt;
    use priority_queue::PriorityQueue;
    use std::collections::BTreeMap;
    use std::fs;
    use std::io;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    const ACTIVE_LOG_FILENAME: &str = "db";

    pub trait Record {
        fn serialize(&self) -> Vec<u8>;
        fn deserialize(data: Vec<u8>) -> Self;
    }

    pub struct Config {
        /// Directory where the database will store its data.
        pub data_dir: String,
        /// The maximum size of a segment file in bytes.
        /// Once a segment file reaches this size, it is closed and a new one is created.
        /// Closed segment files can be compacted.
        pub segment_size: u64,
        /// The maximum size of a single memtable in bytes.
        /// Note that each secondary index will have its own memtable.
        pub memtable_size: u64,
    }

    pub struct DB<T: Record> {
        data_dir: String,
        segment_size: u64,
        memtable_size: u64,

        pub log_path: PathBuf,
        primary_memtable: BTreeMap<u64, T>,
    }

    impl<T: Record> DB<T> {
        pub fn initialize(config: &Config) -> Result<DB<T>, io::Error> {
            info!("Initializing DB");
            // If data_dir does not exist, create it
            if !fs::exists(&config.data_dir)? {
                fs::create_dir_all(&config.data_dir)?;
            }

            let log_path = Path::new(&config.data_dir).join(ACTIVE_LOG_FILENAME);

            let db = DB::<T> {
                data_dir: config.data_dir.clone(),
                segment_size: config.segment_size,
                memtable_size: config.memtable_size,

                log_path,
                primary_memtable: BTreeMap::new(),
            };
            Ok(db)
        }

        pub fn upsert(&self, record: &T) -> Result<(), io::Error> {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_path)?;

            // Acquire an exclusive lock for writing
            file.lock_exclusive()?;

            // Write the record to the log
            let serialized = record.serialize();
            file.write_all(&serialized)?;

            // Sync to disk
            file.flush()?;
            file.sync_all()?;

            file.unlock()?;

            Ok(())
        }
    }
}

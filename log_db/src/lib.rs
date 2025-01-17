#[macro_use]
extern crate log;

use fs2::FileExt;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fs::{self};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::*;
use std::path::{Path, PathBuf};

#[macro_use]
mod common;
mod config;
mod engine;
mod log_reader_forward;
mod memtable_primary;
mod memtable_secondary;
mod record;

pub use common::{DBError, DBResult, Type, Value};
pub use config::{ReadConsistency, WriteDurability};
pub use record::Recordable;

use common::*;
use config::*;
use engine::*;
use log_reader_forward::*;
use memtable_primary::PrimaryMemtable;
use memtable_secondary::SecondaryMemtable;
use record::*;

pub struct DB<R: Recordable> {
    engine: Engine<R>,
}

impl<R: Recordable> DB<R> {
    /// Create a new database configuration builder.
    pub fn configure() -> ConfigBuilder<R> {
        ConfigBuilder::new()
    }

    fn initialize(config: Config<R>) -> DBResult<DB<R>> {
        let engine = Engine::initialize(config)?;
        Ok(DB { engine })
    }

    /// Insert a record into the database. If the primary key value already exists,
    /// the existing record will be replaced by the supplied one.
    pub fn upsert(&mut self, recordable: R) -> DBResult<()> {
        let record = Record::from(&recordable.into_record());
        debug!("Upserting record: {:?}", record);

        record.validate(&self.engine.config.fields)?;
        debug!("Record is valid");

        self.engine.batch_upsert_records(std::iter::once(record))
    }

    /// Insert a batch of records into the database. If the primary key value for a record already exists,
    /// the existing record will be replaced by the supplied one. Records are inserted in the order they are given.
    pub fn batch_upsert(&mut self, recordables: Vec<R>) -> DBResult<()> {
        let records = recordables
            .into_iter()
            .map(|r| Record::from(&r.into_record()))
            .collect::<Vec<Record>>();
        debug!("Batch upserting {} records", records.len());

        for record in &records {
            record.validate(&self.engine.config.fields)?;
        }
        debug!("Records are valid");

        self.engine.batch_upsert_records(records.into_iter())
    }

    /// Get a record by its primary index value.
    /// E.g. `db.get(Value::Int(10))`.
    pub fn get(&mut self, value: &Value) -> DBResult<Option<R>> {
        let value_batch = std::iter::once(value);
        let records = self
            .engine
            // TODO: This clone is only here to appease the borrow checker
            .batch_find_by_records(&self.engine.config.primary_key.clone(), value_batch)?;
        assert!(records.len() <= 1);

        Ok(records
            .into_iter()
            .next()
            .map(|(_, rec)| R::from_record(rec.values)))
    }

    /// Get a collection of records based on a field value.
    /// Indexes will be used if they are applicable.
    pub fn find_by(&mut self, field: &R::Field, value: &Value) -> DBResult<Vec<R>> {
        let value_batch = std::iter::once(value);
        Ok(self
            .engine
            .batch_find_by_records(field, value_batch)?
            .into_iter()
            .map(|(_, rec)| R::from_record(rec.values))
            .collect())
    }

    /// Get a collection of records based on a sequence of field values.
    /// Indexes will be used if they are applicable.
    /// Returns a vector of pairs where the first value is an index into the given sequence of values,
    /// and the second value is the record.
    pub fn batch_find_by(
        &mut self,
        field: &R::Field,
        values: &[Value],
    ) -> DBResult<Vec<(usize, R)>> {
        Ok(self
            .engine
            .batch_find_by_records(field, values.iter())?
            .into_iter()
            .map(|(tag, rec)| (tag, R::from_record(rec.values)))
            .collect())
    }

    pub fn range_by<B: RangeBounds<Value>>(
        &mut self,
        field: &R::Field,
        range: B,
    ) -> DBResult<Vec<R>> {
        Ok(self
            .engine
            .range_by_records(field, range)?
            .into_iter()
            .map(|rec| R::from_record(rec.values))
            .collect())
    }

    /// Delete records by a field value.
    /// E.g. `db.delete_by(Field::Name, "John")`, assuming `Field` is the DB field type and `Field::Name` is secondary indexed.
    /// Returns a vector of deleted records. If no records were deleted, the vector will be empty.
    ///
    /// Deletion is done by marking the record as a tombstone. The record will still be present in the log file,
    /// but will be ignored by reads. Upon compaction, tombstoned records will be removed.
    pub fn delete_by(&mut self, field: &R::Field, value: &Value) -> DBResult<Vec<R>> {
        let recs = self.engine.delete_by_field(field, value)?;

        Ok(recs
            .into_iter()
            .map(|rec| R::from_record(rec.values))
            .collect())
    }

    /// Delete record by primary key.
    pub fn delete(&mut self, pk: &Value) -> DBResult<Option<R>> {
        let recs = self
            .engine
            // TODO: This clone is only here to appease the borrow checker
            .delete_by_field(&self.engine.config.primary_key.clone(), pk)?;
        assert!(recs.len() <= 1);

        Ok(recs
            .into_iter()
            .next()
            .map(|rec| R::from_record(rec.values)))
    }

    /// Check if there are any pending tasks and do them. Tasks include:
    /// - Rotating the active log file if it has reached capacity and compacting it.
    ///
    /// This function should be called periodically to ensure that the database remains in an optimal state.
    /// Note that this function is synchronous and may block for a relatively long time.
    /// You may call this function in a separate thread or process to avoid blocking the main thread.
    /// However, the database will be exclusively locked, so all writes and reads will be blocked during the tasks.
    pub fn do_maintenance_tasks(&mut self) -> DBResult<()> {
        self.engine.do_maintenance_tasks()
    }

    /// Refresh the in-memory indexes from the log files.
    /// This needs to only be called if the read consistency is set to `ReadConsistency::Eventual`.
    pub fn refresh_indexes(&mut self) -> DBResult<()> {
        self.engine.refresh_indexes()
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

    struct TestInst1 {
        id: i64,
    }

    impl Recordable for TestInst1 {
        type Field = Field;
        fn schema() -> Vec<(Field, Type)> {
            vec![(Field::Id, Type::int())]
        }
        fn primary_key() -> Self::Field {
            Field::Id
        }

        fn into_record(self) -> Vec<Value> {
            vec![Value::Int(self.id)]
        }

        fn from_record(record: Vec<Value>) -> Self {
            let mut it = record.into_iter();
            TestInst1 {
                id: match it.next().unwrap() {
                    Value::Int(i) => i,
                    _ => panic!("Expected int"),
                },
            }
        }
    }

    struct TestInst2 {
        id: i64,
        name: String,
    }

    impl Recordable for TestInst2 {
        type Field = Field;
        fn primary_key() -> Self::Field {
            Field::Id
        }
        fn secondary_keys() -> Vec<Self::Field> {
            vec![Field::Name]
        }

        fn into_record(self) -> Vec<Value> {
            vec![Value::Int(self.id), Value::String(self.name)]
        }

        fn from_record(record: Vec<Value>) -> Self {
            let mut it = record.into_iter();
            TestInst2 {
                id: match it.next().unwrap() {
                    Value::Int(i) => i,
                    _ => panic!("Expected int"),
                },
                name: match it.next().unwrap() {
                    Value::String(s) => s,
                    _ => panic!("Expected string"),
                },
            }
        }

        fn schema() -> Vec<(Field, Type)> {
            vec![(Field::Id, Type::int()), (Field::Name, Type::string())]
        }
    }

    #[test]
    fn test_compaction() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let capacity = 5;
        let segment_size = capacity * 2 * 8 + METADATA_FILE_HEADER_SIZE;
        let mut db = DB::<TestInst1>::configure()
            .data_dir(data_dir.to_str().unwrap())
            .segment_size(segment_size)
            .initialize()
            .expect("Failed to create DB");

        // Insert records with same value until we reach the capacity
        for _ in 0..capacity {
            db.upsert(TestInst1 { id: 0 })
                .expect("Failed to insert record");
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
        db.upsert(TestInst1 { id: 1 })
            .expect("Failed to insert record");

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
        let inst0 = db
            .get(&Value::Int(0 as i64))
            .expect("Failed to get record")
            .expect("Record not found");

        assert!(inst0.id == 0);

        let inst1 = db
            .get(&Value::Int(1 as i64))
            .expect("Failed to get record")
            .expect("Record not found");

        assert!(inst1.id == 1);
    }

    #[test]
    fn test_repair() {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path();

        let mut db = DB::<TestInst1>::configure()
            .data_dir(data_dir.to_str().unwrap())
            .initialize()
            .expect("Failed to create DB");

        // Insert records
        let n_recs: u64 = 100;
        for i in 0..n_recs {
            db.upsert(TestInst1 { id: i as i64 })
                .expect("Failed to insert record");
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

        let mut db = DB::<TestInst2>::configure()
            .data_dir(data_dir.to_str().unwrap())
            .initialize()
            .expect("Failed to create DB");

        // Check that the key is not indexed before write
        assert_eq!(
            db.engine.primary_memtable.get(&IndexableValue::Int(0)),
            None
        );
        assert_eq!(
            db.engine.secondary_memtables[0].find_by(&IndexableValue::String("John".to_string())),
            &HashSet::new()
        );

        // Insert record
        db.upsert(TestInst2 {
            id: 0,
            name: "John".to_owned(),
        })
        .expect("Failed to insert record");

        // Check that the key is now indexed
        let expected_log_key = LogKey::new(1, 0);
        assert_eq!(
            db.engine.primary_memtable.get(&IndexableValue::Int(0)),
            Some(&expected_log_key)
        );
        let mut expected_set: HashSet<LogKey> = HashSet::new();
        expected_set.insert(expected_log_key);
        assert_eq!(
            db.engine.secondary_memtables[0].find_by(&IndexableValue::String("John".to_string())),
            &expected_set,
        );
    }
}

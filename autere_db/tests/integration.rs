#[macro_use]
extern crate log;
extern crate ctor;
extern crate tempfile;

use autere_db::*;
use ctor::ctor;
use env_logger;
use serial_test::serial;
use std::fs::{self};
use std::path::Path;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

pub fn tmp_dir() -> String {
    let dir = tempdir()
        .expect("Failed to create temporary directory")
        .path()
        .to_str()
        .expect("Failed to convert temporary directory path to string")
        .to_string();
    fs::create_dir_all(&dir).expect("Failed to create temporary directory");
    dir
}

#[ctor]
fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum Field {
    Id,
    Name,
    Data,
}

impl AsRef<str> for Field {
    fn as_ref(&self) -> &str {
        match self {
            Field::Id => "id",
            Field::Name => "name",
            Field::Data => "data",
        }
    }
}

impl From<Field> for String {
    fn from(field: Field) -> String {
        field.as_ref().to_owned()
    }
}

#[derive(Debug)]
struct Inst {
    pub id: i64,
    pub name: Option<String>,
    pub data: Vec<u8>,
}

impl From<Inst> for Record {
    fn from(inst: Inst) -> Record {
        vec![
            Value::Int(inst.id),
            match inst.name {
                Some(name) => Value::String(name),
                None => Value::Null,
            },
            Value::Bytes(inst.data),
        ]
        .into()
    }
}

impl From<Record> for Inst {
    fn from(record: Record) -> Self {
        let mut it = record.into_iter();
        Inst {
            id: match it.next().unwrap() {
                Value::Int(id) => id,
                other => panic!("Invalid value type: {:?}", other),
            },
            name: match it.next().unwrap() {
                Value::String(name) => Some(name),
                Value::Null => None,
                other => panic!("Invalid value type: {:?}", other),
            },
            data: match it.next().unwrap() {
                Value::Bytes(data) => data,
                other => panic!("Invalid value type: {:?}", other),
            },
        }
    }
}

impl Inst {
    fn schema() -> Vec<Field> {
        vec![Field::Id, Field::Name, Field::Data]
    }
    fn primary_key() -> Field {
        Field::Id
    }
    fn secondary_keys() -> Vec<Field> {
        vec![Field::Name]
    }
}

#[test]
fn test_initialize_only() {
    let data_dir = tmp_dir();
    let _db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");
}

#[test]
fn test_upsert_and_get_with_primary_memtable() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    let id = 1;
    let inst = Inst {
        id,
        name: Some("Alice".to_string()),
        data: vec![0, 1, 2],
    };
    db.upsert(inst).unwrap();

    let result: Inst = db.get(&Value::Int(1)).unwrap().unwrap().into();

    // Check that the IDs match
    assert!(result.id == id);
}

#[test]
fn test_upsert_and_get() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    db.upsert(Inst {
        id: 0,
        name: None,
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("Alice".to_string()),
        data: vec![0, 1, 2],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("Bob".to_string()),
        data: vec![0, 1, 2],
    })
    .unwrap();

    db.upsert(Inst {
        id: 2,
        name: Some("George".to_string()),
        data: vec![],
    })
    .unwrap();

    // Get with ID = 0
    let result: Inst = db.get(&Value::Int(0)).unwrap().unwrap().into();

    // Should match id == 0
    assert!(result.id == 0);
    assert!(result.name == None);

    // Get with ID = 1
    let result: Inst = db.get(&Value::Int(1)).unwrap().unwrap().into();

    // Should match newest inst with id == 1
    assert!(result.id == 1);
    assert!(result.name == Some("Bob".to_owned()));
}

#[test]
fn test_get_nonexistant() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    let result = db.get(&Value::Int(0)).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_upsert_and_find_by() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    db.upsert(Inst {
        id: 0,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("John".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.upsert(Inst {
        id: 2,
        name: Some("George".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    // There should be 2 Johns
    let johns = db
        .find_by(&Field::Name, &Value::String("John".to_string()))
        .expect("Failed to find all Johns");

    assert_eq!(johns.len(), 2);
}

struct InstSingleId {
    pub id: i64,
}

impl From<InstSingleId> for Record {
    fn from(inst: InstSingleId) -> Record {
        vec![Value::Int(inst.id)].into()
    }
}

impl From<Record> for InstSingleId {
    fn from(record: Record) -> Self {
        let mut it = record.into_iter();
        InstSingleId {
            id: match it.next().unwrap() {
                Value::Int(id) => id,
                other => panic!("Invalid value type: {:?}", other),
            },
        }
    }
}

impl InstSingleId {
    fn schema() -> Vec<Field> {
        vec![Field::Id]
    }
    fn primary_key() -> Field {
        Field::Id
    }
    fn secondary_keys() -> Vec<Field> {
        vec![]
    }
}

#[test]
#[serial]
fn test_multiple_writing_threads() {
    let data_dir = tmp_dir();
    debug!("Data dir: {:?}", data_dir);
    let mut threads = vec![];
    let threads_n = 100;

    for i in 0..threads_n {
        let data_dir = data_dir.clone();
        threads.push(thread::spawn(move || {
            let mut db = DB::configure()
                .fields(InstSingleId::schema())
                .primary_key(InstSingleId::primary_key())
                .secondary_keys(InstSingleId::secondary_keys())
                .data_dir(&data_dir)
                .initialize()
                .expect("Failed to initialize DB instance");

            db.upsert(InstSingleId { id: i })
                .expect("Failed to upsert record");
        }));
    }

    for thread in threads {
        thread.join().expect("Failed to join thread");
    }

    // Read the records
    let mut db = DB::configure()
        .fields(InstSingleId::schema())
        .primary_key(InstSingleId::primary_key())
        .secondary_keys(InstSingleId::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    for i in 0..threads_n {
        let result: InstSingleId = db
            .get(&Value::Int(i))
            .expect("Failed to get record")
            .expect("Record not found")
            .into();

        assert!(result.id == i);
    }
}

#[test]
#[serial]
fn test_one_writer_and_multiple_reading_threads() {
    let data_dir = tmp_dir();
    let mut threads = vec![];
    let threads_n = 100;

    // Add readers that poll for the records
    for i in 0..threads_n {
        let data_dir = data_dir.clone();
        threads.push(thread::spawn(move || {
            let mut db = DB::configure()
                .fields(InstSingleId::schema())
                .primary_key(InstSingleId::primary_key())
                .secondary_keys(InstSingleId::secondary_keys())
                .data_dir(&data_dir)
                .segment_size(1000) // should cause rotations
                .initialize()
                .expect("Failed to initialize DB instance");

            let mut timeout = 5;
            loop {
                let result = db.get(&Value::Int(i)).expect("Failed to get record");
                match result {
                    None => {
                        thread::sleep(Duration::from_millis(timeout));
                        timeout = std::cmp::min(timeout * 2, 100);
                        continue;
                    }
                    Some(result) => {
                        let result: InstSingleId = result.into();
                        assert!(result.id == i);
                        break;
                    }
                };
            }
        }));
    }

    // Add a writer that inserts the records
    threads.push(thread::spawn(move || {
        let mut db = DB::configure()
            .fields(InstSingleId::schema())
            .primary_key(InstSingleId::primary_key())
            .secondary_keys(InstSingleId::secondary_keys())
            .data_dir(&data_dir)
            .initialize()
            .expect("Failed to initialize DB instance");

        for i in 0..threads_n {
            db.upsert(InstSingleId { id: i })
                .expect("Failed to upsert record");

            db.do_maintenance_tasks() // Run maintenance tasks after every write, just to test it
                .expect("Failed to do maintenance tasks");
        }
    }));

    for thread in threads {
        thread.join().expect("Failed to join thread");
    }
}

#[test]
fn test_log_is_rotated_when_capacity_reached() {
    let data_dir = tmp_dir();
    let data_dir_path = Path::new(&data_dir);

    // Hand-calculated record length, find record below
    let record_len = 1 // tombstone tag
        + (1 + 8)      // int tag + i64
        + (1 + 8 + 4)  // string tag + string length + string data
        + (1 + 8 + 3); // bytes tag + bytes length + bytes data

    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .segment_size(10 * record_len) // small log segment size
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert more records than fits the capacity
    for _ in 0..25 {
        db.upsert(Inst {
            id: 0,
            name: Some("John".to_string()),
            data: vec![3, 4, 5],
        })
        .expect("Failed to upsert record");

        db.do_maintenance_tasks()
            .expect("Failed to do maintenance tasks");
    }

    // Check that the rotated segments exist
    assert!(data_dir_path.join("metadata").with_extension("1").exists());
    assert!(data_dir_path.join("metadata").with_extension("2").exists());

    // 3rd segment should not exist (note negation)
    assert!(!data_dir_path.join("metadata").with_extension("3").exists());
}

#[test]
fn test_delete() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    db.upsert(Inst {
        id: 0,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("John".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.delete(&Value::Int(0)).unwrap();

    // Check that the record is deleted
    assert!(db.get(&Value::Int(0)).unwrap().is_none());

    // Check that the secondary index is updated
    assert_eq!(
        db.find_by(&Field::Name, &Value::String("John".to_string()))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn test_delete_by() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    db.upsert(Inst {
        id: 0,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("John".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.upsert(Inst {
        id: 2,
        name: Some("Bob".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.delete_by(&Field::Name, &Value::String("John".to_string()))
        .unwrap();

    // Check that the record is deleted
    assert!(db.get(&Value::Int(0)).unwrap().is_none());
    assert!(db.get(&Value::Int(1)).unwrap().is_none());

    // Check that the secondary index is updated
    assert_eq!(
        db.find_by(&Field::Name, &Value::String("John".to_string()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        db.find_by(&Field::Name, &Value::String("Bob".to_string()))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn test_range_by_id() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    let inserted_ids = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

    for id in inserted_ids.iter() {
        db.upsert(Inst {
            id: *id,
            name: Some("Foobar".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    // Test range [3, 7)
    let received = db
        .range_by(&Field::Id, &Value::Int(3)..&Value::Int(7))
        .unwrap();
    let received_ids: Vec<i64> = received.into_iter().map(|rec| Inst::from(rec).id).collect();

    assert_eq!(received_ids, vec![3, 4, 5, 6]);

    // Test range [3, 7]
    let received = db
        .range_by(&Field::Id, &Value::Int(3)..=&Value::Int(7))
        .unwrap();
    let received_ids: Vec<i64> = received.into_iter().map(|rec| Inst::from(rec).id).collect();

    assert_eq!(received_ids, vec![3, 4, 5, 6, 7]);

    // Test range (-inf, 7]
    let received = db.range_by(&Field::Id, ..=&Value::Int(7)).unwrap();
    let received_ids: Vec<i64> = received.into_iter().map(|rec| Inst::from(rec).id).collect();

    assert_eq!(received_ids, vec![0, 1, 2, 3, 4, 5, 6, 7]);

    // Test range (3, inf)
    let received = db.range_by(&Field::Id, &Value::Int(3)..).unwrap();
    let received_ids: Vec<i64> = received.into_iter().map(|rec| Inst::from(rec).id).collect();

    assert_eq!(received_ids, vec![3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn test_batch_find_by() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    let inserted_ids = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

    for id in inserted_ids.iter() {
        db.upsert(Inst {
            id: *id,
            name: Some("Foobar".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    let batch: Vec<Value> = (2..5).map(Value::Int).collect();
    let result = db.batch_find_by(Field::Id, &batch).unwrap();

    assert_eq!(result.len(), batch.len());
    assert_eq!(
        result.iter().map(|(tag, _)| *tag).collect::<Vec<usize>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        result
            .into_iter()
            .map(|(_, rec)| Inst::from(rec).id)
            .collect::<Vec<i64>>(),
        vec![2, 3, 4]
    );
}

#[test]
fn test_commit_transaction() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    db.tx_begin().expect("Failed to begin transaction");

    db.upsert(Inst {
        id: 0,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("John".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.tx_commit().expect("Failed to commit transaction");

    let johns = db
        .find_by(&Field::Name, &Value::String("John".to_string()))
        .unwrap();

    assert_eq!(johns.len(), 2);
}

#[test]
fn test_rollback_transaction() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    db.tx_begin().expect("Failed to begin transaction");

    db.upsert(Inst {
        id: 0,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
    })
    .unwrap();

    db.upsert(Inst {
        id: 1,
        name: Some("John".to_string()),
        data: vec![1, 2, 3],
    })
    .unwrap();

    db.tx_rollback().expect("Failed to roll back transaction");

    let johns = db
        .find_by(&Field::Name, &Value::String("John".to_string()))
        .unwrap();

    assert_eq!(johns.len(), 0);
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum FieldWithNewNullableField {
    Id,
    Name,
    Data,
    MaybeStr,
}

impl AsRef<str> for FieldWithNewNullableField {
    fn as_ref(&self) -> &str {
        match self {
            FieldWithNewNullableField::Id => "id",
            FieldWithNewNullableField::Name => "name",
            FieldWithNewNullableField::Data => "data",
            FieldWithNewNullableField::MaybeStr => "maybe_str",
        }
    }
}

impl Into<String> for FieldWithNewNullableField {
    fn into(self) -> String {
        self.as_ref().to_owned()
    }
}

struct InstWithNewNullableField {
    pub id: i64,
    pub name: Option<String>,
    pub data: Vec<u8>,
    pub maybe_str: Option<String>,
}

impl From<InstWithNewNullableField> for Record {
    fn from(inst: InstWithNewNullableField) -> Record {
        vec![
            Value::Int(inst.id),
            match inst.name {
                Some(name) => Value::String(name),
                None => Value::Null,
            },
            Value::Bytes(inst.data),
            match inst.maybe_str {
                Some(maybe_str) => Value::String(maybe_str),
                None => Value::Null,
            },
        ]
        .into()
    }
}

impl From<Record> for InstWithNewNullableField {
    fn from(record: Record) -> Self {
        let mut it = record.into_iter();
        InstWithNewNullableField {
            id: match it.next().unwrap() {
                Value::Int(id) => id,
                other => panic!("Invalid value type: {:?}", other),
            },
            name: match it.next().unwrap() {
                Value::String(name) => Some(name),
                Value::Null => None,
                other => panic!("Invalid value type: {:?}", other),
            },
            data: match it.next().unwrap() {
                Value::Bytes(data) => data,
                other => panic!("Invalid value type: {:?}", other),
            },
            maybe_str: match it.next() {
                Some(Value::String(maybe_str)) => Some(maybe_str),
                Some(Value::Null) => None,
                None => None,
                other => panic!("Invalid value type: {:?}", other),
            },
        }
    }
}

impl InstWithNewNullableField {
    fn schema() -> Vec<FieldWithNewNullableField> {
        vec![
            FieldWithNewNullableField::Id,
            FieldWithNewNullableField::Name,
            FieldWithNewNullableField::Data,
            FieldWithNewNullableField::MaybeStr,
        ]
    }

    fn primary_key() -> FieldWithNewNullableField {
        FieldWithNewNullableField::Id
    }

    fn secondary_keys() -> Vec<FieldWithNewNullableField> {
        vec![FieldWithNewNullableField::Name]
    }
}

#[test]
fn test_add_nullable_field() {
    let data_dir = tmp_dir();

    // Insert a record with 3 fields
    {
        let mut db = DB::configure()
            .fields(Inst::schema())
            .primary_key(Inst::primary_key())
            .secondary_keys(Inst::secondary_keys())
            .data_dir(&data_dir)
            .initialize()
            .expect("Failed to initialize DB instance");

        db.upsert(Inst {
            id: 0,
            name: Some("John".to_string()),
            data: vec![3, 4, 5],
        })
        .unwrap();
    }

    // Insert a record with 4 fields (last is nullable)
    let mut db = DB::configure()
        .fields(InstWithNewNullableField::schema())
        .primary_key(InstWithNewNullableField::primary_key())
        .secondary_keys(InstWithNewNullableField::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    db.upsert(InstWithNewNullableField {
        id: 1,
        name: Some("John".to_string()),
        data: vec![3, 4, 5],
        maybe_str: None,
    })
    .unwrap();

    let johns = db
        .find_by(
            &FieldWithNewNullableField::Name,
            &Value::String("John".to_string()),
        )
        .unwrap();

    assert_eq!(johns.len(), 2);
}

#[test]
fn test_delete_by_multiple_indexes() {
    let data_dir = tmp_dir();

    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some identical records
    for _ in 0..10 {
        db.upsert(Inst {
            id: 0,
            name: Some("foo".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    // Delete by name
    db.delete_by(&Field::Name, &Value::String("foo".to_string()))
        .unwrap();

    // Check that the records are deleted by finding by name
    let result = db
        .find_by(&Field::Name, &Value::String("foo".to_string()))
        .unwrap();
    assert_eq!(result.len(), 0);

    // Double check with find by id
    let result = db.find_by(&Field::Id, &Value::Int(0)).unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_find_by_with_offset_and_limit() {
    let data_dir = tmp_dir();

    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    for i in 0..10 {
        db.upsert(Inst {
            id: i,
            name: Some("foo".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    // Find by name with offset and limit
    let result = db
        .find_by_with_params(
            &Field::Name,
            &Value::String("foo".to_string()),
            &QueryParams {
                offset: 2,
                limit: 3,
                sort_asc: true,
            },
        )
        .unwrap()
        .into_iter()
        .map(|rec| Inst::from(rec))
        .collect::<Vec<Inst>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].id, 2);
    assert_eq!(result[1].id, 3);
    assert_eq!(result[2].id, 4);
}

#[test]
fn test_batch_find_by_with_offset_and_limit() {
    let data_dir = tmp_dir();

    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    for i in 0..10 {
        db.upsert(Inst {
            id: i,
            name: Some("foo".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    // Batch find by id with offset and limit
    let batch: Vec<Value> = (2..5).map(Value::Int).collect();
    let result = db
        .batch_find_by_with_params(
            &Field::Id,
            &batch,
            &QueryParams {
                offset: 1,
                limit: 2,
                sort_asc: true,
            },
        )
        .unwrap()
        .into_iter()
        .map(|(_, rec)| Inst::from(rec))
        .collect::<Vec<Inst>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 3);
    assert_eq!(result[1].id, 4);
}

#[test]
fn test_range_by_with_offset_and_limit() {
    let data_dir = tmp_dir();

    let mut db = DB::configure()
        .fields(Inst::schema())
        .primary_key(Inst::primary_key())
        .secondary_keys(Inst::secondary_keys())
        .data_dir(&data_dir)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    for i in 0..10 {
        db.upsert(Inst {
            id: i,
            name: Some("foo".to_string()),
            data: vec![],
        })
        .unwrap();
    }

    // Range by id with offset and limit
    let result = db
        .range_by_with_params(
            &Field::Id,
            &Value::Int(2)..&Value::Int(8),
            &QueryParams {
                offset: 1,
                limit: 3,
                sort_asc: true,
            },
        )
        .unwrap()
        .into_iter()
        .map(|rec| Inst::from(rec))
        .collect::<Vec<Inst>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].id, 3);
    assert_eq!(result[1].id, 4);
    assert_eq!(result[2].id, 5);
}

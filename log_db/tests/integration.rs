#[macro_use]
extern crate log;
extern crate ctor;
extern crate tempfile;

use ctor::ctor;
use env_logger;
use log_db::*;
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

#[test]
fn test_initialize_only() {
    let data_dir = tmp_dir();
    let _db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");
}

#[test]
fn test_upsert_and_get_with_primary_memtable() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record::from(&[
        Value::Int(1),
        Value::String("Alice".to_string()),
        Value::Bytes(vec![0, 1, 2]),
    ]);
    db.upsert(&record).unwrap();

    let result = db.get(&Value::Int(1)).unwrap().unwrap();

    // Check that the IDs match
    assert!(match (result.at(0), record.at(0)) {
        (Value::Int(a), Value::Int(b)) => a == b,
        _ => false,
    });
}

#[test]
fn test_upsert_and_get() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string().nullable()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    let record0 = Record::from(&[Value::Int(0), Value::Null, Value::Bytes(vec![3, 4, 5])]);
    db.upsert(&record0).unwrap();

    let record1 = Record::from(&[
        Value::Int(1),
        Value::String("Alice".to_string()),
        Value::Bytes(vec![0, 1, 2]),
    ]);
    db.upsert(&record1).unwrap();

    let record2 = Record::from(&[
        Value::Int(1),
        Value::String("Bob".to_string()),
        Value::Bytes(vec![0, 1, 2]),
    ]);
    db.upsert(&record2).unwrap();

    let record3 = Record::from(&[
        Value::Int(2),
        Value::String("George".to_string()),
        Value::Bytes(vec![]),
    ]);
    db.upsert(&record3).unwrap();

    // Get with ID = 0
    let result = db.get(&Value::Int(0)).unwrap().unwrap();

    // Should match record0
    assert!(match (result.at(0), record0.at(0)) {
        (Value::Int(a), Value::Int(b)) => a == b,
        _ => false,
    });
    assert!(match (result.at(1), record0.at(1)) {
        (Value::Null, Value::Null) => true,
        _ => false,
    });

    // Get with ID = 1
    let result = db.get(&Value::Int(1)).unwrap().unwrap();

    // Should match record2
    assert!(match (result.at(0), record2.at(0)) {
        (Value::Int(a), Value::Int(b)) => a == b,
        _ => false,
    });
    assert!(match (result.at(1), record2.at(1)) {
        (Value::String(a), Value::String(b)) => a == b,
        _ => false,
    });
}

#[test]
fn test_get_nonexistant() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string().nullable()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let result = db.get(&Value::Int(0)).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_upsert_fails_on_null_in_non_nullable_field() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[(Field::Id, ValueType::int())])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Null value
    let record = Record::from(&[Value::Null]);
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_fails_on_invalid_number_of_values() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Missing primary key
    let record = Record::from(&[
        Value::String("Alice".to_string()),
        Value::Bytes(vec![0, 1, 2]),
    ]);
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_fails_on_invalid_value_type() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record::from(&[
        Value::String("foo".to_string()),
        Value::String("bar".to_string()),
        Value::String("baz".to_string()),
    ]);
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_and_find_all() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .secondary_keys(&[Field::Name])
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    let record0 = Record::from(&[
        Value::Int(0),
        Value::String("John".to_string()),
        Value::Bytes(vec![3, 4, 5]),
    ]);
    db.upsert(&record0).unwrap();

    let record1 = Record::from(&[
        Value::Int(1),
        Value::String("John".to_string()),
        Value::Bytes(vec![1, 2, 3]),
    ]);
    db.upsert(&record1).unwrap();

    let record2 = Record::from(&[
        Value::Int(2),
        Value::String("George".to_string()),
        Value::Bytes(vec![1, 2, 3]),
    ]);
    db.upsert(&record2).unwrap();

    // There should be 2 Johns
    let johns = db
        .find_all(&Field::Name, &Value::String("John".to_string()))
        .expect("Failed to find all Johns");

    assert_eq!(johns.len(), 2);
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
                .data_dir(&data_dir)
                .fields(&[(Field::Id, ValueType::int())])
                .primary_key(Field::Id)
                .initialize()
                .expect("Failed to initialize DB instance");

            let record = Record::from(&[Value::Int(i)]);
            db.upsert(&record).expect("Failed to upsert record");
        }));
    }

    for thread in threads {
        thread.join().expect("Failed to join thread");
    }

    // Read the records
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&[(Field::Id, ValueType::int())])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    for i in 0..threads_n {
        let result = db
            .get(&Value::Int(i))
            .expect("Failed to get record")
            .expect("Record not found");

        assert!(match &result.values() {
            [Value::Int(a)] => a == &i,
            _ => false,
        });
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
                .data_dir(&data_dir)
                .segment_size(1000) // should cause rotations
                .fields(&[(Field::Id, ValueType::int())])
                .primary_key(Field::Id)
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
                        assert!(match &result.values() {
                            [Value::Int(a)] => a == &i,
                            _ => false,
                        });
                        break;
                    }
                };
            }
        }));
    }

    // Add a writer that inserts the records
    threads.push(thread::spawn(move || {
        let mut db = DB::configure()
            .data_dir(&data_dir)
            .fields(&[(Field::Id, ValueType::int())])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB instance");

        for i in 0..threads_n {
            let record = Record::from(&[Value::Int(i)]);
            db.upsert(&record).expect("Failed to upsert record");

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

    let record = Record::from(&[Value::Int(1), Value::Bytes(vec![1, 2, 3, 4])]);
    let record_len = &record.serialize().len();

    let mut db = DB::configure()
        .data_dir(&data_dir)
        .segment_size(10 * record_len) // small log segment size
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert more records than fits the capacity
    for _ in 0..25 {
        db.upsert(&record).expect("Failed to upsert record");

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
        .data_dir(&data_dir)
        .fields(&[
            (Field::Id, ValueType::int()),
            (Field::Name, ValueType::string()),
            (Field::Data, ValueType::bytes()),
        ])
        .primary_key(Field::Id)
        .secondary_keys(&[Field::Name])
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    let record0 = Record::from(&[
        Value::Int(0),
        Value::String("John".to_string()),
        Value::Bytes(vec![3, 4, 5]),
    ]);
    db.upsert(&record0).unwrap();

    let record1 = Record::from(&[
        Value::Int(1),
        Value::String("John".to_string()),
        Value::Bytes(vec![1, 2, 3]),
    ]);
    db.upsert(&record1).unwrap();

    db.delete(&Value::Int(0)).unwrap();

    // Check that the record is deleted
    assert!(db.get(&Value::Int(0)).unwrap().is_none());

    // Check that the secondary index is updated
    assert_eq!(
        db.find_all(&Field::Name, &Value::String("John".to_string()))
            .unwrap()
            .len(),
        1
    );
}

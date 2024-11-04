#[macro_use]
extern crate log;
extern crate ctor;
extern crate tempfile;

use ctor::ctor;
use env_logger;
use log_db::*;
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
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string()),
            (Field::Data, RecordField::bytes()),
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
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string()),
            (Field::Data, RecordField::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record {
        values: vec![
            RecordValue::Int(1),
            RecordValue::String("Alice".to_string()),
            RecordValue::Bytes(vec![0, 1, 2]),
        ],
    };
    db.upsert(&record).unwrap();

    let result = db.get(&RecordValue::Int(1)).unwrap().unwrap();

    // Check that the IDs match
    assert!(match (&result.values[0], &record.values[0]) {
        (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
        _ => false,
    });
}

#[test]
fn test_upsert_and_get_without_memtable() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .memtable_capacity(0)
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string().nullable()),
            (Field::Data, RecordField::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    let record0 = Record {
        values: vec![
            RecordValue::Int(0),
            RecordValue::Null,
            RecordValue::Bytes(vec![3, 4, 5]),
        ],
    };
    db.upsert(&record0).unwrap();

    let record1 = Record {
        values: vec![
            RecordValue::Int(1),
            RecordValue::String("Alice".to_string()),
            RecordValue::Bytes(vec![0, 1, 2]),
        ],
    };
    db.upsert(&record1).unwrap();

    let record2 = Record {
        values: vec![
            RecordValue::Int(1),
            RecordValue::String("Bob".to_string()),
            RecordValue::Bytes(vec![0, 1, 2]),
        ],
    };
    db.upsert(&record2).unwrap();

    let record3 = Record {
        values: vec![
            RecordValue::Int(2),
            RecordValue::String("George".to_string()),
            RecordValue::Bytes(vec![]),
        ],
    };
    db.upsert(&record3).unwrap();

    // Get with ID = 0
    let result = db.get(&RecordValue::Int(0)).unwrap().unwrap();

    // Should match record0
    assert!(match (&result.values[0], &record0.values[0]) {
        (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
        _ => false,
    });
    assert!(match (&result.values[1], &record0.values[1]) {
        (RecordValue::Null, RecordValue::Null) => true,
        _ => false,
    });

    // Get with ID = 1
    let result = db.get(&RecordValue::Int(1)).unwrap().unwrap();

    // Should match record2
    assert!(match (&result.values[0], &record2.values[0]) {
        (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
        _ => false,
    });
    assert!(match (&result.values[1], &record2.values[1]) {
        (RecordValue::String(a), RecordValue::String(b)) => a == b,
        _ => false,
    });
}

#[test]
fn test_upsert_fails_on_null_in_non_nullable_field() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(vec![(Field::Id, RecordField::int())])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record {
        // Null value
        values: vec![RecordValue::Null],
    };
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_fails_on_invalid_number_of_values() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string()),
            (Field::Data, RecordField::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record {
        // Missing primary key
        values: vec![
            RecordValue::String("Alice".to_string()),
            RecordValue::Bytes(vec![0, 1, 2]),
        ],
    };
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_fails_on_invalid_value_type() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string()),
            (Field::Data, RecordField::bytes()),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    let record = Record {
        values: vec![
            RecordValue::String("foo".to_string()),
            RecordValue::String("bar".to_string()),
            RecordValue::String("baz".to_string()),
        ],
    };
    assert!(db.upsert(&record).is_err());
}

#[test]
fn test_upsert_and_get_from_secondary_memtable() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Name, RecordField::string()),
            (Field::Data, RecordField::bytes()),
        ])
        .primary_key(Field::Id)
        .secondary_keys(vec![Field::Name])
        .initialize()
        .expect("Failed to initialize DB instance");

    // Insert some records
    let record0 = Record {
        values: vec![
            RecordValue::Int(0),
            RecordValue::String("John".to_string()),
            RecordValue::Bytes(vec![3, 4, 5]),
        ],
    };
    db.upsert(&record0).unwrap();

    let record1 = Record {
        values: vec![
            RecordValue::Int(1),
            RecordValue::String("John".to_string()),
            RecordValue::Bytes(vec![1, 2, 3]),
        ],
    };
    db.upsert(&record1).unwrap();

    let record2 = Record {
        values: vec![
            RecordValue::Int(2),
            RecordValue::String("George".to_string()),
            RecordValue::Bytes(vec![1, 2, 3]),
        ],
    };
    db.upsert(&record2).unwrap();

    // Delete the DB so that any results must come from a memtable
    fs::remove_file(Path::new(&data_dir).join(ACTIVE_SYMLINK_FILENAME))
        .expect("Failed to delete the DB log file");

    // There should be 2 Johns
    let johns = db
        .find_all(&Field::Name, &RecordValue::String("John".to_string()))
        .expect("Failed to find all Johns");

    assert_eq!(johns.len(), 2);
}

#[test]
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
                .fields(vec![(Field::Id, RecordField::int())])
                .primary_key(Field::Id)
                .initialize()
                .expect("Failed to initialize DB instance");

            let record = Record {
                values: vec![RecordValue::Int(i)],
            };
            db.upsert(&record).expect("Failed to upsert record");
        }));
    }

    for thread in threads {
        thread.join().expect("Failed to join thread");
    }

    // Read the records
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(vec![(Field::Id, RecordField::int())])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    for i in 0..threads_n {
        let result = db
            .get(&RecordValue::Int(i))
            .expect("Failed to get record")
            .expect("Record not found");
        let expected = RecordValue::Int(i);
        assert!(match (&result.values[0], &expected) {
            (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
            _ => false,
        });
    }
}

#[test]
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
                .fields(vec![(Field::Id, RecordField::int())])
                .primary_key(Field::Id)
                .initialize()
                .expect("Failed to initialize DB instance");

            let mut timeout = 5;
            loop {
                let result = db.get(&RecordValue::Int(i)).expect("Failed to get record");
                match result {
                    None => {
                        thread::sleep(Duration::from_millis(timeout));
                        timeout = std::cmp::min(timeout * 2, 100);
                        continue;
                    }
                    Some(result) => {
                        let expected = RecordValue::Int(i);
                        assert!(match (&result.values[0], &expected) {
                            (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
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
            .fields(vec![(Field::Id, RecordField::int())])
            .primary_key(Field::Id)
            .initialize()
            .expect("Failed to initialize DB instance");

        for i in 0..threads_n {
            let record = Record {
                values: vec![RecordValue::Int(i)],
            };
            db.upsert(&record).expect("Failed to upsert record");
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

    let record = Record {
        values: vec![RecordValue::Int(1), RecordValue::Bytes(vec![1, 2, 3, 4])],
    };
    let record_len = &record.serialize().len();

    let mut db = DB::configure()
        .data_dir(&data_dir)
        .memtable_capacity(0) // disable memtables
        .segment_size(10 * record_len) // small log segment size
        .fields(vec![
            (Field::Id, RecordField::int()),
            (Field::Data, RecordField::bytes()),
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

    // TODO re-implement rest of the test
    // when refactor is done

    // // Check that the active file only contains five rows
    // let mut file = OpenOptions::new()
    //     .read(true)
    //     .open(data_dir_path.join(ACTIVE_SYMLINK_FILENAME))
    //     .expect("File could not be opened");

    // let records_in_active_log = ForwardLogReader::new(&mut file).count();
    // assert_eq!(records_in_active_log, 5);

    // // Check that each rotated file contains only 1 record
    // // because of compaction
    // for i in &[1, 2] {
    //     let mut file = OpenOptions::new()
    //         .read(true)
    //         .open(
    //             Path::new(&data_dir)
    //                 .join(ACTIVE_SYMLINK_FILENAME)
    //                 .with_extension(i.to_string()),
    //         )
    //         .expect("File could not be opened");
    //     let records_in_rotated_log = ForwardLogReader::new(&mut file).count();
    //     assert_eq!(records_in_rotated_log, 1);
    // }

    // // Look for nonexistant record to scan all segment files
    // let found = db.get(&RecordValue::Int(2)).expect("Failed to get record");
    // assert!(found.is_none());
}

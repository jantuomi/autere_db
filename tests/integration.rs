extern crate ctor;
extern crate tempfile;

use ctor::ctor;
use env_logger;
use log_db;
use log_db::{ForwardLogReader, Record, RecordFieldType, RecordValue, ReverseLogReader, DB};
use serial_test::serial;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

const TEST_RESOURCES_DIR: &str = "tests/resources";

#[ctor]
fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn tmp_dir() -> String {
    let dir = tempdir()
        .expect("Failed to create temporary directory")
        .path()
        .to_str()
        .expect("Failed to convert temporary directory path to string")
        .to_string();
    fs::create_dir_all(&dir).expect("Failed to create temporary directory");
    dir
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum Field {
    Id,
    Name,
    Data,
}

#[test]
fn test_initialize() {
    let data_dir = tmp_dir();
    let _db = DB::configure()
        .data_dir(&data_dir)
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
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
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
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
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ])
        .primary_key(Field::Id)
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
        (RecordValue::String(a), RecordValue::String(b)) => a == b,
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
fn test_upsert_fails_on_invalid_number_of_values() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
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
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
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
fn test_reverse_log_reader_fixture_db1() {
    let db_path = Path::new(TEST_RESOURCES_DIR).join("test_db1");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&db_path)
        .expect("Failed to open file");
    let mut reverse_log_reader = ReverseLogReader::new(&mut file).unwrap();

    // There are two records in the log with "schema": Int, Null

    let last_record = reverse_log_reader
        .next()
        .expect("Failed to read the last record");
    assert!(match last_record.values.as_slice() {
        [RecordValue::Int(10), RecordValue::Null] => true,
        _ => false,
    });

    let first_record = reverse_log_reader
        .next()
        .expect("Failed to read the first record");
    assert!(match first_record.values.as_slice() {
        // Note: the int value is equal to the escape byte
        [RecordValue::Int(0x1D), RecordValue::Null] => true,
        _ => false,
    });

    assert!(reverse_log_reader.next().is_none());
}

#[test]
fn test_forward_log_reader_fixture_db1() {
    let db_path = Path::new(TEST_RESOURCES_DIR).join("test_db1");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&db_path)
        .expect("Failed to open file");
    let mut forward_log_reader = ForwardLogReader::new(&mut file);

    // There are two records in the log with "schema": Int, Null

    let first_record = forward_log_reader
        .next()
        .expect("Failed to read the first record");
    assert!(match first_record.values.as_slice() {
        [RecordValue::Int(0x1D), RecordValue::Null] => true,
        _ => false,
    });

    let last_record = forward_log_reader
        .next()
        .expect("Failed to read the last record");
    assert!(match last_record.values.as_slice() {
        [RecordValue::Int(10), RecordValue::Null] => true,
        _ => false,
    });

    assert!(forward_log_reader.next().is_none());
}

#[test]
fn test_upsert_and_get_from_secondary_memtable() {
    let data_dir = tmp_dir();
    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
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
    fs::remove_file(Path::new(&data_dir).join("db")).expect("Failed to delete the DB log file");

    // There should be 2 Johns
    let johns = db
        .find_all(&Field::Name, &RecordValue::String("John".to_string()))
        .expect("Failed to find all Johns");

    assert_eq!(johns.len(), 2);
}

#[test]
fn test_initialize_and_read_from_primary_memtable_fixture_db2() {
    let data_dir = tmp_dir();
    // Copy the fixture DB to the test data directory
    fs::create_dir_all(&data_dir).expect("Failed to create the test data directory");
    fs::copy(
        &Path::new(TEST_RESOURCES_DIR).join("test_db2"),
        &Path::new(&data_dir).join("db"),
    )
    .expect("Failed to copy the fixture DB");

    let mut db = DB::configure()
        .data_dir(&data_dir)
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Delete the DB so that any results must come from a memtable
    fs::remove_file(Path::new(&data_dir).join("db")).expect("Failed to delete the DB log file");

    let result = db.get(&RecordValue::Int(1)).unwrap().unwrap();

    // Check that the IDs match
    let expected = RecordValue::Int(1);
    assert!(match (&result.values[0], &expected) {
        (RecordValue::Int(a), RecordValue::Int(b)) => a == b,
        _ => false,
    });
}

#[test]
fn test_multiple_writing_threads() {
    let data_dir = tmp_dir();
    let mut threads = vec![];
    let threads_n = 100;

    for i in 0..threads_n {
        let data_dir = data_dir.clone();
        threads.push(thread::spawn(move || {
            let mut db = DB::configure()
                .data_dir(&data_dir)
                .fields(&vec![(Field::Id, RecordFieldType::Int)])
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
        .fields(&vec![(Field::Id, RecordFieldType::Int)])
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
    let threads_n = 20;

    // Add readers that poll for the records
    for i in 0..threads_n {
        let data_dir = data_dir.clone();
        threads.push(thread::spawn(move || {
            let mut db = DB::configure()
                .data_dir(&data_dir)
                .fields(&vec![(Field::Id, RecordFieldType::Int)])
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
            .fields(&vec![(Field::Id, RecordFieldType::Int)])
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

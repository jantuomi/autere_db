use env_logger;
use log_db;
use log_db::{LogReader, Record, RecordFieldType, RecordValue, DB};
use serial_test::serial;
use std::fs;
use std::path::Path;

const TEST_DATA_DIR: &str = "test_db_data";
const TEST_RESOURCES_DIR: &str = "tests/resources";

#[derive(Eq, PartialEq, Clone, Debug)]
enum Field {
    Id,
    Name,
    Data,
}

#[test]
#[serial]
fn test_initialize() {
    let _db = DB::configure()
        .data_dir(TEST_DATA_DIR)
        .fields(&vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ])
        .primary_key(Field::Id)
        .initialize()
        .expect("Failed to initialize DB instance");

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_and_get_with_memtable() {
    let mut db = DB::configure()
        .data_dir(TEST_DATA_DIR)
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

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_and_get_without_memtable() {
    let _ = env_logger::builder().is_test(true).try_init();
    let mut db = DB::configure()
        .data_dir(TEST_DATA_DIR)
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

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_fails_on_invalid_number_of_values() {
    let mut db = DB::configure()
        .data_dir(TEST_DATA_DIR)
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

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_fails_on_invalid_value_type() {
    let mut db = DB::configure()
        .data_dir(TEST_DATA_DIR)
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

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_log_reader_fixture_db1() {
    let _ = env_logger::builder().is_test(true).try_init();
    let db_path = Path::new(TEST_RESOURCES_DIR).join("test_db1");
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&db_path)
        .expect("Failed to open file");
    let mut log_reader = LogReader::new(&mut file).unwrap();

    // There are two records in the log with "schema": Int, Null

    let last_record = log_reader.next().expect("Failed to read the last record");
    assert!(match last_record.values.as_slice() {
        [RecordValue::Int(10), RecordValue::Null] => true,
        _ => false,
    });

    let first_record = log_reader.next().expect("Failed to read the first record");
    assert!(match first_record.values.as_slice() {
        // Note: the int value is equal to the escape byte
        [RecordValue::Int(0x1D), RecordValue::Null] => true,
        _ => false,
    });
}

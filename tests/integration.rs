use log_db::log_db::{Config, Record, RecordFieldType, RecordValue, DB};
use serial_test::serial;

const TEST_DATA_DIR: &str = "test_db_data";
const TEST_SEGMENT_SIZE: u64 = 1024 * 1024; // 1 MB
const TEST_MEMTABLE_SIZE: u64 = 1024 * 1024; // 1 MB

#[derive(Eq, PartialEq, Clone)]
enum Field {
    Id,
    Name,
    Data,
}

#[test]
#[serial]
fn test_initialize() {
    let _db = DB::initialize(&Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
        fields: vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ],
        primary_key: Field::Id,
        secondary_keys: vec![],
    })
    .unwrap();

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_to_empty_db() {
    let mut db = DB::initialize(&Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
        fields: vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ],
        primary_key: Field::Id,
        secondary_keys: vec![],
    })
    .unwrap();

    let record = Record {
        values: vec![
            RecordValue::Int(1),
            RecordValue::String("Alice".to_string()),
            RecordValue::Bytes(vec![0, 1, 2]),
        ],
    };
    db.upsert(&record).unwrap();

    let result = db.get(Field::Id, RecordValue::Int(1)).unwrap().unwrap();

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
fn test_upsert_fails_on_invalid_number_of_values() {
    let mut db = DB::initialize(&Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
        fields: vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ],
        primary_key: Field::Id,
        secondary_keys: vec![],
    })
    .unwrap();

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
    let mut db = DB::initialize(&Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
        fields: vec![
            (Field::Id, RecordFieldType::Int),
            (Field::Name, RecordFieldType::String),
            (Field::Data, RecordFieldType::Bytes),
        ],
        primary_key: Field::Id,
        secondary_keys: vec![],
    })
    .unwrap();

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

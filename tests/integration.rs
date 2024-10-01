use log_db::log_db;
use serial_test::serial;

const TEST_DATA_DIR: &str = "test_db_data";
const TEST_SEGMENT_SIZE: u64 = 1024 * 1024; // 1 MB
const TEST_MEMTABLE_SIZE: u64 = 1024 * 1024; // 1 MB

struct TestRecord {
    id: u64,
    data: String,
}

impl log_db::Record for TestRecord {
    fn serialize(&self) -> Vec<u8> {
        format!("{}:{}", self.id, self.data).into_bytes()
    }

    fn deserialize(data: Vec<u8>) -> Self {
        let data = String::from_utf8(data).unwrap();
        let parts: Vec<&str> = data.split(':').collect();
        TestRecord {
            id: parts[0].parse().unwrap(),
            data: parts[1].to_string(),
        }
    }
}

#[test]
#[serial]
fn test_initialize() {
    let _db: log_db::DB<TestRecord> = log_db::DB::initialize(&log_db::Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
    })
    .unwrap();

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

#[test]
#[serial]
fn test_upsert_to_empty_db() {
    let db: log_db::DB<TestRecord> = log_db::DB::initialize(&log_db::Config {
        data_dir: TEST_DATA_DIR.to_string(),
        segment_size: TEST_SEGMENT_SIZE,
        memtable_size: TEST_MEMTABLE_SIZE,
    })
    .unwrap();

    let record = TestRecord {
        id: 1,
        data: "hello".to_string(),
    };
    db.upsert(&record).unwrap();

    // Clean up
    std::fs::remove_dir_all(TEST_DATA_DIR.to_string()).unwrap();
}

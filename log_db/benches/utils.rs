use log_db::*;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fmt::Debug;

// Function to generate a random integer
pub fn random_int(from: i64, to: i64) -> i64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(from..to)
}

// Function to generate a random string
pub fn random_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.sample(Alphanumeric) as char).collect()
}

// Function to generate random bytes
pub fn random_bytes(len: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.gen()).collect()
}

// Function to generate a random record
pub fn random_record(from_id: i64, to_id: i64) -> Record {
    Record::from(&[
        Value::Int(random_int(from_id, to_id)), // Random int value between 0..1000
        Value::String(random_string(5)),        // Random string of length 5
        Value::Bytes(random_bytes(10)),         // Random bytes of length 10
    ])
}

pub fn prefill_db<T: Eq + Clone + Debug>(
    db: &mut DB<T>,
    n_records: usize,
    compact: bool,
) -> Result<(), DBError> {
    for _ in 0..n_records {
        let record = random_record(0, n_records as i64);
        db.upsert(&record)?;
        if compact {
            db.do_maintenance_tasks()?;
        }
    }

    Ok(())
}

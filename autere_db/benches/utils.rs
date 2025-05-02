use autere_db::*;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fmt::Debug;

#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Field {
    Id,
    Name,
    Data,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Inst {
    pub id: i64,
    pub name: String,
    pub data: Vec<u8>,
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

impl Into<String> for Field {
    fn into(self) -> String {
        self.as_ref().to_string()
    }
}

impl From<Inst> for Record {
    fn from(inst: Inst) -> Self {
        vec![
            Value::Int(inst.id),
            Value::String(inst.name),
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
                _ => panic!("Expected Int"),
            },
            name: match it.next().unwrap() {
                Value::String(name) => name,
                _ => panic!("Expected String"),
            },
            data: match it.next().unwrap() {
                Value::Bytes(data) => data,
                _ => panic!("Expected Bytes"),
            },
        }
    }
}

impl Inst {
    pub fn fields() -> Vec<Field> {
        vec![Field::Id, Field::Name, Field::Data]
    }
    pub fn primary_key() -> Field {
        Field::Id
    }
    pub fn secondary_keys() -> Vec<Field> {
        vec![Field::Name]
    }
}

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

// Function to generate a random Inst
pub fn random_inst(from_id: i64, to_id: i64) -> Inst {
    Inst {
        id: random_int(from_id, to_id), // Random int value between 0..1000
        name: random_string(5),         // Random string of length 5
        data: random_bytes(10),         // Random bytes of length 10
    }
}

pub fn prefill_db(
    db: &mut DB,
    insts: &mut Vec<Inst>,
    n_records: usize,
    compact: bool,
) -> DBResult<()> {
    for i in 0..(n_records - insts.len()) {
        let inst = random_inst(0, n_records as i64);
        insts.push(inst.clone());
        db.upsert(inst)?;
        if i % 1000 == 0 && compact {
            db.do_maintenance_tasks()?;
        }
    }

    Ok(())
}

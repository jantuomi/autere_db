# LogDB

An educational endeavor in implementing a log-structured database with a focus on simplicity, understandability and performance.

LogDB has the following features:

- Log-structured single-table storage, based on a durable append-only log
- In-memory indexes for fast lookups (primary and secondary)
- Log rotation and compaction for efficient storage even with larger databases
- Multiple concurrent readers and a single writer, using filesystem locks for synchronization
- Simple data types: `Int`, `Decimal`, `String`, `Bytes` (arbitrary bytestring), and `Null`
- A Rust API for interacting with the database, as well as Python bindings for the Rust API
- Transactions based on eager exclusive locking
- Batch read operations for improved performance

LogDB does not support:

- Authentication or authorization in any capacity
- Multiple tables
- Schema evolution, other than adding new nullable fields

See the [ARCHITECTURE.md](ARCHITECTURE.md) document for more details on the design and implementation of LogDB.

## Inspiration

The most significant sources of inspiration for LogDB are:

- [SQLite](https://www.sqlite.org/index.html) for its filesystem storage and
  locking mechanisms.
- [Designing Data-Intensive Applications (book)](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
  for its excellent overview of database internals and in-depth analysis of log-structured storage.
  LogDB is heavily based on the design outlined in chapter 3.

## Usage in Rust

Add LogDB as a dependency in your `Cargo.toml`.

```toml
[dependencies]
log_db = { git = "https://github.com/jantuomi/log_db.git" }
```

Then use it in your code like so:

```rust
use log_db::*;

// Define a type that represents your fields (columns)
#[derive(Eq, PartialEq, Clone, Debug)]
enum Field {
    Id,
    Name,
}

// Define your data type that represents a database row
struct Inst {
    pub id: i64,
    pub name: Option<String>,
}

// Implement the `Recordable` trait for your data type
impl Recordable for Inst {
  // Use the `Field` enum
  type Field = Field;

  // Define the schema as a vector of field names and corresponding types
  fn schema() -> Vec<(Self::Field, Type)> {
    vec![
      (Field::Id, Type::int()),
      (Field::Name, Type::string().nullable()),
    ]
  }

  // Select the primary key field
  fn primary_key() -> Self::Field {
    Field::Id
  }

  // Select the secondary key fields. All queries must be
  // based on the primary key or secondary keys.
  fn secondary_keys() -> Vec<Self::Field> {
    vec![Field::Name]
  }

  // Describe how to convert the data type to a vector of `Value`s
  fn into_record(self) -> Vec<Value> {
    vec![
      Value::Int(self.id),
      match self.name {
        Some(name) => Value::String(name),
        None => Value::Null,
      },
    ]
  }

  // Similarly, describe how to convert a vector of database values to the data type
  fn from_record(record: Vec<Value>) -> Self {
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
    }
  }
}

fn main() {
  // Initialize the database
  let mut db = DB::<Inst>::configure()
    .data_dir("data")
    .initialize()?;

  // Insert or update the record based on the primary key
  db.upsert(Inst {
    id: 1,
    name: Some("Alice".to_string()),
  })?;

  // Get the record by primary key
  let found = db.get(Value::Int(1))?;

  ...
}
```

## Tests

Run the tests with:

```sh
cargo test
```

Generate the benchmark reports with:

```sh
cargo bench
```

## Python bindings

To build the Python bindings, run:

```sh
cd py_bindings
python -m venv venv
. venv/bin/activate
pip install maturin
maturin develop             # for the development version, or
maturin build --release     # for the release version
```

Then you can use the Python bindings like so:

```python
from log_db_py import DB, Value, ValueType, Record

config = DB.configure();
config.primary_key = "id"
config.fields = [("id", ValueType.int().nullable())]

db = config.initialize()
db.upsert(Record(Value.int(10)))
```

## Copyright and license

LogDB is licensed under the Apache License, Version 2.0. © 2024 Jan Tuomi.

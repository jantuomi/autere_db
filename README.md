# LogDB

An educational endeavor in implementing a log-structured database with a focus on simplicity, understandability and performance.

LogDB has the following features:

- Log-structured single-table storage, based on a durable append-only log
- In-memory indexes for fast lookups (primary and secondary)
- Log rotation and compaction for efficient storage even with larger databases
- Multiple concurrent readers and a single writer, using filesystem locks for synchronization
- Simple data types: `Int`, `Float`, `String`, `Bytes` (arbitrary bytestring), and `Null`
- A Rust API for interacting with the database, as well as Python bindings for the Rust API

LogDB does not support:

- Authentication or authorization in any capacity
- Multiple tables
- Schema evolution, other than adding new nullable fields

Possible future features:

- Transactions

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

// Configure and initialize the database
let mut db = DB::configure()
  .fields(vec![
    (Field::Id, RecordField::int()),
    (Field::Data, RecordField::bytes()),
  ])
  .primary_key(Field::Id)
  .initialize()?;

// Define a record matching the `fields` schema
let record = Record {
  values: vec![
    RecordValue::Int(1),
    RecordValue::Bytes(vec![1, 2, 3, 4]),
  ],
};

// Insert or update the record based on the primary key (ID, first value)
db.upsert(&record)?;

// Get the record by primary key
let found = db.get(RecordValue::Int(1))?;
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
from log_db_py import DB, RecordValue, RecordField, Record

config = DB.configure();
config.primary_key = "id"
config.fields = [("id", RecordField.int().nullable())]

db = config.initialize()
db.upsert(Record(RecordValue.int(10)))
```

## Copyright and license

LogDB is licensed under the Apache License, Version 2.0. © 2024 Jan Tuomi.

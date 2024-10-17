# Architecture decision log

This document is used to record architectural decisions made on the project,
their context, and the (believed) consequences of those decisions. Pros and
cons of the decisions are also recorded.

## 2024-10-01 Build a log-structured database

This is a snippet from the README that describes the main intended features of
the database:

<blockquote>
An educational endeavor in implementing a log-structured database with a focus
on simplicity, understandability and performance.

LogDB has the following features:

- Log-structured single-table storage, based on a durable append-only log
- In-memory indexes for fast lookups (primary and secondary)
- Log rotation and compaction for efficient storage even with larger databases
- Multiple concurrent readers and a single writer, using filesystem locks for
  synchronization
- Simple data types: `Int`, `Float`, `String`, `Bytes` (arbitrary bytestring),
  and `Null`
- A Rust API for interacting with the database, as well as Python bindings for
  the Rust API

LogDB does not support:

- Authentication or authorization in any capacity
- Multiple tables
- Schema evolution, other than adding new nullable fields

Possible future features:

- Transactions
</blockquote>

**Pros**  
`+` Log-structured databases are generally simpler to implement and understand
than traditional B-tree based databases.

**Cons**  
`-` Log-structured databases are generally good for certain access patterns.

## 2024-10-01 Access patterns

As a log-structured database, LogDB is optimized for write-heavy and read-light
workloads.

**Pros**  
`+` Good fit for application that write a lot and read little, such as event
logging, time series data, or IoT data.

**Cons**  
`-` Not a good fit for read-heavy workloads, such as analytics or web services.

## 2024-10-01 Simple Rust

LogDB is implemented in Rust, which is a systems programming language that is
known for its performance, safety, and expressiveness. Asynchronous Rust is
infamous for being difficult to learn and use, so the project aims to keep the
Rust code as simple as possible. This means that there are no async functions,
`Arc`, `Mutex`, `RwLock`, `Rc`, `RefCell` or `unsafe` blocks in the codebase.

Concurrency is handled by using filesystem locks for synchronization, which lets
us keep the code simple and easy to understand.

The user is permitted to use async functions in their own code, but the database
itself does not use them.

## 2024-10-01 Data types

LogDB supports the following data types:

- `Int`: 64-bit signed integer
- `Float`: 64-bit floating point number
- `String`: UTF-8 encoded string
- `Bytes`: Arbitrary bytestring
- `Null`: A null value

A column is declared with a data type, not including `Null`. The column will
contain values of that specified type, and that data type cannot be changed
after the fact (no schema evolution). A column can be additionally declared as
nullable, which means that it can contain `Null` values too.

Schema evolution is supported only by adding new nullable fields to the schema.
Users can theorethically add stuff like Avro encoded data to a `Bytes` field,
but the database itself does not support any kind of schema evolution.

Only `Int` and `String` can be used as index keys. A primary index is mandatory,
and secondary indexes are optional.

**Pros**  
`+` Simple and easy to understand data model  
`+` No need for complex types or type conversions  
`+` No need to support schema evolution

**Cons**  
`-` Limited flexibility in data types  
`-` Database not that well applicable to use cases requiring complex data types
or schema evolution,
e.g. web applications with changing requirements

## 2024-10-03 Forward and backward readers

LogDB has in-memory indexes (memtables) for fast lookups. Queries that do not
hit an index are performed by scanning the log from the end to the beginning.
This requires a backward reader that can read the log in reverse order. There
exist crates that have this functionality but a quick spike showed that they
are either buggy or work in a way that is not suitable for LogDB, so a custom
solution is implemented.

On startup, LogDB populates the indexes by reading the log from the beginning,
as if replaying writes in chronological order. This requires a forward reader
that can read the log in forward order. A regular `BufReader` is used for this.

**Pros**  
`+` Things work as expected

**Cons**  
`-` Custom implementation required for backward reader, requiring effort and
testing that does not go into actual database development

## 2024-10-03 Filesystem locks

LogDB uses filesystem locks for synchronization between multiple readers and a
single writer. This is a simple and effective way to ensure that only one writer
is active at a time, and that readers do not read from a file that is being
written to. The locks are implemented using the `fs2` crate.

A writer acquires an exclusive lock which blocks all other readers and writers.
A reader acquires a shared lock which blocks writers but allows other readers
to acquire the shared lock as well.

In a scenario where one writer and numerous readers with retry policies are
competing for a lock, writer starvation could occur (writer never gets the lock
). This is a known limitation of the filesystem lock approach. To remedy this,
a separate "lock request file" is used to signal that a writer is waiting for
the lock. Readers check if this file is locked and do not enter the lock queue
if it is. This way, the writer is guaranteed to get the lock eventually.

**Pros**  
`+` Simple and proven way to synchronize readers and writers

**Cons**  
`-` Using the filesystem incurs a performance penalty compared to in-memory
synchronization. However, this is a trade-off to allow for readers and writers
to be separate processes.

## 2024-10-05 Log rotation and compaction

LogDB uses log rotation and compaction to keep the active log from growing
indefinitely. Obsolete writes are expunged from the log. This reduces the
amortized worst-case time complexity of reads from `O(m)` where `m` is the
number of writes to `O(n)` where `n` is the number of records in the database.
Note that `n <= m` always.

Log rotation is triggered when the active log segment reaches the configured
capacity, and the maintenance function is called by the user. The segment is
rotated by renaming the current log file to a new name, and creating a new
empty log file. The rotated file names are suffixed `1..n`, where `n` is the
number of rotations that have occurred.

To find the newest record, LogDB scans backwards log segment files starting
from the active log file `log`, then `log.n`, `log.n-1`, and so on until
`log.1`. This is done until the record is found or all log segment files have
been scanned.

After rotation, the rotated log file is also compacted. Compaction is done by
forward reading the log file, adding the records to a map, and writing them
back to a temporary file. The new file is then renamed to the original file
name.

**Pros**  
`+` Keeps the active log from growing indefinitely and frees up space that is
used for obsolete data  
`+` Reduces the time complexity of reads

**Cons**  
`-` Log rotation and compaction are expensive operations that require disk I/O  
`-` Synchronous rotation blocks writers and readers

## 2024-10-07 Python bindings

LogDB provides Python bindings using PyO3 to allow users to interact with the
database. The rationale behind this decision is to make the database accessible
to a language with a more approachable ecosystem for things like web services.

These should be implemented as a separate crate to keep the core database code
clean and focused. The Python bindings should provide a high-level, pythonic
API that abstracts away the details of the Rust implementation.

These bindings should be implemented when the database API design is stable.

## 2024-10-15 Index invariants

![LogDB indexes](./logdb_indexes.drawio.svg)

The structure of primary and secondary indexes is shown in the diagram above
(here PK = primary key, SK = secondary key). The primary index is a map from
primary key the record value in memory. This makes the primary index act as a
heap of sorts, since the data is inlined in the index. The secondary index is a
map from secondary key to a set of primary keys.

The design space here allows for a couple of approaches:

1. We can specify the invariant that all indexes must contain the same set of
   primary keys. Then, we can use a SK to find a set of PKs than can be quickly
   looked up in the primary index. However, this approach has a flaw: if we
   evict a PK from the primary index, we must also evict it from all secondary
   index value sets, to keep the invariant. This means that some SK -> Set<PK>
   maps are incomplete. This in turn means that we cannot be sure that a
   secondary index lookup will return all matches, forcing us to scan the log
   for the rest of the matches. This makes the secondary index useless, since a
   scan is required anyway.
2. We can specify that the all secondary indexes must contain the full set of
   matching record PKs. This means that a secondary index might have a reference
   to a record that is no longer in the primary index. In this case the log
   is scanned to find the record (which must exist).

LogDB uses the second approach.

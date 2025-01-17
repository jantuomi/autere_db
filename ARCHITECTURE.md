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
   is scanned to find the record (which must exist). This has the property that an index lookup is always complete: if there is an index hit, we can be sure that that's all the matching records. If there is no index hit, we know that the results are either not indexed or not in the database. We must do a scan
   to be sure.
3. A different design would be to store data in a separate heap and only store
   references in the indexes. This would require some sort of reference counting
   or other garbage collection to ensure that the data is removed from the heap when no longer referenced. An index of key -> key mappings is very lightweight: a million records fits in around 20 MB. It's so little that we can probably afford to keep the entire indexes in memory. A database could easily grow to hundreds of millions of records though, so we would still need to have some kind of eviction policy.

The second approach is chosen for now, although the third one might be the most efficient in the long run.

## 2024-10-18 In memory heap vs. log offsets

Upon further consideration, the option of not storing data in memory at all but
instead storing log offsets and segment numbers in the indexes is intriguing. This would allow for a very lightweight index structure, since the log offsets are just 64-bit integers and segment numbers are 16-bit. We can easily store millions of these in memory. We would still have to do eviction, but we could do it based on the segment number + log offset, which is a simple integer comparison. The evicted entry is the least recently written: compare log segment first and then log offset. This would remove the need for a priority queue that is currently in use.

The downside is that we have to do a disk seek to read the record, but this is not that big of a deal, since it is a constant time operation.

See diagram below:

![LogDB indexes with offsets](./logdb_offset_indexes.drawio.svg)

## 2024-11-02 Opaque handles to metadata files

Taking the previous point further, we can make the index entries mappings from
key -> (segment number, segment index), and add a segment metadata file that
maps segment index -> (file offset, file length). This approach would allow us
to:

- Keep the indexes lightweight (just a `BTreeMap<PK, u64>` or
  `BTreeMap<SK, Vec<u64>>`)
- Pack data tightly in the log files with no need for separator byte sequences

On the other hand:

- Full scans now need to iterate over the metadata file and read from the log
  file based on the metadata row, so two files need to be read instead of one

The new file formats are as follows:

### Metadata files (`metadata.1`, `metadata.2`, ...)

Metadata files are logs of entries that map segment indexes to file offsets. A metadata file begins with a header and followed by a dynamic number of fixed-width entries. The nth entry in the file corresponds to a LogKey index.

Header structure:

| description    | size (bytes) | example                              |
| -------------- | ------------ | ------------------------------------ |
| version        | 1            | 0x1                                  |
| padding        | 7            |                                      |
| data file uuid | 16           | 5ddd53de-1c61-4916-aadd-67208bbf2bb5 |

Entry structure:

| description | size (bytes) | example |
| ----------- | ------------ | ------- |
| offset      | 8            | 0xF0    |
| length      | 8            | 1024    |

The special `active` file is a symlink to the latest metadata file, and can be used to find it quickly.

### Data files (`5ddd53de-1c61-4916-aadd-67208bbf2bb5`, ...)

Data files are tightly packed records of variable length. Data files can not be read without the corresponding metadata file, which stores information about
the location of each record in the data file.

The reasoning for this naming scheme (only a UUIDv4) is that rotation can happen
without two-phase commit: the new data file is created first, then the metadata
file referencing it is written to a tmp file, and finally the metadata file is
renamed to the correct name. If the metadata file is not present, the data file is effectively orphaned.

This two-file model means that every time a record is accessed, two locks need to be acquired.

## 2024-11-17 Index consistency

An in-memory index can be inconsistent with the log file if another process writes to the log file after an index entry has been recorded in this process.
To solve this, we need to refresh the index before every read operation.

There are two ways to do this:

1. Strong consistency: Read new data into the index before every read operation. This makes reads potentially very slow: there might be a lot of data to read since last refresh.
2. Eventual consistency: Read new data into the index during maintenance tasks. This makes reads fast. The downside is that the index drifts out of sync with the log, until the next maintenance task.

If we mandate that indexes must fit in the memory, we can postulate that indexes are complete. Even in the case where there is no entry in the index matching a query, we can be sure that there won't be a match in the log file either.

This leads to the fact that a full table scan needs only ever be done for non-indexed tables.

## 2024-11-17 Actions per API function

The following actions are performed per API function:

### `get` (find one record by primary key)

1. (Strong) Refresh indexes
2. If primary index contains PK  
   a. Get segment number, segment index and read record from file
   b. Return record
3. Else, return None

### `find_by` (find multiple records by non-PK key)

1. (Strong) Refresh indexes
2. If key is indexed and secondary index contains key
   a. Get segment number, segment index and read record from file
   b. Return record
3. Else if key is indexed and secondary index does not contain key
   a. Return empty list
4. Else (key not indexed), do a full table scan from newest to oldest segment
   a. Acquire shared locks on path, defer unlock
   b. Check that path -> open file, otherwise restart
   c. Read data file at offset and length from metadata file
5. Return matching records

### `upsert` (insert or update one record by primary key)

1. Acquire exclusive lock on path
2. Check that `active` -> path, otherwise restart
3. Check that path -> open file, otherwise restart
4. Write record to log file

Note that writes have nothing to do with memtable indexes.

## 2025-01-17 Batch API & transactions

The user should be able to perform batch operations with the following API functions:

- `batch_find_by(field, keys)`: Find multiple records by sequence of keys
- `batch_upsert(records)`: Insert or update multiple records

The single record API functions are implemented as special cases of the batch functions.

These operations have overlap with the planned transaction API, which allows you to group multiple operations which
will be executed with a single FS write. The transaction API will be implemented as follows:

- `transact()`: Start a new transaction, exclusively locking the database
- `commit()`: Commit the transaction, writing all changes to the log in one go
- `rollback()`: Roll back the transaction, discarding all changes

All interim writes and deletes are recorded into a sequence. Reads are done as normal, and reflect the state of the
database as it was at the time of the `transact()` call.

## 2025-01-17 Locking mechanism

Working with the current locking system, where each segment file that is accessed is locked separately, is cumbersome
and error-prone. It can also lead to a lot of lock acquisitions (FS operations) upon reads. Considering the
requirements of the upcoming transaction API, a simpler locking mechanism is needed.

The new locking mechanism will be based on a single lock file that is locked either exclusively or in shared mode. The lock file is used for all concurrency handling, even initialization of the DB. As such, it should be the first file to be added to the database directory. This also reduces the number of "do I have the most up to date file locked" checks.

Using a single lock file relies on more cooperation and well-behavedness of the processes accessing the database, since the lock system won't protect from accesses without the lock. This is a trade-off for simplicity and performance.

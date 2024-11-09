use fs2::{lock_contended_error, FileExt};
use once_cell::sync::Lazy;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::fs::{self, metadata, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::num::ParseIntError;
use std::path::{Path, PathBuf};
use std::thread;
use uuid::Uuid;

// For Unix-like systems
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

// For Windows
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub const ACTIVE_SYMLINK_FILENAME: &str = "active";
pub const METADATA_FILE_HEADER_SIZE: usize = 24;
pub const EXCL_LOCK_REQUEST_FILENAME: &str = "excl_lock_req";
pub const INIT_LOCK_FILENAME: &str = "init_lock";
pub const DEFAULT_READ_BUF_SIZE: usize = 1024 * 1024; // 1 MB
pub const TEST_RESOURCES_DIR: &str = "tests/resources";

#[derive(Debug, Eq, PartialEq)]
pub enum SpecialSequence {
    RecordSeparator,
    LiteralFieldSeparator,
    LiteralEscape,
}

/// LogKey is a packed struct that contains:
/// - a log segment number (16 bits)
/// - a log index within the segment (48 bits)
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct LogKey(u64);

impl LogKey {
    pub fn new(segment_num: u16, index: u64) -> Self {
        assert!(index < (1 << 48), "Index must fit in 48 bits");
        LogKey((segment_num as u64) << 48 | index)
    }

    pub fn from(u: u64) -> Self {
        LogKey(u)
    }

    pub fn segment_num(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    pub fn index(&self) -> u64 {
        self.0 & 0x0000_FFFF_FFFF_FFFF
    }
}

/// LogKeySet is a non-empty set of LogKeys.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogKeySet {
    set: HashSet<LogKey>,
}

impl PartialOrd for LogKeySet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let self_max_elem = self.set.iter().max()?;
        let other_max_elem = other.set.iter().max()?;
        Some(self_max_elem.cmp(other_max_elem))
    }
}

impl LogKeySet {
    /// Create a new LogKeySet with an initial LogKey.
    /// The initial LogKey is required since LogKeySet must be non-empty.
    pub fn new_with_initial(key: &LogKey) -> Self {
        let mut set = HashSet::new();
        set.insert(key.clone());
        LogKeySet { set }
    }

    pub fn from_slice(keys: &[LogKey]) -> Self {
        assert!(
            !keys.is_empty(),
            "LogKeySet::from_slice must be supplied a non-empty slice"
        );
        let mut set = HashSet::with_capacity(keys.len());
        keys.iter().for_each(|key| {
            set.insert(key.clone());
        });
        LogKeySet { set }
    }

    pub fn iter(&self) -> std::collections::hash_set::Iter<'_, LogKey> {
        self.set.iter()
    }

    /// The number of LogKeys in the set.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Insert a LogKey into the set.
    pub fn insert(&mut self, key: LogKey) {
        self.set.insert(key);
    }

    /// Remove a LogKey from the set. Return Ok(()) if the key was found and removed.
    /// Return io::Error::InvalidInput if trying to remove the last element.
    /// Return io::Error::NotFound if the key was not found.
    pub fn remove(&mut self, key: &LogKey) -> Result<(), io::Error> {
        if self.set.len() == 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot remove the last element from LogKeySet",
            ));
        }
        let removed = self.set.remove(key);

        if !removed {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "LogKey not found in LogKeySet",
            ));
        }

        assert!(
            self.set.len() > 0,
            "LogKeySet should not be empty after removal"
        );

        Ok(())
    }

    /// Get a reference to the set of LogKeys.
    pub fn log_keys(&self) -> &HashSet<LogKey> {
        &self.set
    }
}

impl Ord for LogKeySet {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("LogKeySet comparison failed, possibly due to empty set")
    }
}

pub static APPEND_MODE: Lazy<fs::OpenOptions> = Lazy::new(|| {
    let mut options = fs::OpenOptions::new();
    options.read(true).append(true);
    options
});
pub static READ_MODE: Lazy<fs::OpenOptions> = Lazy::new(|| {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    options
});
pub static WRITE_MODE: Lazy<fs::OpenOptions> = Lazy::new(|| {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true);
    options
});

pub struct MetadataHeader {
    pub version: u8,
    pub uuid: Uuid,
}

const METADATA_HEADER_PADDING: &[u8] = &[0; 7];
impl MetadataHeader {
    pub fn serialize(&self) -> Vec<u8> {
        let uuid_bytes = self.uuid.as_bytes().to_vec();

        let mut header = vec![self.version];
        header.extend(METADATA_HEADER_PADDING);
        header.extend(uuid_bytes);

        assert_eq!(header.len(), METADATA_FILE_HEADER_SIZE);

        header
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), METADATA_FILE_HEADER_SIZE);

        let version = bytes[0];
        let uuid = Uuid::from_slice(&bytes[8..24]).expect("Failed to deserialize Uuid");

        MetadataHeader { version, uuid }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WriteDurability {
    /// Changes are written to the OS write buffer but not immediately synced to disk.
    /// This is generally recommended. Most OSes will sync the write buffer to disk within a few seconds.
    Flush,
    /// Changes are written to the OS write buffer and synced to disk immediately.
    /// Offers the best durability guarantees but is a lot slower.
    FlushSync,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Reads are **not** guaranteed to see the latest writes.
    /// Indexes are only updated when running maintenance tasks.
    /// This is the fastest option, but can produce stale reads.
    Eventual,
    /// Reads are guaranteed to see the latest writes from the same client, but
    /// not from other clients. Written values are indexed after writing to file.
    /// Indexes are updated when running maintenance tasks.
    ReadMyWrites,
    /// Reads are guaranteed to see the latest writes.
    /// Indexes are updated synchronously before reads.
    /// This is the slowest option, and can cause long waits for reads.
    Strong,
}

impl Display for WriteDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum IndexableValue {
    Int(i64),
    String(String),
}

#[derive(Debug, Clone)]
pub enum RecordFieldType {
    Int,
    Float,
    String,
    Bytes,
}

#[derive(Debug, Clone)]
pub struct RecordField {
    pub field_type: RecordFieldType,
    pub nullable: bool,
}

impl RecordField {
    pub fn int() -> Self {
        RecordField {
            field_type: RecordFieldType::Int,
            nullable: false,
        }
    }

    pub fn float() -> Self {
        RecordField {
            field_type: RecordFieldType::Float,
            nullable: false,
        }
    }

    pub fn string() -> Self {
        RecordField {
            field_type: RecordFieldType::String,
            nullable: false,
        }
    }

    pub fn bytes() -> Self {
        RecordField {
            field_type: RecordFieldType::Bytes,
            nullable: false,
        }
    }

    pub fn nullable(&mut self) -> Self {
        let mut new = self.clone();
        new.nullable = true;
        new
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}
impl Eq for Value {}

impl Value {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Value::Null => {
                vec![0] // Tag for Null
            }
            Value::Int(i) => {
                let mut bytes = vec![1]; // Tag for Int
                bytes.extend(&i.to_be_bytes());
                bytes
            }
            Value::Float(f) => {
                let mut bytes = vec![2]; // Tag for Float
                bytes.extend(&f.to_be_bytes());
                bytes
            }
            Value::String(s) => {
                let mut bytes = vec![3]; // Tag for String
                let length = s.len() as u64;
                bytes.extend(&length.to_be_bytes());
                bytes.extend(s.as_bytes());
                bytes
            }
            Value::Bytes(b) => {
                let mut bytes = vec![4]; // Tag for Bytes
                let length = b.len() as u64;
                bytes.extend(&length.to_be_bytes());
                bytes.extend(b);
                bytes
            }
        }
    }

    /// Deserialize a Value from a byte slice.
    /// Returns the deserialized Value and the number of bytes consumed.
    pub fn deserialize(bytes: &[u8]) -> (Value, usize) {
        match bytes[0] {
            0 => (Value::Null, 1),
            1 => {
                let mut int_bytes = [0; 8];
                int_bytes.copy_from_slice(&bytes[1..1 + 8]);
                (Value::Int(i64::from_be_bytes(int_bytes)), 1 + 8)
            }
            2 => {
                let mut float_bytes = [0; 8];
                float_bytes.copy_from_slice(&bytes[1..1 + 8]);
                (Value::Float(f64::from_be_bytes(float_bytes)), 1 + 8)
            }
            3 => {
                let length_bytes = &bytes[1..1 + 8];
                let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                (
                    Value::String(
                        String::from_utf8(bytes[1 + 8..1 + 8 + length].to_vec()).unwrap(),
                    ),
                    1 + 8 + length,
                )
            }
            4 => {
                let length_bytes = &bytes[1..1 + 8];
                let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                (
                    Value::Bytes(bytes[1 + 8..1 + 8 + length].to_vec()),
                    1 + 8 + length,
                )
            }
            _ => panic!("Invalid tag: {}", bytes[0]),
        }
    }

    pub fn as_indexable(&self) -> Option<IndexableValue> {
        match self {
            Value::Int(i) => Some(IndexableValue::Int(*i)),
            Value::String(s) => Some(IndexableValue::String(s.clone())),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Record(Vec<Value>);

impl Record {
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in &self.0 {
            bytes.extend(value.serialize());
        }
        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Record {
        let mut values = Vec::new();
        let mut start = 0;
        while start < bytes.len() {
            let (rv, consumed) = Value::deserialize(&bytes[start..]);
            values.push(rv);
            start += consumed;
        }
        Record(values)
    }

    pub fn from(values: &[Value]) -> Record {
        Record(values.to_vec())
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn at(&self, index: usize) -> &Value {
        &self.0[index]
    }

    pub fn validate<Field: Eq>(&self, schema: &Vec<(Field, RecordField)>) -> Result<(), io::Error> {
        // Validate the record length
        if self.0.len() != schema.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Record has an incorrect number of fields: {}, expected {}",
                    self.0.len(),
                    schema.len()
                ),
            ));
        }

        // Validate that record fields match schema types
        for (i, (_, field)) in schema.iter().enumerate() {
            match (&self.0[i], field) {
                (
                    Value::Null,
                    RecordField {
                        nullable: true,
                        field_type: _,
                    },
                ) => {}
                (
                    Value::Int(_),
                    RecordField {
                        field_type: RecordFieldType::Int,
                        ..
                    },
                ) => {}
                (
                    Value::String(_),
                    RecordField {
                        field_type: RecordFieldType::String,
                        ..
                    },
                ) => {}
                (
                    Value::Bytes(_),
                    RecordField {
                        field_type: RecordFieldType::Bytes,
                        ..
                    },
                ) => {}
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Record field {} has incorrect type: {:?}, expected {:?}",
                            &i, &self.0[i], &field.field_type
                        ),
                    ))
                }
            }
        }
        Ok(())
    }
}

pub fn get_secondary_memtable_index_by_field<Field: Eq>(
    sks: &Vec<Field>,
    field: &Field,
) -> Option<usize> {
    sks.iter().position(|schema_field| schema_field == field)
}

pub struct GetSecondaryIndexPositionsResult {
    pub schema_index: usize,
    pub secondary_index: usize,
}

pub fn get_secondary_index_positions<Field: Eq>(
    sks: &Vec<Field>,
    schema: &Vec<(Field, RecordField)>,
) -> Vec<GetSecondaryIndexPositionsResult> {
    let mut ret = vec![];
    for (i, (schema_field, _)) in schema.iter().enumerate() {
        for (j, sk) in sks.iter().enumerate() {
            if schema_field == sk {
                ret.push(GetSecondaryIndexPositionsResult {
                    schema_index: i,
                    secondary_index: j,
                });
                break;
            }
        }
    }
    ret
}

/// A path to a log segment file along with its type
pub enum SegmentPath {
    /// A symbolic link to the active log file
    ActiveSymlink(String),
    /// A compacted segment that is no longer being written to
    Compacted(String),
}

pub fn is_file_same_as_path(file: &File, path: &PathBuf) -> io::Result<bool> {
    // Get the metadata for the open file handle
    let file_metadata = file.metadata()?;

    // Get the metadata for the file at the specified path
    let path_metadata = metadata(path)?;

    // Platform-specific comparison
    #[cfg(unix)]
    {
        Ok(
            file_metadata.dev() == path_metadata.dev()
                && file_metadata.ino() == path_metadata.ino(),
        )
    }

    #[cfg(windows)]
    {
        Ok(file_metadata.file_index() == path_metadata.file_index()
            && file_metadata.volume_serial_number() == path_metadata.volume_serial_number())
    }
}

pub fn symlink(original: &Path, link: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(original, link)
    }

    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(original, link)
    }
}

/// Set the active segment to the segment with the given ordinal number.
pub fn set_active_segment(data_dir_path: &Path, segment_num: u16) -> Result<(), io::Error> {
    let tmp_uuid = Uuid::new_v4();
    let tmp_filename = format!("active_{}", tmp_uuid.to_string());
    let tmp_path = data_dir_path.join(tmp_filename);

    let metadata_filename = format!("metadata.{}", segment_num);
    let metadata_path = Path::new(&metadata_filename);
    let active_symlink = data_dir_path.join(ACTIVE_SYMLINK_FILENAME);

    symlink(&metadata_path, &tmp_path)?;
    fs::rename(&tmp_path, &active_symlink)?;

    Ok(())
}

/// Create a new segment metadata file and return its number and path.
/// A metadata file contains the segment metadata, including the UUID of the data file.
/// See `ARCHITECTURE.md` for the file format.
pub fn create_segment_metadata_file(
    data_dir_path: &Path,
    data_file_uuid: &Uuid,
) -> Result<(u16, PathBuf), io::Error> {
    let current_greatest_num = greatest_segment_number(data_dir_path)?;
    let new_num = current_greatest_num + 1;

    let metadata_filename = format!("metadata.{}", new_num);
    let metadata_path = data_dir_path.join(metadata_filename);

    let mut metadata_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&metadata_path)?;

    let metadata_header = MetadataHeader {
        version: 1,
        uuid: *data_file_uuid,
    };

    let header_serialized = metadata_header.serialize();
    metadata_file.write_all(&header_serialized)?;
    metadata_file.flush()?;

    let len = metadata_file.seek(io::SeekFrom::End(0))?;
    assert!(len >= METADATA_FILE_HEADER_SIZE as u64);
    assert_eq!((len - METADATA_FILE_HEADER_SIZE as u64) % 16, 0);

    Ok((new_num, metadata_path))
}

/// Parse number from format "metadata.1"
pub fn parse_segment_num(segment_filename: &Path) -> Result<u16, ParseIntError> {
    segment_filename
        .file_name()
        .expect("Not a valid path")
        .to_str()
        .expect("Not a valid UTF-8 string")
        .split(".")
        .last()
        .expect("No extension")
        .parse::<u16>()
}

/// Get the number of the segment with the greatest ordinal.
/// This is the newest segment, i.e. the one that is pointed to by the `active` symlink.
/// If there are no segments yet, returns 0.
pub fn greatest_segment_number(data_dir_path: &Path) -> Result<u16, io::Error> {
    let active_symlink = data_dir_path.join(ACTIVE_SYMLINK_FILENAME);

    if !fs::exists(&active_symlink)? {
        return Ok(0);
    }

    let segment_metadata_path = fs::read_link(&active_symlink)?;

    parse_segment_num(&segment_metadata_path).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to parse segment number from filename",
        )
    })
}

/// Create a new segment data file and return its UUID.
/// A data file contains the segment data, tightly packed without separators.
/// An accompanying metadata file is required to interpret the data.
pub fn create_segment_data_file(data_dir_path: &Path) -> Result<(Uuid, PathBuf), io::Error> {
    let uuid = Uuid::new_v4();
    let new_segment_path = data_dir_path.join(uuid.to_string());
    fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&new_segment_path)?;

    Ok((uuid, new_segment_path))
}

/// Reads the metadata header from the metadata file.
/// Leaves the file seek head at the beginning of the records, after the header.
pub fn read_metadata_header(metadata_file: &mut fs::File) -> Result<MetadataHeader, io::Error> {
    metadata_file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; METADATA_FILE_HEADER_SIZE];
    metadata_file.read_exact(&mut buf)?;

    let header = MetadataHeader::deserialize(&buf);
    Ok(header)
}

pub fn validate_metadata_header(header: &MetadataHeader) -> Result<(), io::Error> {
    if header.version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unsupported metadata file version",
        ));
    }

    Ok(())
}

pub enum IsMetadatafileValidResult {
    Ok,
    ReplaceFile,
    TruncateToSize(u64),
}

pub fn is_metadata_file_valid(
    metadata_file: &mut fs::File,
) -> Result<IsMetadatafileValidResult, io::Error> {
    let size = metadata_file.seek(SeekFrom::End(0))? as usize;

    if size < METADATA_FILE_HEADER_SIZE {
        return Ok(IsMetadatafileValidResult::ReplaceFile);
    }

    // The data section must be a multiple of 16 bytes.
    // Otherwise, the non-aligned part of the file is dropped.
    let data_section_len = size - METADATA_FILE_HEADER_SIZE;
    let remainder = data_section_len % 16;
    if remainder != 0 {
        return Ok(IsMetadatafileValidResult::TruncateToSize(
            (size - remainder) as u64,
        ));
    }

    Ok(IsMetadatafileValidResult::Ok)
}

/// Check that the active metadata file is well-formed and repair it if necessary.
/// The metadata file is considered well-formed if its size is, in pseudocode, `header_size + n * record_size`.
/// If the file is not well-formed, it is truncated to the last well-formed record using
/// a temporary file and an atomic move operation.
///
/// `self.active_metadata_file` must be a locked file handle opened with read permissions.
/// The function leaves the seek head in an unspecified position.
///
/// Returns `false` if the file was repaired and rotated, `true` if no action was taken.
pub fn ensure_active_metadata_is_valid(
    data_dir: &Path,
    metadata_file: &mut fs::File,
) -> Result<bool, io::Error> {
    let current_len = metadata_file.seek(SeekFrom::End(0))? as usize;

    match is_metadata_file_valid(metadata_file)? {
        IsMetadatafileValidResult::Ok => return Ok(true),
        IsMetadatafileValidResult::ReplaceFile => {
            let active_target = fs::read_link(data_dir.join(ACTIVE_SYMLINK_FILENAME))?;
            let active_path = data_dir.join(&active_target);
            warn!(
                "Metadata file \"{}\" is malformed ({} bytes), replacing it with an empty file",
                active_target.display(),
                current_len,
            );
            let mut tmp_file = tempfile::NamedTempFile::new()?;

            let header = MetadataHeader {
                version: 1,
                uuid: Uuid::new_v4(),
            };

            tmp_file.write_all(&header.serialize())?;
            tmp_file.flush()?;

            fs::rename(tmp_file.path(), active_path)?;

            debug!("Replaced metadata file");
            return Ok(false);
        }
        IsMetadatafileValidResult::TruncateToSize(new_size) => {
            let active_target = fs::read_link(data_dir.join(ACTIVE_SYMLINK_FILENAME))?;
            let active_path = data_dir.join(&active_target);
            warn!(
                "Metadata file \"{}\" is malformed ({} bytes), truncating it to {} bytes",
                active_target.display(),
                current_len,
                new_size
            );

            let mut tmp_file = tempfile::NamedTempFile::new()?;

            let mut buf = vec![0; new_size as usize];
            metadata_file.seek(SeekFrom::Start(0))?;
            metadata_file.read_exact(&mut buf)?;

            tmp_file.write_all(&buf)?;
            tmp_file.flush()?;

            fs::rename(tmp_file.path(), active_path)?;

            debug!("Truncated metadata file");
            return Ok(false);
        }
    }
}

pub fn is_exclusive_lock_requested(data_dir: &Path) -> Result<bool, io::Error> {
    let lock_request_path = data_dir.join(EXCL_LOCK_REQUEST_FILENAME);
    let lock_request_file = fs::OpenOptions::new()
        .create(true)
        .write(true) // When requesting a lock, we need to have either read or write permissions
        .open(&lock_request_path)?;

    // Attempt to acquire a shared lock on the lock request file
    // If the file is already locked, return false
    match lock_request_file.try_lock_shared() {
        Err(e) => {
            if e.kind() == lock_contended_error().kind() {
                return Ok(true);
            }
            return Err(e);
        }
        Ok(_) => {
            // Check that the exclusive lock request file is still the same as the one we opened
            if !is_file_same_as_path(&lock_request_file, &lock_request_path)? {
                // The lock request file has been removed
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Lock request file was removed unexpectedly",
                ));
            }

            lock_request_file.unlock()?;
            return Ok(false);
        }
    }
}

pub fn request_shared_lock(data_dir: &Path, file: &mut fs::File) -> Result<(), io::Error> {
    const SHARED_LOCK_WAIT_MAX_MS: u64 = 100;
    let mut timeout = 5;
    loop {
        if is_exclusive_lock_requested(data_dir)? {
            debug!(
                "Exclusive lock requested, waiting for {}ms before requesting a shared lock again",
                timeout
            );
            thread::sleep(std::time::Duration::from_millis(timeout));
            timeout = std::cmp::min(timeout * 2, SHARED_LOCK_WAIT_MAX_MS);
        } else {
            file.lock_shared()?;
            return Ok(());
        }
    }
}

pub fn request_exclusive_lock(data_dir: &Path, file: &mut fs::File) -> Result<(), io::Error> {
    // Create a lock on the exclusive lock request file to signal to readers that they should wait
    let lock_request_path = data_dir.join(EXCL_LOCK_REQUEST_FILENAME);
    let lock_request_file = fs::OpenOptions::new()
        .create(true)
        .write(true) // When requesting a lock, we need to have either read or write permissions
        .open(&lock_request_path)?;

    // Attempt to acquire an exclusive lock on the lock request file
    // This will block until the lock is acquired
    lock_request_file.lock_exclusive()?;

    // Acquire an exclusive lock on the segment files
    file.lock_exclusive()?;

    // Unlock the request file
    lock_request_file.unlock()?;

    Ok(())
}

pub fn get_record_by_log_key(data_dir: &Path, log_key: &LogKey) -> Result<Record, io::Error> {
    let segment_num = log_key.segment_num();
    let segment_index = log_key.index();

    let metadata_path = data_dir.join(format!("metadata.{}", segment_num));
    let mut metadata_file = READ_MODE.open(metadata_path)?;

    request_shared_lock(data_dir, &mut metadata_file)?;

    let metadata_header = read_metadata_header(&mut metadata_file)?;
    validate_metadata_header(&metadata_header)?;

    let data_file_path = data_dir.join(metadata_header.uuid.to_string());
    let mut data_file = READ_MODE.open(data_file_path)?;

    request_shared_lock(data_dir, &mut data_file)?;

    let metadata_offset = METADATA_FILE_HEADER_SIZE as u64 + segment_index * 16;
    let mut metadata_buf = vec![0; 16];
    metadata_file.seek(SeekFrom::Start(metadata_offset))?;
    metadata_file.read_exact(&mut metadata_buf)?;

    let data_offset = u64::from_be_bytes(metadata_buf[0..8].try_into().unwrap());
    let data_len = u64::from_be_bytes(metadata_buf[8..16].try_into().unwrap());

    let mut data_buf = vec![0; data_len as usize];
    data_file.seek(SeekFrom::Start(data_offset))?;
    data_file.read_exact(&mut data_buf)?;

    let record = Record::deserialize(&data_buf);

    Ok(record)
}

pub fn get_records_by_log_keys(
    data_dir: &Path,
    log_keys: &[LogKey],
) -> Result<Vec<Record>, io::Error> {
    // Partition by segment number
    let mut segments: BTreeMap<u16, Vec<u64>> = BTreeMap::new();
    for log_key in log_keys {
        segments
            .entry(log_key.segment_num())
            .or_default()
            .push(log_key.index());
    }

    let mut records = Vec::new();

    // Process newest (largest segment num) first
    let mut segments_sorted = segments.iter().collect::<Vec<_>>();
    segments_sorted.sort_by_key(|(&segment_num, _)| -(segment_num as i32));

    for (segment_num, indices) in segments_sorted {
        let metadata_path = data_dir.join(format!("metadata.{}", segment_num));
        let mut metadata_file = READ_MODE.open(metadata_path)?;

        request_shared_lock(data_dir, &mut metadata_file)?;

        let metadata_header = read_metadata_header(&mut metadata_file)?;
        validate_metadata_header(&metadata_header)?;

        let data_file_path = data_dir.join(metadata_header.uuid.to_string());
        let mut data_file = READ_MODE.open(data_file_path)?;

        request_shared_lock(data_dir, &mut data_file)?;

        for index in indices {
            let metadata_offset = METADATA_FILE_HEADER_SIZE as u64 + index * 16;
            let mut metadata_buf = vec![0; 16];
            metadata_file.seek(SeekFrom::Start(metadata_offset))?;
            metadata_file.read_exact(&mut metadata_buf)?;

            let data_offset = u64::from_be_bytes(metadata_buf[0..8].try_into().unwrap());
            let data_len = u64::from_be_bytes(metadata_buf[8..16].try_into().unwrap());

            let mut data_buf = vec![0; data_len as usize];
            data_file.seek(SeekFrom::Start(data_offset))?;
            data_file.read_exact(&mut data_buf)?;

            let record = Record::deserialize(&data_buf);
            records.push(record);
        }
    }

    Ok(records)
}

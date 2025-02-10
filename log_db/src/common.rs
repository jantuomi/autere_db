use super::*;

use std::collections::btree_map::Values;
// For Unix-like systems
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

// For Windows
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

pub const ACTIVE_SYMLINK_FILENAME: &str = "active";
pub const LOCK_FILENAME: &str = "lock";
pub const EXCL_LOCK_REQ_FILENAME: &str = "excl_lock_req";
pub const INITIALIZED_FILENAME: &str = "initialized";

pub const METADATA_FILE_HEADER_SIZE: usize = 24;
pub const METADATA_ROW_LENGTH: usize = 16;
pub const LOCK_WAIT_MAX_MS: u64 = 1000;

// Serialized value tags
pub const B_NULL: u8 = 0x0;
pub const B_INT: u8 = 0x1;
pub const B_DECIMAL: u8 = 0x2;
pub const B_STRING: u8 = 0x3;
pub const B_BYTES: u8 = 0x4;
// Tombstone marker tags
pub const B_LIVE: u8 = 0x0;
pub const B_TOMBSTONE: u8 = 0xFF;

pub fn metadata_filename(num: u16) -> String {
    format!("metadata.{}", num)
}

pub type DBResult<A> = Result<A, DBError>;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("lock request failed: {0}")]
    LockRequestError(String),
    #[error("validation failed: {0}")]
    ValidationError(String),
    #[error("consistency check failed: {0}")]
    ConsistencyError(String),
    #[error("invalid transaction: {0}")]
    TransactionError(String),
    #[error("unexpected IO error: {0}")]
    IOError(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum LogKeyMapError {
    #[error("log key not found in map")]
    NotFoundError,
    #[error("attempted to remove last element of non-empty map")]
    RemovingLastElementError,
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

    pub fn segment_num(&self) -> u16 {
        (self.0 >> 48) as u16
    }

    pub fn index(&self) -> u64 {
        self.0 & 0x0000_FFFF_FFFF_FFFF
    }
}

/// LogKeyMap is a non-empty map of PK => LogKey mappings.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LogKeyMap {
    map: BTreeMap<IndexableValue, LogKey>,
}

impl LogKeyMap {
    /// Create a new LogKeyMap with an initial mapping.
    /// The initial mapping is required since LogKeyMap must be non-empty.
    pub fn new_with_initial(pk: IndexableValue, log_key: LogKey) -> Self {
        let mut map = BTreeMap::new();
        map.insert(pk, log_key);
        LogKeyMap { map }
    }

    pub fn contains_pk(&self, key: &IndexableValue) -> bool {
        self.map.contains_key(key)
    }

    /// The number of LogKeys in the map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Insert a PK -> LogKey mapping into the map.
    pub fn insert(&mut self, key: IndexableValue, log_key: LogKey) {
        self.map.insert(key, log_key);
    }

    /// Remove a mapping from the map. Return Ok(()) if the key was found and removed.
    /// Return `LogKeyMapError::RemovingLastElementError` if trying to remove the last element.
    /// Return `LogKeyMapError::NotFoundError` if the key was not found.
    pub fn remove_pk(&mut self, key: &IndexableValue) -> Result<(), LogKeyMapError> {
        if self.map.len() == 1 {
            return Err(LogKeyMapError::RemovingLastElementError);
        }
        let removed = self.map.remove(key);

        if removed.is_none() {
            return Err(LogKeyMapError::NotFoundError);
        }

        assert!(
            self.map.len() > 0,
            "LogKeyMap should not be empty after removal"
        );

        Ok(())
    }

    /// Get a reference to the set of LogKeys.
    pub fn log_keys(&self) -> Values<IndexableValue, LogKey> {
        self.map.values()
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
    pub fn serialize(&self) -> [u8; METADATA_FILE_HEADER_SIZE] {
        let mut header = [0u8; METADATA_FILE_HEADER_SIZE];
        header[0] = self.version;
        header[1..8].copy_from_slice(METADATA_HEADER_PADDING);
        header[8..].copy_from_slice(self.uuid.as_bytes());

        header
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), METADATA_FILE_HEADER_SIZE);

        let version = bytes[0];
        let uuid = Uuid::from_slice(&bytes[8..24]).expect("Failed to deserialize Uuid");

        MetadataHeader { version, uuid }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum IndexableValue {
    Null,
    Int(i64),
    Decimal(Decimal),
    String(String),
}

/// A primitive type
#[derive(Debug, Clone)]
pub enum PrimitiveType {
    Int,
    Decimal,
    String,
    Bytes,
}

/// A primitive type + a nullability bit
#[derive(Debug, Clone)]
pub struct Type {
    pub primitive: PrimitiveType,
    pub nullable: bool,
}

impl Type {
    pub fn int() -> Self {
        Type {
            primitive: PrimitiveType::Int,
            nullable: false,
        }
    }

    pub fn decimal() -> Self {
        Type {
            primitive: PrimitiveType::Decimal,
            nullable: false,
        }
    }

    pub fn string() -> Self {
        Type {
            primitive: PrimitiveType::String,
            nullable: false,
        }
    }

    pub fn bytes() -> Self {
        Type {
            primitive: PrimitiveType::Bytes,
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
    Decimal(Decimal),
    String(String),
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
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
            Value::Null => vec![B_NULL],
            Value::Int(i) => {
                let mut bytes = Vec::with_capacity(1 + 16);
                bytes.push(B_INT);
                bytes.extend_from_slice(&i.to_be_bytes());
                bytes
            }
            Value::Decimal(d) => {
                let mut bytes = Vec::with_capacity(1 + 16);
                bytes.push(B_DECIMAL);
                bytes.extend_from_slice(&d.serialize());
                bytes
            }
            Value::String(s) => {
                let len = s.len();
                let mut bytes = Vec::with_capacity(1 + 8 + len);
                bytes.push(B_STRING);
                bytes.extend_from_slice(&(len as u64).to_be_bytes());
                bytes.extend_from_slice(s.as_bytes());
                bytes
            }
            Value::Bytes(b) => {
                let len = b.len();
                let mut bytes = Vec::with_capacity(1 + 8 + len);
                bytes.push(B_BYTES);
                bytes.extend_from_slice(&(len as u64).to_be_bytes());
                bytes.extend_from_slice(b);
                bytes
            }
        }
    }

    /// Deserialize a Value from a byte slice.
    /// Returns the deserialized Value and the number of bytes consumed.
    pub fn deserialize(bytes: &[u8]) -> (Value, usize) {
        match bytes[0] {
            B_NULL => (Value::Null, 1),
            B_INT => {
                let mut int_bytes = [0; 8];
                int_bytes.copy_from_slice(&bytes[1..1 + 8]);
                (Value::Int(i64::from_be_bytes(int_bytes)), 1 + 8)
            }
            B_DECIMAL => {
                let mut decimal_bytes = [0; 16];
                decimal_bytes.copy_from_slice(&bytes[1..1 + 16]);
                (Value::Decimal(Decimal::deserialize(decimal_bytes)), 1 + 16)
            }
            B_STRING => {
                let length_bytes = &bytes[1..1 + 8];
                let length = u64::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
                (
                    Value::String(
                        String::from_utf8(bytes[1 + 8..1 + 8 + length].to_vec()).unwrap(),
                    ),
                    1 + 8 + length,
                )
            }
            B_BYTES => {
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
            Value::Null => Some(IndexableValue::Null),
            Value::Int(i) => Some(IndexableValue::Int(*i)),
            Value::Decimal(d) => Some(IndexableValue::Decimal(d.clone())),
            Value::String(s) => Some(IndexableValue::String(s.clone())),
            _ => None,
        }
    }
}

pub fn type_check(value: &Value, value_type: &Type) -> bool {
    match (value, value_type) {
        (
            Value::Int(_),
            Type {
                primitive: PrimitiveType::Int,
                ..
            },
        ) => true,
        (
            Value::Decimal(_),
            Type {
                primitive: PrimitiveType::Decimal,
                ..
            },
        ) => true,
        (
            Value::Bytes(_),
            Type {
                primitive: PrimitiveType::Bytes,
                ..
            },
        ) => true,
        (
            Value::String(_),
            Type {
                primitive: PrimitiveType::String,
                ..
            },
        ) => true,
        (Value::Null, Type { nullable: true, .. }) => true,
        _ => false,
    }
}

pub fn get_secondary_memtable_index_by_field<Field: Eq>(
    sks: &Vec<Field>,
    field: &Field,
) -> Option<usize> {
    sks.iter().position(|schema_field| schema_field == field)
}

pub fn is_file_same_as_path(file: &File, path: &PathBuf) -> DBResult<bool> {
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
pub fn set_active_segment(data_dir_path: &Path, segment_num: u16) -> DBResult<()> {
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
) -> DBResult<(u16, PathBuf)> {
    let current_greatest_num = greatest_segment_number(data_dir_path)?;
    let new_num = current_greatest_num + 1;

    let metadata_filename = format!("metadata.{}", new_num);
    let metadata_path = data_dir_path.join(metadata_filename);

    let mut metadata_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&metadata_path)?;

    let metadata_header = MetadataHeader {
        version: 1,
        uuid: *data_file_uuid,
    };

    metadata_file.write_all(&metadata_header.serialize())?;
    metadata_file.flush()?;

    let len = metadata_file.seek(io::SeekFrom::End(0))?;
    assert!(len >= METADATA_FILE_HEADER_SIZE as u64);
    assert_eq!((len - METADATA_FILE_HEADER_SIZE as u64) % 16, 0);

    Ok((new_num, metadata_path))
}

/// Parse the segment number from a metadata file path
pub fn parse_segment_number(metadata_path: &Path) -> DBResult<u16> {
    let filename = metadata_path
        .file_name()
        .expect("No filename in symlink")
        .to_str()
        .expect("Filename was not valid UTF-8");

    // parse number from format "metadata.1"
    let segment_number = filename
        .split('.')
        .last()
        .expect("Filename did not have a number")
        .parse::<u16>();

    segment_number.map_err(|_| {
        DBError::ValidationError("Failed to parse segment number from filename".to_owned())
    })
}

/// Get the number of the segment with the greatest ordinal.
/// This is the newest segment, i.e. the one that is pointed to by the `active` symlink.
/// If there are no segments yet, returns 0.
pub fn greatest_segment_number(data_dir_path: &Path) -> DBResult<u16> {
    let active_symlink = data_dir_path.join(ACTIVE_SYMLINK_FILENAME);

    if !fs::exists(&active_symlink)? {
        return Ok(0);
    }

    let segment_metadata_path = fs::read_link(&active_symlink)?;
    parse_segment_number(&segment_metadata_path)
}

/// Create a new segment data file and return its UUID.
/// A data file contains the segment data, tightly packed without separators.
/// An accompanying metadata file is required to interpret the data.
pub fn create_segment_data_file(data_dir_path: &Path) -> DBResult<(Uuid, PathBuf)> {
    let uuid = Uuid::new_v4();
    let new_segment_path = data_dir_path.join(uuid.to_string());
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_segment_path)?;

    Ok((uuid, new_segment_path))
}

/// Reads the metadata header from the metadata file.
/// Leaves the file seek head at the beginning of the records, after the header.
pub fn read_metadata_header(metadata_file: &mut fs::File) -> DBResult<MetadataHeader> {
    metadata_file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; METADATA_FILE_HEADER_SIZE];
    metadata_file.read_exact(&mut buf)?;

    let header = MetadataHeader::deserialize(&buf);
    Ok(header)
}

pub fn validate_metadata_header(header: &MetadataHeader) -> DBResult<()> {
    if header.version != 1 {
        return Err(DBError::ValidationError(
            "Unsupported metadata file version".to_owned(),
        ));
    }

    Ok(())
}

pub enum IsMetadatafileValidResult {
    Ok,
    ReplaceFile,
    TruncateToSize(u64),
}

pub fn is_metadata_file_valid(metadata_file: &mut fs::File) -> DBResult<IsMetadatafileValidResult> {
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
) -> DBResult<bool> {
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

pub struct OwnedBounds<T> {
    start: Bound<T>,
    end: Bound<T>,
}

impl<T> OwnedBounds<T> {
    pub fn new(start: Bound<T>, end: Bound<T>) -> Self {
        OwnedBounds { start, end }
    }
}

impl<T> RangeBounds<T> for OwnedBounds<T> {
    fn start_bound(&self) -> Bound<&T> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> Bound<&T> {
        self.end.as_ref()
    }
}

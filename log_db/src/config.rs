use super::*;

pub struct ConfigBuilder<R: Recordable> {
    data_dir: Option<String>,
    segment_size: Option<usize>,
    write_durability: Option<WriteDurability>,
    read_consistency: Option<ReadConsistency>,
    _marker: PhantomData<R>,
}

impl<R: Recordable> ConfigBuilder<R> {
    pub fn new() -> ConfigBuilder<R> {
        ConfigBuilder {
            data_dir: None,
            segment_size: None,
            write_durability: None,
            read_consistency: None,
            _marker: PhantomData,
        }
    }

    /// The directory where the database will store its data.
    pub fn data_dir(&mut self, data_dir: &str) -> &mut Self {
        self.data_dir = Some(data_dir.to_string());
        self
    }

    /// The maximum size of a segment file in bytes.
    /// Once a segment file reaches this size, it can be closed, rotated and compacted.
    /// Note that this is not a hard limit: if `db.do_maintenance_tasks()` is not called,
    /// the segment file may continue to grow.
    pub fn segment_size(&mut self, segment_size: usize) -> &mut Self {
        self.segment_size = Some(segment_size);
        self
    }

    /// The write durability policy for the database.
    /// This determines how writes are persisted to disk.
    /// The default is WriteDurability::Flush.
    pub fn write_durability(&mut self, write_durability: WriteDurability) -> &mut Self {
        self.write_durability = Some(write_durability);
        self
    }

    /// The read consistency policy for the database.
    /// This determines how recent writes are visible when reading.
    /// See individual `ReadConsistency` enum values for more information.
    /// The default is ReadConsistency::Strong.
    pub fn read_consistency(&mut self, read_consistency: ReadConsistency) -> &mut Self {
        self.read_consistency = Some(read_consistency);
        self
    }

    pub fn initialize(&self) -> DBResult<DB<R>> {
        let config = Config {
            fields: R::schema(),
            primary_key: R::primary_key(),
            secondary_keys: R::secondary_keys(),
            data_dir: self.data_dir.clone().unwrap_or("db_data".to_string()),
            segment_size: self.segment_size.unwrap_or(4 * 1024 * 1024), // 4MB
            write_durability: self
                .write_durability
                .clone()
                .unwrap_or(WriteDurability::Flush),
            read_consistency: self
                .read_consistency
                .clone()
                .unwrap_or(ReadConsistency::Strong),
        };

        DB::initialize(config)
    }
}

#[derive(Clone)]
pub struct Config<R: Recordable> {
    pub fields: Vec<(R::Field, ValueType)>,
    pub primary_key: R::Field,
    pub secondary_keys: Vec<R::Field>,
    pub data_dir: String,
    pub segment_size: usize,
    pub write_durability: WriteDurability,
    pub read_consistency: ReadConsistency,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Reads by client A are guaranteed to see writes by themselves and any writes by other clients B
    /// that were done before last index refresh. You must call `refresh_indexes()` manually to refresh indexes.
    Eventual,
    /// Reads by client A are guaranteed to see all writes. This is slower: all reads must first
    /// refresh indexes.
    Strong,
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

impl Display for WriteDurability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{:?}", self)?;
        Ok(())
    }
}

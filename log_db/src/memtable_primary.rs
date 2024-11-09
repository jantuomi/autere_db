use super::common::*;
use std::collections::BTreeMap;

pub struct PrimaryMemtable {
    /// Map of record locations indexed by key. Use the `LogKey` values to look up
    /// the actual records in the log files.
    records: BTreeMap<IndexableValue, LogKey>,
}

impl PrimaryMemtable {
    pub fn new() -> PrimaryMemtable {
        PrimaryMemtable {
            records: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: &IndexableValue, value: &LogKey) {
        self.records.insert(key.clone(), value.clone());
    }

    pub fn get(&self, key: &IndexableValue) -> Option<&LogKey> {
        self.records.get(key)
    }
}

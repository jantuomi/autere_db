use super::*;
use std::collections::BTreeMap;

pub struct PrimaryMemtable {
    /// Map of records indexed by key. Used as a shared heap of records
    /// for all secondary memtables also. Secondary memtables store an
    /// IndexableValue as their record value, which is used to get
    /// the actual record from the primary memtable `records` map.
    ///
    /// Note: it must be invariant that all memtables (primary and secondary)
    /// contain the same keys.
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

    pub fn remove(&mut self, key: &IndexableValue) -> Option<LogKey> {
        self.records.remove(key)
    }

    pub fn range<B: RangeBounds<IndexableValue>>(&self, range: B) -> Vec<LogKey> {
        self.records
            .range(range)
            .map(|(_, log_key)| log_key.clone())
            .collect()
    }
}

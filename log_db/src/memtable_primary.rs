use super::common::*;
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
}

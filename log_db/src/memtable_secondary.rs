use super::*;
use std::collections::BTreeMap;

pub struct SecondaryMemtable {
    /// Map of records indexed by key. The value is the set of primary key values of records
    /// that have the secondary key value. The actual `Record` objects are stored in the
    /// primary memtable, which acts as the shared heap.
    records: BTreeMap<IndexableValue, LogKeySet>,
}

impl SecondaryMemtable {
    pub fn new() -> SecondaryMemtable {
        SecondaryMemtable {
            records: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: &IndexableValue, value: &IndexableValue) {
        unimplemented!();
    }

    pub fn set_all(&mut self, key: &IndexableValue, values: &LogKeySet) {
        unimplemented!();
    }

    pub fn find_all(&self, key: &IndexableValue) -> &LogKeySet {
        unimplemented!();
    }
}

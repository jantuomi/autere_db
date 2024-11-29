use once_cell::sync::Lazy;

use super::*;
use std::collections::{BTreeMap, HashSet};

pub struct SecondaryMemtable {
    /// Map of records indexed by key. The value is the set of primary key values of records
    /// that have the secondary key value. The actual `Record` objects are stored in the
    /// primary memtable, which acts as the shared heap.
    records: BTreeMap<IndexableValue, LogKeySet>,
}

static EMPTY_SET: Lazy<HashSet<LogKey>> = Lazy::new(|| HashSet::new());

impl SecondaryMemtable {
    pub fn new() -> SecondaryMemtable {
        SecondaryMemtable {
            records: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: &IndexableValue, value: &LogKey) {
        match self.records.get_mut(key) {
            Some(set) => {
                set.insert(value.clone());
            }
            None => {
                self.records
                    .insert(key.clone(), LogKeySet::new_with_initial(&value));
            }
        };
    }

    pub fn find_all(&self, key: &IndexableValue) -> &HashSet<LogKey> {
        match self.records.get(key) {
            Some(set) => set.log_keys(),
            None => &EMPTY_SET,
        }
    }

    pub fn remove(&mut self, key: &IndexableValue) -> Option<LogKeySet> {
        self.records.remove(key)
    }
}

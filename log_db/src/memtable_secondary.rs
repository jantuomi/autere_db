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

    pub fn set(&mut self, key: IndexableValue, value: LogKey) {
        match self.records.get_mut(&key) {
            Some(set) => {
                set.insert(value);
            }
            None => {
                self.records.insert(key, LogKeySet::new_with_initial(value));
            }
        };
    }

    pub fn find_by(&self, key: &IndexableValue) -> &HashSet<LogKey> {
        match self.records.get(key) {
            Some(set) => set.log_keys(),
            None => &EMPTY_SET,
        }
    }

    // Remove a single log key associated with the given key. Returns `true`
    // if the log key existed and was removed, `false` otherwise.
    pub fn remove(&mut self, key: &IndexableValue, log_key: &LogKey) -> bool {
        let set = match self.records.get_mut(key) {
            Some(set) => set,
            None => return false,
        };
        if set.len() == 1 && set.contains(log_key) {
            self.records.remove(key);
            true
        } else {
            return match set.remove(log_key) {
                Ok(_) => true,
                Err(LogKeySetError::NotFoundError) => false,
                Err(e) => panic!("{:?}", e),
            };
        }
    }

    pub fn range<B: RangeBounds<IndexableValue>>(&self, range: B) -> Vec<&LogKey> {
        let mut keys = Vec::new();
        for (_, set) in self.records.range(range) {
            keys.extend(set.log_keys().iter());
        }
        keys
    }
}

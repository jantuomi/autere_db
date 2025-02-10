use once_cell::sync::Lazy;

use super::*;
use std::collections::{btree_map::Values, BTreeMap};

pub struct SecondaryMemtable {
    /// A 2-layer map of records indexed by SK => PK => LogKey.
    /// The PK information is required to tell two records apart.
    records: BTreeMap<IndexableValue, LogKeyMap>,
}

static EMPTY_MAP: Lazy<BTreeMap<IndexableValue, LogKey>> = Lazy::new(|| BTreeMap::new());

impl SecondaryMemtable {
    pub fn new() -> SecondaryMemtable {
        SecondaryMemtable {
            records: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, pk: IndexableValue, sk: IndexableValue, value: LogKey) {
        match self.records.get_mut(&sk) {
            Some(map) => {
                map.insert(pk, value);
            }
            None => {
                self.records
                    .insert(sk, LogKeyMap::new_with_initial(pk, value));
            }
        };
    }

    pub fn find_by(&self, key: &IndexableValue) -> Values<IndexableValue, LogKey> {
        match self.records.get(key) {
            Some(set) => set.log_keys(),
            None => EMPTY_MAP.values(),
        }
    }

    // Remove a single mapping associated with the given PK and SK. Returns `true`
    // if the log key existed and was removed, `false` otherwise.
    pub fn remove(&mut self, pk: &IndexableValue, sk: &IndexableValue) -> bool {
        let map = match self.records.get_mut(sk) {
            Some(set) => set,
            None => return false,
        };
        if map.len() == 1 && map.contains_pk(pk) {
            self.records.remove(sk);
            true
        } else {
            return match map.remove_pk(pk) {
                Ok(_) => true,
                Err(LogKeyMapError::NotFoundError) => false,
                Err(e) => panic!("{:?}", e),
            };
        }
    }

    pub fn range<B: RangeBounds<IndexableValue>>(&self, range: B) -> Vec<&LogKey> {
        let mut keys = Vec::new();
        for (_, map) in self.records.range(range) {
            keys.extend(map.log_keys());
        }
        keys
    }
}

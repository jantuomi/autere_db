use super::*;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::collections::HashSet;

pub struct SecondaryMemtable {
    /// Map of records indexed by key. Values are non-empty sets of `LogKey` values.
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
        debug!(
            "Inserting/updating record in secondary memtable with key {:?} = segment {} index {}",
            &key,
            &value.segment_num(),
            &value.index()
        );

        match self.records.get_mut(key) {
            Some(existing) => {
                debug!(
                    "Existing entry found with {} records in the set",
                    &existing.len()
                );
                existing.insert(value.clone());
            }
            None => {
                debug!("No existing entry found, creating one.");
                let set = LogKeySet::new_with_initial(&value);
                self.records.insert(key.clone(), set);
            }
        }
    }

    pub fn set_all(&mut self, key: &IndexableValue, values: &[LogKey]) {
        debug!(
            "Replacing set of records in secondary memtable with key {:?} ({} values)",
            &key,
            &values.len(),
        );

        let set = LogKeySet::from_slice(values);
        self.records.insert(key.clone(), set);
    }

    pub fn find_all(&mut self, key: &IndexableValue) -> &HashSet<LogKey> {
        match self.records.get(key) {
            Some(set) => set.log_keys(),
            None => &EMPTY_SET,
        }
    }
}

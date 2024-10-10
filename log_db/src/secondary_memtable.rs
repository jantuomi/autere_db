use super::*;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;

pub struct SecondaryMemtable<Field: Eq + Clone + Debug> {
    pub field: Field,
    field_index: usize,
    primary_key_index: usize,

    /// Map of records indexed by key. The value is the set of primary key values of records
    /// that have the secondary key value. The actual `Record` objects are stored in the
    /// primary memtable, which acts as the shared heap.
    pub records: BTreeMap<IndexableValue, HashSet<IndexableValue>>,
}

impl<Field: Eq + Clone + Debug> SecondaryMemtable<Field> {
    pub fn new(
        field_schema: &Vec<(Field, RecordField)>,
        field: &Field,
        primary_key_index: usize,
    ) -> SecondaryMemtable<Field> {
        let field_index = field_schema
            .iter()
            .position(|(f, _)| f == field)
            .expect("Field not found in schema");

        SecondaryMemtable {
            field: field.clone(),
            field_index,
            primary_key_index,
            records: BTreeMap::new(),
        }
    }

    pub fn set(&mut self, key: &IndexableValue, value: &IndexableValue) {
        debug!(
            "Inserting/updating record in secondary memtable with key {:?} = {:?}",
            &key, &value,
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
                let mut set = HashSet::with_capacity(1);
                set.insert(value.clone());
                self.records.insert(key.clone(), set);
            }
        }
    }

    pub fn set_all(&mut self, key: &IndexableValue, values: &[IndexableValue]) {
        debug!(
            "Replacing set of records in secondary memtable with key {:?} ({} values)",
            &key,
            &values.len(),
        );

        let mut set = HashSet::with_capacity(values.len());
        values.iter().for_each(|value| {
            set.insert(value.clone());
        });

        self.records.insert(key.clone(), set);
    }

    pub fn find_all(
        &mut self,
        primary_memtable: &PrimaryMemtable,
        key: &IndexableValue,
    ) -> Vec<Record> {
        match self.records.get(key) {
            None => vec![],
            Some(set) => set
                .iter()
                .map(|key| {
                    primary_memtable
                        .get_without_update(key)
                        .expect("Record not found")
                        .clone()
                })
                .collect(),
        }
    }

    pub fn remove(&mut self, record: &Record) {
        let key = record.values[self.field_index]
            .as_indexable()
            .expect("Field is not indexable");

        let primary_key = record.values[self.primary_key_index]
            .as_indexable()
            .expect("Primary key is not indexable");

        match self.records.get_mut(&key) {
            Some(set) => {
                set.remove(&primary_key);
            }
            None => {
                panic!("Record not found in secondary memtable");
            }
        }
    }
}

use super::common::*;
use priority_queue::PriorityQueue;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

struct UniqueRecord {
    /// The value of the record's primary key field
    primary_value: IndexableValue,
    /// The record itself.
    record: Record,
}

impl PartialEq for UniqueRecord {
    fn eq(&self, other: &UniqueRecord) -> bool {
        self.primary_value == other.primary_value
    }
}

impl Eq for UniqueRecord {}

impl Hash for UniqueRecord {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.primary_value.hash(state)
    }
}

pub struct SecondaryMemtable<Field: Eq + Clone + Debug> {
    pub field: Field,
    pub primary_field_index: usize,
    capacity: usize,
    /// Running counter of memtable operations, used as priority
    /// in evict_queue.
    n_operations: u64,
    records: BTreeMap<IndexableValue, HashSet<UniqueRecord>>,
    /// A max heap priority queue of keys. Note: n_operations must
    /// be negated upon append to evict oldest values first.
    evict_queue: PriorityQueue<IndexableValue, i64>,
    evict_policy: MemtableEvictPolicy,
}

impl<Field: Eq + Clone + Debug> SecondaryMemtable<Field> {
    pub fn new(
        field: &Field,
        primary_field_index: usize,
        capacity: usize,
        evict_policy: MemtableEvictPolicy,
    ) -> SecondaryMemtable<Field> {
        SecondaryMemtable {
            field: field.clone(),
            primary_field_index,
            capacity,
            n_operations: 0,
            records: BTreeMap::new(),
            evict_queue: PriorityQueue::new(),
            evict_policy,
        }
    }

    pub fn set(&mut self, key: &IndexableValue, value: &Record) {
        if self.capacity == 0 {
            return;
        }

        debug!(
            "Inserting/updating record in secondary memtable with key {:?} = {:?}",
            &key, &value,
        );

        if self.records.len() >= self.capacity {
            let (evict_key, _prio) = self.evict_queue.pop().expect("Evict queue was empty");
            self.records.remove(&evict_key);
        }

        let unique_record = UniqueRecord {
            primary_value: value.values[self.primary_field_index]
                .as_indexable()
                .expect("Value at primary field index was not indexable"),
            record: value.clone(),
        };

        match self.records.get_mut(key) {
            Some(existing) => {
                existing.insert(unique_record);
            }
            None => {
                let mut set = HashSet::with_capacity(1);
                set.insert(unique_record);
                self.records.insert(key.clone(), set);
            }
        }

        if self.evict_policy == MemtableEvictPolicy::LeastWritten
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }
    }

    pub fn set_all(&mut self, key: &IndexableValue, values: &[Record]) {
        if self.capacity == 0 {
            return;
        }

        debug!(
            "Replacing set of records in secondary memtable with key {:?} ({} values)",
            &key,
            &values.len(),
        );

        if self.records.len() >= self.capacity {
            let (evict_key, _prio) = self.evict_queue.pop().expect("Evict queue was empty");
            self.records.remove(&evict_key);
        }

        let mut set = HashSet::with_capacity(values.len());
        values.iter().for_each(|value| {
            let unique_record = UniqueRecord {
                primary_value: value.values[self.primary_field_index]
                    .as_indexable()
                    .expect("Value at primary field index was not indexable"),
                record: value.clone(),
            };
            set.insert(unique_record);
        });

        self.records.insert(key.clone(), set);

        if self.evict_policy == MemtableEvictPolicy::LeastWritten
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }
    }

    pub fn find_all(&mut self, key: &IndexableValue) -> Vec<&Record> {
        if self.evict_policy == MemtableEvictPolicy::LeastRead
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }

        match self.records.get(key) {
            None => vec![],
            Some(set) => set
                .iter()
                .map(|unique_record| &unique_record.record)
                .collect(),
        }
    }

    fn set_priority(&mut self, key: &IndexableValue) {
        let priority = self.get_and_increment_current_priority();
        match self.evict_queue.get(key) {
            Some(_) => {
                self.evict_queue.change_priority(key, priority);
            }
            None => {
                self.evict_queue.push(key.clone(), priority);
            }
        }
    }

    fn get_and_increment_current_priority(&mut self) -> i64 {
        let ret = -(self.n_operations as i64);
        self.n_operations += 1;
        ret
    }
}

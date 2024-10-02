use super::common::*;
use priority_queue::PriorityQueue;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub struct PrimaryMemtable<Field: Eq + Clone + Debug> {
    pub field: Field,
    capacity: usize,
    /// Running counter of memtable operations, used as priority
    /// in evict_queue.
    n_operations: u64,
    records: BTreeMap<IndexableValue, Record>,
    /// A max heap priority queue of keys. Note: n_operations must
    /// be negated upon append to evict oldest values first.
    evict_queue: PriorityQueue<IndexableValue, i64>,
    evict_policy: MemtableEvictPolicy,
}

impl<Field: Eq + Clone + Debug> PrimaryMemtable<Field> {
    pub fn new(
        field: &Field,
        capacity: usize,
        evict_policy: MemtableEvictPolicy,
    ) -> PrimaryMemtable<Field> {
        PrimaryMemtable {
            field: field.clone(),
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
            "Inserting/updating record in primary memtable with key {:?} = {:?}",
            &key, &value,
        );

        if self.records.len() >= self.capacity {
            let (evict_key, _prio) = self.evict_queue.pop().expect("Evict queue was empty");
            self.records.remove(&evict_key);
        }

        self.records.insert(key.clone(), value.clone());

        if self.evict_policy == MemtableEvictPolicy::LeastWritten
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }
    }

    pub fn get(&mut self, key: &IndexableValue) -> Option<&Record> {
        if self.evict_policy == MemtableEvictPolicy::LeastRead
            || self.evict_policy == MemtableEvictPolicy::LeastReadOrWritten
        {
            self.set_priority(&key);
        }

        self.records.get(key)
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

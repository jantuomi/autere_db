use super::common::*;
use priority_queue::PriorityQueue;
use std::collections::BTreeMap;

pub struct PrimaryMemtable {
    /// Maximum number of records that can be stored in the memtable
    /// before evicting the oldest records. The oldest record is
    /// determined by the `evict_policy`.
    capacity: usize,
    /// Map of records indexed by key. Used as a shared heap of records
    /// for all secondary memtables also. Secondary memtables store an
    /// IndexableValue as their record value, which is used to get
    /// the actual record from the primary memtable `records` map.
    ///
    /// Note: it must be invariant that all memtables (primary and secondary)
    /// contain the same keys.
    records: BTreeMap<IndexableValue, Record>,
    /// A max heap priority queue of keys. The record with least priority is evicted
    /// from the primary memtable and any secondary memtables that reference it, when
    /// the memtable reaches capacity.
    ///
    /// Note: n_operations must be negated upon append to evict oldest values first.
    evict_queue: PriorityQueue<IndexableValue, i64>,
    /// Policy for prioritizing records for eviction.
    evict_policy: MemtableEvictPolicy,
    /// Running counter of memtable operations, used as priority
    /// in evict_queue.
    n_operations: u64,
}

impl PrimaryMemtable {
    pub fn new(capacity: usize, evict_policy: MemtableEvictPolicy) -> PrimaryMemtable {
        PrimaryMemtable {
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

    pub fn get_without_update(&self, key: &IndexableValue) -> Option<&Record> {
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

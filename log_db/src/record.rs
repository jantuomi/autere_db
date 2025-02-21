use super::*;

pub struct Record {
    values: Vec<Value>,
}

impl Record {
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

impl IntoIterator for Record {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl From<Vec<Value>> for Record {
    fn from(values: Vec<Value>) -> Self {
        Record { values }
    }
}

impl From<Record> for Vec<Value> {
    fn from(record: Record) -> Self {
        record.values
    }
}

impl From<Row> for Record {
    fn from(row: Row) -> Self {
        Record { values: row.values }
    }
}

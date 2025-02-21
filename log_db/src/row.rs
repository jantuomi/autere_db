use super::*;

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
    pub tombstone: bool,
}

impl Row {
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        if self.tombstone {
            bytes.extend(&[B_TOMBSTONE]);
        } else {
            bytes.extend(&[B_LIVE]);
        }

        for value in &self.values {
            bytes.extend(value.serialize());
        }
        bytes
    }

    pub fn deserialize(bytes: &[u8]) -> Row {
        assert!(bytes.len() > 0);

        let mut values = Vec::new();

        let tombstone = bytes[0] == B_TOMBSTONE;

        let mut start = 1;
        while start < bytes.len() {
            let (rv, consumed) = Value::deserialize(&bytes[start..]);
            values.push(rv);
            start += consumed;
        }
        Row { values, tombstone }
    }

    pub fn from(values: &[Value]) -> Row {
        Row {
            values: values.to_vec(),
            tombstone: false,
        }
    }

    pub fn at(&self, index: usize) -> &Value {
        &self.values[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialize_deserialize() {
        let record = Row {
            values: vec![
                Value::Int(1),
                Value::String("hello".to_string()),
                Value::Bytes(vec![0, 1, 2, 3]),
            ],
            tombstone: true,
        };

        let serialized = record.serialize();
        let deserialized = Row::deserialize(&serialized);
        let reserialized = deserialized.serialize();

        assert_eq!(serialized.len(), reserialized.len());
        assert_eq!(record.values, deserialized.values);
    }
}

#[derive(Clone, Debug)]
pub enum TxEntry {
    Upsert { record: Row },
    Delete { record: Row },
}

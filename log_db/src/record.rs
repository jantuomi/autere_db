use super::*;

#[derive(Debug, Clone)]
pub struct Record {
    pub values: Vec<Value>,
    pub tombstone: bool,
}

impl Record {
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

    pub fn deserialize(bytes: &[u8]) -> Record {
        assert!(bytes.len() > 0);

        let mut values = Vec::new();

        let tombstone = bytes[0] == B_TOMBSTONE;

        let mut start = 1;
        while start < bytes.len() {
            let (rv, consumed) = Value::deserialize(&bytes[start..]);
            values.push(rv);
            start += consumed;
        }
        Record { values, tombstone }
    }

    pub fn from(values: &[Value]) -> Record {
        Record {
            values: values.to_vec(),
            tombstone: false,
        }
    }

    pub fn at(&self, index: usize) -> &Value {
        &self.values[index]
    }

    pub fn validate<Field: Eq>(&self, schema: &Vec<(Field, Type)>) -> DBResult<()> {
        // Validate the record length
        if self.values.len() != schema.len() {
            return Err(DBError::ValidationError(format!(
                "Record has an incorrect number of fields: {}, expected {}",
                self.values.len(),
                schema.len()
            )));
        }

        // Validate that record fields match schema types
        for (i, (_, field)) in schema.iter().enumerate() {
            match (&self.values[i], field) {
                (
                    Value::Null,
                    Type {
                        nullable: true,
                        primitive: _,
                    },
                ) => {}
                (
                    Value::Int(_),
                    Type {
                        primitive: PrimitiveType::Int,
                        ..
                    },
                ) => {}
                (
                    Value::String(_),
                    Type {
                        primitive: PrimitiveType::String,
                        ..
                    },
                ) => {}
                (
                    Value::Bytes(_),
                    Type {
                        primitive: PrimitiveType::Bytes,
                        ..
                    },
                ) => {}
                _ => {
                    return Err(DBError::ValidationError(format!(
                        "Record field {} has incorrect type: {:?}, expected {:?}",
                        &i, &self.values[i], &field.primitive
                    )));
                }
            }
        }
        Ok(())
    }
}

/// A trait that describes how to convert a data structure into a database record and vice versa.
pub trait Recordable {
    /// The field type of the data structure implementing the `Recordable` trait.
    type Field: Eq + Clone + Debug;
    /// Define the schema of the instance implementing the `Recordable` trait.
    fn schema() -> Vec<(Self::Field, Type)>;
    /// Define the primary key of the instance implementing the `Recordable` trait.
    fn primary_key() -> Self::Field;
    /// Define the secondary keys of the instance implementing the `Recordable` trait.
    fn secondary_keys() -> Vec<Self::Field> {
        Vec::new()
    }

    /// Convert the data structure implementing the `Recordable` trait into a vector of database values.
    fn into_record(self) -> Vec<Value>;
    /// Convert a vector of database values into the data structure implementing the `Recordable` trait.
    fn from_record(record: Vec<Value>) -> Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialize_deserialize() {
        let record = Record {
            values: vec![
                Value::Int(1),
                Value::String("hello".to_string()),
                Value::Bytes(vec![0, 1, 2, 3]),
            ],
            tombstone: true,
        };

        let serialized = record.serialize();
        let deserialized = Record::deserialize(&serialized);
        let reserialized = deserialized.serialize();

        assert_eq!(serialized.len(), reserialized.len());
        assert_eq!(record.values, deserialized.values);
    }
}

#[derive(Clone, Debug)]
pub enum TxEntry {
    Upsert { record: Record },
    Delete { record: Record },
}

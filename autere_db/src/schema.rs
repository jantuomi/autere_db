use super::*;

pub struct Schema {
    pub fields: Vec<String>,
    pub primary_key: String,
    pub secondary_keys: Vec<String>,
}

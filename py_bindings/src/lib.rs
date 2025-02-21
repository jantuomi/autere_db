use std::str::FromStr;
use std::usize;

use log_db::{self, OwnedBounds, QueryParams, Record, DEFAULT_QUERY_PARAMS};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use rust_decimal::Decimal;
use std::ops::Bound as StdBound;

type PyRecord = Vec<Value>;
type PyField = String;

pub const WRITE_DURABILITY_FLUSH: u8 = 0;
pub const WRITE_DURABILITY_FLUSH_SYNC: u8 = 1;

pub const READ_CONSISTENCY_EVENTUAL: u8 = 0;
pub const READ_CONSISTENCY_STRONG: u8 = 1;

#[pyclass]
struct Config {
    data_dir: Option<PyField>,
    segment_size: Option<usize>,
    write_durability: Option<log_db::WriteDurability>,
    read_consistency: Option<log_db::ReadConsistency>,
    fields: Option<Vec<PyField>>,
    primary_key: Option<PyField>,
    secondary_keys: Option<Vec<PyField>>,
}

#[pymethods]
impl Config {
    pub fn data_dir<'a>(
        mut slf: PyRefMut<'a, Self>,
        data_dir: &str,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.data_dir = Some(data_dir.into());
        Ok(slf)
    }

    pub fn segment_size<'a>(
        mut slf: PyRefMut<'a, Self>,
        segment_size: usize,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.segment_size = Some(segment_size);
        Ok(slf)
    }

    pub fn write_durability<'a>(
        mut slf: PyRefMut<'a, Self>,
        write_durability: u8,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.write_durability = Some(match write_durability {
            WRITE_DURABILITY_FLUSH => log_db::WriteDurability::Flush,
            WRITE_DURABILITY_FLUSH_SYNC => log_db::WriteDurability::FlushSync,
            _ => {
                return Err(PyException::new_err(format!(
                    "Invalid write_durability value: {}",
                    write_durability,
                )))
            }
        });
        Ok(slf)
    }

    pub fn read_consistency<'a>(
        mut slf: PyRefMut<'a, Self>,
        read_consistency: u8,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.read_consistency = Some(match read_consistency {
            READ_CONSISTENCY_EVENTUAL => log_db::ReadConsistency::Eventual,
            READ_CONSISTENCY_STRONG => log_db::ReadConsistency::Strong,
            _ => {
                return Err(PyException::new_err(format!(
                    "Invalid read_consistency value: {}",
                    read_consistency,
                )))
            }
        });
        Ok(slf)
    }

    pub fn fields<'a>(
        mut slf: PyRefMut<'a, Self>,
        fields: Vec<PyField>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.fields = Some(fields);
        Ok(slf)
    }

    pub fn primary_key<'a>(
        mut slf: PyRefMut<'a, Self>,
        primary_key: PyField,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.primary_key = Some(primary_key);
        Ok(slf)
    }

    pub fn secondary_keys<'a>(
        mut slf: PyRefMut<'a, Self>,
        secondary_keys: Vec<PyField>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.secondary_keys = Some(secondary_keys);
        Ok(slf)
    }

    pub fn initialize(&self) -> PyResult<DB> {
        let mut config = log_db::DB::configure();
        if self.data_dir.is_some() {
            config = config.data_dir(&self.data_dir.as_ref().unwrap().to_string());
        }
        if self.segment_size.is_some() {
            config = config.segment_size(self.segment_size.unwrap());
        }
        if self.write_durability.is_some() {
            let tmp = self.write_durability.as_ref().unwrap();
            config = config.write_durability(tmp.clone());
        }
        if self.read_consistency.is_some() {
            let tmp = self.read_consistency.as_ref().unwrap();
            config = config.read_consistency(tmp.clone());
        }
        if self.fields.is_some() {
            let fields = self
                .fields
                .as_ref()
                .unwrap()
                .iter()
                .map(|name| name.clone())
                .collect();
            config = config.fields(fields);
        }
        if self.primary_key.is_some() {
            config = config.primary_key(self.primary_key.as_ref().unwrap().to_string());
        }
        if self.secondary_keys.is_some() {
            config = config.secondary_keys(self.secondary_keys.as_ref().unwrap().clone());
        }

        let db = config
            .initialize()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(DB { db })
    }
}

fn py_from_record(record: Record) -> Vec<Value> {
    record
        .into_iter()
        .map(|value| Value {
            record_value: value,
        })
        .collect::<Vec<Value>>()
        .into()
}

fn py_into_record(record: Vec<Value>) -> Record {
    record
        .into_iter()
        .map(|value| value.record_value)
        .collect::<Vec<log_db::Value>>()
        .into()
}

const VALUE_INT: u8 = 0;
const VALUE_DECIMAL: u8 = 1;
const VALUE_STRING: u8 = 2;
const VALUE_BYTES: u8 = 3;
const VALUE_NULL: u8 = 4;

#[pyclass]
#[derive(Clone, PartialEq, Eq)]
pub struct Value {
    record_value: log_db::Value,
}

#[pymethods]
impl Value {
    fn __repr__(&self) -> String {
        match &self.record_value {
            log_db::Value::Int(value) => format!("Value.int({})", value),
            log_db::Value::Decimal(value) => format!("Value.decimal({})", value),
            log_db::Value::String(value) => {
                format!("Value.string(\"{}\")", value.replace("\"", "\\\""))
            }
            log_db::Value::Bytes(value) => format!("Value.bytes({:?})", value),
            log_db::Value::Null => "Value.null()".to_string(),
        }
    }

    #[staticmethod]
    fn int(value: i64) -> Self {
        Value {
            record_value: log_db::Value::Int(value),
        }
    }

    #[staticmethod]
    fn decimal(value: String) -> Self {
        Value {
            record_value: log_db::Value::Decimal(
                Decimal::from_str(&value).expect(&format!("Invalid Decimal: {}", value)),
            ),
        }
    }

    #[staticmethod]
    fn string(value: String) -> Self {
        Value {
            record_value: log_db::Value::String(value),
        }
    }

    #[staticmethod]
    fn bytes(value: &[u8]) -> Self {
        Value {
            record_value: log_db::Value::Bytes(value.to_vec()),
        }
    }

    #[staticmethod]
    fn null() -> Self {
        Value {
            record_value: log_db::Value::Null,
        }
    }

    pub fn kind(&self) -> u8 {
        match &self.record_value {
            log_db::Value::Int(_) => VALUE_INT,
            log_db::Value::Decimal(_) => VALUE_DECIMAL,
            log_db::Value::String(_) => VALUE_STRING,
            log_db::Value::Bytes(_) => VALUE_BYTES,
            log_db::Value::Null => VALUE_NULL,
        }
    }

    pub fn as_int(&self) -> PyResult<i64> {
        match &self.record_value {
            log_db::Value::Int(value) => Ok(*value),
            _ => Err(PyException::new_err("Value is not an Int")),
        }
    }

    pub fn as_decimal(&self) -> PyResult<String> {
        match &self.record_value {
            log_db::Value::Decimal(value) => Ok(value.to_string()),
            _ => Err(PyException::new_err("Value is not a Decimal")),
        }
    }

    pub fn as_string(&self) -> PyResult<String> {
        match &self.record_value {
            log_db::Value::String(value) => Ok(value.clone()),
            _ => Err(PyException::new_err("Value is not a String")),
        }
    }

    pub fn as_bytes(&self) -> PyResult<Vec<u8>> {
        match &self.record_value {
            log_db::Value::Bytes(value) => Ok(value.clone()),
            _ => Err(PyException::new_err("Value is not Bytes")),
        }
    }

    pub fn as_null(&self) -> PyResult<()> {
        match &self.record_value {
            log_db::Value::Null => Ok(()),
            _ => Err(PyException::new_err("Value is not Null")),
        }
    }
}

#[pyclass]
struct DB {
    db: log_db::DB,
}

#[pymethods]
impl DB {
    #[staticmethod]
    pub fn configure() -> Config {
        Config {
            data_dir: None,
            segment_size: None,
            write_durability: None,
            read_consistency: None,
            fields: None,
            primary_key: None,
            secondary_keys: None,
        }
    }

    pub fn upsert(&mut self, record: PyRecord) -> PyResult<()> {
        let record: Record = py_into_record(record);
        self.db
            .upsert(record)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn get(&mut self, key: Value) -> PyResult<Option<PyRecord>> {
        let recs = self
            .db
            .get(&key.record_value)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs.map(|rec| py_from_record(rec)))
    }

    #[pyo3(signature = (field, key, offset = DEFAULT_QUERY_PARAMS.offset, limit = DEFAULT_QUERY_PARAMS.limit))]
    pub fn find_by(
        &mut self,
        field: PyField,
        key: &Value,
        offset: usize,
        limit: usize,
    ) -> PyResult<Vec<PyRecord>> {
        let params = QueryParams { offset, limit };
        let recs = self
            .db
            .find_by_with_params(&field, &key.record_value, &params)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs.into_iter().map(|rec| py_from_record(rec)).collect())
    }

    #[pyo3(signature = (field, keys, offset = DEFAULT_QUERY_PARAMS.offset, limit = DEFAULT_QUERY_PARAMS.limit))]
    pub fn batch_find_by(
        &mut self,
        field: PyField,
        keys: Vec<Value>,
        offset: usize,
        limit: usize,
    ) -> PyResult<Vec<(usize, PyRecord)>> {
        let params = QueryParams { offset, limit };
        let keys: Vec<log_db::Value> = keys.into_iter().map(|key| key.record_value).collect();
        let recs = self
            .db
            .batch_find_by_with_params(&field, &keys, &params)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs
            .into_iter()
            .map(|(idx, rec)| (idx, py_from_record(rec)))
            .collect())
    }

    #[pyo3(signature = (field, start, end, offset = DEFAULT_QUERY_PARAMS.offset, limit = DEFAULT_QUERY_PARAMS.limit))]
    pub fn range_by(
        &mut self,
        field: PyField,
        start: &PyRangeBound,
        end: &PyRangeBound,
        offset: usize,
        limit: usize,
    ) -> PyResult<Vec<PyRecord>> {
        let params = QueryParams { offset, limit };
        let range = OwnedBounds::new(
            match start {
                PyRangeBound::Unbounded() => StdBound::Unbounded,
                PyRangeBound::Included(value) => StdBound::Included(value.record_value.clone()),
                PyRangeBound::Excluded(value) => StdBound::Excluded(value.record_value.clone()),
            },
            match end {
                PyRangeBound::Unbounded() => StdBound::Unbounded,
                PyRangeBound::Included(value) => StdBound::Included(value.record_value.clone()),
                PyRangeBound::Excluded(value) => StdBound::Excluded(value.record_value.clone()),
            },
        );

        let recs = self
            .db
            .range_by_with_params(&field, range, &params)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs.into_iter().map(|rec| py_from_record(rec)).collect())
    }

    pub fn delete(&mut self, key: &Value) -> PyResult<Option<PyRecord>> {
        let recs = self
            .db
            .delete(&key.record_value)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs.map(|rec| py_from_record(rec)))
    }

    pub fn delete_by(&mut self, field: PyField, key: &Value) -> PyResult<Vec<PyRecord>> {
        let recs = self
            .db
            .delete_by(&field, &key.record_value)
            .map_err(|e| PyException::new_err(e.to_string()))?;

        Ok(recs.into_iter().map(|rec| py_from_record(rec)).collect())
    }

    pub fn tx_begin(&mut self) -> PyResult<()> {
        self.db
            .tx_begin()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn tx_commit(&mut self) -> PyResult<()> {
        self.db
            .tx_commit()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn tx_rollback(&mut self) -> PyResult<()> {
        self.db
            .tx_rollback()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn do_maintenance_tasks(&mut self) -> PyResult<()> {
        self.db
            .do_maintenance_tasks()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }

    pub fn refresh_indexes(&mut self) -> PyResult<()> {
        self.db
            .refresh_indexes()
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(())
    }
}

#[pyclass(name = "Bound", eq)]
#[derive(Clone, PartialEq, Eq)]
pub enum PyRangeBound {
    Unbounded(),
    Included(Value),
    Excluded(Value),
}

#[pymethods]
impl PyRangeBound {
    #[staticmethod]
    pub fn unbounded() -> Self {
        PyRangeBound::Unbounded()
    }

    #[staticmethod]
    pub fn included(value: Value) -> Self {
        PyRangeBound::Included(value)
    }

    #[staticmethod]
    pub fn excluded(value: Value) -> Self {
        PyRangeBound::Excluded(value)
    }
}

#[pymodule(name = "log_db")]
fn log_db_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<DB>()?;
    m.add_class::<Value>()?;
    m.add_class::<PyRangeBound>()?;

    m.add("WRITE_DURABILITY_FLUSH", WRITE_DURABILITY_FLUSH)?;
    m.add("WRITE_DURABILITY_FLUSH_SYNC", WRITE_DURABILITY_FLUSH_SYNC)?;

    m.add("READ_CONSISTENCY_EVENTUAL", READ_CONSISTENCY_EVENTUAL)?;
    m.add("READ_CONSISTENCY_STRONG", READ_CONSISTENCY_STRONG)?;

    m.add("VALUE_INT", VALUE_INT)?;
    m.add("VALUE_DECIMAL", VALUE_DECIMAL)?;
    m.add("VALUE_STRING", VALUE_STRING)?;
    m.add("VALUE_BYTES", VALUE_BYTES)?;
    m.add("VALUE_NULL", VALUE_NULL)?;

    Ok(())
}

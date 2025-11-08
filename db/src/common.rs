use crate::errors::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::Text(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Real(f) => Some(f.to_string()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Integer(i) => Some(*i != 0),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }
}

/// 表示数据库中的一行数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub data: HashMap<String, Value>,
}

impl Row {
    pub fn new() -> Self {
        Row {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: String, value: Value) {
        self.data.insert(column, value);
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.data.get(column)
    }

    pub fn get_string(&self, column: &str) -> Option<String> {
        self.get(column)?.as_string()
    }

    pub fn get_i64(&self, column: &str) -> Option<i64> {
        self.get(column)?.as_i64()
    }

    pub fn get_f64(&self, column: &str) -> Option<f64> {
        self.get(column)?.as_f64()
    }

    pub fn get_bool(&self, column: &str) -> Option<bool> {
        self.get(column)?.as_bool()
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

/// 查询结果集
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// 列名列表
    pub columns: Vec<String>,
    /// 数据行列表
    pub rows: Vec<Row>,
    /// 受影响的行数（用于 INSERT/UPDATE/DELETE）
    pub affected_rows: usize,
}

impl QueryResult {
    pub fn new() -> Self {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
        }
    }

    pub fn with_columns(columns: Vec<String>) -> Self {
        QueryResult {
            columns,
            rows: Vec::new(),
            affected_rows: 0,
        }
    }

    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn into_struct<T>(&self) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut results = Vec::new();
        for row in &self.rows {
            // 将 HashMap<String, Value> 转换为 HashMap<String, serde_json::Value>
            let mut map = serde_json::Map::new();
            for (key, value) in &row.data {
                let json_val = match value {
                    Value::Null => serde_json::Value::Null,
                    Value::Integer(i) => serde_json::Value::Number((*i).into()),
                    Value::Real(f) => serde_json::Number::from_f64(*f)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Value::Text(s) => serde_json::Value::String(s.clone()),
                    Value::Blob(b) => serde_json::Value::Array(
                        b.iter()
                            .map(|&byte| serde_json::Value::Number(byte.into()))
                            .collect(),
                    ),
                };
                map.insert(key.clone(), json_val);
            }
            let typed_row: T = serde_json::from_value(serde_json::Value::Object(map))?;
            results.push(typed_row);
        }
        Ok(results)
    }

    pub fn first(&self) -> Option<&Row> {
        self.rows.first()
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        Self::new()
    }
}

use crate::errors::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 通用值类型，表示数据库中的任意值
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    /// 尝试转换为字符串
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::Text(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            Value::Real(f) => Some(f.to_string()),
            _ => None,
        }
    }

    /// 尝试转换为 i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// 尝试转换为 f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Real(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// 尝试转换为布尔值
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
    /// 创建空行
    pub fn new() -> Self {
        Row {
            data: HashMap::new(),
        }
    }

    /// 插入列值
    pub fn insert(&mut self, column: String, value: Value) {
        self.data.insert(column, value);
    }

    /// 获取列值
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.data.get(column)
    }

    /// 获取字符串值
    pub fn get_string(&self, column: &str) -> Option<String> {
        self.get(column)?.as_string()
    }

    /// 获取整数值
    pub fn get_i64(&self, column: &str) -> Option<i64> {
        self.get(column)?.as_i64()
    }

    /// 获取浮点数值
    pub fn get_f64(&self, column: &str) -> Option<f64> {
        self.get(column)?.as_f64()
    }

    /// 获取布尔值
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
    /// 创建空结果集
    pub fn new() -> Self {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
        }
    }

    /// 创建带列名的结果集
    pub fn with_columns(columns: Vec<String>) -> Self {
        QueryResult {
            columns,
            rows: Vec::new(),
            affected_rows: 0,
        }
    }

    /// 添加行
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// 获取行数
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// 转换为强类型结构
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

    /// 获取第一行
    pub fn first(&self) -> Option<&Row> {
        self.rows.first()
    }
}

impl Default for QueryResult {
    fn default() -> Self {
        Self::new()
    }
}

/// 数据库操作 trait - 并发安全
///
/// 所有实现此 trait 的类型都必须是线程安全的（Send + Sync）
pub trait DB: Send + Sync {
    /// 执行查询操作，返回结果集
    ///
    /// # 参数
    /// * `query` - SQL 查询语句
    /// * `params` - 查询参数
    fn execute_query(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<QueryResult>;

    /// 执行更新操作（INSERT/UPDATE/DELETE），返回受影响的行数
    ///
    /// # 参数
    /// * `query` - SQL 语句
    /// * `params` - 参数
    fn execute_update(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<usize>;

    /// 开始事务
    fn begin_transaction(&self) -> Result<()>;

    /// 提交事务
    fn commit_transaction(&self) -> Result<()>;

    /// 回滚事务
    fn rollback_transaction(&self) -> Result<()>;

    /// 在事务中执行操作
    ///
    /// # 参数
    /// * `operation` - 需要在事务中执行的操作
    fn with_transaction<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>;

    /// 简单查询（无参数）
    fn query(&self, query: &str) -> Result<QueryResult> {
        self.execute_query(query, &[])
    }

    /// 查询单行
    fn query_one(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<Option<Row>> {
        let result = self.execute_query(query, params)?;
        Ok(result.rows.into_iter().next())
    }

    /// 查询并转换为强类型
    fn query_typed<T>(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let result = self.execute_query(query, params)?;
        result.into_struct()
    }

    /// 插入数据并返回最后插入的行 ID
    fn insert(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<i64>;

    /// 检查表是否存在
    fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// 获取数据库版本或其他元信息
    fn get_metadata(&self, key: &str) -> Result<Option<String>>;
}

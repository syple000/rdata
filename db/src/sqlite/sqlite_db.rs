use crate::common::{QueryResult, Row, Value};
use crate::errors::{DBError, Result};
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

pub struct SQLiteDB {
    connection: Arc<Mutex<RefCell<Connection>>>,
}

impl SQLiteDB {
    pub fn new(db_path: &str) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        let _ = conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()));
        let _ = conn.execute("PRAGMA synchronous = NORMAL", []);
        let _ = conn.execute("PRAGMA cache_size = 1000000", []);
        let _ = conn.execute("PRAGMA temp_store = memory", []);
        conn.busy_timeout(std::time::Duration::from_secs(5))?;

        Ok(SQLiteDB {
            connection: Arc::new(Mutex::new(RefCell::new(conn))),
        })
    }

    pub fn execute_query(
        &self,
        query: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<QueryResult> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        let mut stmt = conn.prepare(query)?;

        // 获取列名
        let column_count = stmt.column_count();
        let mut columns = Vec::with_capacity(column_count);
        for i in 0..column_count {
            columns.push(stmt.column_name(i)?.to_string());
        }

        let mut result = QueryResult::with_columns(columns.clone());

        // 查询数据
        let rows = stmt.query_map(params, |row| {
            let mut result_row = Row::new();
            for (i, column_name) in columns.iter().enumerate() {
                let value = match row.get_ref(i)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(i) => Value::Integer(i),
                    ValueRef::Real(f) => Value::Real(f),
                    ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).to_string()),
                    ValueRef::Blob(b) => Value::Blob(b.to_vec()),
                };
                result_row.insert(column_name.clone(), value);
            }
            Ok(result_row)
        })?;

        for row in rows {
            result.add_row(row?);
        }

        Ok(result)
    }

    pub fn execute_update(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<usize> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        let affected = conn.execute(query, params)?;
        Ok(affected)
    }

    pub fn begin_transaction(&self) -> Result<()> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        conn.execute("BEGIN TRANSACTION", [])?;
        Ok(())
    }

    pub fn commit_transaction(&self) -> Result<()> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        conn.execute("COMMIT", [])?;
        Ok(())
    }

    pub fn rollback_transaction(&self) -> Result<()> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        conn.execute("ROLLBACK", [])?;
        Ok(())
    }

    pub fn with_transaction<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.begin_transaction()?;

        match operation() {
            Ok(result) => {
                self.commit_transaction()?;
                Ok(result)
            }
            Err(e) => {
                self.rollback_transaction()?;
                Err(e)
            }
        }
    }

    pub fn insert(&self, query: &str, params: &[&dyn rusqlite::ToSql]) -> Result<i64> {
        let lock = self.connection.lock().map_err(|e| DBError::LockError {
            message: format!("Failed to acquire lock: {}", e),
        })?;

        let conn = lock.borrow();
        conn.execute(query, params)?;
        let last_id = conn.last_insert_rowid();
        Ok(last_id)
    }

    pub fn table_exists(&self, table_name: &str) -> Result<bool> {
        let result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
            &[&table_name],
        )?;
        Ok(!result.is_empty())
    }

    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        match key {
            "version" => {
                let result = self.execute_query("SELECT sqlite_version()", &[])?;
                Ok(result
                    .first()
                    .and_then(|row| row.get_string("sqlite_version()")))
            }
            "database_list" => {
                let result = self.execute_query("PRAGMA database_list", &[])?;
                Ok(Some(format!("{:?}", result)))
            }
            _ => Ok(None),
        }
    }
}

// 实现 Clone，使得 SQLiteDB 可以在多线程环境中共享
impl Clone for SQLiteDB {
    fn clone(&self) -> Self {
        SQLiteDB {
            connection: Arc::clone(&self.connection),
        }
    }
}

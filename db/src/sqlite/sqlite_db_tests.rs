#[cfg(test)]
mod tests {
    use crate::sqlite::SQLiteDB;
    use crate::traits::DB;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct User {
        id: i64,
        name: String,
        email: String,
        age: i64,
    }

    #[test]
    fn test_create_table() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        let result = db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_insert_and_query() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        // 创建表
        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        // 插入数据
        let user_id = db
            .insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[&"张三", &"zhangsan@test.com", &25i64],
            )
            .unwrap();

        assert!(user_id > 0);

        // 查询数据
        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.len(), 1);

        let row = &result.rows[0];
        assert_eq!(row.get_string("name"), Some("张三".to_string()));
        assert_eq!(
            row.get_string("email"),
            Some("zhangsan@test.com".to_string())
        );
        assert_eq!(row.get_i64("age"), Some(25));
    }

    #[test]
    fn test_update() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        // 创建表并插入数据
        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"张三", &"zhangsan@test.com", &25i64],
        )
        .unwrap();

        // 更新数据
        let affected = db
            .execute_update(
                "UPDATE users SET age = ?1 WHERE name = ?2",
                &[&30i64, &"张三"],
            )
            .unwrap();

        assert_eq!(affected, 1);

        // 验证更新
        let result = db
            .query("SELECT age FROM users WHERE name = '张三'")
            .unwrap();
        assert_eq!(result.rows[0].get_i64("age"), Some(30));
    }

    #[test]
    fn test_delete() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        // 创建表并插入数据
        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"张三", &"zhangsan@test.com", &25i64],
        )
        .unwrap();

        // 删除数据
        let affected = db
            .execute_update("DELETE FROM users WHERE name = ?1", &[&"张三"])
            .unwrap();

        assert_eq!(affected, 1);

        // 验证删除
        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_query_one() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"张三", &"zhangsan@test.com", &25i64],
        )
        .unwrap();

        // 查询单行
        let row = db
            .query_one("SELECT * FROM users WHERE name = ?1", &[&"张三"])
            .unwrap();

        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row.get_string("name"), Some("张三".to_string()));
    }

    #[test]
    fn test_query_typed() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"张三", &"zhangsan@test.com", &25i64],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"李四", &"lisi@test.com", &30i64],
        )
        .unwrap();

        // 查询并转换为强类型
        let users: Vec<User> = db.query_typed("SELECT * FROM users", &[]).unwrap();

        assert_eq!(users.len(), 2);
        assert_eq!(users[0].name, "张三");
        assert_eq!(users[1].name, "李四");
    }

    #[test]
    fn test_transaction() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        // 使用事务
        let result = db.with_transaction(|| {
            db.insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[&"张三", &"zhangsan@test.com", &25i64],
            )?;

            db.insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[&"李四", &"lisi@test.com", &30i64],
            )?;

            Ok(())
        });

        assert!(result.is_ok());

        // 验证数据已提交
        let users = db.query("SELECT * FROM users").unwrap();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn test_transaction_rollback() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        // 使用事务，但会失败回滚
        let result = db.with_transaction(|| {
            db.insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[&"张三", &"zhangsan@test.com", &25i64],
            )?;

            // 插入重复的 email，会失败
            db.insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[&"李四", &"zhangsan@test.com", &30i64],
            )?;

            Ok(())
        });

        assert!(result.is_err());

        // 验证数据已回滚
        let users = db.query("SELECT * FROM users").unwrap();
        assert_eq!(users.len(), 0);
    }

    #[test]
    fn test_table_exists() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        assert!(!db.table_exists("users").unwrap());

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )",
            &[],
        )
        .unwrap();

        assert!(db.table_exists("users").unwrap());
    }

    #[test]
    fn test_get_metadata() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        let version = db.get_metadata("version").unwrap();
        assert!(version.is_some());
        println!("SQLite version: {:?}", version);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let db = Arc::new(SQLiteDB::new(":memory:").expect("Failed to create database"));

        db.execute_update(
            "CREATE TABLE counter (
                id INTEGER PRIMARY KEY,
                value INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert("INSERT INTO counter (id, value) VALUES (1, 0)", &[])
            .unwrap();

        let mut handles = vec![];

        // 启动多个线程并发更新
        for _ in 0..10 {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    db_clone
                        .execute_update("UPDATE counter SET value = value + 1 WHERE id = 1", &[])
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        // 验证结果
        let result = db.query("SELECT value FROM counter WHERE id = 1").unwrap();
        assert_eq!(result.rows[0].get_i64("value"), Some(100));
    }

    #[test]
    fn test_multiple_inserts() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        // 批量插入
        for i in 0..100 {
            db.insert(
                "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
                &[
                    &format!("User{}", i) as &dyn rusqlite::ToSql,
                    &format!("user{}@test.com", i),
                    &(20i64 + i),
                ],
            )
            .unwrap();
        }

        // 验证数据
        let result = db.query("SELECT COUNT(*) as count FROM users").unwrap();
        assert_eq!(result.rows[0].get_i64("count"), Some(100));
    }

    #[test]
    fn test_query_with_params() {
        let db = SQLiteDB::new(":memory:").expect("Failed to create database");

        db.execute_update(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL
            )",
            &[],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"张三", &"zhangsan@test.com", &25i64],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"李四", &"lisi@test.com", &30i64],
        )
        .unwrap();

        db.insert(
            "INSERT INTO users (name, email, age) VALUES (?1, ?2, ?3)",
            &[&"王五", &"wangwu@test.com", &35i64],
        )
        .unwrap();

        // 使用参数查询
        let result = db
            .execute_query("SELECT * FROM users WHERE age > ?1 ORDER BY age", &[&25i64])
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.rows[0].get_string("name"), Some("李四".to_string()));
        assert_eq!(result.rows[1].get_string("name"), Some("王五".to_string()));
    }
}

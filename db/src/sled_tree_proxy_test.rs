#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::sled_tree_proxy::{SledTreeProxy, SledTreeProxyHook};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestData {
        id: u64,
        name: String,
        value: f64,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SimpleData {
        count: i32,
    }

    // Mock hook for testing
    #[derive(Default)]
    struct MockHook {
        inserts: Arc<Mutex<Vec<(String, Vec<u8>, TestData)>>>,
        removes: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
        batches: Arc<Mutex<Vec<String>>>,
    }

    impl SledTreeProxyHook for MockHook {
        type Item = TestData;

        fn on_insert(&self, name: &str, key: &[u8], value: &Self::Item) {
            self.inserts
                .lock()
                .unwrap()
                .push((name.to_string(), key.to_vec(), value.clone()));
        }

        fn on_remove(&self, name: &str, key: &[u8]) {
            self.removes
                .lock()
                .unwrap()
                .push((name.to_string(), key.to_vec()));
        }

        fn on_apply_batch(&self, name: &str, _batch: &[(&[u8], Option<&Self::Item>)]) {
            self.batches.lock().unwrap().push(name.to_string());
        }
    }

    fn setup_db() -> (TempDir, sled::Db) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db = sled::open(temp_dir.path()).expect("Failed to open sled db");
        (temp_dir, db)
    }

    #[test]
    fn test_new_tree_proxy() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let _proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;
        assert!(true); // Proxy created successfully
        Ok(())
    }

    #[test]
    fn test_insert_and_get() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let key = b"test_key_1";
        let data = TestData {
            id: 1,
            name: "Test Item".to_string(),
            value: 42.5,
        };

        // Insert data
        let prev = proxy.insert(key, &data)?;
        assert!(prev.is_none());

        // Get data
        let retrieved = proxy.get(key)?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        Ok(())
    }

    #[test]
    fn test_insert_overwrites_existing() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let key = b"test_key_1";
        let data1 = TestData {
            id: 1,
            name: "First".to_string(),
            value: 10.0,
        };
        let data2 = TestData {
            id: 2,
            name: "Second".to_string(),
            value: 20.0,
        };

        // First insert
        proxy.insert(key, &data1)?;

        // Second insert should return previous value
        let prev = proxy.insert(key, &data2)?;
        assert!(prev.is_some());
        assert_eq!(prev.unwrap(), data1);

        // Verify new value is stored
        let retrieved = proxy.get(key)?;
        assert_eq!(retrieved.unwrap(), data2);

        Ok(())
    }

    #[test]
    fn test_contains_key() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let key1 = b"existing_key";
        let key2 = b"non_existing_key";

        // Initially key should not exist
        assert!(!proxy.contains_key(key1)?);

        // Insert data
        let data = SimpleData { count: 100 };
        proxy.insert(key1, &data)?;

        // Now key should exist
        assert!(proxy.contains_key(key1)?);
        assert!(!proxy.contains_key(key2)?);

        Ok(())
    }

    #[test]
    fn test_remove() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let key = b"test_key";
        let data = TestData {
            id: 1,
            name: "To Remove".to_string(),
            value: 99.9,
        };

        // Insert data
        proxy.insert(key, &data)?;
        assert!(proxy.contains_key(key)?);

        // Remove data
        let removed = proxy.remove(key)?;
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), data);

        // Verify key no longer exists
        assert!(!proxy.contains_key(key)?);
        assert!(proxy.get(key)?.is_none());

        Ok(())
    }

    #[test]
    fn test_remove_non_existing_key() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let key = b"non_existing";
        let removed = proxy.remove(key)?;
        assert!(removed.is_none());

        Ok(())
    }

    #[test]
    fn test_iter() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert multiple items
        for i in 0..5 {
            let key = format!("key_{}", i);
            let data = SimpleData { count: i };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Count items using iterator
        let count = proxy.iter().count();
        assert_eq!(count, 5);

        Ok(())
    }

    #[test]
    fn test_range() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert items with sortable keys
        for i in 0..10 {
            let key = format!("key_{:02}", i); // key_00, key_01, ..., key_09
            let data = SimpleData { count: i };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Test range query - need to use Vec<u8> for proper type inference
        let start: Vec<u8> = b"key_02".to_vec();
        let end: Vec<u8> = b"key_06".to_vec();
        let range_count = proxy.range(start..end).count();
        assert_eq!(range_count, 4); // key_02, key_03, key_04, key_05

        Ok(())
    }

    #[test]
    fn test_apply_batch_insert() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let data1 = TestData {
            id: 1,
            name: "Batch1".to_string(),
            value: 1.0,
        };
        let data2 = TestData {
            id: 2,
            name: "Batch2".to_string(),
            value: 2.0,
        };
        let data3 = TestData {
            id: 3,
            name: "Batch3".to_string(),
            value: 3.0,
        };

        let batch = vec![
            (b"key1".as_ref(), Some(&data1)),
            (b"key2".as_ref(), Some(&data2)),
            (b"key3".as_ref(), Some(&data3)),
        ];

        proxy.apply_batch(batch)?;

        // Verify all items were inserted
        assert_eq!(proxy.get(b"key1")?.unwrap(), data1);
        assert_eq!(proxy.get(b"key2")?.unwrap(), data2);
        assert_eq!(proxy.get(b"key3")?.unwrap(), data3);

        Ok(())
    }

    #[test]
    fn test_apply_batch_remove() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert some data first
        let data = SimpleData { count: 42 };
        proxy.insert(b"key1", &data)?;
        proxy.insert(b"key2", &data)?;
        proxy.insert(b"key3", &data)?;

        // Batch remove
        let batch = vec![(b"key1".as_ref(), None), (b"key2".as_ref(), None)];

        proxy.apply_batch(batch)?;

        // Verify items were removed
        assert!(!proxy.contains_key(b"key1")?);
        assert!(!proxy.contains_key(b"key2")?);
        assert!(proxy.contains_key(b"key3")?); // key3 should still exist

        Ok(())
    }

    #[test]
    fn test_apply_batch_mixed() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert initial data
        let initial_data = TestData {
            id: 0,
            name: "Initial".to_string(),
            value: 0.0,
        };
        proxy.insert(b"key1", &initial_data)?;
        proxy.insert(b"key2", &initial_data)?;

        // Mixed batch: insert new, update existing, remove
        let new_data = TestData {
            id: 99,
            name: "New".to_string(),
            value: 99.0,
        };
        let update_data = TestData {
            id: 100,
            name: "Updated".to_string(),
            value: 100.0,
        };

        let batch = vec![
            (b"key1".as_ref(), Some(&update_data)), // Update
            (b"key2".as_ref(), None),               // Remove
            (b"key3".as_ref(), Some(&new_data)),    // Insert
        ];

        proxy.apply_batch(batch)?;

        // Verify results
        assert_eq!(proxy.get(b"key1")?.unwrap(), update_data);
        assert!(proxy.get(b"key2")?.is_none());
        assert_eq!(proxy.get(b"key3")?.unwrap(), new_data);

        Ok(())
    }

    #[test]
    fn test_hook_on_insert() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let hook = Arc::new(MockHook::default());
        let proxy: SledTreeProxy<TestData> =
            SledTreeProxy::new(&db, "test_tree", Some(hook.clone()))?;

        let key = b"hook_key";
        let data = TestData {
            id: 1,
            name: "Hook Test".to_string(),
            value: 123.45,
        };

        proxy.insert(key, &data)?;

        // Verify hook was called
        let inserts = hook.inserts.lock().unwrap();
        assert_eq!(inserts.len(), 1);
        assert_eq!(inserts[0].0, "test_tree");
        assert_eq!(inserts[0].1, key);
        assert_eq!(inserts[0].2, data);

        Ok(())
    }

    #[test]
    fn test_hook_on_remove() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let hook = Arc::new(MockHook::default());
        let proxy: SledTreeProxy<TestData> =
            SledTreeProxy::new(&db, "test_tree", Some(hook.clone()))?;

        let key = b"remove_key";
        let data = TestData {
            id: 1,
            name: "To Remove".to_string(),
            value: 1.0,
        };

        proxy.insert(key, &data)?;
        proxy.remove(key)?;

        // Verify hook was called
        let removes = hook.removes.lock().unwrap();
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].0, "test_tree");
        assert_eq!(removes[0].1, key);

        Ok(())
    }

    #[test]
    fn test_hook_on_apply_batch() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let hook = Arc::new(MockHook::default());
        let proxy: SledTreeProxy<TestData> =
            SledTreeProxy::new(&db, "test_tree", Some(hook.clone()))?;

        let data = TestData {
            id: 1,
            name: "Batch".to_string(),
            value: 1.0,
        };

        let batch = vec![(b"key1".as_ref(), Some(&data))];
        proxy.apply_batch(batch)?;

        // Verify hook was called
        let batches = hook.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], "test_tree");

        Ok(())
    }

    #[test]
    fn test_get_non_existing_key() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let result = proxy.get(b"non_existing_key")?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_multiple_trees() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy1: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "tree1", None)?;
        let proxy2: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "tree2", None)?;

        let data1 = SimpleData { count: 1 };
        let data2 = SimpleData { count: 2 };

        proxy1.insert(b"key", &data1)?;
        proxy2.insert(b"key", &data2)?;

        // Verify data is isolated between trees
        assert_eq!(proxy1.get(b"key")?.unwrap(), data1);
        assert_eq!(proxy2.get(b"key")?.unwrap(), data2);

        Ok(())
    }

    #[test]
    fn test_empty_batch() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let batch: Vec<(&[u8], Option<&TestData>)> = vec![];
        proxy.apply_batch(batch)?; // Should not panic

        Ok(())
    }

    #[test]
    fn test_large_data() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let large_string = "x".repeat(10000);
        let data = TestData {
            id: 1,
            name: large_string.clone(),
            value: 1.0,
        };

        proxy.insert(b"large_key", &data)?;
        let retrieved = proxy.get(b"large_key")?;

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, large_string);

        Ok(())
    }

    #[test]
    fn test_binary_keys() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        let binary_key = vec![0u8, 1, 2, 3, 255, 254, 253];
        let data = SimpleData { count: 42 };

        proxy.insert(&binary_key, &data)?;
        let retrieved = proxy.get(&binary_key)?;

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), data);

        Ok(())
    }
}

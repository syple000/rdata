#[cfg(test)]
mod tests {
    use crate::error::{Error, Result};
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

        // Verify we can iterate and get deserialized values
        let mut collected: Vec<(Vec<u8>, SimpleData)> = Vec::new();
        for item in proxy.iter() {
            collected.push(item?);
        }
        assert_eq!(collected.len(), 5);

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

        // Verify we can iterate over range and get deserialized values
        let start: Vec<u8> = b"key_02".to_vec();
        let end: Vec<u8> = b"key_06".to_vec();
        let mut collected: Vec<(Vec<u8>, SimpleData)> = Vec::new();
        for item in proxy.range(start..end) {
            collected.push(item?);
        }
        assert_eq!(collected.len(), 4);

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

    #[test]
    fn test_iterator_returns_deserialized_values() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
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
        let data3 = TestData {
            id: 3,
            name: "Third".to_string(),
            value: 30.0,
        };

        proxy.insert(b"key1", &data1)?;
        proxy.insert(b"key2", &data2)?;
        proxy.insert(b"key3", &data3)?;

        // Iterate and verify we get (Vec<u8>, T) tuples
        let results: std::result::Result<Vec<(Vec<u8>, TestData)>, _> = proxy.iter().collect();
        let results = results?;

        assert_eq!(results.len(), 3);

        // Verify the data is correctly deserialized
        for (key, value) in results {
            match key.as_slice() {
                b"key1" => assert_eq!(value, data1),
                b"key2" => assert_eq!(value, data2),
                b"key3" => assert_eq!(value, data3),
                _ => panic!("Unexpected key: {:?}", key),
            }
        }

        Ok(())
    }

    #[test]
    fn test_range_iterator_returns_deserialized_values() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert items with sortable keys
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let data = SimpleData { count: i };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Test range query and verify deserialized values
        let start: Vec<u8> = b"key_03".to_vec();
        let end: Vec<u8> = b"key_07".to_vec();

        let results: std::result::Result<Vec<(Vec<u8>, SimpleData)>, _> =
            proxy.range(start..end).collect();
        let results = results?;

        assert_eq!(results.len(), 4);

        // Verify values are in expected range and correctly deserialized
        for (i, (key, value)) in results.iter().enumerate() {
            let expected_key = format!("key_{:02}", i + 3);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value.count, (i + 3) as i32);
        }

        Ok(())
    }

    #[test]
    fn test_iterator_rev() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert items with sortable keys
        for i in 0..5 {
            let key = format!("key_{}", i);
            let data = SimpleData { count: i };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Iterate in reverse order
        let results: std::result::Result<Vec<(Vec<u8>, SimpleData)>, _> =
            proxy.iter().rev().collect();
        let results = results?;

        assert_eq!(results.len(), 5);

        // Verify items are in reverse order
        for (i, (key, value)) in results.iter().enumerate() {
            let expected_idx = 4 - i;
            let expected_key = format!("key_{}", expected_idx);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value.count, expected_idx as i32);
        }

        Ok(())
    }

    #[test]
    fn test_iterator_for_each() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 1..=3 {
            let key = format!("key{}", i);
            let data = TestData {
                id: i as u64,
                name: format!("Item {}", i),
                value: i as f64 * 10.0,
            };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Use for_each to collect data
        let mut collected: Vec<(Vec<u8>, TestData)> = Vec::new();
        proxy.iter().for_each(|key, value| {
            collected.push((key, value));
        })?;

        assert_eq!(collected.len(), 3);

        Ok(())
    }

    #[test]
    fn test_iterator_keys_and_values() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        proxy.insert(b"key1", &SimpleData { count: 10 })?;
        proxy.insert(b"key2", &SimpleData { count: 20 })?;
        proxy.insert(b"key3", &SimpleData { count: 30 })?;

        // Test keys()
        let keys: std::result::Result<Vec<Vec<u8>>, _> = proxy.iter().keys().collect();
        let keys = keys?;
        assert_eq!(keys.len(), 3);

        // Test values()
        let values: std::result::Result<Vec<SimpleData>, _> = proxy.iter().values().collect();
        let values = values?;
        assert_eq!(values.len(), 3);

        // Verify sum of values
        let sum: i32 = values.iter().map(|v| v.count).sum();
        assert_eq!(sum, 60);

        Ok(())
    }

    #[test]
    fn test_iterator_find() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let data = SimpleData { count: i * 5 };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Find element with count == 25
        let result = proxy.iter().find(|_key, value| value.count == 25)?;
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, b"key_05");
        assert_eq!(value.count, 25);

        // Find non-existing element
        let result = proxy.iter().find(|_key, value| value.count == 999)?;
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_iterator_any_all() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 1..=5 {
            let key = format!("key{}", i);
            let data = SimpleData { count: i * 10 };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Test any - at least one element has count > 30
        let has_large = proxy.iter().any(|_key, value| value.count > 30)?;
        assert!(has_large);

        // Test any - no element has count > 100
        let has_very_large = proxy.iter().any(|_key, value| value.count > 100)?;
        assert!(!has_very_large);

        // Test all - all elements have count > 0
        let all_positive = proxy.iter().all(|_key, value| value.count > 0)?;
        assert!(all_positive);

        // Test all - not all elements have count > 30
        let all_large = proxy.iter().all(|_key, value| value.count > 30)?;
        assert!(!all_large);

        Ok(())
    }

    #[test]
    fn test_iterator_count_items() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 0..7 {
            let key = format!("key{}", i);
            proxy.insert(key.as_bytes(), &SimpleData { count: i })?;
        }

        let count = proxy.iter().count_items()?;
        assert_eq!(count, 7);

        Ok(())
    }

    #[test]
    fn test_iterator_nth_and_last() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data with sortable keys
        for i in 0..5 {
            let key = format!("key_{:02}", i);
            let data = SimpleData { count: i * 10 };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Test nth_item
        let second = proxy.iter().nth_item(2)?;
        assert!(second.is_some());
        let (key, value) = second.unwrap();
        assert_eq!(key, b"key_02");
        assert_eq!(value.count, 20);

        // Test nth_item out of bounds
        let out_of_bounds = proxy.iter().nth_item(100)?;
        assert!(out_of_bounds.is_none());

        // Test last_item
        let last = proxy.iter().last_item()?;
        assert!(last.is_some());
        let (key, value) = last.unwrap();
        assert_eq!(key, b"key_04");
        assert_eq!(value.count, 40);

        Ok(())
    }

    #[test]
    fn test_iterator_take_skip() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let data = SimpleData { count: i };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Test take(3) - first 3 elements
        let taken: std::result::Result<Vec<(Vec<u8>, SimpleData)>, _> =
            proxy.iter().take(3).collect();
        let taken = taken?;
        assert_eq!(taken.len(), 3);

        // Test skip(7) - skip first 7 elements
        let skipped: std::result::Result<Vec<(Vec<u8>, SimpleData)>, _> =
            proxy.iter().skip(7).collect();
        let skipped = skipped?;
        assert_eq!(skipped.len(), 3); // 10 - 7 = 3

        // Test skip(5).take(3) - skip 5, take next 3
        let skip_take: std::result::Result<Vec<(Vec<u8>, SimpleData)>, _> =
            proxy.iter().skip(5).take(3).collect();
        let skip_take = skip_take?;
        assert_eq!(skip_take.len(), 3);

        Ok(())
    }

    #[test]
    fn test_iterator_collect_vec() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<TestData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 1..=4 {
            let key = format!("key{}", i);
            let data = TestData {
                id: i as u64,
                name: format!("Test {}", i),
                value: i as f64,
            };
            proxy.insert(key.as_bytes(), &data)?;
        }

        // Use collect_vec convenience method
        let items = proxy.iter().collect_vec()?;
        assert_eq!(items.len(), 4);

        Ok(())
    }

    #[test]
    fn test_iterator_try_for_each() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 0..5 {
            let key = format!("key{}", i);
            proxy.insert(key.as_bytes(), &SimpleData { count: i * 2 })?;
        }

        // Use try_for_each to validate all values
        let mut sum = 0;
        proxy.iter().try_for_each(|_key, value| {
            if value.count < 0 {
                return Err(Error::SerDesError("Negative value".to_string()));
            }
            sum += value.count;
            Ok(())
        })?;

        assert_eq!(sum, 0 + 2 + 4 + 6 + 8);

        Ok(())
    }

    #[test]
    fn test_double_ended_iterator() -> Result<()> {
        let (_temp_dir, db) = setup_db();
        let proxy: SledTreeProxy<SimpleData> = SledTreeProxy::new(&db, "test_tree", None)?;

        // Insert test data
        for i in 0..5 {
            let key = format!("key_{}", i);
            proxy.insert(key.as_bytes(), &SimpleData { count: i })?;
        }

        // Use next_back from both ends
        let mut iter = proxy.iter();

        // Get first element
        let first = iter.next();
        assert!(first.is_some());
        let (key, value) = first.unwrap()?;
        assert_eq!(key, b"key_0");
        assert_eq!(value.count, 0);

        // Get last element
        let last = iter.next_back();
        assert!(last.is_some());
        let (key, value) = last.unwrap()?;
        assert_eq!(key, b"key_4");
        assert_eq!(value.count, 4);

        Ok(())
    }
}

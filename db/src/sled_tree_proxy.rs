use crate::error::{Error, Result};
use bincode::config::standard as bcs_config;
use bincode::serde as bcs;
use log::{debug, error};
use serde::{de::DeserializeOwned, Serialize};
use sled::Batch;
use std::{fmt::Debug, sync::Arc};

pub trait SledTreeProxyHook {
    type Item;

    fn on_insert(&self, name: &str, key: &[u8], value: &Self::Item);
    fn on_remove(&self, name: &str, key: &[u8]);
    fn on_apply_batch(&self, name: &str, batch: &[(&[u8], Option<&Self::Item>)]);
}

pub struct SledTreeProxy<T>
where
    T: Serialize + DeserializeOwned + Debug,
{
    name: String,
    tree: sled::Tree,
    hook: Option<Arc<dyn SledTreeProxyHook<Item = T> + Send + Sync>>,
}

impl<T> SledTreeProxy<T>
where
    T: Serialize + DeserializeOwned + Debug,
{
    pub fn new(
        db: &sled::Db,
        tree_name: &str,
        hook: Option<Arc<dyn SledTreeProxyHook<Item = T> + Send + Sync>>,
    ) -> Result<Self> {
        let tree = db.open_tree(tree_name).map_err(|e| {
            error!("new sled proxy open tree err: {:?}", e);
            Error::SledError(e)
        })?;
        Ok(Self {
            name: tree_name.to_string(),
            tree,
            hook,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<T>> {
        debug!("Get key: {:?}", key);
        let result = self.tree.get(key).map_err(|e| {
            error!("Get key: {:?} err: {:?}", key, e);
            Error::SledError(e)
        })?;
        debug!("Get key: {:?} success, value: {:?}", key, result);
        let result = match result {
            None => None,
            Some(bytes) => {
                let v = bcs::decode_from_slice(&bytes, bcs_config());
                match v {
                    Ok((v, _)) => Some(v),
                    Err(e) => {
                        error!(
                            "Get key: {:?} deserialize err: {:?}, bytes: {:?}",
                            key, e, bytes
                        );
                        return Err(Error::SerDesError(format!("Deserialize err: {:?}", e)));
                    }
                }
            }
        };
        Ok(result)
    }

    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        debug!("Contains key: {:?}", key);
        let result = self.tree.contains_key(key).map_err(|e| {
            error!("Contains key: {:?} err: {:?}", key, e);
            Error::SledError(e)
        })?;
        debug!("Contains key: {:?} success, exists: {}", key, result);
        Ok(result)
    }

    pub fn iter(&self) -> sled::Iter {
        self.tree.iter()
    }

    pub fn range<K, R>(&self, range: R) -> sled::Iter
    where
        R: std::ops::RangeBounds<K>,
        K: AsRef<[u8]>,
    {
        self.tree.range(range)
    }

    pub fn insert(&self, key: &[u8], value: &T) -> Result<Option<T>> {
        debug!("Insert key: {:?}, value: {:?}", key, value);
        let value_bytes = bcs::encode_to_vec(value, bcs_config()).map_err(|e| {
            error!(
                "Insert key: {:?}, value: {:?} serialize err: {:?}",
                key, value, e
            );
            Error::SerDesError(format!("Serialize err: {:?}", e))
        })?;
        let last_value = self.tree.insert(key, value_bytes).map_err(|e| {
            error!("Insert key: {:?}, value: {:?} err: {:?}", key, value, e);
            Error::SledError(e)
        })?;
        debug!("Insert key: {:?}, value: {:?} success", key, value);
        if let Some(hook) = &self.hook {
            hook.on_insert(&self.name, key, value);
        }
        match last_value {
            Some(v) => {
                let v = bcs::decode_from_slice(&v, bcs_config());
                match v {
                    Ok((v, _)) => Ok(Some(v)),
                    Err(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn remove(&self, key: &[u8]) -> Result<Option<T>> {
        debug!("Remove key: {:?}", key);
        let result = self.tree.remove(key).map_err(|e| {
            error!("Remove key: {:?} err: {:?}", key, e);
            Error::SledError(e)
        })?;
        debug!("Remove key: {:?} success", key);
        if let Some(hook) = &self.hook {
            hook.on_remove(&self.name, key);
        }
        match result {
            Some(v) => {
                let v = bcs::decode_from_slice(&v, bcs_config());
                match v {
                    Ok((v, _)) => Ok(Some(v)),
                    Err(_) => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    pub fn apply_batch(&self, batch_vec: Vec<(&[u8], Option<&T>)>) -> Result<()> {
        debug!("Apply batch: {:?}", batch_vec);

        let mut batch = Batch::default();
        for (key, value_opt) in batch_vec.iter() {
            match value_opt {
                Some(value) => {
                    let value_bytes = bcs::encode_to_vec(value, bcs_config()).map_err(|e| {
                        error!(
                            "Apply batch key: {:?}, value: {:?} serialize err: {:?}",
                            key, value, e
                        );
                        Error::SerDesError(format!("Serialize err: {:?}", e))
                    })?;
                    batch.insert(key.as_ref(), value_bytes);
                }
                None => {
                    batch.remove(key.as_ref());
                }
            }
        }

        self.tree.apply_batch(batch).map_err(|e| {
            error!("Apply batch {:?} err: {:?}", batch_vec, e);
            Error::SledError(e)
        })?;

        if let Some(hook) = &self.hook {
            hook.on_apply_batch(&self.name, &batch_vec);
        }

        debug!("Apply batch: {:?} success", batch_vec);
        Ok(())
    }
}

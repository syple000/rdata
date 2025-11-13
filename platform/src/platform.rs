use crate::{config::Config, errors::Result};
use std::sync::Arc;

pub struct Platform {}

impl Platform {
    pub fn new(_config: Arc<Config>) -> Result<Self> {
        todo!()
    }
}

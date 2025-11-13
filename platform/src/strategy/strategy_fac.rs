use std::sync::Arc;

use crate::{
    errors::{PlatformError, Result},
    strategy::Strategy,
};

pub fn create_strategy(name: &str) -> Result<Arc<dyn Strategy>> {
    Err(PlatformError::StrategyError {
        message: format!("unsupport strategy name: {}", name),
    })
}

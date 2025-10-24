use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoExtra {}

pub mod market;
pub use market::*;

pub mod enums;
pub use enums::*;

pub mod trade;
pub use trade::*;

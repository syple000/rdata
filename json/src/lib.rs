pub mod error;
pub mod json;
pub use error::{JsonError, Result};
pub use json::{dump, dumps, load, loads};

#[cfg(test)]
mod json_test;

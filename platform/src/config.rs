use log::info;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Configuration file error: {message}")]
    FileError { message: String },
    #[error("Configuration parse error: {message}")]
    ParseError { message: String },
}

pub struct Config {
    settings: config::Config,
}

impl Config {
    pub fn from_json(filepath: &str) -> Result<Self, ConfigError> {
        config::builder::ConfigBuilder::<config::builder::DefaultState>::default()
            .add_source(config::File::with_name(filepath).format(config::FileFormat::Json))
            .build()
            .map(|settings| {
                info!("settings: {:?}", settings);
                Config { settings }
            })
            .map_err(|e| ConfigError::FileError {
                message: e.to_string(),
            })
    }

    pub fn from_yaml(filepath: &str) -> Result<Self, ConfigError> {
        config::builder::ConfigBuilder::<config::builder::DefaultState>::default()
            .add_source(config::File::with_name(filepath).format(config::FileFormat::Yaml))
            .build()
            .map(|settings| {
                info!("settings: {:?}", settings);
                Config { settings }
            })
            .map_err(|e| ConfigError::FileError {
                message: e.to_string(),
            })
    }

    pub fn from_toml(filepath: &str) -> Result<Self, ConfigError> {
        config::builder::ConfigBuilder::<config::builder::DefaultState>::default()
            .add_source(config::File::with_name(filepath).format(config::FileFormat::Toml))
            .build()
            .map(|settings| {
                info!("settings: {:?}", settings);
                Config { settings }
            })
            .map_err(|e| ConfigError::FileError {
                message: e.to_string(),
            })
    }

    pub fn get<'de, T: Deserialize<'de>>(&self, key: &str) -> Result<T, ConfigError> {
        self.settings
            .get::<T>(key)
            .map_err(|e| ConfigError::ParseError {
                message: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test_load_json_config() {
        let mut tempfile = NamedTempFile::new().unwrap();
        writeln!(tempfile, r#"{{ "api": {{ "key": "my_api_key" }} }}"#).unwrap();
        let config = Config::from_json(tempfile.path().to_str().unwrap()).unwrap();
        let api_key: String = config.get("api.key").unwrap();
        assert_eq!(api_key, "my_api_key");
    }

    #[test]
    fn test_load_yaml_config() {
        let mut tempfile = NamedTempFile::new().unwrap();
        writeln!(tempfile, "api:\n  key: my_api_key").unwrap();
        let config = Config::from_yaml(tempfile.path().to_str().unwrap()).unwrap();
        let api_key: String = config.get("api.key").unwrap();
        assert_eq!(api_key, "my_api_key");
    }

    #[test]
    fn test_load_toml_config() {
        let mut tempfile = NamedTempFile::new().unwrap();
        writeln!(tempfile, "[api]\nkey = \"my_api_key\"").unwrap();
        let config = Config::from_toml(tempfile.path().to_str().unwrap()).unwrap();
        let api_key: String = config.get("api.key").unwrap();
        assert_eq!(api_key, "my_api_key");
    }
}

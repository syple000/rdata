#[cfg(test)]
mod tests {
    use crate::json::{dump, dumps, load, loads};
    use std::fs;

    #[test]
    fn test_dump() {
        let data = vec!["example", "test"];
        let filepath = "test_output.json";

        let result = dump(&data, filepath);
        assert!(result.is_ok());

        let file_content = fs::read_to_string(filepath).expect("Failed to read test file");
        let expected_content = serde_json::to_string_pretty(&data).unwrap();
        assert_eq!(file_content, expected_content);

        fs::remove_file(filepath).expect("Failed to clean up test file");
    }

    #[test]
    fn test_dumps() {
        let data = vec!["example", "test"];

        let result = dumps(&data);
        assert!(result.is_ok());

        let json_string = result.unwrap();
        let expected_string = serde_json::to_string_pretty(&data).unwrap();
        assert_eq!(json_string, expected_string);
    }

    #[test]
    fn test_load() {
        let data = vec!["example", "test"];
        let filepath = "test_input.json";

        // Prepare a test file
        fs::write(filepath, serde_json::to_string_pretty(&data).unwrap())
            .expect("Failed to write test file");
        let result: Result<Vec<String>, _> = load(filepath);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
        fs::remove_file(filepath).expect("Failed to clean up test file");
    }

    #[test]
    fn test_loads() {
        let data = vec!["example", "test"];
        let json_string = serde_json::to_string_pretty(&data).unwrap();

        let result: Result<Vec<String>, _> = loads(&json_string);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }
}

use chrono::{DateTime, Local, NaiveDateTime, TimeZone};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_current_nano_timestamp() -> u128 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

pub fn get_current_milli_timestamp() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn fmt_mill_timestamp(ts: u64) -> String {
    let seconds = (ts / 1000) as i64;
    let nanos = ((ts % 1000) * 1_000_000) as u32;

    if let Some(datetime) = DateTime::from_timestamp(seconds, nanos) {
        let local: DateTime<Local> = datetime.into();
        local.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        String::from("Invalid timestamp")
    }
}

pub fn parse_timestamp(time_str: &str) -> Result<u64, String> {
    let format = "%Y-%m-%d %H:%M:%S";

    NaiveDateTime::parse_from_str(time_str, format)
        .map_err(|e| format!("Failed to parse time string: {}", e))
        .and_then(|naive_dt| {
            Local
                .from_local_datetime(&naive_dt)
                .single()
                .ok_or_else(|| String::from("Ambiguous or invalid local time"))
                .map(|dt| dt.timestamp_millis() as u64)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_mill_timestamp() {
        // 测试一个已知的时间戳
        let ts = 1700000000000u64; // 2023-11-15 00:13:20 UTC
        let formatted = fmt_mill_timestamp(ts);
        assert!(!formatted.is_empty());
        assert!(formatted.contains("-"));
        assert!(formatted.contains(":"));
    }

    #[test]
    fn test_parse_and_format_roundtrip() {
        let original = "2024-01-01 12:30:45";
        let ts = parse_timestamp(original).unwrap();
        let formatted = fmt_mill_timestamp(ts);
        assert_eq!(original, formatted);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        let result = parse_timestamp("invalid time");
        assert!(result.is_err());
    }

    #[test]
    fn test_current_timestamp() {
        let ts = get_current_milli_timestamp();
        let formatted = fmt_mill_timestamp(ts);
        assert!(!formatted.is_empty());
        println!("Current time: {}", formatted);
    }
}

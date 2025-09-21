use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_current_nano_timestamp() -> u128 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

pub fn get_current_milli_timestamp() -> u128 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_millis()
}

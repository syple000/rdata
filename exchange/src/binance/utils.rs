// binance http工具

pub fn sort_params(params: &mut Vec<(&str, String)>) {
    params.sort_by(|a, b| a.0.cmp(b.0));
}

pub fn encode_params(params: &Vec<(&str, String)>) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
        .collect::<Vec<String>>()
        .join("&")
}

pub fn hmac_sha256(key: &str, data: &str) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
    mac.update(data.as_bytes());
    let result = mac.finalize();
    let code_bytes = result.into_bytes();
    hex::encode(code_bytes)
}

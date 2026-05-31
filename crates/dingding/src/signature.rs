use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::{Error, error::Result};

pub(crate) fn current_timestamp_millis() -> Result<String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string())
}

pub(crate) fn create_signature(timestamp: &str, secret: &str) -> Result<String> {
    let string_to_sign = format!("{timestamp}\n{secret}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).map_err(|_error| Error::Signature)?;
    mac.update(string_to_sign.as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_expected_signature() {
        let signature = create_signature("1700000000000", "secret").expect("signature");
        assert_eq!(signature, "OuzzJR5+xZ4/EYwqtNt6sMYZQMTa/HEGvc9miJe7XzY=");
    }
}

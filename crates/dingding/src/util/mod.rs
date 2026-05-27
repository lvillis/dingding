pub(crate) mod redact;
pub(crate) mod url;

pub(crate) fn non_empty_trimmed(value: &str, field: &'static str) -> crate::Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(crate::Error::invalid_input(
            field,
            "value must not be empty",
        ))
    } else {
        Ok(trimmed.to_string())
    }
}

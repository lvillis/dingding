const SENSITIVE_KEYS: &[&str] = &[
    "access-token",
    "accessToken",
    "access_token",
    "appsecret",
    "appSecret",
    "app_secret",
    "authorization",
    "clientSecret",
    "client_secret",
    "cookie",
    "passwd",
    "password",
    "secret",
    "token",
    "x-acs-dingtalk-access-token",
];

pub(crate) fn redact_text(input: &str) -> String {
    let mut output = input.to_owned();
    for key in SENSITIVE_KEYS {
        let mut search_from = 0;
        while search_from < output.len()
            && let Some(relative_index) = find_ascii_case_insensitive(&output[search_from..], key)
        {
            let index = search_from + relative_index;
            if !is_sensitive_key_boundary(output.as_bytes(), index, key.len()) {
                search_from = index + key.len();
                continue;
            }

            let Some((start, end)) = redaction_range(&output, index, key.len()) else {
                search_from = index + key.len();
                continue;
            };
            if start < end {
                output.replace_range(start..end, "<redacted>");
                search_from = start + "<redacted>".len();
            } else {
                search_from = start;
            }
        }
    }
    output
}

fn is_sensitive_key_boundary(bytes: &[u8], index: usize, len: usize) -> bool {
    let before = index
        .checked_sub(1)
        .and_then(|index| bytes.get(index))
        .copied();
    let after = bytes.get(index + len).copied();

    !before.is_some_and(is_identifier_byte) && !after.is_some_and(is_identifier_byte)
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')
}

fn redaction_range(input: &str, key_start: usize, key_len: usize) -> Option<(usize, usize)> {
    let bytes = input.as_bytes();
    let mut cursor = key_start + key_len;

    if let Some(quote) = bytes.get(cursor).copied().filter(|byte| is_quote(*byte))
        && key_start
            .checked_sub(1)
            .and_then(|index| bytes.get(index))
            .copied()
            == Some(quote)
    {
        cursor += 1;
    }

    cursor = skip_ascii_whitespace(bytes, cursor);
    if !matches!(bytes.get(cursor), Some(b'=' | b':')) {
        return None;
    }

    cursor += 1;
    cursor = skip_ascii_whitespace(bytes, cursor);
    let start = cursor;
    let end = match bytes.get(start).copied() {
        Some(quote) if is_quote(quote) => {
            let value_start = start + 1;
            return Some((value_start, quoted_string_end(input, value_start, quote)));
        }
        Some(_) => unquoted_value_end(input, start),
        None => start,
    };

    Some((start, end))
}

fn skip_ascii_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    index
}

fn is_quote(byte: u8) -> bool {
    matches!(byte, b'"' | b'\'')
}

fn quoted_string_end(input: &str, start: usize, quote: u8) -> usize {
    let bytes = input.as_bytes();
    let mut index = start;
    let mut escaped = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == quote {
            return index;
        }
        index += 1;
    }

    input.len()
}

fn unquoted_value_end(input: &str, start: usize) -> usize {
    let bytes = input.as_bytes();
    let mut end = start;
    while end < bytes.len()
        && !matches!(bytes[end], b'&' | b';' | b',' | b'}' | b']' | b'\r' | b'\n')
    {
        end += 1;
    }
    while !input.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return Some(0);
    }

    haystack
        .as_bytes()
        .windows(needle.len())
        .position(|window| {
            window
                .iter()
                .zip(needle)
                .all(|(left, right)| left.eq_ignore_ascii_case(right))
        })
}

pub(crate) fn truncate_snippet(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }

    let mut end = max_bytes;
    while !input.is_char_boundary(end) {
        end -= 1;
    }

    let mut value = input[..end].to_string();
    value.push_str("...(truncated)");
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_text_handles_unicode_before_sensitive_key() {
        let redacted = redact_text("İ access_token=abcdef1234567890");

        assert!(redacted.contains("access_token=<redacted>"));
    }

    #[test]
    fn redact_text_redacts_repeated_sensitive_values() {
        let redacted =
            redact_text("access_token=first-secret&access_token=second-secret&secret=third-secret");

        assert_eq!(redacted.matches("access_token=<redacted>").count(), 2);
        assert_eq!(redacted.matches("secret=<redacted>").count(), 1);
        assert!(!redacted.contains("first-secret"));
        assert!(!redacted.contains("second-secret"));
        assert!(!redacted.contains("third-secret"));
    }

    #[test]
    fn redact_text_does_not_match_key_suffixes() {
        let redacted = redact_text("access_token=secret-token&token=standalone");

        assert_eq!(redacted.matches("access_token=<redacted>").count(), 1);
        assert_eq!(redacted.matches("&token=<redacted>").count(), 1);
        assert!(!redacted.contains("secret-token"));
        assert!(!redacted.contains("standalone"));
    }

    #[test]
    fn redact_text_redacts_json_string_values() {
        let redacted = redact_text(
            r#"{"errmsg":"invalid secret value","access_token":"abc,def","other":"visible"}"#,
        );

        assert!(redacted.contains(r#""access_token":"<redacted>""#));
        assert!(redacted.contains("invalid secret value"));
        assert!(redacted.contains(r#""other":"visible""#));
        assert!(!redacted.contains("abc,def"));
    }

    #[test]
    fn redact_text_handles_colon_and_cookie_delimiters() {
        let redacted = redact_text("Authorization: Bearer abc; cookie=session=def; ok=true");

        assert!(redacted.contains("Authorization: <redacted>"));
        assert!(redacted.contains("cookie=<redacted>"));
        assert!(redacted.contains("ok=true"));
        assert!(!redacted.contains("Bearer abc"));
        assert!(!redacted.contains("session=def"));
    }

    #[test]
    fn redact_text_redacts_camel_case_and_header_tokens() {
        let redacted =
            redact_text("accessToken: abc\nclientSecret=def\nx-acs-dingtalk-access-token: ghi");

        assert!(redacted.contains("accessToken: <redacted>"));
        assert!(redacted.contains("clientSecret=<redacted>"));
        assert!(redacted.contains("x-acs-dingtalk-access-token: <redacted>"));
        assert!(!redacted.contains("abc"));
        assert!(!redacted.contains("def"));
        assert!(!redacted.contains("ghi"));
    }

    #[test]
    fn truncate_snippet_preserves_char_boundaries() {
        assert_eq!(truncate_snippet("钉钉token", 4), "钉...(truncated)");
    }
}

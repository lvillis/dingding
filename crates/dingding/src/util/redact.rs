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
    "conversationId",
    "conversation_id",
    "cookie",
    "corpId",
    "corp_id",
    "dingtalkId",
    "dingtalk_id",
    "downloadCode",
    "download_code",
    "fileId",
    "file_id",
    "mediaId",
    "media_id",
    "msgId",
    "msg_id",
    "openConversationId",
    "open_conversation_id",
    "operatorUnionId",
    "operatorUserId",
    "passwd",
    "password",
    "pictureDownloadCode",
    "picture_download_code",
    "secret",
    "senderId",
    "senderStaffId",
    "sender_id",
    "sender_staff_id",
    "sessionWebhook",
    "session_webhook",
    "sign",
    "signature",
    "spaceId",
    "space_id",
    "staffId",
    "staff_id",
    "ticket",
    "token",
    "unionId",
    "union_id",
    "userId",
    "user_id",
    "x-acs-dingtalk-access-token",
];

pub(crate) fn redact_text(input: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<serde_json::Value>(input) {
        redact_json_value(&mut value);
        return value.to_string();
    }

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

fn redact_json_value(value: &mut serde_json::Value) {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, value) in fields {
                    if SENSITIVE_KEYS
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(key))
                    {
                        *value = serde_json::Value::String("<redacted>".into());
                    } else {
                        pending.push(value);
                    }
                }
            }
            serde_json::Value::Array(values) => pending.extend(values.iter_mut()),
            // Stream data and callback content can themselves contain encoded JSON.
            serde_json::Value::String(text) => *text = redact_text(text),
            _ => {}
        }
    }
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
    fn redact_text_handles_encoded_json_and_nested_sensitive_values() {
        let inner = r#"{"access_token":"nested-secret","note":"visible"}"#;
        let input = serde_json::json!({
            "data": inner,
            "items": [{"clientSecret": {"value": "object-secret"}}],
            "plain": "Authorization: Bearer plain-secret",
        });
        let output = redact_text(&input.to_string());
        assert!(!output.contains("nested-secret"));
        assert!(!output.contains("object-secret"));
        assert!(!output.contains("plain-secret"));
        let output: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
        let data: serde_json::Value = serde_json::from_str(output["data"].as_str().expect("data"))
            .expect("valid encoded JSON");
        assert_eq!(data["access_token"], "<redacted>");
        assert_eq!(data["note"], "visible");
        assert_eq!(output["items"][0]["clientSecret"], "<redacted>");
    }

    #[test]
    fn redact_text_handles_unicode_escaped_json_keys() {
        let output = redact_text(r#"{"access_\u0074oken":"escaped-secret","visible":true}"#);
        assert!(!output.contains("escaped-secret"));
        let output: serde_json::Value = serde_json::from_str(&output).expect("valid JSON");
        assert_eq!(output["access_token"], "<redacted>");
        assert_eq!(output["visible"], true);
    }

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
    fn redact_text_redacts_temporary_stream_and_webhook_credentials() {
        let redacted = redact_text(
            r#"{"ticket":"stream-ticket","downloadCode":"file-code","pictureDownloadCode":"picture-code","media_id":"media-secret","fileId":"file-secret","spaceId":"space-secret","sessionWebhook":"https://example.test/webhook?token=session-token","sign":"webhook-sign","signature":"callback-sign","corpId":"corp-secret","userId":"user-secret","unionId":"union-secret","operatorUserId":"operator-user-secret","operatorUnionId":"operator-union-secret","senderId":"sender-secret","senderStaffId":"sender-staff-secret","staffId":"staff-secret","dingtalkId":"dingtalk-secret","openConversationId":"conversation-secret","msgId":"message-secret"}"#,
        );

        assert!(redacted.contains(r#""ticket":"<redacted>""#));
        assert!(redacted.contains(r#""downloadCode":"<redacted>""#));
        assert!(redacted.contains(r#""pictureDownloadCode":"<redacted>""#));
        assert!(redacted.contains(r#""media_id":"<redacted>""#));
        assert!(redacted.contains(r#""fileId":"<redacted>""#));
        assert!(redacted.contains(r#""spaceId":"<redacted>""#));
        assert!(redacted.contains(r#""sessionWebhook":"<redacted>""#));
        assert!(redacted.contains(r#""sign":"<redacted>""#));
        assert!(redacted.contains(r#""signature":"<redacted>""#));
        assert!(redacted.contains(r#""corpId":"<redacted>""#));
        assert!(redacted.contains(r#""userId":"<redacted>""#));
        assert!(redacted.contains(r#""unionId":"<redacted>""#));
        assert!(redacted.contains(r#""operatorUserId":"<redacted>""#));
        assert!(redacted.contains(r#""operatorUnionId":"<redacted>""#));
        assert!(redacted.contains(r#""senderId":"<redacted>""#));
        assert!(redacted.contains(r#""senderStaffId":"<redacted>""#));
        assert!(redacted.contains(r#""staffId":"<redacted>""#));
        assert!(redacted.contains(r#""dingtalkId":"<redacted>""#));
        assert!(redacted.contains(r#""openConversationId":"<redacted>""#));
        assert!(redacted.contains(r#""msgId":"<redacted>""#));
        assert!(!redacted.contains("stream-ticket"));
        assert!(!redacted.contains("file-code"));
        assert!(!redacted.contains("picture-code"));
        assert!(!redacted.contains("media-secret"));
        assert!(!redacted.contains("file-secret"));
        assert!(!redacted.contains("space-secret"));
        assert!(!redacted.contains("session-token"));
        assert!(!redacted.contains("webhook-sign"));
        assert!(!redacted.contains("callback-sign"));
        assert!(!redacted.contains("corp-secret"));
        assert!(!redacted.contains("user-secret"));
        assert!(!redacted.contains("union-secret"));
        assert!(!redacted.contains("operator-user-secret"));
        assert!(!redacted.contains("operator-union-secret"));
        assert!(!redacted.contains("sender-secret"));
        assert!(!redacted.contains("sender-staff-secret"));
        assert!(!redacted.contains("staff-secret"));
        assert!(!redacted.contains("dingtalk-secret"));
        assert!(!redacted.contains("conversation-secret"));
        assert!(!redacted.contains("message-secret"));
    }

    #[test]
    fn truncate_snippet_preserves_char_boundaries() {
        assert_eq!(truncate_snippet("钉钉token", 4), "钉...(truncated)");
    }
}

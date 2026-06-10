use url::Url;

use crate::{Error, Result};

pub(crate) fn normalize_base_url(value: impl AsRef<str>) -> Result<Url> {
    let raw = value.as_ref();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidConfig(
            "base_url must not be empty".to_string(),
        ));
    }
    if trimmed != raw {
        return Err(Error::InvalidConfig(
            "base_url must not contain leading or trailing whitespace".to_string(),
        ));
    }
    let mut url = Url::parse(raw).map_err(|source| Error::InvalidConfig(source.to_string()))?;

    if !matches!(url.scheme(), "http" | "https") {
        return Err(Error::InvalidConfig(
            "base_url scheme must be http or https".to_string(),
        ));
    }

    if url.cannot_be_a_base() {
        return Err(Error::InvalidConfig(
            "base_url must be hierarchical".to_string(),
        ));
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidConfig(
            "base_url must not contain username or password".to_string(),
        ));
    }

    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::InvalidConfig(
            "base_url must not contain query or fragment".to_string(),
        ));
    }

    if url.path() != "/" {
        let path = url.path().trim_end_matches('/').to_string();
        url.set_path(if path.is_empty() { "/" } else { &path });
    }

    Ok(url)
}

pub(crate) fn endpoint_url(base_url: &Url, segments: &[&str]) -> Result<Url> {
    if segments.is_empty() {
        return Err(Error::invalid_input(
            "endpoint_segments",
            "at least one path segment is required",
        ));
    }

    let mut url = base_url.clone();
    {
        let mut path_segments = url
            .path_segments_mut()
            .map_err(|()| Error::InvalidConfig("base_url must be hierarchical".to_string()))?;
        for segment in segments {
            validate_endpoint_segment(segment)?;
            path_segments.push(segment);
        }
    }
    Ok(url)
}

fn validate_endpoint_segment(value: &str) -> Result<()> {
    if value.chars().any(char::is_control) {
        return Err(Error::invalid_input(
            "endpoint_segments",
            "path segment must not contain control characters",
        ));
    }
    if value.trim().is_empty() {
        return Err(Error::invalid_input(
            "endpoint_segments",
            "path segment must not be empty",
        ));
    }
    if value.trim() != value {
        return Err(Error::invalid_input(
            "endpoint_segments",
            "path segment must not contain leading or trailing whitespace",
        ));
    }
    if value.contains(['/', '\\']) || matches!(value, "." | "..") {
        return Err(Error::invalid_input(
            "endpoint_segments",
            "path segment must not be a relative path",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_url_appends_segments_to_base_path() {
        let base = normalize_base_url("https://api.example.test/base/").expect("base url");
        let url = endpoint_url(&base, &["v1.0", "robot", "messages"]).expect("endpoint");

        assert_eq!(
            url.as_str(),
            "https://api.example.test/base/v1.0/robot/messages"
        );
    }

    #[test]
    fn endpoint_url_rejects_invalid_segments() {
        let base = normalize_base_url("https://api.example.test").expect("base url");
        for segments in [
            Vec::<&str>::new(),
            vec![""],
            vec![" robot "],
            vec!["robot/messages"],
            vec![".."],
        ] {
            let error = endpoint_url(&base, &segments).expect_err("segment should fail");

            assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        }
    }
}

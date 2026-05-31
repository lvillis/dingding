use url::Url;

use crate::{Error, Result};

pub(crate) fn normalize_base_url(value: impl AsRef<str>) -> Result<Url> {
    let raw = value.as_ref().trim();
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
    let mut url = base_url.clone();
    {
        let mut path_segments = url
            .path_segments_mut()
            .map_err(|()| Error::InvalidConfig("base_url must be hierarchical".to_string()))?;
        for segment in segments {
            path_segments.push(segment);
        }
    }
    Ok(url)
}

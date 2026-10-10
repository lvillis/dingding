pub(crate) const HTTP_BACKEND: reqx::TlsBackend = if cfg!(feature = "async-tls-rustls-ring") {
    reqx::TlsBackend::RustlsRing
} else if cfg!(feature = "async-tls-rustls-aws-lc-rs") {
    reqx::TlsBackend::RustlsAwsLcRs
} else if cfg!(feature = "async-tls-rustls-graviola") {
    reqx::TlsBackend::RustlsGraviola
} else {
    reqx::TlsBackend::NativeTls
};

#[cfg(feature = "stream")]
pub(crate) fn websocket_connector() -> crate::Result<tokio_tungstenite::Connector> {
    #[cfg(feature = "async-tls-native")]
    {
        native_tls::TlsConnector::new()
            .map(tokio_tungstenite::Connector::NativeTls)
            .map_err(|error| {
                crate::Error::stream(format!("websocket TLS initialization failed: {error}"))
            })
    }
    #[cfg(not(feature = "async-tls-native"))]
    {
        rustls_connector(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        })
    }
}

#[cfg(all(feature = "stream", not(feature = "async-tls-native")))]
fn rustls_provider() -> rustls::crypto::CryptoProvider {
    #[cfg(feature = "async-tls-rustls-ring")]
    {
        rustls::crypto::ring::default_provider()
    }
    #[cfg(all(
        not(feature = "async-tls-rustls-ring"),
        feature = "async-tls-rustls-aws-lc-rs"
    ))]
    {
        rustls::crypto::aws_lc_rs::default_provider()
    }
    #[cfg(all(
        not(feature = "async-tls-rustls-ring"),
        not(feature = "async-tls-rustls-aws-lc-rs"),
        feature = "async-tls-rustls-graviola"
    ))]
    {
        rustls_graviola::default_provider()
    }
}

#[cfg(all(feature = "stream", not(feature = "async-tls-native")))]
fn rustls_connector(roots: rustls::RootCertStore) -> crate::Result<tokio_tungstenite::Connector> {
    let config =
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|error| {
                crate::Error::stream(format!("websocket TLS initialization failed: {error}"))
            })?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(tokio_tungstenite::Connector::Rustls(std::sync::Arc::new(
        config,
    )))
}

#[cfg(all(test, feature = "stream"))]
mod tests {
    use super::*;

    #[test]
    fn websocket_connector_uses_the_selected_backend() {
        let connector = websocket_connector().expect("connector");
        #[cfg(feature = "async-tls-native")]
        assert!(matches!(
            connector,
            tokio_tungstenite::Connector::NativeTls(_)
        ));
        #[cfg(not(feature = "async-tls-native"))]
        assert!(matches!(connector, tokio_tungstenite::Connector::Rustls(_)));
    }

    #[cfg(not(feature = "async-tls-native"))]
    #[test]
    fn client_construction_does_not_install_a_global_provider() {
        let before = rustls::crypto::CryptoProvider::get_default().cloned();
        crate::DingTalk::new().expect("HTTP client");
        websocket_connector().expect("WebSocket connector");
        let after = rustls::crypto::CryptoProvider::get_default().cloned();
        match (before, after) {
            (None, None) => {}
            (Some(before), Some(after)) => assert!(std::sync::Arc::ptr_eq(&before, &after)),
            _ => panic!("client construction changed the process-wide provider"),
        }
    }

    #[cfg(not(feature = "async-tls-native"))]
    #[tokio::test]
    async fn rustls_websocket_handshake_and_certificate_verification() {
        use futures_util::{SinkExt, StreamExt};
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
        use std::{sync::Arc, time::Duration};
        use tokio_tungstenite::{
            accept_async, connect_async_tls_with_config, tungstenite::Message,
        };

        // These certificates and the private key are test-only loopback fixtures.
        let certificate =
            CertificateDer::from(include_bytes!("../tests/fixtures/tls/localhost.der").to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            include_bytes!("../tests/fixtures/tls/localhost-key.der").to_vec(),
        ));
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls_provider()))
            .with_safe_default_protocol_versions()
            .expect("TLS versions")
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .expect("server certificate");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

        for trusted in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listen");
            let address = listener.local_addr().expect("address");
            let acceptor = acceptor.clone();
            let server = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let (socket, _) = listener.accept().await.expect("accept");
                    let tls = acceptor.accept(socket).await;
                    if !trusted {
                        assert!(tls.is_err());
                        return;
                    }
                    let mut socket = accept_async(tls.expect("TLS handshake"))
                        .await
                        .expect("WebSocket handshake");
                    let message = socket.next().await.expect("request").expect("message");
                    assert_eq!(message, Message::Text("ping".into()));
                    socket
                        .send(Message::Text("pong".into()))
                        .await
                        .expect("response");
                })
                .await
                .expect("server deadline");
            });
            let connector = if trusted {
                let mut roots = rustls::RootCertStore::empty();
                roots
                    .add(CertificateDer::from(
                        include_bytes!("../tests/fixtures/tls/ca.der").to_vec(),
                    ))
                    .expect("CA");
                rustls_connector(roots).expect("connector")
            } else {
                websocket_connector().expect("connector")
            };
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                connect_async_tls_with_config(
                    format!("wss://{address}"),
                    None,
                    false,
                    Some(connector),
                ),
            )
            .await
            .expect("connection deadline");
            if trusted {
                let (mut socket, _) = result.expect("TLS WebSocket connection");
                socket
                    .send(Message::Text("ping".into()))
                    .await
                    .expect("request");
                let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
                    .await
                    .expect("response deadline")
                    .expect("response")
                    .expect("message");
                assert_eq!(message, Message::Text("pong".into()));
            } else {
                assert!(result.is_err(), "untrusted certificates must be rejected");
            }
            server.await.expect("server task");
        }
    }
}

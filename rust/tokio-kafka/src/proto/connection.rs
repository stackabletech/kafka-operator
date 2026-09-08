//! A single mTLS connection to one Kafka broker — the wire driver.
//!
//! The tokio-zookeeper `active_packetizer.rs` analog, but far smaller: Kafka admin has **no
//! server-push** (no watches) and **no session** to resume, so there is no background reader task,
//! no reply/​watcher maps, and no reconnect-with-session-id. A connection is just a TLS stream plus a
//! correlation counter and the broker's negotiated api-version table. The reused-connection lifecycle
//! (open once, route to the controller, drop on error) lives one level up in [`crate::Kafka`].

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use kafka_protocol::{
    messages::{ApiVersionsRequest, MetadataRequest, MetadataResponse},
    protocol::{Request, StrBytes, VersionRange},
};
use rustls::{ClientConfig, pki_types::ServerName};
use snafu::ResultExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use super::codec;
use crate::error::{
    ConnectSnafu, Error, InvalidServerNameSnafu, ReadResponseSnafu, TlsHandshakeSnafu,
    UnsupportedApiSnafu, WriteRequestSnafu,
};

/// Client id advertised in the request header (informational; shows up in broker request logs).
pub(crate) const CLIENT_ID: &str = "stackable-tokio-kafka";

/// Client-side backstop for a single request round-trip. A wedged broker/controller (e.g. mid
/// quorum-loss) can leave `read_exact` blocked forever; this converts that into a retryable
/// `ReadResponse` error. Set comfortably above the per-request `timeout_ms` we send (≤60s), so a
/// slow-but-working broker still gets to answer.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// A live mTLS connection to one broker, with the negotiated api-version table.
pub(crate) struct Connection {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
    /// The broker this connection is pointed at (`host`, `port`) — used to decide whether a
    /// controller re-route actually needs to reconnect.
    peer: (String, u16),
    /// Next correlation id to hand out; incremented per request.
    correlation_id: i32,
    /// api key -> broker-supported `[min, max]` version range, from the `ApiVersions` handshake.
    api_versions: HashMap<i16, VersionRange>,
}

impl Connection {
    /// Opens a TCP + rustls connection to `host:port` and performs the `ApiVersions`(18) handshake.
    pub(crate) async fn connect(
        host: &str,
        port: u16,
        config: Arc<ClientConfig>,
    ) -> Result<Self, Error> {
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(&addr)
            .await
            .context(ConnectSnafu { addr: addr.clone() })?;
        // The server certificate is issued for the listener FQDN, so the SNI/verification name is the
        // configured host, not the resolved IP.
        let server_name = ServerName::try_from(host.to_owned())
            .context(InvalidServerNameSnafu { host: host.to_owned() })?;
        let stream = TlsConnector::from(config)
            .connect(server_name, tcp)
            .await
            .context(TlsHandshakeSnafu { addr })?;

        let mut conn = Connection {
            stream,
            peer: (host.to_owned(), port),
            correlation_id: 0,
            api_versions: HashMap::new(),
        };
        conn.handshake().await?;
        Ok(conn)
    }

    /// Whether this connection is already pointed at `host:port`.
    pub(crate) fn peer_is(&self, host: &str, port: u16) -> bool {
        self.peer.0 == host && self.peer.1 == port
    }

    /// Learns the broker's supported api-version ranges (`ApiVersions`, api 18).
    ///
    /// Pins the *request* to v3 (the first flexible version, supported by any modern broker); the
    /// reply gives the real range for every other api so [`Self::pick_version`] can negotiate.
    async fn handshake(&mut self) -> Result<(), Error> {
        let req = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str(CLIENT_ID))
            .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")));
        let resp = self.send(3, "ApiVersions", req).await?;
        if resp.error_code != 0 {
            return Err(Error::from_code("ApiVersions", resp.error_code, None));
        }
        self.api_versions = resp
            .api_keys
            .iter()
            .map(|k| {
                (
                    k.api_key,
                    VersionRange {
                        min: k.min_version,
                        max: k.max_version,
                    },
                )
            })
            .collect();
        Ok(())
    }

    /// Picks the highest api version both we (`R::VERSIONS`) and the broker support.
    pub(crate) fn pick_version<R: Request>(
        &self,
        request: &'static str,
    ) -> Result<i16, Error> {
        let ours = R::VERSIONS;
        // If the broker didn't advertise the api at all, fall back to our own range and let the
        // request fail loudly with a protocol error rather than silently.
        let theirs = self.api_versions.get(&R::KEY).copied().unwrap_or(ours);
        let range = ours.intersect(&theirs);
        if range.is_empty() {
            return UnsupportedApiSnafu { request }.fail();
        }
        Ok(range.max)
    }

    /// The single generic request/response round-trip: frame → write → read length-prefixed reply →
    /// decode. Framing/decoding is delegated to [`codec`]; this method owns only the socket IO.
    pub(crate) async fn send<R>(
        &mut self,
        api_version: i16,
        request: &'static str,
        body: R,
    ) -> Result<R::Response, Error>
    where
        R: Request,
    {
        let correlation_id = self.correlation_id;
        self.correlation_id += 1;

        let framed = codec::frame_request(CLIENT_ID, api_version, correlation_id, request, &body)?;

        // The write + read round-trip, bounded by REQUEST_TIMEOUT so a wedged peer can't block forever.
        let body_buf = tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.stream
                .write_all(&framed)
                .await
                .context(WriteRequestSnafu { request })?;
            self.stream
                .flush()
                .await
                .context(WriteRequestSnafu { request })?;

            // Read the 4-byte length prefix, then exactly that many bytes.
            let mut len_buf = [0u8; 4];
            self.stream
                .read_exact(&mut len_buf)
                .await
                .context(ReadResponseSnafu { request })?;
            let len = i32::from_be_bytes(len_buf).max(0) as usize;
            let mut body_buf = vec![0u8; len];
            self.stream
                .read_exact(&mut body_buf)
                .await
                .context(ReadResponseSnafu { request })?;
            Ok::<Vec<u8>, Error>(body_buf)
        })
        .await
        // Elapsed → a retryable, connection-fatal `ReadResponse` (the stream state is now undefined).
        .map_err(|_elapsed| Error::ReadResponse {
            source: std::io::Error::new(std::io::ErrorKind::TimedOut, "request timed out"),
            request,
        })??;

        codec::decode_response::<R>(Bytes::from(body_buf), api_version, correlation_id, request)
    }

    /// Fetches metadata for all topics (`Metadata`, api 3). Also carries `controller_id` + the broker
    /// list, which [`crate::cluster`] uses to route controller-only admin ops.
    pub(crate) async fn metadata(&mut self) -> Result<MetadataResponse, Error> {
        let version = self.pick_version::<MetadataRequest>("Metadata")?;
        let req = MetadataRequest::default()
            .with_topics(None)
            .with_allow_auto_topic_creation(false);
        self.send(version, "Metadata", req).await
    }
}

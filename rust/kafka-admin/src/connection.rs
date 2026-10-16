//! A connection to a single Kafka broker.

use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc, time::Duration};

use futures::{SinkExt, StreamExt};
use kafka_protocol::{
    ResponseError,
    error::ParseResponseErrorCode,
    messages::{ApiVersionsRequest, ApiVersionsResponse},
    protocol::{Request, StrBytes, VersionRange},
};
use rustls::{ClientConfig, pki_types::ServerName};
use snafu::{OptionExt, ResultExt, Snafu, ensure};
use tokio::{
    net::TcpStream,
    time::{error::Elapsed, timeout},
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_util::{
    codec::{Framed, LengthDelimitedCodec},
    either::Either,
};

use crate::{
    codec::{self, decode_response, encode_request},
    tls,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const API_VERSIONS_VERSION: i16 = 3;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to connect to {address:?}"))]
    Connect { source: io::Error, address: String },

    #[snafu(display("timed out connecting to {address:?}"))]
    ConnectTimeout { source: Elapsed, address: String },

    #[snafu(display("failed to configure TLS"))]
    Tls { source: tls::Error },

    #[snafu(display("{host:?} is not a valid TLS server name"))]
    InvalidServerName { host: String },

    #[snafu(display("failed the TLS handshake with {address:?}"))]
    TlsHandshake { source: io::Error, address: String },

    #[snafu(display("failed to write to the broker"))]
    Write { source: io::Error },

    #[snafu(display("failed to read from the broker"))]
    Read { source: io::Error },

    #[snafu(display("the broker closed the connection"))]
    ConnectionClosed,

    #[snafu(display("the connection is broken by an earlier failed or cancelled request"))]
    ConnectionBroken,

    #[snafu(display("timed out waiting for the response to the request with API key {api_key}"))]
    RequestTimeout { source: Elapsed, api_key: i16 },

    #[snafu(display("failed to encode or decode a message"))]
    Codec { source: codec::Error },

    #[snafu(display("the broker returned {error:?} for the request with API key {api_key}"))]
    Kafka { error: ResponseError, api_key: i16 },

    #[snafu(display("the broker does not support the API key {api_key}"))]
    UnsupportedApi { api_key: i16 },

    #[snafu(display(
        "the broker supports the versions {broker_versions} of the API key {api_key}, but not {version}"
    ))]
    UnsupportedVersion {
        api_key: i16,
        version: i16,
        broker_versions: VersionRange,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// How to connect to the broker. Paths point to PEM files.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Security {
    Plaintext,
    Tls {
        ca: PathBuf,
    },
    MutualTls {
        ca: PathBuf,
        certificate: PathBuf,
        key: PathBuf,
    },
}

pub struct Connection {
    stream: Framed<Either<TcpStream, TlsStream<TcpStream>>, LengthDelimitedCodec>,
    client_id: String,
    correlation_id: i32,
    awaiting_response: bool,
    broker_api_versions: BTreeMap<i16, VersionRange>,
}

impl Connection {
    /// Connects to the broker and fetches the API versions it supports.
    pub async fn connect(
        host: &str,
        port: u16,
        security: &Security,
        client_id: &str,
    ) -> Result<Self> {
        let address = format!("{host}:{port}");
        let tcp_stream = connect_tcp(host, port, &address).await?;
        let stream = match tls::client_config(security).context(TlsSnafu)? {
            None => Either::Left(tcp_stream),
            Some(config) => Either::Right(tls_handshake(tcp_stream, host, &address, config).await?),
        };

        let mut connection = Self::new(stream, client_id);
        let response = connection
            .send_with_version(api_versions_request(), API_VERSIONS_VERSION)
            .await?;
        connection.broker_api_versions = api_versions_result(response)?;
        Ok(connection)
    }

    fn new(stream: Either<TcpStream, TlsStream<TcpStream>>, client_id: &str) -> Self {
        let codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .max_frame_length(MAX_FRAME_SIZE)
            .new_codec();
        Self {
            stream: Framed::new(stream, codec),
            client_id: client_id.to_owned(),
            correlation_id: 0,
            awaiting_response: false,
            broker_api_versions: BTreeMap::new(),
        }
    }

    /// Sends the request with the given version, if the broker supports it.
    pub async fn send<R: Request>(&mut self, request: R, version: i16) -> Result<R::Response> {
        // Each API that is relevant for us has a version that is supported by all of our Kafka versions.
        // We use these versions and pin them in client.rs.
        // We will have to add API version negotiation at some point in the future.
        check_version(&self.broker_api_versions, R::KEY, version)?;
        self.send_with_version(request, version).await
    }

    async fn send_with_version<R: Request>(
        &mut self,
        request: R,
        version: i16,
    ) -> Result<R::Response> {
        ensure!(!self.awaiting_response, ConnectionBrokenSnafu);
        self.awaiting_response = true;

        self.correlation_id = self.correlation_id.wrapping_add(1);
        let frame = encode_request(&request, version, self.correlation_id, &self.client_id)
            .context(CodecSnafu)?;

        let response = timeout(REQUEST_TIMEOUT, async {
            self.stream.send(frame).await.context(WriteSnafu)?;
            self.stream
                .next()
                .await
                .context(ConnectionClosedSnafu)?
                .context(ReadSnafu)
        })
        .await
        .context(RequestTimeoutSnafu { api_key: R::KEY })??;

        let response = decode_response::<R>(response.freeze(), version, self.correlation_id)
            .context(CodecSnafu)?;

        self.awaiting_response = false;
        Ok(response)
    }
}

async fn connect_tcp(host: &str, port: u16, address: &str) -> Result<TcpStream> {
    timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .context(ConnectTimeoutSnafu { address })?
        .context(ConnectSnafu { address })
}

fn api_versions_request() -> ApiVersionsRequest {
    ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("stackable-kafka-admin"))
        .with_client_software_version(StrBytes::from_static_str(env!("CARGO_PKG_VERSION")))
}

fn api_versions_result(response: ApiVersionsResponse) -> Result<BTreeMap<i16, VersionRange>> {
    if let Some(error) = response.error_code.err() {
        return KafkaSnafu {
            error,
            api_key: ApiVersionsRequest::KEY,
        }
        .fail();
    }

    Ok(response
        .api_keys
        .iter()
        .map(|api| {
            (
                api.api_key,
                VersionRange {
                    min: api.min_version,
                    max: api.max_version,
                },
            )
        })
        .collect())
}

fn check_version(
    broker_api_versions: &BTreeMap<i16, VersionRange>,
    api_key: i16,
    version: i16,
) -> Result<()> {
    let broker_versions = broker_api_versions
        .get(&api_key)
        .context(UnsupportedApiSnafu { api_key })?;
    ensure!(
        (broker_versions.min..=broker_versions.max).contains(&version),
        UnsupportedVersionSnafu {
            api_key,
            version,
            broker_versions: *broker_versions,
        }
    );
    Ok(())
}

async fn tls_handshake(
    tcp_stream: TcpStream,
    host: &str,
    address: &str,
    config: ClientConfig,
) -> Result<TlsStream<TcpStream>> {
    let server_name = ServerName::try_from(host.to_owned())
        .ok()
        .context(InvalidServerNameSnafu { host })?;
    timeout(
        CONNECT_TIMEOUT,
        TlsConnector::from(Arc::new(config)).connect(server_name, tcp_stream),
    )
    .await
    .context(ConnectTimeoutSnafu { address })?
    .context(TlsHandshakeSnafu { address })
}

/// A mock Kafka broker for tests, speaking the wire protocol over a local TCP socket.
#[cfg(test)]
pub(crate) mod test_support {
    use bytes::{BufMut, Bytes, BytesMut};
    use kafka_protocol::{
        messages::{
            ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
            api_versions_response::ApiVersion,
        },
        protocol::{Encodable, HeaderVersion, Request, decode_request_header_from_buffer},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::API_VERSIONS_VERSION;

    pub async fn listen() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the mock broker should be able to listen");
        let port = listener
            .local_addr()
            .expect("the mock broker should have an address")
            .port();
        (listener, port)
    }

    /// Accepts a connection and answers its ApiVersions request with the given
    /// `(api_key, min_version, max_version)` ranges.
    pub async fn accept(listener: &TcpListener, apis: &[(i16, i16, i16)]) -> TcpStream {
        let (mut broker, _) = listener
            .accept()
            .await
            .expect("the mock broker should accept the connection");
        let (request, _) = read_request(&mut broker).await;
        assert_eq!(ApiVersionsRequest::KEY, request.request_api_key);
        assert_eq!(API_VERSIONS_VERSION, request.request_api_version);
        write_response(
            &mut broker,
            &request,
            request.correlation_id,
            api_versions(apis),
        )
        .await;
        broker
    }

    pub async fn read_request(broker: &mut TcpStream) -> (RequestHeader, Bytes) {
        let size = broker
            .read_i32()
            .await
            .expect("the request size should be readable");
        let mut frame = vec![0; size as usize];
        broker
            .read_exact(&mut frame)
            .await
            .expect("the request should be readable");
        let mut frame = Bytes::from(frame);
        let header = decode_request_header_from_buffer(&mut frame)
            .expect("the request header should be decodable");
        (header, frame)
    }

    pub async fn read_request_body<R: Request>(broker: &mut TcpStream) -> (RequestHeader, R) {
        let (header, mut body) = read_request(broker).await;
        assert_eq!(R::KEY, header.request_api_key);
        let request = R::decode(&mut body, header.request_api_version)
            .expect("the request should be decodable");
        (header, request)
    }

    pub async fn write_response<M: Encodable + HeaderVersion>(
        broker: &mut TcpStream,
        request: &RequestHeader,
        correlation_id: i32,
        response: M,
    ) {
        let mut body = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation_id)
            .encode(&mut body, M::header_version(request.request_api_version))
            .expect("the response header should be encodable");
        response
            .encode(&mut body, request.request_api_version)
            .expect("the response should be encodable");
        let mut frame = BytesMut::new();
        frame.put_i32(body.len() as i32);
        frame.extend_from_slice(&body);
        broker
            .write_all(&frame)
            .await
            .expect("the response should be writable");
    }

    fn api_versions(apis: &[(i16, i16, i16)]) -> ApiVersionsResponse {
        ApiVersionsResponse::default().with_api_keys(
            apis.iter()
                .map(|(api_key, min_version, max_version)| {
                    ApiVersion::default()
                        .with_api_key(*api_key)
                        .with_min_version(*min_version)
                        .with_max_version(*max_version)
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{
        MetadataRequest, MetadataResponse, api_versions_response::ApiVersion,
    };
    use tokio::io::AsyncWriteExt;

    use super::{
        test_support::{accept, listen, read_request, write_response},
        *,
    };

    /// Connects to a mock broker supporting Metadata versions 0 to 13.
    async fn connect() -> (Connection, TcpStream) {
        let (listener, port) = listen().await;
        let (connection, broker) = tokio::join!(
            Connection::connect("127.0.0.1", port, &Security::Plaintext, "test"),
            accept(&listener, &[(MetadataRequest::KEY, 0, 13)])
        );
        (connection.expect("the connection should succeed"), broker)
    }

    #[test]
    fn read_api_versions() {
        let response = ApiVersionsResponse::default().with_api_keys(vec![
            ApiVersion::default()
                .with_api_key(MetadataRequest::KEY)
                .with_min_version(0)
                .with_max_version(13),
        ]);

        assert_eq!(
            BTreeMap::from([(MetadataRequest::KEY, VersionRange { min: 0, max: 13 })]),
            api_versions_result(response).expect("the API versions should be valid")
        );
    }

    #[test]
    fn reject_api_versions_with_error() {
        let response = ApiVersionsResponse::default()
            .with_error_code(ResponseError::UnsupportedVersion.code());

        let error = api_versions_result(response).expect_err("the response should be rejected");

        assert!(matches!(
            error,
            Error::Kafka {
                error: ResponseError::UnsupportedVersion,
                ..
            }
        ));
    }

    #[test]
    fn accept_supported_version() {
        let broker_api_versions =
            BTreeMap::from([(MetadataRequest::KEY, VersionRange { min: 0, max: 13 })]);

        check_version(&broker_api_versions, MetadataRequest::KEY, 12)
            .expect("the version should be supported");
    }

    #[test]
    fn reject_api_unknown_to_the_broker() {
        let error = check_version(&BTreeMap::new(), MetadataRequest::KEY, 12)
            .expect_err("the API should be unsupported");

        assert!(matches!(error, Error::UnsupportedApi { .. }));
    }

    #[test]
    fn reject_version_unsupported_by_the_broker() {
        let broker_api_versions =
            BTreeMap::from([(MetadataRequest::KEY, VersionRange { min: 0, max: 9 })]);

        let error = check_version(&broker_api_versions, MetadataRequest::KEY, 12)
            .expect_err("the version should be unsupported");

        assert!(matches!(
            error,
            Error::UnsupportedVersion { version: 12, .. }
        ));
    }

    #[tokio::test]
    async fn send_request_and_receive_response() {
        let (mut connection, mut broker) = connect().await;

        let broker_side = async {
            let (request, _) = read_request(&mut broker).await;
            assert_eq!(MetadataRequest::KEY, request.request_api_key);
            assert_eq!(12, request.request_api_version);
            write_response(
                &mut broker,
                &request,
                request.correlation_id,
                MetadataResponse::default().with_controller_id(2.into()),
            )
            .await;
        };
        let (response, ()) =
            tokio::join!(connection.send(MetadataRequest::default(), 12), broker_side);

        assert_eq!(
            2,
            *response.expect("the request should succeed").controller_id
        );
    }

    #[tokio::test]
    async fn reject_response_with_other_correlation_id() {
        let (mut connection, mut broker) = connect().await;

        let broker_side = async {
            let (request, _) = read_request(&mut broker).await;
            write_response(
                &mut broker,
                &request,
                request.correlation_id + 1,
                MetadataResponse::default(),
            )
            .await;
        };
        let (response, ()) =
            tokio::join!(connection.send(MetadataRequest::default(), 12), broker_side);

        assert!(matches!(
            response.expect_err("the correlation id should not match"),
            Error::Codec {
                source: codec::Error::CorrelationIdMismatch { .. }
            }
        ));
    }

    #[tokio::test]
    async fn reject_invalid_frame_size() {
        let (mut connection, mut broker) = connect().await;

        let broker_side = async {
            read_request(&mut broker).await;
            broker
                .write_i32(-1)
                .await
                .expect("the frame size should be writable");
        };
        let (response, ()) =
            tokio::join!(connection.send(MetadataRequest::default(), 12), broker_side);

        assert!(matches!(
            response.expect_err("the frame size should be invalid"),
            Error::Read { .. }
        ));
    }

    #[tokio::test]
    async fn time_out_without_response() {
        let (mut connection, mut broker) = connect().await;
        tokio::time::pause();

        let broker_side = async {
            read_request(&mut broker).await;
        };
        let (response, ()) =
            tokio::join!(connection.send(MetadataRequest::default(), 12), broker_side);

        assert!(matches!(
            response.expect_err("the request should time out"),
            Error::RequestTimeout { .. }
        ));
    }

    #[tokio::test]
    async fn reject_requests_after_a_failed_request() {
        let (mut connection, mut broker) = connect().await;

        let broker_side = async {
            read_request(&mut broker).await;
            drop(broker);
        };
        let (failed, ()) =
            tokio::join!(connection.send(MetadataRequest::default(), 12), broker_side);
        assert!(matches!(
            failed.expect_err("the broker should have closed the connection"),
            Error::ConnectionClosed
        ));

        let error = connection
            .send(MetadataRequest::default(), 12)
            .await
            .expect_err("the connection should be broken");

        assert!(matches!(error, Error::ConnectionBroken));
    }

    #[tokio::test]
    async fn reject_requests_after_a_cancelled_request() {
        let (mut connection, _broker) = connect().await;

        {
            let request = connection.send(MetadataRequest::default(), 12);
            tokio::pin!(request);
            let _ = futures::poll!(request.as_mut());
        }

        let error = connection
            .send(MetadataRequest::default(), 12)
            .await
            .expect_err("the connection should be broken");

        assert!(matches!(error, Error::ConnectionBroken));
    }
}

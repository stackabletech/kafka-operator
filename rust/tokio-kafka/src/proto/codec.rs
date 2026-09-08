    //! Wire framing + header-version handling — the pure (no-IO) half of the round-trip.
//!
//! This is the tokio-zookeeper `proto/request.rs` + `proto/response.rs` analog, but instead of a
//! hand-rolled `enum Request` / `enum Response` with manual `byteorder` serialization, it delegates
//! the actual message encode/decode to the generated [`kafka_protocol`] types and only owns the two
//! Kafka-specific framing concerns:
//!
//! * **Length framing** — every message on the wire is `INT32(len)` followed by the header+body.
//! * **Header versions** — the header struct wrapping a request/response is itself versioned, and its
//!   version is a function of the *message* api version, derived per-type via [`HeaderVersion`].
//!   Note the `ApiVersions` quirk: its *request* header is flexible (v2) for api v3+, but its
//!   *response* header is always v0 so an old broker can still reply — deriving the header version
//!   from the message type handles this automatically; never hard-code it.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::{
    messages::{RequestHeader, ResponseHeader},
    protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes},
};

use crate::error::Error;

/// Encodes `[RequestHeader][body]` at the right header/message versions and prepends the `INT32`
/// length prefix, returning the ready-to-write frame.
pub(crate) fn frame_request<R>(
    client_id: &'static str,
    api_version: i16,
    correlation_id: i32,
    request: &'static str,
    body: &R,
) -> Result<BytesMut, Error>
where
    R: Request,
{
    let header = RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str(client_id)));

    let req_header_version = R::header_version(api_version);
    let mut payload = BytesMut::new();
    header
        .encode(&mut payload, req_header_version)
        .map_err(|source| Error::Encode { source, request })?;
    body.encode(&mut payload, api_version)
        .map_err(|source| Error::Encode { source, request })?;

    let mut framed = BytesMut::with_capacity(payload.len() + 4);
    framed.put_i32(payload.len() as i32);
    framed.extend_from_slice(&payload);
    Ok(framed)
}

/// Decodes a response body (the bytes *after* the length prefix): reads the `ResponseHeader` at the
/// response type's header version, checks the correlation id, then decodes `R::Response`.
pub(crate) fn decode_response<R>(
    mut buf: Bytes,
    api_version: i16,
    expected_correlation_id: i32,
    request: &'static str,
) -> Result<R::Response, Error>
where
    R: Request,
{
    let resp_header_version = R::Response::header_version(api_version);
    let resp_header = ResponseHeader::decode(&mut buf, resp_header_version)
        .map_err(|source| Error::Decode { source, request })?;
    if resp_header.correlation_id != expected_correlation_id {
        return Err(Error::CorrelationMismatch {
            request,
            expected: expected_correlation_id,
            actual: resp_header.correlation_id,
        });
    }

    R::Response::decode(&mut buf, api_version).map_err(|source| Error::Decode { source, request })
}

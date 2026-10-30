//! Encoding of request and decoding of response frames: a header followed by a message body. The
//! length prefix is added and removed by the connection.

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    messages::{ApiKey, RequestHeader, ResponseHeader},
    protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes},
};
use snafu::{ResultExt, Snafu, ensure};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to encode the {api_key:?} request"))]
    EncodeRequest {
        source: anyhow::Error,
        api_key: ApiKey,
    },

    #[snafu(display("failed to decode the {api_key:?} response"))]
    DecodeResponse {
        source: anyhow::Error,
        api_key: ApiKey,
    },

    #[snafu(display("expected correlation id {expected}, received {received}"))]
    CorrelationIdMismatch { expected: i32, received: i32 },
}

pub fn api_key<R: Request>() -> ApiKey {
    ApiKey::try_from(R::KEY).expect("kafka-protocol generates the requests and their API keys")
}

pub fn encode_request<R: Request>(
    request: &R,
    version: i16,
    correlation_id: i32,
    client_id: &str,
) -> Result<Bytes, Error> {
    let header = RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_owned())));

    let mut frame = BytesMut::new();
    header
        .encode(&mut frame, R::header_version(version))
        .context(EncodeRequestSnafu {
            api_key: api_key::<R>(),
        })?;
    request
        .encode(&mut frame, version)
        .context(EncodeRequestSnafu {
            api_key: api_key::<R>(),
        })?;
    Ok(frame.freeze())
}

pub fn decode_response<R: Request>(
    mut frame: Bytes,
    version: i16,
    correlation_id: i32,
) -> Result<R::Response, Error> {
    let header = ResponseHeader::decode(&mut frame, R::Response::header_version(version)).context(
        DecodeResponseSnafu {
            api_key: api_key::<R>(),
        },
    )?;
    ensure!(
        header.correlation_id == correlation_id,
        CorrelationIdMismatchSnafu {
            expected: correlation_id,
            received: header.correlation_id,
        }
    );
    R::Response::decode(&mut frame, version).context(DecodeResponseSnafu {
        api_key: api_key::<R>(),
    })
}

#[cfg(test)]
mod tests {
    use kafka_protocol::{
        messages::{MetadataRequest, MetadataResponse},
        protocol::decode_request_header_from_buffer,
    };

    use super::*;

    #[test]
    fn encode_request_with_header() {
        let mut frame = encode_request(&MetadataRequest::default(), 12, 7, "agent")
            .expect("the request should be encodable");

        let header =
            decode_request_header_from_buffer(&mut frame).expect("the header should be decodable");
        assert_eq!(ApiKey::Metadata as i16, header.request_api_key);
        assert_eq!(12, header.request_api_version);
        assert_eq!(7, header.correlation_id);
        assert_eq!(Some("agent"), header.client_id.as_deref());
    }

    fn response_frame(correlation_id: i32, version: i16, response: &MetadataResponse) -> Bytes {
        let mut frame = BytesMut::new();
        ResponseHeader::default()
            .with_correlation_id(correlation_id)
            .encode(&mut frame, MetadataResponse::header_version(version))
            .expect("the header should be encodable");
        response
            .encode(&mut frame, version)
            .expect("the response should be encodable");
        frame.freeze()
    }

    #[test]
    fn decode_response_with_matching_correlation_id() {
        let response = MetadataResponse::default().with_controller_id(2.into());

        let decoded = decode_response::<MetadataRequest>(response_frame(7, 12, &response), 12, 7)
            .expect("the response should be decodable");

        assert_eq!(response, decoded);
    }

    #[test]
    fn reject_response_with_other_correlation_id() {
        let frame = response_frame(8, 12, &MetadataResponse::default());

        let error = decode_response::<MetadataRequest>(frame, 12, 7)
            .expect_err("the correlation id should not match");

        assert!(matches!(
            error,
            Error::CorrelationIdMismatch {
                expected: 7,
                received: 8
            }
        ));
    }
}

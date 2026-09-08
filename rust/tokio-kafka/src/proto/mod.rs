//! The wire layer: transport, framing/codec, and mTLS setup.
//!
//! Mirrors tokio-zookeeper's `proto/` split, minus everything watch/session-related. The public
//! surface out of here is just [`Connection`]; the generated message types live in
//! [`kafka_protocol`] rather than in hand-rolled `request`/`response` modules.

mod codec;
mod connection;
pub(crate) mod tls;

pub(crate) use connection::Connection;

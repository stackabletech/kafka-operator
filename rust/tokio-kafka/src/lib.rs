//! An asynchronous, native-Rust client for the Apache Kafka **admin** wire protocol.
//!
//! Modelled on [`tokio-zookeeper`](https://docs.rs/tokio-zookeeper)'s structure, but scoped to the
//! admin operations a Stackable per-cluster agent needs — topic CRUD, broker draining (partition
//! reassignment) and post-drain broker unregister — spoken directly over `tokio` + `rustls` mTLS via
//! the generated [`kafka_protocol`] message types. No librdkafka, no JVM: it builds in a bare
//! sandbox.
//!
//! # Shape vs. tokio-zookeeper
//!
//! The module layout mirrors tokio-zookeeper (`proto/` codec, `types/`, a `recipes/`-style
//! [`reassignment`] module, a builder + client entrypoint), but with the parts that don't apply to
//! Kafka admin deliberately dropped:
//!
//! * **No background packetizer task / `Enqueuer`.** tokio-zookeeper runs a driver task multiplexing
//!   requests over one long-lived session — machinery it needs for **watches** (server-push events
//!   interleaved with responses) and **session resumption**. Kafka admin has neither, so the honest
//!   analog is a single connection reused behind a lock ([`tokio::sync::Mutex`]). Ops are sequential;
//!   that's fine at an agent's op rate.
//! * **No hand-rolled `request`/`response` enums.** [`kafka_protocol`] generates the message types
//!   and their codec; [`proto::codec`](crate) only owns Kafka's length framing + header versioning.
//! * **Controller routing instead of watches** — the one Kafka-specific addition (see [`cluster`]).
//!
//! # Example
//!
//! ```no_run
//! use tokio_kafka::{Kafka, KafkaConfig, NewTopic};
//! # async fn run() -> Result<(), tokio_kafka::Error> {
//! let admin = Kafka::connect(KafkaConfig {
//!     bootstrap_servers: "broker-0:9093,broker-1:9093".to_owned(),
//!     cert_dir: Some("/stackable/tls".into()),
//!     server_ca_dir: None,
//! })
//! .await?;
//!
//! admin
//!     .ensure_topic(&NewTopic {
//!         name: "events".to_owned(),
//!         partitions: 6,
//!         replication_factor: 2,
//!         config: Default::default(),
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use backon::{ExponentialBuilder, Retryable};
use kafka_protocol::messages::MetadataResponse;
use snafu::OptionExt;
use tokio::sync::Mutex;

mod admin;
mod cluster;
pub mod error;
mod proto;
mod reassignment;
mod types;

pub use admin::KafkaAdmin;
pub use error::Error;
pub use reassignment::ReassignProgress;
pub use types::NewTopic;

use crate::{
    error::{MissingCredentialSnafu, NoBootstrapServerSnafu},
    proto::{Connection, tls},
};

/// Bounded exponential backoff for a single admin op — the fast, in-client retry layer. Kept short
/// (a few attempts over a few seconds) so one call doesn't block a reconcile for long; the caller's
/// reconcile-requeue is the slow outer retry for longer outages. Only retryable errors are retried
/// (see [`Error::is_retryable`]).
const ADMIN_BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_jitter()
    .with_min_delay(Duration::from_millis(200))
    .with_max_delay(Duration::from_secs(3))
    .with_max_times(3);

/// Connection parameters for [`Kafka::connect`].
#[derive(Clone, Debug)]
pub struct KafkaConfig {
    /// Comma-separated `<host>:<port>` bootstrap servers; tried in order until one connects.
    pub bootstrap_servers: String,
    /// Directory the client credential (`tls.crt` / `tls.key` / `ca.crt`) is mounted at. Required —
    /// this client is mTLS-only (SASL/Kerberos are out of scope).
    pub cert_dir: Option<PathBuf>,
    /// Directory the Kafka *server's* CA (`ca.crt`) is mounted at (cross-CA mTLS). Falls back to
    /// `cert_dir` when unset.
    pub server_ca_dir: Option<PathBuf>,
}

/// A builder mirroring tokio-zookeeper's `ZooKeeperBuilder` (minus the returned watch stream, which
/// Kafka admin has no equivalent for).
#[derive(Clone, Debug, Default)]
pub struct KafkaBuilder {
    cert_dir: Option<PathBuf>,
    server_ca_dir: Option<PathBuf>,
}

impl KafkaBuilder {
    /// A builder with no TLS configured yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure mTLS: client credential dir, and optionally a distinct server-CA dir.
    pub fn tls(mut self, cert_dir: PathBuf, server_ca_dir: Option<PathBuf>) -> Self {
        self.cert_dir = Some(cert_dir);
        self.server_ca_dir = server_ca_dir;
        self
    }

    /// Connect to the given bootstrap servers.
    pub async fn connect(self, bootstrap_servers: impl Into<String>) -> Result<Kafka, Error> {
        Kafka::connect(KafkaConfig {
            bootstrap_servers: bootstrap_servers.into(),
            cert_dir: self.cert_dir,
            server_ca_dir: self.server_ca_dir,
        })
        .await
    }
}

/// A Kafka admin client over one reused mTLS connection.
///
/// Cheaply cloneable (all clones share the same connection). The connection is opened at
/// [`connect`](Self::connect), re-pointed at the controller as needed, and reused across ops.
#[derive(Clone)]
pub struct Kafka {
    /// The reused connection, behind a lock (ops are sequential — see the crate docs on why there is
    /// no background driver task). `None` only transiently, between construction and the first dial.
    conn: Arc<Mutex<Option<Connection>>>,
    params: Arc<ConnParams>,
}

/// The immutable connection parameters resolved once at [`Kafka::connect`].
struct ConnParams {
    /// Parsed bootstrap list, tried in order on (re)connect.
    bootstrap: Vec<(String, u16)>,
    /// The mTLS client config, built once.
    tls: Arc<rustls::ClientConfig>,
}

impl ConnParams {
    fn from_config(config: KafkaConfig) -> Result<Self, Error> {
        let bootstrap = parse_bootstrap(&config.bootstrap_servers)?;
        let cert_dir = config.cert_dir.context(MissingCredentialSnafu)?;
        let server_ca_dir = config.server_ca_dir.unwrap_or_else(|| cert_dir.clone());
        let tls = tls::build_client_config(&cert_dir, &server_ca_dir)?;
        Ok(ConnParams { bootstrap, tls })
    }

    /// Dial one specific broker.
    async fn dial(&self, host: &str, port: u16) -> Result<Connection, Error> {
        Connection::connect(host, port, self.tls.clone()).await
    }

    /// Dial each bootstrap server in order until one connects.
    async fn dial_any(&self) -> Result<Connection, Error> {
        let mut last_err = None;
        for (host, port) in &self.bootstrap {
            match self.dial(host, *port).await {
                Ok(conn) => return Ok(conn),
                Err(error) => {
                    tracing::debug!(host = %host, port, error = %error, "bootstrap dial failed; trying next");
                    last_err = Some(error);
                }
            }
        }
        Err(last_err.unwrap_or(Error::NoBootstrapServer {
            bootstrap_servers: String::new(),
        }))
    }
}

impl Kafka {
    /// Open a client: resolve the config (parse bootstrap, build the mTLS config) and dial.
    pub async fn connect(config: KafkaConfig) -> Result<Self, Error> {
        let params = Arc::new(ConnParams::from_config(config)?);
        // Dial eagerly so an unreachable cluster fails here rather than on the first op.
        let conn = params.dial_any().await?;
        Ok(Kafka {
            conn: Arc::new(Mutex::new(Some(conn))),
            params,
        })
    }

    /// Ensures the reused connection is open, dialing a bootstrap server if it isn't.
    async fn ensure_conn<'a>(
        &self,
        guard: &'a mut Option<Connection>,
    ) -> Result<&'a mut Connection, Error> {
        if guard.is_none() {
            *guard = Some(self.params.dial_any().await?);
        }
        Ok(guard.as_mut().expect("just connected"))
    }

    /// Fetches `Metadata` and re-points the reused connection at the cluster controller if it isn't
    /// already there (see [`cluster`]). Returns the `Metadata` so callers can reuse it for topology.
    async fn route_to_controller(
        &self,
        guard: &mut Option<Connection>,
    ) -> Result<MetadataResponse, Error> {
        let conn = self.ensure_conn(guard).await?;
        let meta = conn.metadata().await?;

        if let Some((host, port)) = cluster::controller_endpoint(&meta) {
            let already = guard
                .as_ref()
                .map(|c| c.peer_is(&host, port))
                .unwrap_or(false);
            if !already {
                tracing::debug!(controller = %host, port, "routing admin connection to the controller");
                *guard = Some(self.params.dial(&host, port).await?);
            }
        }
        Ok(meta)
    }

    /// Runs `op` with bounded exponential backoff, retrying only retryable errors. The in-client
    /// (Layer 1) retry: each admin op is idempotent, so re-running the whole op is safe, and pairing
    /// this with [`Self::poison_if_fatal`] means a retry after a connection-fatal error re-dials and
    /// re-routes to the controller.
    async fn with_backoff<T, F, Fut>(&self, op: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        op.retry(ADMIN_BACKOFF)
            .when(|e: &Error| e.is_retryable())
            .notify(|e: &Error, delay: Duration| {
                tracing::warn!(error = %e, ?delay, "transient Kafka error; retrying");
            })
            .await
    }

    /// Drops the reused connection when the op failed with a connection-fatal error, so the next
    /// attempt re-dials (and re-routes via `route_to_controller`).
    fn poison_if_fatal<T>(&self, guard: &mut Option<Connection>, result: &Result<T, Error>) {
        if let Err(error) = result
            && error.is_connection_fatal()
        {
            *guard = None;
        }
    }
}

/// Parses a comma-separated `host:port,host:port` list into `(host, port)` pairs, skipping blanks.
fn parse_bootstrap(bootstrap_servers: &str) -> Result<Vec<(String, u16)>, Error> {
    let mut out = Vec::new();
    for addr in bootstrap_servers.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (host, port) = addr
            .rsplit_once(':')
            .and_then(|(h, p)| p.parse::<u16>().ok().map(|p| (h.to_owned(), p)))
            .ok_or_else(|| Error::InvalidBootstrapAddr {
                addr: addr.to_owned(),
            })?;
        out.push((host, port));
    }
    if out.is_empty() {
        return NoBootstrapServerSnafu {
            bootstrap_servers: bootstrap_servers.to_owned(),
        }
        .fail();
    }
    Ok(out)
}

//! The health endpoints of the agent, which the probes of its Deployment call.

use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use snafu::{ResultExt, Snafu};
use stackable_kafka_shared::agent::{HEALTH_PORT, LIVENESS_PATH, READINESS_PATH};
use stackable_operator::webhook::health::HealthCheckRegistry;
use tokio::net::TcpListener;
use tracing::info;

const SOCKET_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), HEALTH_PORT);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to listen on {address} for the health endpoints"))]
    Bind {
        source: std::io::Error,
        address: SocketAddr,
    },

    #[snafu(display("failed to serve the health endpoints"))]
    Serve { source: std::io::Error },
}

/// Serves the endpoints until `shutdown` completes. Liveness only shows that the agent answers,
/// readiness is decided by the checks of the registry.
pub async fn serve(
    readiness: Arc<HealthCheckRegistry>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Error> {
    let router = Router::new()
        .route(LIVENESS_PATH, get(StatusCode::OK))
        .route(READINESS_PATH, get(ready))
        .with_state(readiness);
    let listener = TcpListener::bind(SOCKET_ADDRESS).await.context(BindSnafu {
        address: SOCKET_ADDRESS,
    })?;
    info!(address = %SOCKET_ADDRESS, "Serving the health endpoints");
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .context(ServeSnafu)
}

async fn ready(State(readiness): State<Arc<HealthCheckRegistry>>) -> Response {
    (&*readiness).into_response()
}

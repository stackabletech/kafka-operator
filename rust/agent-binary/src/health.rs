//! The health endpoints of the agent, which the probes of its Deployment call, and the checks
//! behind them.

use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use snafu::{ResultExt, Snafu};
use stackable_kafka_shared::{
    agent::{HEALTH_PORT, LIVENESS_PATH, READINESS_PATH},
    crd::topic::v1alpha1::KafkaTopic,
};
use stackable_operator::{
    kube::{Api, api::ListParams},
    webhook::health::{HealthCheck, HealthCheckRegistry},
};
use tokio::net::TcpListener;
use tracing::{info, warn};

const SOCKET_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), HEALTH_PORT);

const API_ACCESS_INTERVAL: Duration = Duration::from_secs(30);

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

/// Marks `check` by listing the KafkaTopics selected by `params` every [`API_ACCESS_INTERVAL`],
/// because a failing watch of the controller is not visible otherwise.
pub async fn check_k8s_api_access(
    check: HealthCheck,
    api: Api<KafkaTopic>,
    params: ListParams,
    shutdown: impl Future<Output = ()>,
) {
    let mut interval = tokio::time::interval(API_ACCESS_INTERVAL);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            _ = interval.tick() => match api.list_metadata(&params).await {
                Ok(_) => check.mark_passed(),
                Err(error) => {
                    warn!(
                        error = &error as &dyn std::error::Error,
                        "Cannot list the KafkaTopics, reporting not ready"
                    );
                    check.mark_not_passed();
                }
            },
        }
    }
}

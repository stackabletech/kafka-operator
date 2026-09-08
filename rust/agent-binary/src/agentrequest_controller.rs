//! The kafka-agent's `AgentRequest` reconcile surface (spike, gated one-shots).
//!
//! Watches the generic [`AgentRequest`](stackable_operator::crd::action) CRD filtered to this
//! agent's cluster (and `product == kafka`). The agent is the **sole writer** of `status`. On a
//! `ScaleDown` request it drains the broker(s) natively via the pure-Rust [`KafkaAdmin`] client
//! (`reassign_off` — `AlterPartitionReassignments` + a `ListPartitionReassignments` poll, spike
//! R2.5), mapping progress to the request phase: remaining > 0 ⇒ `InProgress`, remaining == 0 ⇒
//! `Done`, an error ⇒ `Failed`. On the post-scale `Unregister` request it removes the
//! already-drained brokers' stale registrations (`unregister_brokers` — `UnregisterBroker`). No JVM
//! Job, no product image.
//!
//! The agent does **not** watch the Scaler — it only knows AgentRequests. That decoupling is the
//! design invariant this surface exists to prove.

use std::{ops::ControlFlow, path::PathBuf, sync::Arc};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::action::{
        ActionType, AgentRequestPhase, AgentRequestProgress, AgentRequestStatus,
        v1alpha1 as action_v1alpha1,
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tokio_kafka::{Error as KafkaClientError, Kafka, KafkaConfig, ReassignProgress};

use crate::discovery::read_bootstrap_servers;

/// The product string this agent claims. Only `AgentRequest`s with this product are reconciled.
const PRODUCT_KAFKA: &str = "kafka";

/// Context for the AgentRequest reconcile loop.
pub struct Ctx {
    pub client: Client,
    /// Name of the `KafkaCluster` this agent owns; requests for other clusters are ignored.
    pub cluster_name: String,
    /// Directory the cluster's discovery `ConfigMap` is mounted at; the drain reads the `KAFKA`
    /// bootstrap key from it each reconcile.
    pub discovery_config_dir: PathBuf,
    /// Directory the platform-access client credential (`tls.crt` / `tls.key` / `ca.crt`) is mounted
    /// at. When set, the drain connects over mTLS; `None` ⇒ plaintext (dev only).
    pub platform_access_cert_dir: Option<PathBuf>,
    /// Directory the Kafka *server's* CA (`ca.crt`) is mounted at (cross-CA mTLS). Falls back to the
    /// credential dir when unset.
    pub platform_access_server_ca_dir: Option<PathBuf>,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("AgentRequest object is invalid"))]
    InvalidAgentRequest {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("the broker drain (partition reassignment) failed"))]
    Drain { source: KafkaClientError },

    #[snafu(display("the broker unregister failed"))]
    Unregister { source: KafkaClientError },

    #[snafu(display("failed to write the AgentRequest status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        None
    }
}

pub async fn reconcile_agent_request(
    request: Arc<DeserializeGuard<action_v1alpha1::AgentRequest>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting AgentRequest reconcile");
    let request = request
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAgentRequestSnafu)?;

    // Ownership: only handle requests for this agent's cluster and product.
    if request.spec.cluster_ref.name != ctx.cluster_name || request.spec.product != PRODUCT_KAFKA {
        tracing::debug!(
            request = request.name_any(),
            cluster_ref = request.spec.cluster_ref.name,
            product = request.spec.product,
            "AgentRequest targets another cluster/product; skipping"
        );
        return Ok(Action::await_change());
    }

    // Terminal requests need no further work (the operator GCs them via TTL/owner-ref).
    if let Some(status) = &request.status {
        if matches!(
            status.phase,
            AgentRequestPhase::Done | AgentRequestPhase::Failed | AgentRequestPhase::Rejected
        ) {
            return Ok(Action::await_change());
        }
    }

    match &request.spec.action_type {
        ActionType::ScaleDown => reconcile_scale_down(&ctx, request).await,
        ActionType::Unregister => reconcile_unregister(&ctx, request).await,
        other => {
            // Only scale-down + unregister are implemented in the spike; reject the rest cleanly.
            tracing::warn!(?other, "Unsupported AgentRequest actionType; rejecting");
            write_status(
                &ctx.client,
                request,
                AgentRequestPhase::Rejected,
                Some("only scaleDown and unregister are implemented in this spike".to_string()),
                None,
            )
            .await?;
            Ok(Action::await_change())
        }
    }
}

async fn reconcile_scale_down(
    ctx: &Ctx,
    request: &action_v1alpha1::AgentRequest,
) -> Result<Action> {
    let broker_ids = broker_ids_from_context(request);

    let kafka = match connect_kafka(ctx, request).await? {
        ControlFlow::Continue(kafka) => kafka,
        // Transient (requeue) or terminal (status already written); just return the Action.
        ControlFlow::Break(action) => return Ok(action),
    };

    // Submit-and-poll: crash-safe because in-progress state is re-derived from the cluster each
    // reconcile (no stored plan). Map the remaining count to the request phase.
    match kafka.reassign_off(&broker_ids).await {
        Ok(progress) if progress.is_done() => {
            write_status(&ctx.client, request, AgentRequestPhase::Done, None, Some(progress)).await?;
            Ok(Action::await_change())
        }
        Ok(progress) => {
            write_status(
                &ctx.client,
                request,
                AgentRequestPhase::InProgress,
                None,
                Some(progress),
            )
            .await?;
            Ok(Action::requeue(*Duration::from_secs(10)))
        }
        Err(error) if error.is_retryable() => {
            // Transient (e.g. `RequestTimedOut` off a flapping controller). The client already retried
            // with backoff; keep the request live and let the reconcile loop retry — do NOT mark Failed.
            // The existing phase/progress is left untouched (a status write would clobber it).
            tracing::warn!(%error, "transient error during drain; will retry");
            Ok(Action::requeue(*Duration::from_secs(10)))
        }
        Err(error) => {
            // Terminal reassignment failure: report Failed and retain the request for inspection.
            let message = error.to_string();
            write_status(
                &ctx.client,
                request,
                AgentRequestPhase::Failed,
                Some(message),
                None,
            )
            .await?;
            Err(error).context(DrainSnafu)
        }
    }
}

/// Post-scale cleanup: unregister the already-drained, already-removed brokers from the cluster
/// metadata (`UnregisterBroker`). The operator's `post_scale` hook creates this request *after* the
/// broker StatefulSet has shrunk, so by now the pods are gone and their registrations are stale.
///
/// Idempotent and reconcile-safe: `unregister_brokers` treats an already-gone broker as success, so
/// re-running (retries, restarts) is harmless. Same transient-vs-terminal split as the drain — a
/// transient error keeps the request live and requeues; only a terminal error marks it `Failed`.
async fn reconcile_unregister(
    ctx: &Ctx,
    request: &action_v1alpha1::AgentRequest,
) -> Result<Action> {
    let broker_ids = broker_ids_from_context(request);
    if broker_ids.is_empty() {
        // Nothing to unregister → done immediately (no need to connect).
        write_status(&ctx.client, request, AgentRequestPhase::Done, None, None).await?;
        return Ok(Action::await_change());
    }

    let kafka = match connect_kafka(ctx, request).await? {
        ControlFlow::Continue(kafka) => kafka,
        ControlFlow::Break(action) => return Ok(action),
    };

    match kafka.unregister_brokers(&broker_ids).await {
        Ok(()) => {
            write_status(&ctx.client, request, AgentRequestPhase::Done, None, None).await?;
            Ok(Action::await_change())
        }
        Err(error) if error.is_retryable() => {
            // Transient (e.g. the removed broker's session hasn't expired yet, or a controller blip).
            // The client already retried with backoff; keep the request live and requeue.
            tracing::warn!(%error, "transient error during unregister; will retry");
            Ok(Action::requeue(*Duration::from_secs(10)))
        }
        Err(error) => {
            // Terminal failure: report Failed and retain the request for inspection.
            let message = error.to_string();
            write_status(
                &ctx.client,
                request,
                AgentRequestPhase::Failed,
                Some(message),
                None,
            )
            .await?;
            Err(error).context(UnregisterSnafu)
        }
    }
}

/// Parses the comma-separated Kafka node ids the operator's hook wrote into `context.brokerIds`.
fn broker_ids_from_context(request: &action_v1alpha1::AgentRequest) -> Vec<i32> {
    request
        .spec
        .context
        .get("brokerIds")
        .map(|raw| {
            raw.split(',')
                .filter_map(|s| s.trim().parse::<i32>().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Reads the bootstrap servers from the mounted discovery `ConfigMap` and opens the mTLS client.
///
/// Returns [`ControlFlow::Continue`] with the connected client on success. On failure it yields the
/// [`Action`] the reconcile should return instead: a transient failure (discovery CM not populated
/// yet, brokers unreachable) requeues without touching the request; a terminal failure (missing
/// credential, invalid bootstrap addr) writes `phase: Failed` first. Shared by the drain and the
/// unregister reconcilers.
async fn connect_kafka(
    ctx: &Ctx,
    request: &action_v1alpha1::AgentRequest,
) -> Result<ControlFlow<Action, Kafka>> {
    let bootstrap_servers = read_bootstrap_servers(&ctx.discovery_config_dir);
    if bootstrap_servers.is_empty() {
        tracing::info!("discovery ConfigMap has no bootstrap servers yet; requeuing");
        return Ok(ControlFlow::Break(Action::requeue(*Duration::from_secs(30))));
    }
    match Kafka::connect(KafkaConfig {
        bootstrap_servers,
        cert_dir: ctx.platform_access_cert_dir.clone(),
        server_ca_dir: ctx.platform_access_server_ca_dir.clone(),
    })
    .await
    {
        Ok(kafka) => Ok(ControlFlow::Continue(kafka)),
        Err(error) if error.is_retryable() => {
            // Transient (brokers not reachable yet); retry without marking the request Failed.
            tracing::warn!(%error, "transient failure connecting to Kafka; will retry");
            Ok(ControlFlow::Break(Action::requeue(*Duration::from_secs(10))))
        }
        Err(error) => {
            // Terminal (missing credential, invalid bootstrap addr, ...): fail the request.
            tracing::error!(%error, "terminal failure connecting to Kafka");
            write_status(
                &ctx.client,
                request,
                AgentRequestPhase::Failed,
                Some(error.to_string()),
                None,
            )
            .await?;
            Ok(ControlFlow::Break(Action::await_change()))
        }
    }
}

/// Writes the AgentRequest status (agent is the sole writer). Idempotent: skips the write when the
/// phase/reason/progress already match, so a status update does not re-trigger the primary watch
/// forever.
async fn write_status(
    client: &Client,
    request: &action_v1alpha1::AgentRequest,
    phase: AgentRequestPhase,
    reason: Option<String>,
    progress: Option<ReassignProgress>,
) -> Result<()> {
    let progress = progress.map(|p| AgentRequestProgress {
        total: p.total,
        remaining: p.remaining,
    });
    if let Some(status) = &request.status {
        if status.phase == phase && status.reason == reason && status.progress == progress {
            return Ok(());
        }
    }
    let status = AgentRequestStatus {
        phase,
        reason,
        progress,
        ..Default::default()
    };
    client
        .merge_patch_status(request, &status)
        .await
        .context(ApplyStatusSnafu)?;
    Ok(())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<action_v1alpha1::AgentRequest>>,
    _error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    Action::requeue(*Duration::from_secs(10))
}

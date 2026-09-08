//! The per-cluster **kafka-agent** (spike): a standalone binary/image (R2.2) that owns two reconcile
//! surfaces for exactly one `KafkaCluster`, scoped to its namespace.
//!
//! 1. [`topic_controller`] — standing reconciliation of `KafkaTopic`s, native via the pure-Rust
//!    [`tokio_kafka`] admin client over mTLS. The frequent, low-latency access path.
//! 2. [`agentrequest_controller`] — the gated one-shot broker drain, driven by the generic
//!    `AgentRequest` CRD and executed natively via [`tokio_kafka`] (`AlterPartitionReassignments`).
//!
//! Plus a liveness [`lease`] heartbeat the operator watches to surface `AgentUnavailable`.
//!
//! Design invariants: the operator holds no Kafka credentials (only the agent mounts the grant); the
//! agent is the sole writer of `AgentRequest` status; the agent does **not** watch the Scaler; RBAC
//! is namespace-scoped (Role + RoleBinding, not a ClusterRole). The agent installs no CRDs and runs
//! no conversion webhook.

// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

use std::path::PathBuf;

use clap::Parser;
use stackable_kafka_operator::{built_info, crd::KAFKA_OPERATOR_NAME};
use stackable_operator::{
    cli::CommonOptions, client, telemetry::Tracing, utils::signal::SignalWatcher,
};

mod agentrequest_controller;
mod condition;
mod discovery;
mod lease;
mod run;
mod topic_controller;

/// The controller-name prefix used in event reporters.
pub const KAFKA_AGENT_CONTROLLER_NAME: &str = "kafkaagent";

/// Arguments for the per-cluster kafka-agent.
///
/// Deliberately *not* the framework's `RunArguments`: that drags in the mandatory
/// `operator_namespace` / `operator_service_name`, which exist only for the conversion webhook the
/// agent does not run. There is no subcommand — the agent binary is its own entrypoint.
#[derive(clap::Parser)]
#[clap(about, author)]
struct AgentArguments {
    /// Name of the `KafkaCluster` this agent provisions topics for.
    #[arg(long, env)]
    kafka_cluster_name: String,

    /// Namespace of the `KafkaCluster` (and the only namespace this agent watches).
    #[arg(long, env)]
    namespace: String,

    /// Directory the cluster's discovery `ConfigMap` is mounted at (spike R3). The agent reads the
    /// bootstrap servers from its `KAFKA` key on every reconcile, tolerating a transient-empty value
    /// while the listener-operator settles the ingress addresses.
    #[arg(long, env)]
    discovery_config_dir: PathBuf,

    /// The stock Kafka product image (informational; the native drain no longer uses a product-image
    /// Job, so this is accepted but not required).
    #[arg(long, env)]
    product_image: Option<String>,

    /// SecretClass the agent's credential is minted from (informational for the agent process).
    #[arg(long, env)]
    credential_secret_class: Option<String>,

    /// Directory the platform-access client credential (`tls.crt` / `tls.key` / `ca.crt`) is mounted
    /// at. When set, the agent connects over mTLS.
    #[arg(long, env)]
    platform_access_cert_dir: Option<PathBuf>,

    /// Directory the Kafka server's CA (`ca.crt`) is mounted at (cross-CA mTLS). Falls back to the
    /// credential dir when unset.
    #[arg(long, env)]
    platform_access_server_ca_dir: Option<PathBuf>,

    #[clap(flatten)]
    common: CommonOptions,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let AgentArguments {
        kafka_cluster_name,
        namespace,
        discovery_config_dir,
        product_image: _product_image,
        credential_secret_class: _credential_secret_class,
        platform_access_cert_dir,
        platform_access_server_ca_dir,
        common,
    } = AgentArguments::parse();

    let _tracing_guard = Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

    tracing::info!(
        built_info.pkg_version = built_info::PKG_VERSION,
        kafka.cluster.name = kafka_cluster_name,
        kafka.cluster.namespace = namespace,
        "Starting kafka-agent"
    );

    let sigterm_watcher = SignalWatcher::sigterm()?;

    let client =
        client::initialize_operator(Some(KAFKA_OPERATOR_NAME.to_string()), &common.cluster_info)
            .await?;

    // Renew the agent's liveness Lease alongside the controllers, so the operator can surface
    // "agent not running" on the KafkaTopics (spike). The agent does NOT watch the Scaler.
    let holder =
        std::env::var("HOSTNAME").unwrap_or_else(|_| format!("{kafka_cluster_name}-kafka-agent"));
    let lease_task = lease::renew_forever(
        client.clone(),
        namespace.clone(),
        kafka_cluster_name.clone(),
        holder,
        sigterm_watcher.handle(),
    );

    let agent_config = run::AgentConfig {
        cluster_name: kafka_cluster_name,
        namespace,
        discovery_config_dir,
        platform_access_cert_dir,
        platform_access_server_ca_dir,
    };

    // NB: the agent must NOT call `signal::crd_established` (needs cluster-scoped CRD list/watch) and
    // must NOT create a conversion webhook. It runs the two controllers plus the liveness lease.
    let controllers = run::run(
        client,
        agent_config,
        sigterm_watcher.handle(),
        sigterm_watcher.handle(),
    );
    futures::join!(lease_task, controllers);

    Ok(())
}

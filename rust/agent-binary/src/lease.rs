//! The kafka-agent's liveness Lease (spike). Ported from the zk-agent's `lease.rs`.
//!
//! The agent renews a `coordination.k8s.io/v1` Lease from inside its process on a short period. The
//! operator watches that Lease: staleness beyond [`LEASE_DURATION_SECONDS`] means "no functioning
//! agent", one signal covering every cause (crashloop, OOM, unschedulable, bad image, dead node).
//!
//! Honest boundary (spike): a Lease renewed by a timer proves the *process* is alive and its API
//! connection works, not that the reconcile loop is healthy.

use std::{future::Future, time::Duration};

use stackable_operator::{
    client::Client,
    k8s_openapi::{
        api::coordination::v1::{Lease, LeaseSpec},
        apimachinery::pkg::apis::meta::v1::MicroTime,
        jiff::Timestamp,
    },
    kube::api::ObjectMeta,
};

use stackable_kafka_operator::agent_lease::agent_lease_name;

/// How often the agent renews — comfortably inside the operator-set lease duration (30s).
const RENEW_PERIOD: Duration = Duration::from_secs(10);

/// Field manager for the agent's Lease heartbeat. DISTINCT from the operator's manager: the operator
/// CREATES the Lease (owning its existence, owner reference and `leaseDurationSeconds`), and this side
/// owns only `holderIdentity`/`renewTime`, so neither's server-side apply clobbers the other's fields.
const AGENT_FIELD_MANAGER: &str = "kafka-agent";

/// Renews the agent's liveness Lease until `shutdown` fires.
pub async fn renew_forever(
    client: Client,
    namespace: String,
    cluster_name: String,
    holder: String,
    shutdown: impl Future<Output = ()>,
) {
    let name = agent_lease_name(&cluster_name);
    tracing::info!(lease = name, namespace, "Starting agent liveness lease renewal");
    tokio::pin!(shutdown);
    loop {
        if let Err(error) = renew_once(&client, &namespace, &name, &holder).await {
            tracing::warn!(%error, lease = name, "Failed to renew the agent liveness lease");
        }
        tokio::select! {
            _ = tokio::time::sleep(RENEW_PERIOD) => {}
            _ = &mut shutdown => break,
        }
    }
    tracing::info!(lease = name, "Stopping agent liveness lease renewal");
}

async fn renew_once(
    client: &Client,
    namespace: &str,
    name: &str,
    holder: &str,
) -> Result<(), stackable_operator::client::Error> {
    let lease = Lease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..ObjectMeta::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: Some(holder.to_string()),
            renew_time: Some(MicroTime(Timestamp::now())),
            ..LeaseSpec::default()
        }),
    };
    // Server-side apply with the agent's OWN field manager: sets only the heartbeat fields, so the
    // operator's apply of the Lease (existence / owner-ref / duration) and this renewal never conflict.
    // The operator creates the Lease; if it hasn't yet, this apply creates it and the operator's next
    // apply adds the owner reference.
    client.apply_patch(AGENT_FIELD_MANAGER, &lease, &lease).await?;
    Ok(())
}


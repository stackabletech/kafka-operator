//! The per-cluster kafka-agent's liveness Lease (spike): shared name, duration, and freshness check.
//!
//! The agent (`agent-binary`) renews a `coordination.k8s.io/v1` Lease on a short period (the writing
//! side lives in `agent-binary/src/lease.rs`); the **operator** reads it here to decide whether the
//! agent is alive — the `AgentUnavailable` signal surfaced on the `KafkaCluster` status. This lives in
//! the operator crate so both sides share one source of truth (the agent crate depends on the
//! operator crate, not the other way round).

use stackable_operator::{
    client::Client,
    k8s_openapi::{api::coordination::v1::Lease, jiff::Timestamp},
};

/// How long a renewal is valid. The operator treats a Lease older than this as "agent not running".
/// The agent renews comfortably inside this window (see `agent-binary`'s `RENEW_PERIOD`).
pub const LEASE_DURATION_SECONDS: i32 = 30;

/// The Lease name for a cluster's agent (`<cluster>-kafka-agent`).
pub fn agent_lease_name(cluster_name: &str) -> String {
    format!("{cluster_name}-kafka-agent")
}

/// Whether the agent for `cluster_name` in `namespace` is alive: its Lease exists and was renewed
/// within [`LEASE_DURATION_SECONDS`]. A missing Lease (agent never started / deleted) ⇒ not alive.
pub async fn is_agent_alive(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> Result<bool, stackable_operator::client::Error> {
    let name = agent_lease_name(cluster_name);
    let Some(lease) = client.get_opt::<Lease>(&name, namespace).await? else {
        return Ok(false);
    };
    let fresh = lease
        .spec
        .and_then(|spec| spec.renew_time)
        .is_some_and(|renew| {
            let age_seconds = Timestamp::now().as_second() - renew.0.as_second();
            age_seconds <= LEASE_DURATION_SECONDS as i64
        });
    Ok(fresh)
}

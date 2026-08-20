//! Container probes for the Kafka `kafka` container (broker and controller roles).

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::probe::{self, ProbeBuilder},
    k8s_openapi::{
        api::core::v1::{Probe, TCPSocketAction},
        apimachinery::pkg::util::intstr::IntOrString,
    },
    shared::time::Duration,
    v2::types::common::Port,
};

use crate::controller::{
    build::security::kcat_prober_container_commands, security::ValidatedKafkaSecurity,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the {name} probe"))]
    BuildProbe {
        source: probe::Error,
        name: &'static str,
    },
}

/// The broker `kafka` container's readiness probe.
///
/// Uses `kcat` rather than the official Kafka tools, since they incur a lot of unacceptable perf
/// overhead when run repeatedly as a probe. Only allow the global load balancing service to send
/// traffic to pods that are members of the quorum. This also acts as a hint to the StatefulSet
/// controller to wait for each pod to enter quorum before taking down the next.
pub fn broker_kcat_readiness_probe(
    kafka_security: &ValidatedKafkaSecurity,
) -> Result<Probe, Error> {
    ProbeBuilder::exec_command(
        // If the broker is able to get its fellow cluster members then it has at least
        // completed basic registration at some point
        kcat_prober_container_commands(kafka_security),
    )
    .with_period(Duration::from_secs(2))
    .with_timeout(Duration::from_secs(5))
    // `ProbeBuilder` otherwise defaults this to 1; kept at Kubernetes' own default (3) to match
    // the pre-`ProbeBuilder` behaviour, which left this field unset.
    .with_failure_threshold(3)
    .build()
    .context(BuildProbeSnafu {
        name: "kcat readiness",
    })
}

/// A `Probe` combining a plain TCP check of the broker's client listener with a check that the
/// broker's own JMX `BrokerState` metric reports `RUNNING` (state `3`).
///
/// Used for both the `startupProbe` (so the `livenessProbe` doesn't start counting failures
/// until the broker has actually finished starting - the client port can accept connections
/// before the broker reaches `RUNNING`, e.g. while still replaying its log) and the
/// `livenessProbe` (restarting a broker stuck in some other state, e.g. `RECOVERY` after a
/// crash); only the timing parameters differ between the two uses.
pub fn broker_running_probe(
    client_port: Port,
    metrics_port: Port,
    timeout_seconds: u64,
    period_seconds: u64,
    failure_threshold: i32,
) -> Result<Probe, Error> {
    ProbeBuilder::exec_command([
        "bash".to_string(),
        "-c".to_string(),
        format!(
            "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/localhost/{client_port}' || exit 1\n\
             curl -s --max-time 2 localhost:{metrics_port}/metrics | grep -qE 'kafka_server_kafkaserver_brokerstate 3(\\.0)?$'"
        ),
    ])
    .with_period(Duration::from_secs(period_seconds))
    .with_timeout(Duration::from_secs(timeout_seconds))
    .with_failure_threshold(failure_threshold)
    .build()
    .context(BuildProbeSnafu {
        name: "broker running",
    })
}

/// A `Probe` that dials the controller's KRaft listener socket via a plain TCP connect.
///
/// This only proves the socket is open, not that the node has a healthy Raft state (leader,
/// follower, or voted). Used for `startupProbe`: there is no meaningful Raft state to check
/// yet while the process is still starting, so a bare TCP check is all that's meaningful this
/// early.
pub fn controller_tcp_probe(
    port: Port,
    timeout_seconds: u64,
    period_seconds: u64,
    failure_threshold: i32,
) -> Result<Probe, Error> {
    ProbeBuilder::tcp_socket(TCPSocketAction {
        port: IntOrString::Int(port.into()),
        ..Default::default()
    })
    .with_period(Duration::from_secs(period_seconds))
    .with_timeout(Duration::from_secs(timeout_seconds))
    .with_failure_threshold(failure_threshold)
    .build()
    .context(BuildProbeSnafu {
        name: "controller startup",
    })
}

/// A `Probe` that curls the JMX Prometheus exporter's `/metrics` endpoint and checks that the
/// controller's Raft state is one of the healthy states (`leader`, `follower`, or `voted`)
/// rather than stuck in `unattached` or `candidate`.
pub fn controller_raft_state_probe(
    metrics_port: Port,
    timeout_seconds: u64,
    period_seconds: u64,
    failure_threshold: i32,
) -> Result<Probe, Error> {
    ProbeBuilder::exec_command([
        "bash".to_string(),
        "-c".to_string(),
        format!(
            "curl -s localhost:{metrics_port}/metrics | grep -E 'kafka_server_raft_metrics_current_state\\{{state=\"(leader|follower|voted)\",?\\}}'"
        ),
    ])
    .with_period(Duration::from_secs(period_seconds))
    .with_timeout(Duration::from_secs(timeout_seconds))
    .with_failure_threshold(failure_threshold)
    .build()
    .context(BuildProbeSnafu {
        name: "controller raft state",
    })
}

/// A `Probe` combining a plain TCP check of the controller's KRaft listener with a check that
/// its local Raft state isn't stuck in `unattached`.
///
/// This is needed to work around a bug in KRaft where a new controller is stuck in a loop
/// trying to fetch Raft metadata from its self.
///
/// This can happen when the headless service used to point to the bootstrap controllers
/// happens to resolve to this exact pod.
pub fn controller_stuck_unattached_liveness_probe(
    client_port: Port,
    metrics_port: Port,
    timeout_seconds: u64,
    period_seconds: u64,
    failure_threshold: i32,
) -> Result<Probe, Error> {
    ProbeBuilder::exec_command([
        "bash".to_string(),
        "-c".to_string(),
        format!(
            "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/localhost/{client_port}' || exit 1\n\
             state=$(curl -s --max-time 2 localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\\{{state=\"[a-z]+\"\\}}' | grep -oE '\"[a-z]+\"' | tr -d '\"')\n\
             [ \"$state\" != \"unattached\" ]"
        ),
    ])
    .with_period(Duration::from_secs(period_seconds))
    .with_timeout(Duration::from_secs(timeout_seconds))
    .with_failure_threshold(failure_threshold)
    .build()
    .context(BuildProbeSnafu {
        name: "controller stuck-unattached liveness",
    })
}

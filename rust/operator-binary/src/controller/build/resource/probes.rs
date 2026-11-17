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
/// overhead when run repeatedly as a probe.
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

/// The broker startup and liveness probe.
/// Combines a plain TCP check of the broker's client listener with a check that the
/// broker's own JMX `BrokerState` metric reports `RUNNING` (state `3`).
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

/// The controller startup probe.
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

/// The controller readiness probe.
///
/// Curls the JMX Prometheus exporter's `/metrics` endpoint and checks that the
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

/// The controller liveness probe.
///
/// A `Probe` combining a plain TCP check of the controller's KRaft listener with a check that
/// its local Raft state isn't stuck in `unattached`.
///
/// This is a safety net: a controller that never learns the quorum leader stays `unattached`
/// forever, and restarting it is the only way out.
///
/// The original cause, a bootstrap address resolving to the controller itself, is avoided by
/// listing the individual controller pods in `controller.quorum.bootstrap.servers` (see
/// [`kraft_controller_pods`][kcp]). A controller can still get stuck when none of the listed pods
/// reaches the leader, for example when more controllers run than the operator predicts.
///
/// [kcp]: crate::controller::build::properties::kraft_controller_pods
///
/// The TCP check uses this pod's own IP (`$POD_IP`, from the downward API) rather than its
/// FQDN so a DNS hiccup would not wrongly kill a healthy controller.
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
            "timeout 2 bash -c 'cat < /dev/null > /dev/tcp/$POD_IP/{client_port}' || exit 1\n\
             state=$(curl -s --max-time 2 localhost:{metrics_port}/metrics | grep -oE 'kafka_server_raft_metrics_current_state\\{{state=\"[a-z]+\",?\\}}' | grep -oE '\"[a-z]+\"' | tr -d '\"')\n\
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

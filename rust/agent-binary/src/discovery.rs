//! Reads the agent's Kafka bootstrap servers from the mounted **discovery ConfigMap** (spike R3).
//!
//! The operator mounts the cluster's discovery `ConfigMap` (named after the cluster) as a volume; its
//! single `KAFKA` key holds the comma-separated bootstrap-listener addresses. A ConfigMap volume is
//! synced in place by the kubelet, so the agent re-reads it on **every reconcile** and picks up
//! address changes without a restart.
//!
//! The value is deliberately allowed to be **empty**: the operator writes an empty `KAFKA` before the
//! listener-operator has assigned ingress addresses (rather than omitting the ConfigMap and risking an
//! orphan delete). Callers treat empty as "not ready yet" and requeue.

use std::path::Path;

/// The discovery ConfigMap key carrying the bootstrap servers (`{host}:{port},...`).
const KAFKA_KEY: &str = "KAFKA";

/// Reads the trimmed bootstrap-servers string from `<discovery_dir>/KAFKA`.
///
/// Returns an empty string when the file is missing/unreadable or blank — the file appears only once
/// the ConfigMap is mounted and populated, and both "not mounted yet" and "no addresses yet" mean the
/// same thing to the caller (requeue and try again).
pub(crate) fn read_bootstrap_servers(discovery_dir: &Path) -> String {
    let path = discovery_dir.join(KAFKA_KEY);
    match std::fs::read_to_string(&path) {
        Ok(contents) => contents.trim().to_string(),
        Err(error) => {
            tracing::debug!(
                ?path,
                %error,
                "could not read the discovery bootstrap file yet; treating as empty"
            );
            String::new()
        }
    }
}

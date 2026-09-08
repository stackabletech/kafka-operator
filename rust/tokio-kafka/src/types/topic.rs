//! Topic domain types.

use std::collections::BTreeMap;

/// A topic definition — name, partitioning, replication and config.
///
/// Named after Kafka's Java `AdminClient.NewTopic` (not the Kubernetes `spec`/`status` idiom). The
/// input to [`Kafka::ensure_topic`](crate::Kafka::ensure_topic), which reconciles it idempotently
/// against the live cluster: create if absent, grow partitions / apply config if present.
#[derive(Clone, Debug)]
pub struct NewTopic {
    /// The Kafka topic name.
    pub name: String,
    /// Desired partition count.
    pub partitions: i32,
    /// Desired replication factor.
    pub replication_factor: i32,
    /// Extra per-topic config entries (e.g. `retention.ms`), applied verbatim.
    pub config: BTreeMap<String, String>,
}

//! Broker topology / controller routing — the one piece with no tokio-zookeeper analog.
//!
//! ZooKeeper clients talk to any ensemble member; Kafka admin ops that mutate metadata
//! (`CreateTopics`, `DeleteTopics`, `CreatePartitions`, `AlterPartitionReassignments`, ...) must be
//! handled by the **active controller**. Crucially this matters because **not every Kafka is KRaft**:
//!
//! * **ZooKeeper-mode** brokers do *not* forward controller-only ops — a non-controller broker
//!   answers `NOT_CONTROLLER`, so the client must find the controller itself.
//! * **KRaft** brokers *do* forward, and the `controller_id` a client sees in `Metadata` is a live,
//!   reachable broker acting as the forwarding proxy (never a controller-only node).
//!
//! Either way the controller named in `Metadata` is a broker we can dial. [`crate::Kafka`] fetches
//! `Metadata` anyway (to check topic existence / plan a drain), so routing to the controller is a
//! near-free reuse of that response rather than a reactive retry after a `NOT_CONTROLLER` round-trip.

use kafka_protocol::messages::MetadataResponse;

/// The `(host, port)` of the broker `Metadata` names as the controller, if any.
///
/// Returns `None` when no controller is advertised (`controller_id < 0`) or the id is not in the
/// broker list — in which case the caller stays on its current connection (in KRaft the broker it is
/// already talking to will forward).
pub(crate) fn controller_endpoint(meta: &MetadataResponse) -> Option<(String, u16)> {
    if meta.controller_id.0 < 0 {
        return None;
    }
    meta.brokers
        .iter()
        .find(|broker| broker.node_id == meta.controller_id)
        .map(|broker| (broker.host.to_string(), broker.port as u16))
}

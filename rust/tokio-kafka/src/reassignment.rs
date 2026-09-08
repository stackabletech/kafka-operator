//! Broker-drain logic: plan a partition reassignment off the draining brokers, submit it, and poll
//! for completion. The tokio-zookeeper `recipes/` analog — pure domain logic layered on the wire
//! primitives, kept out of the connection module.
//!
//! Crash-safe by design: no plan is persisted. Each call re-reads the cluster
//! (`Metadata` → [`build_plan`]) and re-derives the remaining count from
//! `ListPartitionReassignments`, so a restarted agent resumes a drain transparently.

use std::collections::{BTreeMap, HashSet};

use kafka_protocol::messages::{
    AlterPartitionReassignmentsRequest, BrokerId, ListPartitionReassignmentsRequest, MetadataResponse,
    TopicName,
    alter_partition_reassignments_request::{ReassignablePartition, ReassignableTopic},
};
use kafka_protocol::protocol::StrBytes;

use crate::{error::Error, proto::Connection};

/// A topic's replica layout: partition index -> target replica broker ids.
pub(crate) type ReplicaLayout = BTreeMap<i32, Vec<i32>>;

/// Progress of an in-flight partition reassignment (broker drain).
///
/// `remaining == 0` means the drain is complete.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReassignProgress {
    /// Total partitions that had a replica on the draining broker(s) when the drain started.
    pub total: u32,
    /// Partitions still being moved off the draining broker(s).
    pub remaining: u32,
}

impl ReassignProgress {
    /// Whether the drain has completed (nothing left to move).
    pub fn is_done(&self) -> bool {
        self.remaining == 0
    }
}

/// Computes the reassignment plan from cluster `Metadata`: for every partition whose replica set
/// touches a draining broker, a target set that drops the draining brokers and tops up from
/// survivors keeping the RF. Returns `(plan, total_moving)` where `total_moving` counts every
/// touched partition (even those with no feasible plan, so the caller can tell "draining" from
/// "done").
pub(crate) struct DrainPlan {
    /// The feasible reassignments: topic -> (partition -> target replica set).
    pub planned: BTreeMap<String, ReplicaLayout>,
    /// Total partitions that have a replica on a draining broker (what needs to move).
    pub total_moving: u32,
    /// Partitions that touch a draining broker but CANNOT be moved while keeping their replication
    /// factor (too few surviving brokers). `blocked > 0` means the drain cannot complete safely.
    pub blocked: u32,
}

pub(crate) fn build_plan(
    meta: &MetadataResponse,
    draining: &HashSet<i32>,
    in_flight: &HashSet<(String, i32)>,
) -> DrainPlan {
    // Surviving brokers = everything currently in the cluster that is not draining.
    let survivors: Vec<i32> = meta
        .brokers
        .iter()
        .map(|broker| broker.node_id.0)
        .filter(|id| !draining.contains(id))
        .collect();

    let mut planned: BTreeMap<String, ReplicaLayout> = BTreeMap::new();
    let mut total_moving: u32 = 0;
    let mut blocked: u32 = 0;
    for topic in &meta.topics {
        if topic.error_code != 0 {
            continue;
        }
        let Some(name) = topic.name.as_ref() else {
            continue;
        };
        for partition in &topic.partitions {
            let current: Vec<i32> = partition.replica_nodes.iter().map(|r| r.0).collect();
            if !current.iter().any(|id| draining.contains(id)) {
                continue;
            }
            total_moving += 1;
            // Skip partitions whose reassignment is already in flight. While a reassignment runs,
            // `Metadata` reports `replica_nodes` as the *union* of the original and target replicas
            // (the target survivor is already listed, the draining broker not yet removed), so
            // `plan_replacement` would read an inflated replication factor and could wrongly declare
            // the partition `blocked`. It is already being drained and is accounted for by the
            // ongoing-reassignment count, so re-planning it is both unnecessary and incorrect.
            if in_flight.contains(&(name.0.to_string(), partition.partition_index)) {
                continue;
            }
            match plan_replacement(&current, draining, &survivors) {
                Some(target) => {
                    planned
                        .entry(name.0.to_string())
                        .or_default()
                        .insert(partition.partition_index, target);
                }
                // Can't keep RF with the survivors left → this partition can't be safely drained.
                None => blocked += 1,
            }
        }
    }
    DrainPlan {
        planned,
        total_moving,
        blocked,
    }
}

/// `AlterPartitionReassignments`(45) — submit the target replica sets computed by [`build_plan`].
///
/// Must run against the controller (see [`crate::cluster`]). Idempotent: re-submitting an
/// in-progress target comes back as `NoReassignmentInProgress` / `ReassignmentInProgress`, neither of
/// which is fatal for the poll model.
pub(crate) async fn submit(
    conn: &mut Connection,
    planned: &BTreeMap<String, ReplicaLayout>,
) -> Result<(), Error> {
    let version =
        conn.pick_version::<AlterPartitionReassignmentsRequest>("AlterPartitionReassignments")?;
    let topics: Vec<ReassignableTopic> = planned
        .iter()
        .map(|(name, partitions)| {
            let parts: Vec<ReassignablePartition> = partitions
                .iter()
                .map(|(idx, replicas)| {
                    ReassignablePartition::default()
                        .with_partition_index(*idx)
                        // `Some(replicas)` sets a target; `None` would *cancel* an in-flight move.
                        .with_replicas(Some(replicas.iter().map(|r| BrokerId(*r)).collect()))
                })
                .collect();
            ReassignableTopic::default()
                .with_name(TopicName(StrBytes::from_string(name.clone())))
                .with_partitions(parts)
        })
        .collect();
    let req = AlterPartitionReassignmentsRequest::default()
        .with_timeout_ms(60_000)
        .with_topics(topics);
    let resp = conn.send(version, "AlterPartitionReassignments", req).await?;

    if resp.error_code != 0 {
        return Err(Error::from_code(
            "AlterPartitionReassignments",
            resp.error_code,
            resp.error_message.as_deref(),
        ));
    }
    for topic in &resp.responses {
        for partition in &topic.partitions {
            let code = partition.error_code;
            if code != 0
                && code != kafka_protocol::ResponseError::NoReassignmentInProgress.code()
                && code != kafka_protocol::ResponseError::ReassignmentInProgress.code()
            {
                return Err(Error::from_code(
                    "AlterPartitionReassignments",
                    code,
                    partition.error_message.as_deref(),
                ));
            }
        }
    }
    Ok(())
}

/// `ListPartitionReassignments`(46) — the set of `(topic, partition)` still being reassigned that
/// still have a draining broker in their *removing* set (or still list one as a current replica).
///
/// Serves double duty: its `len()` is the remaining count driving `Done`, and the set itself is fed
/// back into [`build_plan`] so a partition already mid-reassignment is not re-planned (see there).
pub(crate) async fn ongoing_off(
    conn: &mut Connection,
    draining: &HashSet<i32>,
) -> Result<HashSet<(String, i32)>, Error> {
    let version =
        conn.pick_version::<ListPartitionReassignmentsRequest>("ListPartitionReassignments")?;
    // `topics = None` lists *all* ongoing reassignments cluster-wide.
    let req = ListPartitionReassignmentsRequest::default()
        .with_timeout_ms(30_000)
        .with_topics(None);
    let resp = conn.send(version, "ListPartitionReassignments", req).await?;

    if resp.error_code != 0 {
        return Err(Error::from_code(
            "ListPartitionReassignments",
            resp.error_code,
            resp.error_message.as_deref(),
        ));
    }

    let mut ongoing = HashSet::new();
    for topic in &resp.topics {
        for partition in &topic.partitions {
            let touches_draining = partition
                .removing_replicas
                .iter()
                .chain(partition.replicas.iter())
                .any(|r| draining.contains(&r.0));
            if touches_draining {
                ongoing.insert((topic.name.0.to_string(), partition.partition_index));
            }
        }
    }
    Ok(ongoing)
}

/// Computes a new replica set for one partition: drop every draining broker, then top back up from
/// survivors (not already in the set) to preserve the original replication factor.
///
/// Returns `None` if there aren't enough survivors to keep the RF. Deterministic in → deterministic
/// out (crash-safe: re-planning the same cluster state yields the same plan).
fn plan_replacement(current: &[i32], draining: &HashSet<i32>, survivors: &[i32]) -> Option<Vec<i32>> {
    let rf = current.len();
    // Keep the replicas that are not draining, preserving order (leadership preference).
    let mut kept: Vec<i32> = current
        .iter()
        .copied()
        .filter(|id| !draining.contains(id))
        .collect();
    // Top up from survivors not already present, in id order (deterministic).
    let mut extra: Vec<i32> = survivors
        .iter()
        .copied()
        .filter(|id| !kept.contains(id))
        .collect();
    extra.sort_unstable();
    for id in extra {
        if kept.len() >= rf {
            break;
        }
        kept.push(id);
    }
    if kept.len() < rf {
        // Not enough brokers left to keep the RF.
        return None;
    }
    Some(kept)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[i32]) -> HashSet<i32> {
        ids.iter().copied().collect()
    }

    #[test]
    fn plan_drops_draining_and_keeps_rf() {
        // RF=3, partition on [0,1,2], draining broker 2, survivors 0,1,3.
        let plan = plan_replacement(&[0, 1, 2], &set(&[2]), &[0, 1, 3]).unwrap();
        assert_eq!(plan.len(), 3);
        assert!(!plan.contains(&2));
        assert!(plan.contains(&0) && plan.contains(&1) && plan.contains(&3));
    }

    #[test]
    fn plan_preserves_kept_order() {
        // The non-draining replicas stay first (leadership preference), survivors appended.
        let plan = plan_replacement(&[2, 0, 1], &set(&[2]), &[0, 1, 3]).unwrap();
        assert_eq!(plan[0], 0);
        assert_eq!(plan[1], 1);
        assert_eq!(plan[2], 3);
    }

    #[test]
    fn plan_none_when_not_enough_survivors() {
        // RF=3 but only 2 survivors → cannot keep RF.
        assert!(plan_replacement(&[0, 1, 2], &set(&[2]), &[0, 1]).is_none());
    }

    #[test]
    fn plan_untouched_when_no_draining_replica() {
        // Partition [0,1] with draining broker 2 not present → nothing to drop, stays [0,1].
        let plan = plan_replacement(&[0, 1], &set(&[2]), &[0, 1, 3]).unwrap();
        assert_eq!(plan, vec![0, 1]);
    }

    /// Minimal `MetadataResponse`: brokers by id + one topic whose partitions have the given replica
    /// sets (partition index = position).
    fn meta(brokers: &[i32], topic: &str, partitions: &[Vec<i32>]) -> MetadataResponse {
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };
        let brokers = brokers
            .iter()
            .map(|id| MetadataResponseBroker::default().with_node_id(BrokerId(*id)))
            .collect();
        let partitions = partitions
            .iter()
            .enumerate()
            .map(|(i, reps)| {
                MetadataResponsePartition::default()
                    .with_partition_index(i as i32)
                    .with_replica_nodes(reps.iter().map(|r| BrokerId(*r)).collect())
            })
            .collect();
        let topic = MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(topic.to_string()))))
            .with_partitions(partitions);
        MetadataResponse::default()
            .with_brokers(brokers)
            .with_topics(vec![topic])
    }

    /// Empty in-flight set — no reassignment running yet (the common first-reconcile case).
    fn none_in_flight() -> HashSet<(String, i32)> {
        HashSet::new()
    }

    #[test]
    fn build_plan_blocks_when_draining_below_rf() {
        // 2 brokers, draining broker 1 → only survivor {0}. An RF=2 partition [0,1] can't keep RF2.
        let plan = build_plan(&meta(&[0, 1], "t", &[vec![0, 1]]), &set(&[1]), &none_in_flight());
        assert_eq!(plan.total_moving, 1);
        assert_eq!(plan.blocked, 1, "the partition cannot keep RF2 with one survivor");
        assert!(plan.planned.is_empty());
    }

    #[test]
    fn build_plan_feasible_with_a_survivor() {
        // 3 brokers, draining 2 → survivors {0,1}. RF=2 partition [0,2] moves to a survivor.
        let plan = build_plan(&meta(&[0, 1, 2], "t", &[vec![0, 2]]), &set(&[2]), &none_in_flight());
        assert_eq!(plan.total_moving, 1);
        assert_eq!(plan.blocked, 0);
        assert_eq!(plan.planned.len(), 1);
    }

    #[test]
    fn build_plan_would_misblock_inflated_replica_set_without_in_flight_tracking() {
        // Regression guard for the live drain failure: mid-reassignment, Kafka reports a partition's
        // replicas as the UNION of original + target — draining broker 2 being replaced by survivor 1
        // shows as [0, 2, 1] (RF *looks* like 3). With only {0,1} as survivors, naive re-planning
        // reads RF=3 as unsatisfiable and falsely blocks it.
        let plan = build_plan(
            &meta(&[0, 1, 2], "t", &[vec![0, 2, 1]]),
            &set(&[2]),
            &none_in_flight(),
        );
        assert_eq!(plan.blocked, 1, "inflated RF=3 looks unsatisfiable with 2 survivors");
    }

    #[test]
    fn build_plan_skips_partitions_already_reassigning() {
        // The fix: the SAME inflated replica set, but flagged as already in flight, must NOT be
        // re-planned or blocked — it is already being drained and is counted by the ongoing count.
        let mut in_flight = HashSet::new();
        in_flight.insert(("t".to_string(), 0));
        let plan = build_plan(&meta(&[0, 1, 2], "t", &[vec![0, 2, 1]]), &set(&[2]), &in_flight);
        assert_eq!(plan.total_moving, 1, "still counted as moving");
        assert_eq!(plan.blocked, 0, "already in flight → not blocked");
        assert!(plan.planned.is_empty(), "already in flight → nothing new to submit");
    }
}

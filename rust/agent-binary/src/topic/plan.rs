//! What the agent has to do in Kafka to bring a topic to the state of its `KafkaTopic` spec.

use std::collections::{BTreeMap, BTreeSet};

use stackable_kafka_admin::client::{PartitionReassignment, Topic};
use stackable_kafka_shared::crd::topic::v1alpha1::KafkaTopic;

/// The state of an existing topic in Kafka.
pub struct ObservedTopic {
    pub topic: Topic,
    pub configs: BTreeMap<String, String>,
    pub reassignments: Vec<PartitionReassignment>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TopicPlan {
    Create {
        partitions: i32,
        replication_factor: i16,
        configs: BTreeMap<String, String>,
    },
    Update(TopicUpdate),
}

/// Changes to an existing topic. No grow, configs or unsupported differences means the topic
/// matches the spec.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct TopicUpdate {
    pub grow_partitions_to: Option<i32>,
    pub set_configs: BTreeMap<String, String>,
    pub delete_configs: BTreeSet<String>,
    /// Differences the agent does not apply, as messages for the status
    pub unsupported: Vec<String>,
    pub reassigning_partitions: Vec<i32>,
}

pub fn plan(
    topic: &KafkaTopic,
    observed: Option<&ObservedTopic>,
    managed_config_keys: &BTreeSet<String>,
) -> TopicPlan {
    let spec = &topic.spec;
    let Some(observed) = observed else {
        return TopicPlan::Create {
            partitions: spec.partitions,
            replication_factor: spec.replication_factor,
            configs: spec.config.clone(),
        };
    };

    let mut update = TopicUpdate::default();

    let observed_partitions = observed.topic.partitions.len() as i32;
    if spec.partitions > observed_partitions {
        update.grow_partitions_to = Some(spec.partitions);
    } else if spec.partitions < observed_partitions {
        update.unsupported.push(format!(
            "the topic has {observed_partitions} partitions, but the spec requests {}; partitions cannot be \
             removed",
            spec.partitions
        ));
    }

    if let Some(message) = replication_factor_difference(
        spec.replication_factor,
        &target_replication_factors(observed),
    ) {
        update.unsupported.push(message);
    }

    update.set_configs = spec
        .config
        .iter()
        .filter(|(key, value)| observed.configs.get(*key) != Some(*value))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    update.delete_configs = managed_config_keys
        .iter()
        .filter(|key| !spec.config.contains_key(*key) && observed.configs.contains_key(*key))
        .cloned()
        .collect();

    update.reassigning_partitions = observed
        .reassignments
        .iter()
        .map(|reassignment| reassignment.index)
        .collect();

    TopicPlan::Update(update)
}

/// The replication factor of each partition, excluding replicas a reassignment removes.
fn target_replication_factors(observed: &ObservedTopic) -> BTreeSet<usize> {
    observed
        .topic
        .partitions
        .iter()
        .map(|partition| {
            let removing = observed
                .reassignments
                .iter()
                .find(|reassignment| reassignment.index == partition.index)
                .map_or(0, |reassignment| reassignment.removing_replicas.len());
            partition.replicas.len().saturating_sub(removing)
        })
        .collect()
}

fn replication_factor_difference(
    desired: i16,
    replication_factors: &BTreeSet<usize>,
) -> Option<String> {
    let (Some(min), Some(max)) = (replication_factors.first(), replication_factors.last()) else {
        return None;
    };

    let hint = "change it with kafka-reassign-partitions";
    if min != max {
        Some(format!(
            "the partitions have between {min} and {max} replicas, but the spec requests a \
             replication factor of {desired}; {hint}"
        ))
    } else if *min != desired as usize {
        Some(format!(
            "the topic has a replication factor of {min}, but the spec requests {desired}; {hint}"
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use stackable_kafka_admin::client::Partition;

    use super::*;
    use crate::topic::test_support::kafka_topic;

    fn topic(partitions: i32, replication_factor: i16, config: &[(&str, &str)]) -> KafkaTopic {
        let mut topic = kafka_topic("orders", "orders");
        topic.spec.partitions = partitions;
        topic.spec.replication_factor = replication_factor;
        topic.spec.config = configs(config);
        topic
    }

    fn configs(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn keys(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|key| key.to_string()).collect()
    }

    /// A topic whose partitions have the given replicas
    fn observed(replicas: &[&[i32]], config: &[(&str, &str)]) -> ObservedTopic {
        ObservedTopic {
            topic: Topic {
                id: "UXyU9i5ARn6W00ON2taeWA".to_owned(),
                partitions: replicas
                    .iter()
                    .zip(0..)
                    .map(|(replicas, index)| Partition {
                        index,
                        replicas: replicas.to_vec(),
                    })
                    .collect(),
            },
            configs: configs(config),
            reassignments: vec![],
        }
    }

    #[test]
    fn create_missing_topic() {
        let plan = plan(&topic(3, 2, &[("retention.ms", "1000")]), None, &keys(&[]));

        assert_eq!(
            TopicPlan::Create {
                partitions: 3,
                replication_factor: 2,
                configs: configs(&[("retention.ms", "1000")]),
            },
            plan
        );
    }

    #[test]
    fn do_nothing_for_a_matching_topic() {
        let observed = observed(&[&[1, 2], &[2, 3]], &[("retention.ms", "1000")]);

        let plan = plan(
            &topic(2, 2, &[("retention.ms", "1000")]),
            Some(&observed),
            &keys(&["retention.ms"]),
        );

        assert_eq!(TopicPlan::Update(TopicUpdate::default()), plan);
    }

    #[test]
    fn grow_partitions() {
        let observed = observed(&[&[1], &[2]], &[]);

        let plan = plan(&topic(4, 1, &[]), Some(&observed), &keys(&[]));

        assert_eq!(
            TopicPlan::Update(TopicUpdate {
                grow_partitions_to: Some(4),
                ..TopicUpdate::default()
            }),
            plan
        );
    }

    #[test]
    fn report_fewer_partitions_as_unsupported() {
        let observed = observed(&[&[1], &[2], &[3]], &[]);

        let TopicPlan::Update(update) = plan(&topic(2, 1, &[]), Some(&observed), &keys(&[])) else {
            panic!("the topic exists");
        };

        assert_eq!(None, update.grow_partitions_to);
        assert_eq!(1, update.unsupported.len());
        assert!(update.unsupported[0].contains("cannot be removed"));
    }

    #[test]
    fn set_changed_and_reset_removed_configs() {
        let observed = observed(
            &[&[1]],
            &[
                ("retention.ms", "1000"),
                ("cleanup.policy", "compact"),
                ("segment.bytes", "1024"),
                ("leader.replication.throttled.replicas", "0:1"),
            ],
        );

        let plan = plan(
            &topic(
                1,
                1,
                &[("retention.ms", "2000"), ("max.message.bytes", "10")],
            ),
            Some(&observed),
            &keys(&["retention.ms", "cleanup.policy", "min.insync.replicas"]),
        );

        assert_eq!(
            TopicPlan::Update(TopicUpdate {
                set_configs: configs(&[("max.message.bytes", "10"), ("retention.ms", "2000")]),
                // `segment.bytes` and the throttle were set by others, `min.insync.replicas` is
                // already gone
                delete_configs: keys(&["cleanup.policy"]),
                ..TopicUpdate::default()
            }),
            plan
        );
    }

    #[test]
    fn report_a_different_replication_factor_as_unsupported() {
        let observed = observed(&[&[1, 2, 3], &[2, 3, 4]], &[]);

        let TopicPlan::Update(update) = plan(&topic(2, 2, &[]), Some(&observed), &keys(&[])) else {
            panic!("the topic exists");
        };

        assert_eq!(1, update.unsupported.len());
        assert!(update.unsupported[0].contains("replication factor of 3"));
    }

    #[test]
    fn report_uneven_replica_counts_as_unsupported() {
        let observed = observed(&[&[1, 2], &[2, 3, 4]], &[]);

        let TopicPlan::Update(update) = plan(&topic(2, 2, &[]), Some(&observed), &keys(&[])) else {
            panic!("the topic exists");
        };

        assert_eq!(1, update.unsupported.len());
        assert!(update.unsupported[0].contains("between 2 and 3 replicas"));
    }

    #[test]
    fn accept_a_reassignment_towards_the_desired_replication_factor() {
        let mut observed = observed(&[&[4, 5, 1, 2], &[2, 3]], &[]);
        observed.reassignments = vec![PartitionReassignment {
            index: 0,
            adding_replicas: vec![4, 5],
            removing_replicas: vec![1, 2],
        }];

        let plan = plan(&topic(2, 2, &[]), Some(&observed), &keys(&[]));

        assert_eq!(
            TopicPlan::Update(TopicUpdate {
                reassigning_partitions: vec![0],
                ..TopicUpdate::default()
            }),
            plan
        );
    }

    #[test]
    fn exclude_removed_replicas_from_the_replication_factor() {
        let mut observed = observed(&[&[4, 5, 1, 2], &[2, 3, 4]], &[]);
        observed.reassignments = vec![PartitionReassignment {
            index: 0,
            adding_replicas: vec![4, 5],
            removing_replicas: vec![1, 2],
        }];

        assert_eq!(
            BTreeSet::from([2, 3]),
            target_replication_factors(&observed)
        );
    }
}

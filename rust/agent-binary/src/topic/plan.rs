//! What the agent has to do in Kafka to bring a topic to the state of its `KafkaTopic` spec.

use std::collections::{BTreeMap, BTreeSet};

use stackable_kafka_admin::client::{
    ConfigSource, CreatedTopic, PartitionReassignment, Topic, TopicConfig,
};
use stackable_kafka_shared::crd::topic::{TopicName, v1alpha1::KafkaTopicSpec};

/// Set by `kafka-reassign-partitions` and Cruise Control for the duration of a reassignment.
/// Resetting them would lift the throttle of a running reassignment.
pub const THROTTLE_CONFIG_KEYS: [&str; 2] = [
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
];

/// The state of an existing topic in Kafka.
pub struct ObservedTopic {
    pub topic: Topic,
    pub effective_configs: BTreeMap<String, TopicConfig>,
    pub reassignments: Vec<PartitionReassignment>,
}

/// The state of a topic in Kafka as shown in the status.
#[derive(Debug, Eq, PartialEq)]
pub struct TopicState {
    pub name: TopicName,
    pub id: String,
    pub partition_count: i32,
    pub replication_factor: Option<i16>,
    pub effective_configs: BTreeMap<String, TopicConfig>,
}

impl TopicState {
    pub fn observed(name: TopicName, observed: &ObservedTopic) -> Self {
        Self {
            name,
            id: observed.topic.id.clone(),
            partition_count: observed.topic.partitions.len() as i32,
            replication_factor: replication_factor(observed),
            effective_configs: observed.effective_configs.clone(),
        }
    }

    pub fn created(name: TopicName, created: CreatedTopic) -> Self {
        Self {
            name,
            id: created.id,
            partition_count: created.partitions,
            replication_factor: Some(created.replication_factor),
            effective_configs: created.configs,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TopicPlan {
    Create {
        partition_count: Option<i32>,
        replication_factor: Option<i16>,
        config_overrides: BTreeMap<String, String>,
    },
    Update {
        topic_update: TopicUpdate,
        /// Applied are only the changes in `topic_update`
        unsupported_changes: Vec<UnsupportedChange>,
    },
}

/// Changes to an existing topic. No changes means the topic matches the spec, apart from
/// [`UnsupportedChange`]s.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct TopicUpdate {
    /// Only increased, see [`UnsupportedChange::DecreasePartitions`]
    pub new_partition_count: Option<i32>,
    pub configs_to_set: BTreeMap<String, String>,
    /// Topic-level configs to remove, so that the broker configs apply
    pub configs_to_reset: BTreeSet<String>,
}

/// A difference to the spec the agent does not apply. The other changes are still applied.
#[derive(Debug, Eq, PartialEq)]
pub enum UnsupportedChange {
    /// Kafka cannot remove partitions.
    DecreasePartitions { desired: i32, actual: i32 },
    /// The partitions have between `actual_min` and `actual_max` replicas, not counting replicas
    /// that a running reassignment removes.
    ///
    /// Kafka can only change the replicas of existing partitions with
    /// `AlterPartitionReassignments` and explicit replica lists. Choosing the brokers is left to
    /// `kafka-reassign-partitions` or Cruise Control.
    ChangeReplicationFactor {
        desired: i16,
        actual_min: usize,
        actual_max: usize,
    },
}

pub fn plan(spec: &KafkaTopicSpec, observed: Option<&ObservedTopic>) -> TopicPlan {
    let Some(observed) = observed else {
        return TopicPlan::Create {
            partition_count: spec.partitions,
            replication_factor: spec.replication_factor,
            config_overrides: spec.config_overrides.clone(),
        };
    };

    let actual_partition_count = observed.topic.partitions.len() as i32;
    let (configs_to_set, configs_to_reset) =
        config_changes(&spec.config_overrides, &observed.effective_configs);

    TopicPlan::Update {
        topic_update: TopicUpdate {
            new_partition_count: spec
                .partitions
                .filter(|desired| *desired > actual_partition_count),
            configs_to_set,
            configs_to_reset,
        },
        unsupported_changes: unsupported_changes(spec, observed),
    }
}

fn unsupported_changes(spec: &KafkaTopicSpec, observed: &ObservedTopic) -> Vec<UnsupportedChange> {
    let mut changes = Vec::new();

    let actual_partition_count = observed.topic.partitions.len() as i32;
    if let Some(desired) = spec.partitions
        && desired < actual_partition_count
    {
        changes.push(UnsupportedChange::DecreasePartitions {
            desired,
            actual: actual_partition_count,
        });
    }

    let replication_factors = target_replication_factors(observed);
    if let Some(desired) = spec.replication_factor
        && let (Some(&actual_min), Some(&actual_max)) =
            (replication_factors.first(), replication_factors.last())
        && (actual_min != actual_max || actual_min != desired as usize)
    {
        changes.push(UnsupportedChange::ChangeReplicationFactor {
            desired,
            actual_min,
            actual_max,
        });
    }

    changes
}

/// The configs to set and the configs to reset, so that the topic-level configs are exactly the
/// desired ones. The throttles of partition reassignments are left alone.
fn config_changes(
    desired: &BTreeMap<String, String>,
    effective_configs: &BTreeMap<String, TopicConfig>,
) -> (BTreeMap<String, String>, BTreeSet<String>) {
    // Topic configs have no PASSWORD type, so Kafka does not redact their topic-level values.
    let topic_level_configs = effective_configs
        .iter()
        .filter(|(_, config)| config.source == ConfigSource::DynamicTopic)
        .map(|(key, config)| (key, config.value.as_deref()))
        .collect::<BTreeMap<_, _>>();

    // Compared with the topic-level value, not the effective one: a desired value that equals the
    // broker config is still pinned on the topic. Otherwise a later change of the broker config
    // would apply to the topic until the next resync. Unchanged values are skipped, so a resync
    // without drift does not write to Kafka.
    let configs_to_set = desired
        .iter()
        .filter(|(key, value)| topic_level_configs.get(key) != Some(&Some(value.as_str())))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let configs_to_reset = topic_level_configs
        .keys()
        .filter(|key| !desired.contains_key(**key) && !THROTTLE_CONFIG_KEYS.contains(&key.as_str()))
        .map(|key| key.to_string())
        .collect();
    (configs_to_set, configs_to_reset)
}

/// The replication factor of the partitions, excluding replicas a reassignment removes. `None` if
/// the partitions have different replication factors.
fn replication_factor(observed: &ObservedTopic) -> Option<i16> {
    match target_replication_factors(observed).as_slice() {
        [replication_factor] => i16::try_from(*replication_factor).ok(),
        _ => None,
    }
}

/// The distinct replication factors of the partitions in ascending order, excluding replicas a
/// reassignment removes.
///
/// Kafka stores no replication factor per topic. During a reassignment, the replicas of a
/// partition include the ones being added and removed. `kafka-topics.sh --describe` shows the
/// replicas minus the ones being added of the first partition instead.
fn target_replication_factors(observed: &ObservedTopic) -> Vec<usize> {
    observed
        .topic
        .partitions
        .iter()
        .map(|partition| {
            let removing = observed
                .reassignments
                .iter()
                .find(|reassignment| reassignment.index == partition.index)
                .map_or(0, |reassignment| reassignment.removing_replicas);
            partition.replicas.saturating_sub(removing)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use stackable_kafka_admin::client::Partition;

    use super::*;
    use crate::topic::test_support::kafka_topic;

    fn spec(
        partitions: Option<i32>,
        replication_factor: Option<i16>,
        config: &[(&str, &str)],
    ) -> KafkaTopicSpec {
        let mut spec = kafka_topic("orders", Some("orders")).spec;
        spec.partitions = partitions;
        spec.replication_factor = replication_factor;
        spec.config_overrides = configs(config);
        spec
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

    fn topic_configs(entries: &[(&str, &str, ConfigSource)]) -> BTreeMap<String, TopicConfig> {
        entries
            .iter()
            .map(|(key, value, source)| {
                (
                    key.to_string(),
                    TopicConfig {
                        value: Some(value.to_string()),
                        source: *source,
                    },
                )
            })
            .collect()
    }

    fn update(topic_update: TopicUpdate, unsupported_changes: Vec<UnsupportedChange>) -> TopicPlan {
        TopicPlan::Update {
            topic_update,
            unsupported_changes,
        }
    }

    /// A topic whose partitions have the given number of replicas
    fn observed(replicas: &[usize], configs: &[(&str, &str, ConfigSource)]) -> ObservedTopic {
        ObservedTopic {
            topic: Topic {
                id: "UXyU9i5ARn6W00ON2taeWA".to_owned(),
                partitions: replicas
                    .iter()
                    .zip(0..)
                    .map(|(replicas, index)| Partition {
                        index,
                        replicas: *replicas,
                    })
                    .collect(),
            },
            effective_configs: topic_configs(configs),
            reassignments: vec![],
        }
    }

    #[test]
    fn create_missing_topic() {
        let plan = plan(&spec(Some(3), None, &[("retention.ms", "1000")]), None);

        assert_eq!(
            TopicPlan::Create {
                partition_count: Some(3),
                replication_factor: None,
                config_overrides: configs(&[("retention.ms", "1000")]),
            },
            plan
        );
    }

    #[test]
    fn do_nothing_for_a_matching_topic() {
        let observed = observed(
            &[2, 2],
            &[("retention.ms", "1000", ConfigSource::DynamicTopic)],
        );
        let spec = spec(Some(2), Some(2), &[("retention.ms", "1000")]);

        assert_eq!(
            update(TopicUpdate::default(), vec![]),
            plan(&spec, Some(&observed))
        );
    }

    #[test]
    fn leave_partitions_and_replication_factor_alone_if_not_set() {
        let observed = observed(&[3, 3, 3], &[]);
        let spec = spec(None, None, &[]);

        assert_eq!(
            update(TopicUpdate::default(), vec![]),
            plan(&spec, Some(&observed))
        );
    }

    #[test]
    fn grow_partitions() {
        let observed = observed(&[1, 1], &[]);

        let plan = plan(&spec(Some(4), Some(1), &[]), Some(&observed));

        assert_eq!(
            update(
                TopicUpdate {
                    new_partition_count: Some(4),
                    ..TopicUpdate::default()
                },
                vec![]
            ),
            plan
        );
    }

    #[test]
    fn report_fewer_partitions_as_unsupported() {
        let observed = observed(&[1, 1, 1], &[]);

        let plan = plan(&spec(Some(2), None, &[]), Some(&observed));

        assert_eq!(
            update(
                TopicUpdate::default(),
                vec![UnsupportedChange::DecreasePartitions {
                    desired: 2,
                    actual: 3
                }]
            ),
            plan
        );
    }

    #[test]
    fn replace_the_topic_level_configs() {
        let observed = observed(
            &[1],
            &[
                ("retention.ms", "1000", ConfigSource::DynamicTopic),
                ("cleanup.policy", "compact", ConfigSource::DynamicTopic),
                ("segment.bytes", "1024", ConfigSource::Default),
                ("max.message.bytes", "10", ConfigSource::StaticBroker),
                (
                    "leader.replication.throttled.replicas",
                    "0:1",
                    ConfigSource::DynamicTopic,
                ),
            ],
        );

        let plan = plan(
            &spec(
                None,
                None,
                &[("retention.ms", "2000"), ("max.message.bytes", "10")],
            ),
            Some(&observed),
        );

        assert_eq!(
            update(
                TopicUpdate {
                    // `max.message.bytes` already has the value, but not on the topic
                    configs_to_set: configs(&[
                        ("max.message.bytes", "10"),
                        ("retention.ms", "2000")
                    ]),
                    // `segment.bytes` is not set on the topic, the throttle belongs to a
                    // reassignment
                    configs_to_reset: keys(&["cleanup.policy"]),
                    ..TopicUpdate::default()
                },
                vec![]
            ),
            plan
        );
    }

    #[test]
    fn report_a_different_replication_factor_as_unsupported() {
        let observed = observed(&[3, 3], &[]);

        assert_eq!(
            vec![UnsupportedChange::ChangeReplicationFactor {
                desired: 2,
                actual_min: 3,
                actual_max: 3
            }],
            unsupported_changes(&spec(None, Some(2), &[]), &observed)
        );
    }

    #[test]
    fn report_uneven_replica_counts_as_unsupported() {
        let observed = observed(&[2, 3], &[]);

        assert_eq!(
            vec![UnsupportedChange::ChangeReplicationFactor {
                desired: 2,
                actual_min: 2,
                actual_max: 3
            }],
            unsupported_changes(&spec(None, Some(2), &[]), &observed)
        );
    }

    #[test]
    fn accept_a_reassignment_towards_the_desired_replication_factor() {
        let mut observed = observed(&[4, 2], &[]);
        observed.reassignments = vec![PartitionReassignment {
            index: 0,
            removing_replicas: 2,
        }];

        assert_eq!(
            Vec::<UnsupportedChange>::new(),
            unsupported_changes(&spec(None, Some(2), &[]), &observed)
        );
    }

    #[test]
    fn report_the_target_replication_factor() {
        let mut reassigning = observed(&[4, 2], &[]);
        reassigning.reassignments = vec![PartitionReassignment {
            index: 0,
            removing_replicas: 2,
        }];

        assert_eq!(Some(2), replication_factor(&reassigning));
        assert_eq!(None, replication_factor(&observed(&[2, 3], &[])));
    }
}

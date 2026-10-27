//! The status of a `KafkaTopic` for the outcome of a sync.

use snafu::ResultExt;
use stackable_kafka_shared::crd::topic::v1alpha1::{
    KafkaTopic, KafkaTopicConfigStatus, KafkaTopicStatus,
};
use stackable_operator::{
    agent::condition::update_conditions,
    k8s_openapi::{apimachinery::pkg::apis::meta::v1::Time, jiff::Timestamp},
};

use super::{
    condition::{TopicSyncOutcome, conditions_from},
    controller::{Ctx, Result, UpdateStatusSnafu},
    plan::TopicState,
};
use crate::AGENT_NAME;

/// Patches the status only if it changed, so the agent does not trigger its own watch.
pub(crate) async fn update_status(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
    sync_outcome: &TopicSyncOutcome,
    topic_state: Option<&TopicState>,
) -> Result<()> {
    let current_status = kafka_topic.status.clone().unwrap_or_default();
    let new_status = status_from(
        &current_status,
        sync_outcome,
        topic_state,
        &Time(Timestamp::now()),
        kafka_topic.metadata.generation,
    );

    if new_status != current_status {
        ctx.client
            .apply_patch_status(AGENT_NAME, kafka_topic, &new_status)
            .await
            .context(UpdateStatusSnafu)?;
    }
    Ok(())
}

/// Fields not known after the outcome are kept from `current_status`.
fn status_from(
    current_status: &KafkaTopicStatus,
    sync_outcome: &TopicSyncOutcome,
    topic_state: Option<&TopicState>,
    now: &Time,
    observed_generation: Option<i64>,
) -> KafkaTopicStatus {
    let conditions = update_conditions(
        &current_status.conditions,
        conditions_from(sync_outcome, topic_state),
        now,
        observed_generation,
    );
    match (sync_outcome, topic_state) {
        (_, Some(topic_state)) => KafkaTopicStatus {
            conditions,
            topic_id: Some(topic_state.id.clone()),
            topic_name: Some(topic_state.name.clone()),
            partitions: Some(topic_state.partition_count),
            replication_factor: topic_state.replication_factor,
            config: topic_state
                .effective_configs
                .iter()
                .map(|(key, config)| {
                    let config = KafkaTopicConfigStatus {
                        value: config.value.clone(),
                        source: config.source.to_string(),
                    };
                    (key.clone(), config)
                })
                .collect(),
        },
        // The topic does not exist or is managed by another KafkaTopic.
        (TopicSyncOutcome::Conflict { .. } | TopicSyncOutcome::ReconciliationPaused, None) => {
            KafkaTopicStatus {
                conditions,
                ..KafkaTopicStatus::default()
            }
        }
        _ => KafkaTopicStatus {
            conditions,
            ..current_status.clone()
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_kafka_admin::client::{ConfigSource, TopicConfig};
    use stackable_kafka_shared::crd::topic::TopicName;

    use super::*;

    fn now() -> Time {
        Time(Timestamp::from_second(10).expect("the timestamp should be valid"))
    }

    fn topic_state() -> TopicState {
        TopicState {
            name: TopicName::from_str("orders").expect("the topic name should be valid"),
            id: "UXyU9i5ARn6W00ON2taeWA".to_owned(),
            partition_count: 6,
            replication_factor: Some(3),
            effective_configs: BTreeMap::from([(
                "retention.ms".to_owned(),
                TopicConfig {
                    value: Some("1000".to_owned()),
                    source: ConfigSource::DynamicTopic,
                },
            )]),
        }
    }

    fn synced() -> TopicSyncOutcome {
        TopicSyncOutcome::Synced {
            reassigning_partition_count: 0,
        }
    }

    #[test]
    fn show_the_topic_in_kafka() {
        let status = status_from(
            &KafkaTopicStatus::default(),
            &synced(),
            Some(&topic_state()),
            &now(),
            Some(2),
        );

        assert_eq!(Some("UXyU9i5ARn6W00ON2taeWA".to_owned()), status.topic_id);
        assert_eq!(Some(6), status.partitions);
        assert_eq!(Some(3), status.replication_factor);
        assert_eq!(
            Some(&KafkaTopicConfigStatus {
                value: Some("1000".to_owned()),
                source: "DYNAMIC_TOPIC_CONFIG".to_owned(),
            }),
            status.config.get("retention.ms")
        );
        assert!(
            status
                .conditions
                .iter()
                .all(|condition| condition.observed_generation == Some(2))
        );
    }

    #[test]
    fn keep_the_topic_if_kafka_cannot_be_reached() {
        let current = status_from(
            &KafkaTopicStatus::default(),
            &synced(),
            Some(&topic_state()),
            &now(),
            Some(1),
        );

        let status = status_from(
            &current,
            &TopicSyncOutcome::ConnectionFailed {
                message: "timed out".to_owned(),
            },
            None,
            &now(),
            Some(1),
        );

        assert_eq!(current.topic_id, status.topic_id);
        assert_eq!(current.config, status.config);
    }

    #[rstest::rstest]
    #[case::conflict(TopicSyncOutcome::Conflict { message: String::new() })]
    #[case::paused_without_topic(TopicSyncOutcome::ReconciliationPaused)]
    fn clear_the_topic_if_it_is_not_managed(#[case] sync_outcome: TopicSyncOutcome) {
        let current = status_from(
            &KafkaTopicStatus::default(),
            &synced(),
            Some(&topic_state()),
            &now(),
            Some(1),
        );

        let status = status_from(&current, &sync_outcome, None, &now(), Some(1));

        assert_eq!(None, status.topic_id);
        assert!(status.config.is_empty());
        assert_eq!(2, status.conditions.len());
    }
}

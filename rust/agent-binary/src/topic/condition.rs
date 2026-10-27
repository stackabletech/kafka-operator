//! The `Ready` and `Synced` conditions of a `KafkaTopic` for the outcome of a sync.

use stackable_kafka_admin::client;
use stackable_operator::agent::condition::{
    ConditionStatus, ManagedCondition, ManagedConditionType,
};

use super::plan::{TopicState, UnsupportedChange};

#[derive(Debug, Eq, PartialEq)]
pub enum TopicSyncOutcome {
    /// The topic matches the spec
    Synced {
        reassigning_partition_count: usize,
    },
    /// The topic matches the spec, apart from the changes the agent does not apply
    UnsupportedChange {
        changes: Vec<UnsupportedChange>,
    },
    ReconciliationPaused,
    InvalidSpec {
        message: String,
    },
    /// Another `KafkaTopic` manages the same topic
    Conflict {
        message: String,
    },
    KafkaRejected {
        message: String,
    },
    ConnectionFailed {
        message: String,
    },
}

impl TopicSyncOutcome {
    pub fn from_client_error(error: &client::Error) -> Self {
        let message = error_chain(error);
        if error.is_rejected_by_kafka() {
            TopicSyncOutcome::KafkaRejected { message }
        } else {
            TopicSyncOutcome::ConnectionFailed { message }
        }
    }
}

/// `topic_state` is the topic in Kafka, if it exists and is managed by this `KafkaTopic`.
pub fn conditions_from(
    sync_outcome: &TopicSyncOutcome,
    topic_state: Option<&TopicState>,
) -> Vec<ManagedCondition> {
    vec![
        ready_from(sync_outcome, topic_state),
        synced_from(sync_outcome, topic_state),
    ]
}

/// Whether the topic exists in Kafka, `Unknown` if the agent could not find out.
fn ready_from(
    sync_outcome: &TopicSyncOutcome,
    topic_state: Option<&TopicState>,
) -> ManagedCondition {
    use ConditionStatus::{False, True, Unknown};

    // Also after a failed change, because the topic was observed before.
    if let Some(topic_state) = topic_state {
        return ready(
            True,
            "Available",
            &format!(
                "Topic {} exists with {} partitions",
                topic_state.name, topic_state.partition_count
            ),
        );
    }
    match sync_outcome {
        TopicSyncOutcome::Synced { .. }
        | TopicSyncOutcome::UnsupportedChange { .. }
        | TopicSyncOutcome::ReconciliationPaused => {
            ready(False, "Unavailable", "the topic does not exist in Kafka")
        }
        TopicSyncOutcome::Conflict { .. } => {
            ready(False, "Conflict", "another KafkaTopic manages the topic")
        }
        TopicSyncOutcome::InvalidSpec { message } => ready(Unknown, "InvalidSpec", message),
        TopicSyncOutcome::KafkaRejected { message } => ready(Unknown, "KafkaRejected", message),
        TopicSyncOutcome::ConnectionFailed { message } => {
            ready(Unknown, "ConnectionFailed", message)
        }
    }
}

/// Whether the last sync applied the spec to the topic.
fn synced_from(
    sync_outcome: &TopicSyncOutcome,
    topic_state: Option<&TopicState>,
) -> ManagedCondition {
    use ConditionStatus::{False, True};

    match sync_outcome {
        TopicSyncOutcome::Synced {
            reassigning_partition_count,
        } => match (reassigning_partition_count, topic_state) {
            (0, _) | (_, None) => synced(True, "Synced", "the topic matches the spec"),
            (reassigning, Some(topic_state)) => synced(
                True,
                "ReassignmentInProgress",
                &format!(
                    "{reassigning} of {} partitions are being reassigned",
                    topic_state.partition_count
                ),
            ),
        },
        TopicSyncOutcome::UnsupportedChange { changes } => {
            let messages = changes.iter().map(message_from).collect::<Vec<_>>();
            synced(False, "UnsupportedChange", &messages.join("; "))
        }
        TopicSyncOutcome::ReconciliationPaused => synced(
            False,
            "ReconciliationPaused",
            "the reconciliation is paused, only the status is updated",
        ),
        TopicSyncOutcome::Conflict { message } => synced(False, "Conflict", message),
        TopicSyncOutcome::InvalidSpec { message } => synced(False, "InvalidSpec", message),
        TopicSyncOutcome::KafkaRejected { message } => synced(False, "KafkaRejected", message),
        TopicSyncOutcome::ConnectionFailed { message } => {
            synced(False, "ConnectionFailed", message)
        }
    }
}

fn message_from(unsupported_change: &UnsupportedChange) -> String {
    match unsupported_change {
        UnsupportedChange::DecreasePartitions { desired, actual } => {
            format!("partitions cannot be decreased (spec: {desired}, actual: {actual})")
        }
        UnsupportedChange::ChangeReplicationFactor {
            desired,
            actual_min,
            actual_max,
        } => {
            let actual = if actual_min == actual_max {
                actual_min.to_string()
            } else {
                format!("between {actual_min} and {actual_max}")
            };
            format!(
                "the replication factor cannot be changed (spec: {desired}, actual: {actual}); \
                 change it with kafka-reassign-partitions"
            )
        }
    }
}

/// Renders the error and its sources, skipping a source whose message the previous error already
/// contains.
fn error_chain(error: &dyn std::error::Error) -> String {
    snafu::CleanedErrorText::new(error)
        .map(|(_, text, _)| text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(": ")
}

fn ready(status: ConditionStatus, reason: &str, message: &str) -> ManagedCondition {
    condition(ManagedConditionType::Ready, status, reason, message)
}

fn synced(status: ConditionStatus, reason: &str, message: &str) -> ManagedCondition {
    condition(ManagedConditionType::Synced, status, reason, message)
}

fn condition(
    type_: ManagedConditionType,
    status: ConditionStatus,
    reason: &str,
    message: &str,
) -> ManagedCondition {
    ManagedCondition {
        type_,
        status,
        reason: reason.to_owned(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use snafu::Snafu;
    use stackable_kafka_shared::crd::topic::TopicName;

    use super::*;
    use crate::topic::condition::ConditionStatus::{False, True, Unknown};

    fn summary(conditions: &[ManagedCondition]) -> Vec<(String, ConditionStatus, &str)> {
        conditions
            .iter()
            .map(|condition| {
                (
                    condition.type_.to_string(),
                    condition.status,
                    condition.reason.as_str(),
                )
            })
            .collect()
    }

    fn topic_state(partition_count: i32) -> TopicState {
        TopicState {
            name: TopicName::from_str("orders").expect("the topic name should be valid"),
            id: "UXyU9i5ARn6W00ON2taeWA".to_owned(),
            partition_count,
            replication_factor: Some(1),
            effective_configs: BTreeMap::new(),
        }
    }

    #[test]
    fn mark_a_synced_topic() {
        let conditions = conditions_from(
            &TopicSyncOutcome::Synced {
                reassigning_partition_count: 0,
            },
            Some(&topic_state(6)),
        );

        assert_eq!(
            vec![
                ("Ready".to_owned(), True, "Available"),
                ("Synced".to_owned(), True, "Synced")
            ],
            summary(&conditions)
        );
        assert_eq!(
            "Topic orders exists with 6 partitions",
            conditions[0].message
        );
    }

    #[test]
    fn keep_a_reassigned_topic_synced() {
        let conditions = conditions_from(
            &TopicSyncOutcome::Synced {
                reassigning_partition_count: 3,
            },
            Some(&topic_state(6)),
        );

        assert_eq!(
            vec![
                ("Ready".to_owned(), True, "Available"),
                ("Synced".to_owned(), True, "ReassignmentInProgress")
            ],
            summary(&conditions)
        );
        assert_eq!(
            "3 of 6 partitions are being reassigned",
            conditions[1].message
        );
    }

    #[test]
    fn mark_an_unsupported_change() {
        let conditions = conditions_from(
            &TopicSyncOutcome::UnsupportedChange {
                changes: vec![
                    UnsupportedChange::DecreasePartitions {
                        desired: 4,
                        actual: 6,
                    },
                    UnsupportedChange::ChangeReplicationFactor {
                        desired: 2,
                        actual_min: 2,
                        actual_max: 3,
                    },
                ],
            },
            Some(&topic_state(6)),
        );

        assert_eq!(
            vec![
                ("Ready".to_owned(), True, "Available"),
                ("Synced".to_owned(), False, "UnsupportedChange")
            ],
            summary(&conditions)
        );
        assert_eq!(
            "partitions cannot be decreased (spec: 4, actual: 6); the replication factor cannot \
             be changed (spec: 2, actual: between 2 and 3); change it with \
             kafka-reassign-partitions",
            conditions[1].message
        );
    }

    #[test]
    fn report_a_missing_topic_while_paused() {
        let conditions = conditions_from(&TopicSyncOutcome::ReconciliationPaused, None);

        assert_eq!(
            vec![
                ("Ready".to_owned(), False, "Unavailable"),
                ("Synced".to_owned(), False, "ReconciliationPaused")
            ],
            summary(&conditions)
        );
    }

    #[test]
    fn mark_a_conflict_as_not_ready() {
        let conditions = conditions_from(
            &TopicSyncOutcome::Conflict {
                message: "managed by older".to_owned(),
            },
            None,
        );

        assert_eq!(
            vec![
                ("Ready".to_owned(), False, "Conflict"),
                ("Synced".to_owned(), False, "Conflict")
            ],
            summary(&conditions)
        );
    }

    #[rstest::rstest]
    #[case::invalid_spec(TopicSyncOutcome::InvalidSpec { message: "invalid".to_owned() }, "InvalidSpec")]
    #[case::kafka_rejected(TopicSyncOutcome::KafkaRejected { message: "denied".to_owned() }, "KafkaRejected")]
    #[case::connection_failed(TopicSyncOutcome::ConnectionFailed { message: "timed out".to_owned() }, "ConnectionFailed")]
    fn mark_ready_as_unknown_if_the_topic_cannot_be_observed(
        #[case] sync_outcome: TopicSyncOutcome,
        #[case] reason: &str,
    ) {
        assert_eq!(
            vec![
                ("Ready".to_owned(), Unknown, reason),
                ("Synced".to_owned(), False, reason)
            ],
            summary(&conditions_from(&sync_outcome, None))
        );
    }

    #[test]
    fn keep_an_observed_topic_ready_if_a_change_fails() {
        let conditions = conditions_from(
            &TopicSyncOutcome::KafkaRejected {
                message: "denied".to_owned(),
            },
            Some(&topic_state(6)),
        );

        assert_eq!(
            vec![
                ("Ready".to_owned(), True, "Available"),
                ("Synced".to_owned(), False, "KafkaRejected")
            ],
            summary(&conditions)
        );
    }

    #[derive(Debug, Snafu)]
    enum Outer {
        #[snafu(display("failed to connect to the bootstrap server kafka-0:9093"))]
        Connect { source: Inner },
    }

    #[derive(Debug, Snafu)]
    #[snafu(display("invalid peer certificate: UnknownIssuer"))]
    struct Inner;

    #[test]
    fn render_the_error_chain() {
        let error = Outer::Connect { source: Inner };

        assert_eq!(
            "failed to connect to the bootstrap server kafka-0:9093: invalid peer certificate: \
             UnknownIssuer",
            error_chain(&error)
        );
    }
}

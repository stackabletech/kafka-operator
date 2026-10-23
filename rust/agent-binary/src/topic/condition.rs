//! The `Ready` and `Synced` conditions of a `KafkaTopic` for the outcome of a sync.

use stackable_kafka_admin::client;
use stackable_operator::commons::platform_access::condition::{
    ManagedCondition, ManagedConditionType,
};

const MAX_LISTED_PARTITIONS: usize = 10;

#[derive(Debug, Eq, PartialEq)]
pub enum TopicSyncOutcome {
    /// The topic matches the spec
    Synced {
        partitions: usize,
        reassigning_partitions: Vec<i32>,
    },
    UnsupportedChange {
        messages: Vec<String>,
    },
    ClusterNotReady,
    InvalidSpec {
        message: String,
    },
    /// Another `KafkaTopic` manages the same topic
    Conflict {
        message: String,
    },
    /// The topic exists in Kafka, but this `KafkaTopic` does not own it and is not allowed to adopt
    /// it
    AlreadyExists {
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

/// The conditions to update. `Ready` says whether the topic is usable through this `KafkaTopic`
/// and is only updated if that is known.
pub fn conditions(outcome: &TopicSyncOutcome) -> Vec<ManagedCondition> {
    let available = ready(true, "Available", "the topic exists in Kafka");
    match outcome {
        TopicSyncOutcome::Synced {
            partitions,
            reassigning_partitions,
        } => {
            let synced = if reassigning_partitions.is_empty() {
                synced(true, "Synced", "the topic matches the spec")
            } else {
                synced(
                    true,
                    "ReassignmentInProgress",
                    &reassignment_message(reassigning_partitions, *partitions),
                )
            };
            vec![available, synced]
        }
        TopicSyncOutcome::UnsupportedChange { messages } => vec![
            available,
            synced(false, "UnsupportedChange", &messages.join("; ")),
        ],
        TopicSyncOutcome::ClusterNotReady => {
            let message = "the KafkaCluster has no bootstrap servers yet";
            vec![
                ready(false, "ClusterNotReady", message),
                synced(false, "ClusterNotReady", message),
            ]
        }
        TopicSyncOutcome::Conflict { message } => vec![
            ready(false, "Conflict", "another KafkaTopic manages the topic"),
            synced(false, "Conflict", message),
        ],
        TopicSyncOutcome::AlreadyExists { message } => vec![
            ready(
                false,
                "AlreadyExists",
                "the topic exists in Kafka, but is not owned by this KafkaTopic",
            ),
            synced(false, "AlreadyExists", message),
        ],
        TopicSyncOutcome::InvalidSpec { message } => vec![synced(false, "InvalidSpec", message)],
        TopicSyncOutcome::KafkaRejected { message } => {
            vec![synced(false, "KafkaRejected", message)]
        }
        TopicSyncOutcome::ConnectionFailed { message } => {
            vec![synced(false, "ConnectionFailed", message)]
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

fn reassignment_message(reassigning_partitions: &[i32], partitions: usize) -> String {
    let listed = reassigning_partitions
        .iter()
        .take(MAX_LISTED_PARTITIONS)
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let more = reassigning_partitions
        .len()
        .saturating_sub(MAX_LISTED_PARTITIONS);
    if more > 0 {
        format!("partitions {listed} and {more} more of {partitions} are being reassigned")
    } else {
        format!("partitions {listed} of {partitions} are being reassigned")
    }
}

fn ready(status: bool, reason: &str, message: &str) -> ManagedCondition {
    condition(ManagedConditionType::Ready, status, reason, message)
}

fn synced(status: bool, reason: &str, message: &str) -> ManagedCondition {
    condition(ManagedConditionType::Synced, status, reason, message)
}

fn condition(
    type_: ManagedConditionType,
    status: bool,
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
    use snafu::Snafu;

    use super::*;

    fn summary(conditions: &[ManagedCondition]) -> Vec<(String, bool, &str)> {
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

    #[test]
    fn mark_a_synced_topic() {
        let conditions = conditions(&TopicSyncOutcome::Synced {
            partitions: 6,
            reassigning_partitions: vec![],
        });

        assert_eq!(
            vec![
                ("Ready".to_owned(), true, "Available"),
                ("Synced".to_owned(), true, "Synced")
            ],
            summary(&conditions)
        );
    }

    #[test]
    fn keep_a_reassigned_topic_synced() {
        let conditions = conditions(&TopicSyncOutcome::Synced {
            partitions: 6,
            reassigning_partitions: vec![0, 2],
        });

        assert_eq!(
            vec![
                ("Ready".to_owned(), true, "Available"),
                ("Synced".to_owned(), true, "ReassignmentInProgress")
            ],
            summary(&conditions)
        );
        assert_eq!(
            "partitions 0, 2 of 6 are being reassigned",
            conditions[1].message
        );
    }

    #[test]
    fn limit_the_listed_reassigning_partitions() {
        let message = reassignment_message(&(0..12).collect::<Vec<_>>(), 20);

        assert_eq!(
            "partitions 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 and 2 more of 20 are being reassigned",
            message
        );
    }

    #[test]
    fn only_update_synced_after_an_error() {
        let conditions = conditions(&TopicSyncOutcome::ConnectionFailed {
            message: "timed out".to_owned(),
        });

        assert_eq!(
            vec![("Synced".to_owned(), false, "ConnectionFailed")],
            summary(&conditions)
        );
    }

    #[test]
    fn mark_an_unsupported_change() {
        let conditions = conditions(&TopicSyncOutcome::UnsupportedChange {
            messages: vec!["first".to_owned(), "second".to_owned()],
        });

        assert_eq!(
            vec![
                ("Ready".to_owned(), true, "Available"),
                ("Synced".to_owned(), false, "UnsupportedChange")
            ],
            summary(&conditions)
        );
        assert_eq!("first; second", conditions[1].message);
    }

    #[test]
    fn leave_ready_untouched_for_an_invalid_spec() {
        let conditions = conditions(&TopicSyncOutcome::InvalidSpec {
            message: "invalid".to_owned(),
        });

        assert_eq!(
            vec![("Synced".to_owned(), false, "InvalidSpec")],
            summary(&conditions)
        );
    }

    #[test]
    fn keep_the_ready_message_fixed_for_a_foreign_topic() {
        let conditions = conditions(&TopicSyncOutcome::AlreadyExists {
            message: "set managementPolicy.adoption to Adopt".to_owned(),
        });

        assert_eq!(
            vec![
                ("Ready".to_owned(), false, "AlreadyExists"),
                ("Synced".to_owned(), false, "AlreadyExists")
            ],
            summary(&conditions)
        );
        assert_eq!(
            "the topic exists in Kafka, but is not owned by this KafkaTopic",
            conditions[0].message
        );
        assert_eq!(
            "set managementPolicy.adoption to Adopt",
            conditions[1].message
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

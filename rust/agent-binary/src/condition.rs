//! Conditions on `KafkaTopic` (spike). Ported from the zk-agent's `ZnodeConditionBuilder`.
//!
//! `ClusterConditionType` is a closed enum, so the topic's provisioning state is mapped onto the
//! `Available` / `Degraded` pair with a distinguishing `reason` — a readable signal for "topic
//! provisioned" vs "the agent could not reach/authenticate to Kafka" vs "no agent is running".

use stackable_operator::status::condition::{
    ClusterCondition, ClusterConditionSet, ClusterConditionStatus, ClusterConditionType,
    ConditionBuilder,
};

/// The provisioning state of a single `KafkaTopic`.
pub enum TopicState {
    /// Provisioned (created/altered) successfully.
    Provisioned,
    /// Provisioning was attempted but failed (bad credential, Kafka rejected the client cert,
    /// connection failed, config clash, …).
    Degraded { reason: String, message: String },
    /// No agent is running to provision this topic — its liveness lease is stale or absent.
    AgentUnavailable { message: String },
}

/// Builds the `Available`/`Degraded` conditions for a [`TopicState`].
pub struct TopicConditionBuilder {
    state: TopicState,
}

impl TopicConditionBuilder {
    pub fn provisioned() -> Self {
        Self {
            state: TopicState::Provisioned,
        }
    }

    pub fn degraded(reason: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            state: TopicState::Degraded {
                reason: reason.into(),
                message: message.into(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn agent_unavailable(message: impl Into<String>) -> Self {
        Self {
            state: TopicState::AgentUnavailable {
                message: message.into(),
            },
        }
    }
}

impl ConditionBuilder for TopicConditionBuilder {
    fn build_conditions(&self) -> ClusterConditionSet {
        // (available status, degraded status, reason, message)
        let (available, degraded, reason, message) = match &self.state {
            TopicState::Provisioned => (
                ClusterConditionStatus::True,
                ClusterConditionStatus::False,
                None,
                "The Kafka topic is provisioned.".to_string(),
            ),
            TopicState::Degraded { reason, message } => (
                ClusterConditionStatus::False,
                ClusterConditionStatus::True,
                Some(reason.clone()),
                message.clone(),
            ),
            TopicState::AgentUnavailable { message } => (
                ClusterConditionStatus::False,
                ClusterConditionStatus::True,
                Some("AgentUnavailable".to_string()),
                message.clone(),
            ),
        };

        vec![
            ClusterCondition {
                type_: ClusterConditionType::Available,
                status: available,
                reason: reason.clone(),
                message: Some(message.clone()),
                last_transition_time: None,
            },
            ClusterCondition {
                type_: ClusterConditionType::Degraded,
                status: degraded,
                reason,
                message: Some(message),
                last_transition_time: None,
            },
        ]
        .into()
    }
}

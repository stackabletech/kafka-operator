//! Takes over `KafkaTopic`s while no agent is available: reports it in their status and removes
//! their finalizer once the KafkaCluster is gone, so deleting the KafkaCluster or the namespace
//! does not block.

use std::sync::Arc;

use const_format::concatcp;
use serde_json::json;
use snafu::{ResultExt, Snafu};
use stackable_kafka_shared::crd::topic::{KAFKA_TOPIC_FINALIZER, v1alpha1::KafkaTopic};
use stackable_operator::{
    agent::condition::{
        ConditionStatus, ManagedCondition, ManagedConditionType, update_conditions,
    },
    k8s_openapi::{
        api::apps::v1::Deployment,
        apimachinery::pkg::apis::meta::v1::Time,
        jiff::{SignedDuration, Timestamp},
    },
    kube::{
        ResourceExt,
        core::{DeserializeGuard, error_boundary},
        runtime::{
            controller::Action,
            reflector::{ObjectRef, Store},
        },
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::agent_resource_names_of,
    crd::{KAFKA_OPERATOR_NAME, v1alpha1::KafkaCluster},
};

pub const TOPIC_CONTROLLER_NAME: &str = "kafkatopic";
pub const TOPIC_FULL_CONTROLLER_NAME: &str =
    concatcp!(TOPIC_CONTROLLER_NAME, '.', KAFKA_OPERATOR_NAME);

const AGENT_UNAVAILABLE_REASON: &str = "AgentUnavailable";

/// `DeploymentConditionType` of `apps/v1`, true once the minimum number of replicas is available
const DEPLOYMENT_CONDITION_AVAILABLE: &str = "Available";

/// Shorter outages of the agent are not reported, such as a rollout that pulls a new image.
const AGENT_UNAVAILABLE_GRACE_PERIOD: SignedDuration = SignedDuration::from_secs(60);

#[derive(Debug, Snafu, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("the KafkaTopic is invalid"))]
    InvalidKafkaTopic {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to get the KafkaCluster"))]
    GetKafkaCluster {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to get the agent Deployment"))]
    GetAgentDeployment {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to update the status of the KafkaTopic"))]
    UpdateStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to remove the finalizer of the KafkaTopic"))]
    RemoveFinalizer {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Debug, Eq, PartialEq)]
enum ClusterState {
    /// Does not exist or has a `deletionTimestamp`
    MissingOrDeleting,
    ManagementAccessDisabled,
    ManagementAccessEnabled,
}

#[derive(Debug, Eq, PartialEq)]
enum AgentState {
    Available,
    /// `None` if the agent Deployment does not exist
    UnavailableSince(Option<Timestamp>),
}

#[derive(Debug, Eq, PartialEq)]
enum OperatorAction {
    LeaveToAgent,
    WaitForAgent { remaining: std::time::Duration },
    RemoveFinalizer,
    SetAgentUnavailableConditions { message: &'static str },
}

pub async fn reconcile_topic(
    kafka_topic: Arc<DeserializeGuard<KafkaTopic>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    let kafka_topic = kafka_topic
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaTopicSnafu)?;
    let namespace = kafka_topic.namespace().unwrap_or_default();
    let cluster_name = &kafka_topic.spec.cluster_ref.name;

    let kafka_cluster = ctx
        .client
        .get_opt::<DeserializeGuard<KafkaCluster>>(cluster_name.as_ref(), &namespace)
        .await
        .context(GetKafkaClusterSnafu)?;
    let agent_deployment_name = agent_resource_names_of(cluster_name).deployment_name();
    let agent_deployment = ctx
        .client
        .get_opt::<Deployment>(agent_deployment_name.as_ref(), &namespace)
        .await
        .context(GetAgentDeploymentSnafu)?;

    match operator_action_for(
        kafka_topic,
        &cluster_state_from(kafka_cluster.as_ref()),
        &agent_state_from(agent_deployment.as_ref()),
        Timestamp::now(),
    ) {
        OperatorAction::LeaveToAgent => {}
        OperatorAction::WaitForAgent { remaining } => return Ok(Action::requeue(remaining)),
        OperatorAction::RemoveFinalizer => remove_finalizer(kafka_topic, &ctx).await?,
        OperatorAction::SetAgentUnavailableConditions { message } => {
            set_agent_unavailable_conditions(kafka_topic, &ctx, message).await?
        }
    }
    Ok(Action::await_change())
}

pub fn error_policy(
    _kafka_topic: Arc<DeserializeGuard<KafkaTopic>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidKafkaTopic { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(30)),
    }
}

pub fn kafka_topics_of_cluster(
    kafka_topics: &Store<DeserializeGuard<KafkaTopic>>,
    namespace: Option<String>,
    cluster_name: Option<&str>,
) -> Vec<ObjectRef<DeserializeGuard<KafkaTopic>>> {
    kafka_topics
        .state()
        .into_iter()
        .filter(|kafka_topic| {
            kafka_topic.0.as_ref().is_ok_and(|kafka_topic| {
                kafka_topic.namespace() == namespace
                    && Some(kafka_topic.spec.cluster_ref.name.as_ref()) == cluster_name
            })
        })
        .map(|kafka_topic| ObjectRef::from_obj(&*kafka_topic))
        .collect()
}

fn cluster_state_from(kafka_cluster: Option<&DeserializeGuard<KafkaCluster>>) -> ClusterState {
    match kafka_cluster.map(|kafka_cluster| &kafka_cluster.0) {
        None => ClusterState::MissingOrDeleting,
        Some(Ok(kafka_cluster)) if kafka_cluster.metadata.deletion_timestamp.is_some() => {
            ClusterState::MissingOrDeleting
        }
        Some(Ok(kafka_cluster))
            if !kafka_cluster
                .spec
                .cluster_config
                .management_access
                .as_ref()
                .is_some_and(|management_access| management_access.enabled) =>
        {
            ClusterState::ManagementAccessDisabled
        }
        // An invalid KafkaCluster keeps its agent, so the state of the agent decides.
        Some(_) => ClusterState::ManagementAccessEnabled,
    }
}

fn operator_action_for(
    kafka_topic: &KafkaTopic,
    cluster_state: &ClusterState,
    agent_state: &AgentState,
    now: Timestamp,
) -> OperatorAction {
    match cluster_state {
        ClusterState::MissingOrDeleting if kafka_topic.metadata.deletion_timestamp.is_some() => {
            if kafka_topic
                .finalizers()
                .iter()
                .any(|finalizer| finalizer == KAFKA_TOPIC_FINALIZER)
            {
                OperatorAction::RemoveFinalizer
            } else {
                OperatorAction::LeaveToAgent
            }
        }
        ClusterState::MissingOrDeleting => OperatorAction::SetAgentUnavailableConditions {
            message: "the KafkaCluster does not exist",
        },
        ClusterState::ManagementAccessDisabled => OperatorAction::SetAgentUnavailableConditions {
            message: "the KafkaCluster has no agent, because clusterConfig.managementAccess is \
                      not enabled",
        },
        ClusterState::ManagementAccessEnabled => match agent_state {
            AgentState::Available => OperatorAction::LeaveToAgent,
            AgentState::UnavailableSince(Some(since))
                if now.duration_since(*since) < AGENT_UNAVAILABLE_GRACE_PERIOD =>
            {
                OperatorAction::WaitForAgent {
                    remaining: (AGENT_UNAVAILABLE_GRACE_PERIOD - now.duration_since(*since))
                        .unsigned_abs(),
                }
            }
            AgentState::UnavailableSince(_) => OperatorAction::SetAgentUnavailableConditions {
                message: "the agent of the KafkaCluster is not available",
            },
        },
    }
}

/// A Deployment that has no `Available` condition yet is unavailable since its creation.
fn agent_state_from(agent_deployment: Option<&Deployment>) -> AgentState {
    let Some(agent_deployment) = agent_deployment else {
        return AgentState::UnavailableSince(None);
    };
    let available_condition = agent_deployment
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.type_ == DEPLOYMENT_CONDITION_AVAILABLE)
        });
    match available_condition {
        Some(condition) if condition.status == "True" => AgentState::Available,
        Some(condition) => {
            AgentState::UnavailableSince(condition.last_transition_time.as_ref().map(|time| time.0))
        }
        None => AgentState::UnavailableSince(
            agent_deployment
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|time| time.0),
        ),
    }
}

/// Only the conditions are applied, so the other fields stay owned by the agent and keep the
/// last state it observed.
async fn set_agent_unavailable_conditions(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
    message: &str,
) -> Result<()> {
    let current_conditions = kafka_topic
        .status
        .as_ref()
        .map(|status| status.conditions.as_slice())
        .unwrap_or_default();
    let new_conditions = update_conditions(
        current_conditions,
        vec![
            agent_unavailable_condition(
                ManagedConditionType::Ready,
                ConditionStatus::Unknown,
                message,
            ),
            agent_unavailable_condition(
                ManagedConditionType::Synced,
                ConditionStatus::False,
                message,
            ),
        ],
        &Time(Timestamp::now()),
        kafka_topic.metadata.generation,
    );

    if new_conditions != current_conditions {
        ctx.client
            .apply_patch_status(
                TOPIC_CONTROLLER_NAME,
                kafka_topic,
                &json!({ "conditions": new_conditions }),
            )
            .await
            .context(UpdateStatusSnafu)?;
    }
    Ok(())
}

fn agent_unavailable_condition(
    type_: ManagedConditionType,
    status: ConditionStatus,
    message: &str,
) -> ManagedCondition {
    ManagedCondition {
        type_,
        status,
        reason: AGENT_UNAVAILABLE_REASON.to_owned(),
        message: message.to_owned(),
    }
}

/// Fails if the `KafkaTopic` was changed since it was read, so concurrent changes to the
/// finalizers, e.g. by an agent that is still shutting down, are not lost.
async fn remove_finalizer(kafka_topic: &KafkaTopic, ctx: &Ctx) -> Result<()> {
    let finalizers = kafka_topic
        .finalizers()
        .iter()
        .filter(|finalizer| *finalizer != KAFKA_TOPIC_FINALIZER)
        .cloned()
        .collect::<Vec<_>>();
    let patch = json!({
        "metadata": {
            "resourceVersion": kafka_topic.metadata.resource_version,
            "finalizers": finalizers,
        }
    });
    ctx.client
        .merge_patch(kafka_topic, patch)
        .await
        .context(RemoveFinalizerSnafu)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use stackable_operator::kube::api::ObjectMeta;

    use super::*;

    fn kafka_topic(deleting: bool, with_finalizer: bool) -> KafkaTopic {
        let kafka_topic: KafkaTopic = serde_json::from_value(json!({
            "apiVersion": "kafka.stackable.tech/v1alpha1",
            "kind": "KafkaTopic",
            "metadata": { "name": "orders", "namespace": "default" },
            "spec": { "clusterRef": { "name": "simple-kafka" } },
        }))
        .expect("the KafkaTopic should be valid");
        KafkaTopic {
            metadata: ObjectMeta {
                deletion_timestamp: deleting.then_some(Time(Timestamp::UNIX_EPOCH)),
                finalizers: with_finalizer.then(|| vec![KAFKA_TOPIC_FINALIZER.to_owned()]),
                ..kafka_topic.metadata
            },
            ..kafka_topic
        }
    }

    fn now() -> Timestamp {
        Timestamp::from_second(1_000).expect("the timestamp should be valid")
    }

    fn seconds_ago(seconds: i64) -> Timestamp {
        now() - SignedDuration::from_secs(seconds)
    }

    #[rstest]
    #[case::cluster_missing(ClusterState::MissingOrDeleting, AgentState::Available, true)]
    #[case::cluster_missing_without_agent(
        ClusterState::MissingOrDeleting,
        AgentState::UnavailableSince(None),
        true
    )]
    #[case::management_access_disabled(
        ClusterState::ManagementAccessDisabled,
        AgentState::UnavailableSince(None),
        false
    )]
    #[case::agent_available(ClusterState::ManagementAccessEnabled, AgentState::Available, false)]
    #[case::agent_unavailable(
        ClusterState::ManagementAccessEnabled,
        AgentState::UnavailableSince(None),
        false
    )]
    fn remove_the_finalizer_only_if_the_cluster_is_missing_or_deleting(
        #[case] cluster_state: ClusterState,
        #[case] agent_state: AgentState,
        #[case] removes_finalizer: bool,
    ) {
        assert_eq!(
            removes_finalizer,
            operator_action_for(
                &kafka_topic(true, true),
                &cluster_state,
                &agent_state,
                now()
            ) == OperatorAction::RemoveFinalizer
        );
    }

    #[rstest]
    #[case::cluster_missing(ClusterState::MissingOrDeleting, AgentState::Available, true)]
    #[case::management_access_disabled(
        ClusterState::ManagementAccessDisabled,
        AgentState::Available,
        true
    )]
    #[case::agent_available(ClusterState::ManagementAccessEnabled, AgentState::Available, false)]
    #[case::agent_deployment_missing(
        ClusterState::ManagementAccessEnabled,
        AgentState::UnavailableSince(None),
        true
    )]
    #[case::agent_unavailable_for_longer_than_the_grace_period(
        ClusterState::ManagementAccessEnabled,
        AgentState::UnavailableSince(Some(seconds_ago(61))),
        true
    )]
    fn set_agent_unavailable_conditions_without_an_available_agent(
        #[case] cluster_state: ClusterState,
        #[case] agent_state: AgentState,
        #[case] sets_conditions: bool,
    ) {
        assert_eq!(
            sets_conditions,
            matches!(
                operator_action_for(
                    &kafka_topic(false, false),
                    &cluster_state,
                    &agent_state,
                    now()
                ),
                OperatorAction::SetAgentUnavailableConditions { .. }
            )
        );
    }

    #[test]
    fn wait_for_an_agent_that_is_unavailable_within_the_grace_period() {
        assert_eq!(
            OperatorAction::WaitForAgent {
                remaining: std::time::Duration::from_secs(50)
            },
            operator_action_for(
                &kafka_topic(false, false),
                &ClusterState::ManagementAccessEnabled,
                &AgentState::UnavailableSince(Some(seconds_ago(10))),
                now()
            )
        );
    }

    #[test]
    fn leave_a_deleting_kafka_topic_without_finalizer_alone() {
        assert_eq!(
            OperatorAction::LeaveToAgent,
            operator_action_for(
                &kafka_topic(true, false),
                &ClusterState::MissingOrDeleting,
                &AgentState::UnavailableSince(None),
                now()
            )
        );
    }

    fn agent_deployment(available: Option<&str>) -> Deployment {
        serde_json::from_value(json!({
            "metadata": { "creationTimestamp": "1970-01-01T00:10:00Z" },
            "status": {
                "conditions": available.map(|status| vec![json!({
                    "type": "Available",
                    "status": status,
                    "lastTransitionTime": "1970-01-01T00:15:00Z",
                })]),
            },
        }))
        .expect("the Deployment should be valid")
    }

    #[rstest]
    #[case::missing(None, AgentState::UnavailableSince(None))]
    #[case::available(Some(agent_deployment(Some("True"))), AgentState::Available)]
    #[case::unavailable_since_the_transition(
        Some(agent_deployment(Some("False"))),
        AgentState::UnavailableSince(Some(Timestamp::from_second(900).expect("valid")))
    )]
    #[case::unavailable_since_the_creation(
        Some(agent_deployment(None)),
        AgentState::UnavailableSince(Some(Timestamp::from_second(600).expect("valid")))
    )]
    fn derive_the_agent_state_from_the_deployment(
        #[case] agent_deployment: Option<Deployment>,
        #[case] expected: AgentState,
    ) {
        assert_eq!(expected, agent_state_from(agent_deployment.as_ref()));
    }
}

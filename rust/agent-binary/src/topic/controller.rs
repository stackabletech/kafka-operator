//! The `KafkaTopic` controller: reconcile and error policy.

use std::{sync::Arc, time::Duration};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_kafka_admin::client::{self, KafkaAdminClient};
use stackable_kafka_shared::crd::topic::v1alpha1::KafkaTopic;
use stackable_operator::{
    kube::{
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::Store},
    },
    logging::controller::ReconcilerError,
    v2::types::operator::ClusterName,
};
use strum::{EnumDiscriminants, IntoStaticStr};
use tokio::sync::Mutex;
use tracing::info;

use super::{
    condition::TopicSyncOutcome,
    finalizer::{add_finalizer, reconcile_finalizer},
    sync::sync,
    update_status::update_status,
};
use crate::AGENT_NAME;

pub const TOPIC_CONTROLLER_NAME: &str = "kafkatopic";
pub const TOPIC_FULL_CONTROLLER_NAME: &str = concatcp!(TOPIC_CONTROLLER_NAME, ".", AGENT_NAME);

const RESYNC_INTERVAL: Duration = Duration::from_secs(5 * 60);
const ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Snafu, EnumDiscriminants)]
#[snafu(visibility(pub(crate)))]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("the KafkaTopic is invalid"))]
    InvalidKafkaTopic {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to talk to Kafka"))]
    Kafka { source: client::Error },

    #[snafu(display("failed to update the status of the KafkaTopic"))]
    UpdateStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to update the finalizers of the KafkaTopic"))]
    UpdateFinalizers {
        source: stackable_operator::client::Error,
    },
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub struct Ctx {
    pub(crate) client: stackable_operator::client::Client,
    pub(crate) cluster_name: ClusterName,
    pub(crate) kafka_topics: Store<DeserializeGuard<KafkaTopic>>,
    /// Reads the TLS files on every new connection and reconnects after a failed request, so
    /// rotated certificates are picked up without restarting the agent.
    pub(crate) admin_client: Mutex<KafkaAdminClient>,
}

impl Ctx {
    /// `kafka_topics` is the store of the controller, used to detect conflicts between
    /// `KafkaTopic`s.
    pub fn new(
        client: stackable_operator::client::Client,
        cluster_name: ClusterName,
        admin_client: KafkaAdminClient,
        kafka_topics: Store<DeserializeGuard<KafkaTopic>>,
    ) -> Self {
        Self {
            client,
            cluster_name,
            kafka_topics,
            admin_client: Mutex::new(admin_client),
        }
    }
}

pub async fn reconcile_topic(
    kafka_topic: Arc<DeserializeGuard<KafkaTopic>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    info!("Starting reconcile");

    // dereference
    let kafka_topic = kafka_topic
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaTopicSnafu)?;
    if kafka_topic.spec.cluster_ref.name != ctx.cluster_name {
        return Ok(Action::await_change());
    }

    // finalizer (Kubernetes)
    // Left untouched while paused, like operators that only update the status then. A deletion
    // waits until the reconciliation is resumed.
    let paused = kafka_topic.spec.topic_operation.reconciliation_paused;
    if !paused && let Some(action) = reconcile_finalizer(kafka_topic, &ctx).await? {
        return Ok(action);
    }

    // sync (Kafka)
    let mut topic_state = None;
    let sync_result = sync(kafka_topic, &ctx, &mut topic_state).await;

    // finalizer (Kubernetes)
    // Added before the status, because the status patch changes the resourceVersion.
    if !paused {
        add_finalizer(kafka_topic, &ctx, topic_state.as_ref()).await?;
    }

    // update status (Kubernetes)
    match sync_result {
        Ok(sync_outcome) => {
            update_status(kafka_topic, &ctx, &sync_outcome, topic_state.as_ref()).await?;
            Ok(requeue(&sync_outcome))
        }
        Err(Error::Kafka { source }) => {
            let sync_outcome = TopicSyncOutcome::from_client_error(&source);
            update_status(kafka_topic, &ctx, &sync_outcome, topic_state.as_ref()).await?;
            Err(Error::Kafka { source })
        }
        Err(error) => Err(error),
    }
}

fn requeue(sync_outcome: &TopicSyncOutcome) -> Action {
    match sync_outcome {
        TopicSyncOutcome::InvalidSpec { .. } => Action::await_change(),
        _ => Action::requeue(RESYNC_INTERVAL),
    }
}

pub fn error_policy(
    _kafka_topic: Arc<DeserializeGuard<KafkaTopic>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidKafkaTopic { .. } => Action::await_change(),
        _ => Action::requeue(ERROR_RETRY_INTERVAL),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::invalid_spec(TopicSyncOutcome::InvalidSpec { message: String::new() }, Action::await_change())]
    #[case::synced(TopicSyncOutcome::Synced { reassigning_partition_count: 0 }, Action::requeue(RESYNC_INTERVAL))]
    #[case::paused(
        TopicSyncOutcome::ReconciliationPaused,
        Action::requeue(RESYNC_INTERVAL)
    )]
    fn requeue_by_outcome(#[case] sync_outcome: TopicSyncOutcome, #[case] expected: Action) {
        assert_eq!(expected, requeue(&sync_outcome));
    }
}

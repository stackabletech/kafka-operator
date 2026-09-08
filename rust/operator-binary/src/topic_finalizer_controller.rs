//! Operator-side safety net that releases a `KafkaTopic`'s finalizer once its `KafkaCluster` is gone
//! (spike).
//!
//! Normal topic-delete cleanup belongs to the credentialed **agent**: on a `KafkaTopic` deletion it
//! runs `DeleteTopics` against the live cluster and then drops the `kafka.stackable.tech/kafkatopic`
//! finalizer. But the agent is **namespaced** and is torn down together with its namespace, so during
//! namespace deletion it can be removed *before* the finalized `KafkaTopic` — leaving the finalizer
//! with no controller left to clear it and wedging the namespace in `Terminating` forever.
//!
//! This controller runs in the **cluster-scoped operator**, which survives namespace teardown. It
//! acts ONLY when a `KafkaTopic` is being deleted AND its `KafkaCluster` is absent or itself
//! terminating: in that case deleting the topic *inside* Kafka is meaningless (it dies with the
//! cluster), so the operator simply removes the finalizer. While the cluster is alive it does
//! nothing but requeue — the agent's credentialed delete owns that path, and removing the finalizer
//! here would orphan the topic in a live cluster (e.g. if the agent is merely down, not gone).

use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    kube::{
        ResourceExt,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::{KAFKA_OPERATOR_NAME, topic::v1alpha1::KafkaTopic, v1alpha1::KafkaCluster};

pub struct Ctx {
    pub client: Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("KafkaTopic object is invalid"))]
    InvalidKafkaTopic {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("KafkaTopic is missing a namespace"))]
    ObjectMissingNamespace,

    #[snafu(display("KafkaTopic does not reference a cluster name"))]
    MissingClusterRefName,

    #[snafu(display("failed to look up the KafkaCluster {name:?}"))]
    LookupCluster {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to release the finalizer on KafkaTopic {name:?}"))]
    ReleaseFinalizer {
        source: stackable_operator::client::Error,
        name: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        None
    }
}

/// The finalizer the agent places on each `KafkaTopic`. MUST match the agent's `topic_controller`.
fn topic_finalizer() -> String {
    format!("{KAFKA_OPERATOR_NAME}/kafkatopic")
}

pub async fn reconcile_topic_finalizer(
    topic: Arc<DeserializeGuard<KafkaTopic>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    let topic = topic
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaTopicSnafu)?;

    // Only concerned with topics that are being deleted and still carry our finalizer. Everything
    // else is the agent's business; await the next change quietly.
    if topic.metadata.deletion_timestamp.is_none() {
        return Ok(Action::await_change());
    }
    let finalizer = topic_finalizer();
    let finalizers = topic.metadata.finalizers.clone().unwrap_or_default();
    if !finalizers.contains(&finalizer) {
        return Ok(Action::await_change());
    }

    let namespace = topic
        .metadata
        .namespace
        .as_deref()
        .context(ObjectMissingNamespaceSnafu)?;
    let cluster_name = topic
        .spec
        .cluster_ref
        .name
        .as_deref()
        .context(MissingClusterRefNameSnafu)?;
    // The topic and its cluster are co-located (namespaced), so default to the topic's namespace.
    let cluster_namespace = topic
        .spec
        .cluster_ref
        .namespace
        .as_deref()
        .unwrap_or(namespace);

    // Is the owning cluster gone, or itself terminating?
    let cluster: Option<KafkaCluster> = ctx
        .client
        .get_opt(cluster_name, cluster_namespace)
        .await
        .context(LookupClusterSnafu {
            name: cluster_name.to_owned(),
        })?;
    let cluster_gone = match &cluster {
        None => true,
        Some(cluster) => cluster.metadata.deletion_timestamp.is_some(),
    };

    if !cluster_gone {
        // The cluster is alive → the credentialed agent owns this deletion (it runs DeleteTopics and
        // clears the finalizer itself). Removing it here would orphan the topic in a live cluster, so
        // do NOT touch it — just re-check periodically. This also covers the teardown race where the
        // topic gets its deletionTimestamp a moment before the KafkaCluster gets its own: the next
        // requeue sees the cluster terminating and releases.
        return Ok(Action::requeue(*Duration::from_secs(30)));
    }

    // Cluster gone/terminating → deleting the topic inside Kafka is meaningless (it dies with the
    // cluster). Release the finalizer so the KafkaTopic (and any terminating namespace) can complete.
    // Preserve any other finalizers; only drop ours.
    let remaining: Vec<String> = finalizers.into_iter().filter(|f| *f != finalizer).collect();
    ctx.client
        .merge_patch(
            topic,
            serde_json::json!({ "metadata": { "finalizers": remaining } }),
        )
        .await
        .context(ReleaseFinalizerSnafu {
            name: topic.name_any(),
        })?;

    tracing::info!(
        topic = topic.name_any(),
        cluster = cluster_name,
        "KafkaCluster is gone/terminating; released the KafkaTopic finalizer (the topic dies with the cluster)"
    );
    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<KafkaTopic>>,
    _error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    Action::requeue(*Duration::from_secs(10))
}

//! The finalizer of a `KafkaTopic` and the cleanup in Kafka when it is deleted.

use std::path::Path;

use serde_json::json;
use snafu::{OptionExt, ResultExt};
use stackable_kafka_shared::crd::topic::v1alpha1::KafkaTopic;
use stackable_operator::{
    agent::deletion::v1alpha1::DeletionPolicy,
    kube::{ResourceExt, runtime::controller::Action},
};
use tracing::info;

use super::{
    controller::{
        Ctx, KafkaSnafu, NoBootstrapServersSnafu, ReadBootstrapServersSnafu, Result,
        UpdateFinalizersSnafu,
    },
    plan::TopicState,
    sync::client_for,
    validate::has_other_kafka_topic,
};
use crate::config::{BOOTSTRAP_SERVERS_FILE, read_bootstrap_servers};

const FINALIZER: &str = "kafka.stackable.tech/kafkatopic";

/// Handles a deletion and removes a finalizer that is no longer needed. Returns `Some` if the
/// reconcile ends here.
pub(crate) async fn reconcile_finalizer(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
) -> Result<Option<Action>> {
    let has_finalizer = has_finalizer(kafka_topic);

    if kafka_topic.metadata.deletion_timestamp.is_some() {
        if has_finalizer {
            cleanup(kafka_topic, ctx).await?;
            set_finalizer(kafka_topic, ctx, false).await?;
        }
        return Ok(Some(Action::await_change()));
    }

    if has_finalizer && kafka_topic.spec.deletion_policy != DeletionPolicy::Delete {
        // The patch triggers another reconcile with the updated object.
        set_finalizer(kafka_topic, ctx, false).await?;
        return Ok(Some(Action::await_change()));
    }
    Ok(None)
}

/// Adds the finalizer only once the topic exists and is managed by this `KafkaTopic`, so deleting
/// a `KafkaTopic` that never managed the topic, e.g. because of an invalid spec or a conflict,
/// does not delete it.
pub(crate) async fn add_finalizer(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
    topic_state: Option<&TopicState>,
) -> Result<()> {
    if kafka_topic.spec.deletion_policy == DeletionPolicy::Delete
        && topic_state.is_some()
        && !has_finalizer(kafka_topic)
    {
        set_finalizer(kafka_topic, ctx, true).await?;
    }
    Ok(())
}

fn has_finalizer(kafka_topic: &KafkaTopic) -> bool {
    kafka_topic
        .finalizers()
        .iter()
        .any(|finalizer| finalizer == FINALIZER)
}

/// Deletes the topic in Kafka if the `deletionPolicy` says so and no other `KafkaTopic` declares
/// it.
///
/// The policy is read from the `KafkaTopic` being deleted, so switching it to `Retain` releases a
/// deletion that is stuck because Kafka cannot be reached.
async fn cleanup(kafka_topic: &KafkaTopic, ctx: &Ctx) -> Result<()> {
    if kafka_topic.spec.deletion_policy != DeletionPolicy::Delete {
        return Ok(());
    }
    // A topic with an invalid name was never created.
    let Ok(topic_name) = kafka_topic.topic_name() else {
        return Ok(());
    };
    if has_other_kafka_topic(kafka_topic, &topic_name, &ctx.kafka_topics) {
        return Ok(());
    }

    let bootstrap_servers = read_bootstrap_servers(Path::new(BOOTSTRAP_SERVERS_FILE))
        .context(ReadBootstrapServersSnafu)?
        .context(NoBootstrapServersSnafu)?;
    let mut client_cache = ctx.client_cache.lock().await;
    client_for(&mut client_cache, bootstrap_servers, &ctx.security)
        .delete_topic(topic_name.as_ref())
        .await
        .context(KafkaSnafu)?;

    info!(
        topic = %topic_name,
        kafka_topic = %kafka_topic.name_any(),
        "Deleted the topic"
    );

    Ok(())
}

/// Fails if the `KafkaTopic` was changed since it was read, so concurrent changes to the
/// finalizers are not lost.
async fn set_finalizer(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
    should_have_finalizer: bool,
) -> Result<()> {
    let mut finalizers = kafka_topic
        .finalizers()
        .iter()
        .filter(|finalizer| *finalizer != FINALIZER)
        .cloned()
        .collect::<Vec<_>>();
    if should_have_finalizer {
        finalizers.push(FINALIZER.to_owned());
    }
    let patch = json!({
        "metadata": {
            "resourceVersion": kafka_topic.metadata.resource_version,
            "finalizers": finalizers,
        }
    });
    ctx.client
        .merge_patch(kafka_topic, patch)
        .await
        .context(UpdateFinalizersSnafu)?;
    Ok(())
}

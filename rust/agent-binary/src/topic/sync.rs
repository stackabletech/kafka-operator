//! Brings the topic in Kafka to the state of its `KafkaTopic` spec.

use std::collections::{BTreeMap, BTreeSet};

use snafu::ResultExt;
use stackable_kafka_admin::client::KafkaAdminClient;
use stackable_kafka_shared::crd::topic::{TopicName, v1alpha1::KafkaTopic};
use stackable_operator::kube::ResourceExt;
use tracing::info;

use super::{
    condition::TopicSyncOutcome,
    controller::{Ctx, KafkaSnafu, Result},
    plan::{ObservedTopic, TopicPlan, TopicState, plan},
    validate::validate,
};

/// Sets `topic_state` to the topic in Kafka, if it exists and is managed by this `KafkaTopic`.
/// It is kept up to date with each applied change, also if a later step fails.
pub(crate) async fn sync(
    kafka_topic: &KafkaTopic,
    ctx: &Ctx,
    topic_state: &mut Option<TopicState>,
) -> Result<TopicSyncOutcome> {
    // validate (no client required)
    let topic_name = match validate(kafka_topic, &ctx.kafka_topics) {
        Ok(topic_name) => topic_name,
        Err(sync_outcome) => return Ok(sync_outcome),
    };

    // observe (Kafka)
    let mut admin_client = ctx.admin_client.lock().await;
    let observed = observe(&mut admin_client, &topic_name).await?;
    *topic_state = observed
        .as_ref()
        .map(|observed| TopicState::observed(topic_name.clone(), observed));
    if kafka_topic.spec.topic_operation.reconciliation_paused {
        return Ok(TopicSyncOutcome::ReconciliationPaused);
    }

    // plan (no client required)
    let topic_plan = plan(&kafka_topic.spec, observed.as_ref());
    let reassigning_partition_count = observed
        .as_ref()
        .map_or(0, |observed| observed.reassignments.len());

    // execute (Kafka)
    execute(
        kafka_topic,
        &mut admin_client,
        topic_name,
        &topic_plan,
        topic_state,
    )
    .await?;

    let sync_outcome = match topic_plan {
        TopicPlan::Update {
            unsupported_changes,
            ..
        } if !unsupported_changes.is_empty() => TopicSyncOutcome::UnsupportedChange {
            changes: unsupported_changes,
        },
        _ => TopicSyncOutcome::Synced {
            reassigning_partition_count,
        },
    };
    Ok(sync_outcome)
}

async fn observe(
    admin_client: &mut KafkaAdminClient,
    topic_name: &TopicName,
) -> Result<Option<ObservedTopic>> {
    let topic_name = topic_name.as_ref();
    let Some(topic) = admin_client
        .describe_topic(topic_name)
        .await
        .context(KafkaSnafu)?
    else {
        return Ok(None);
    };
    let partitions = topic
        .partitions
        .iter()
        .map(|partition| partition.index)
        .collect::<Vec<_>>();
    Ok(Some(ObservedTopic {
        topic_configs: admin_client
            .describe_topic_configs(topic_name)
            .await
            .context(KafkaSnafu)?,
        reassignments: admin_client
            .list_partition_reassignments(topic_name, &partitions)
            .await
            .context(KafkaSnafu)?,
        topic,
    }))
}

/// Applies the changes to `topic_state`, the topic in Kafka before them.
async fn execute(
    kafka_topic: &KafkaTopic,
    admin_client: &mut KafkaAdminClient,
    topic_name: TopicName,
    topic_plan: &TopicPlan,
    topic_state: &mut Option<TopicState>,
) -> Result<()> {
    match topic_plan {
        TopicPlan::Create {
            partition_count,
            replication_factor,
            config_overrides,
        } => {
            let created_state = create(
                kafka_topic,
                admin_client,
                topic_name,
                *partition_count,
                *replication_factor,
                config_overrides,
            )
            .await?;
            *topic_state = Some(created_state);
        }
        TopicPlan::Update { topic_update, .. } => {
            // Partitions
            if let Some(new_partition_count) = topic_update.new_partition_count {
                grow_partitions(kafka_topic, admin_client, &topic_name, new_partition_count)
                    .await?;
                if let Some(topic_state) = topic_state {
                    topic_state.partition_count = new_partition_count;
                }
            }

            // Config
            if !topic_update.configs_to_set.is_empty() || !topic_update.configs_to_reset.is_empty()
            {
                alter_configs(
                    kafka_topic,
                    admin_client,
                    &topic_name,
                    &topic_update.configs_to_set,
                    &topic_update.configs_to_reset,
                )
                .await?;
                if let Some(topic_state) = topic_state {
                    topic_state.apply_config_changes(
                        &topic_update.configs_to_set,
                        &topic_update.configs_to_reset,
                    );
                }
            }
        }
    }
    Ok(())
}

/// The CreateTopics response contains the topic id, the counts and the effective configs, so no
/// describe is needed afterwards. Without the `DESCRIBE_CONFIGS` permission, Kafka creates the
/// topic but the request fails, and so does every later sync, because observing the topic needs
/// that permission as well.
async fn create(
    kafka_topic: &KafkaTopic,
    admin_client: &mut KafkaAdminClient,
    topic_name: TopicName,
    partition_count: Option<i32>,
    replication_factor: Option<i16>,
    config_overrides: &BTreeMap<String, String>,
) -> Result<TopicState> {
    let created = admin_client
        .create_topic(
            topic_name.as_ref(),
            partition_count,
            replication_factor,
            config_overrides,
        )
        .await
        .context(KafkaSnafu)?;
    info!(
        topic = %topic_name,
        partitions = created.partitions,
        replication_factor = created.replication_factor,
        configs = ?config_overrides,
        kafka_topic = %kafka_topic.name_any(),
        "Created the topic"
    );
    Ok(TopicState::created(topic_name, created))
}

async fn grow_partitions(
    kafka_topic: &KafkaTopic,
    admin_client: &mut KafkaAdminClient,
    topic_name: &TopicName,
    partition_count: i32,
) -> Result<()> {
    admin_client
        .create_partitions(topic_name.as_ref(), partition_count)
        .await
        .context(KafkaSnafu)?;
    info!(
        topic = %topic_name,
        partitions = partition_count,
        kafka_topic = %kafka_topic.name_any(),
        "Increased the number of partitions"
    );
    Ok(())
}

async fn alter_configs(
    kafka_topic: &KafkaTopic,
    admin_client: &mut KafkaAdminClient,
    topic_name: &TopicName,
    configs_to_set: &BTreeMap<String, String>,
    configs_to_reset: &BTreeSet<String>,
) -> Result<()> {
    admin_client
        .incremental_alter_configs(topic_name.as_ref(), configs_to_set, configs_to_reset)
        .await
        .context(KafkaSnafu)?;
    info!(
        topic = %topic_name,
        set = ?configs_to_set,
        reset = ?configs_to_reset,
        kafka_topic = %kafka_topic.name_any(),
        "Changed the topic configs"
    );
    Ok(())
}

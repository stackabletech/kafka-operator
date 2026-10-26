//! Brings the topic in Kafka to the state of its `KafkaTopic` spec.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use snafu::ResultExt;
use stackable_kafka_admin::{
    bootstrap::BrokerAddress,
    client::{KafkaAdminClient, TopicConfig},
    connection::Security,
};
use stackable_kafka_shared::crd::topic::{TopicName, v1alpha1::KafkaTopic};
use stackable_operator::kube::ResourceExt;
use tracing::info;

use super::{
    condition::TopicSyncOutcome,
    controller::{Ctx, KafkaSnafu, ReadBootstrapServersSnafu, Result},
    plan::{ObservedTopic, TopicPlan, TopicState, plan},
    validate::validate,
};
use crate::config::{BOOTSTRAP_SERVERS_FILE, read_bootstrap_servers};

const KAFKA_CLIENT_ID: &str = "stackable-kafka-agent";

pub(crate) struct CachedClient {
    bootstrap_servers: Vec<BrokerAddress>,
    admin_client: KafkaAdminClient,
}

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
    let Some(bootstrap_servers) = read_bootstrap_servers(Path::new(BOOTSTRAP_SERVERS_FILE))
        .context(ReadBootstrapServersSnafu)?
    else {
        return Ok(TopicSyncOutcome::ClusterNotReady);
    };
    let mut client_cache = ctx.client_cache.lock().await;
    let admin_client = client_for(&mut client_cache, bootstrap_servers, &ctx.security);
    let observed = observe(admin_client, &topic_name).await?;

    // plan (no client required)
    let topic_plan = plan(&kafka_topic.spec, observed.as_ref());
    let reassigning_partition_count = observed
        .as_ref()
        .map_or(0, |observed| observed.reassignments.len());
    *topic_state = observed.map(|observed| TopicState::observed(topic_name.clone(), observed));
    if kafka_topic.spec.topic_operation.reconciliation_paused {
        return Ok(TopicSyncOutcome::ReconciliationPaused);
    }

    // execute (Kafka)
    execute(
        kafka_topic,
        admin_client,
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
        effective_configs: admin_client
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
                let effective_configs = replace_configs(
                    kafka_topic,
                    admin_client,
                    &topic_name,
                    &topic_update.configs_to_set,
                    &topic_update.configs_to_reset,
                )
                .await?;
                if let Some(topic_state) = topic_state {
                    topic_state.effective_configs = effective_configs;
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

/// Returns the effective configs of the topic after the change. They are described again,
/// because IncrementalAlterConfigs does not return them and a reset key falls back to a broker
/// config the agent does not know.
async fn replace_configs(
    kafka_topic: &KafkaTopic,
    admin_client: &mut KafkaAdminClient,
    topic_name: &TopicName,
    configs_to_set: &BTreeMap<String, String>,
    configs_to_reset: &BTreeSet<String>,
) -> Result<BTreeMap<String, TopicConfig>> {
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
    admin_client
        .describe_topic_configs(topic_name.as_ref())
        .await
        .context(KafkaSnafu)
}

/// Returns the cached client, rebuilt if the bootstrap servers changed.
///
/// The client reads the TLS files on every new connection and reconnects after a failed request,
/// so rotated certificates are picked up without restarting the agent.
pub(crate) fn client_for<'a>(
    client_cache: &'a mut Option<CachedClient>,
    bootstrap_servers: Vec<BrokerAddress>,
    security: &Security,
) -> &'a mut KafkaAdminClient {
    if client_cache
        .as_ref()
        .is_some_and(|cached| cached.bootstrap_servers != bootstrap_servers)
    {
        *client_cache = None;
    }
    let cached = client_cache.get_or_insert_with(|| {
        info!(?bootstrap_servers, "Connecting to new bootstrap servers");
        CachedClient {
            bootstrap_servers: bootstrap_servers.clone(),
            admin_client: KafkaAdminClient::new(
                bootstrap_servers,
                security.clone(),
                KAFKA_CLIENT_ID,
            ),
        }
    });
    &mut cached.admin_client
}

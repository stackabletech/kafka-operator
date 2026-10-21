//! The `KafkaTopic` controller: reconcile, cleanup on deletion and error policy.

use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use const_format::concatcp;
use serde_json::json;
use snafu::{ResultExt, Snafu};
use stackable_kafka_admin::{
    bootstrap::BrokerAddress,
    client::{self, KafkaAdminClient, Topic},
    connection::Security,
};
use stackable_kafka_shared::crd::topic::v1alpha1::{
    KafkaTopic, KafkaTopicPartitionStatus, KafkaTopicStatus,
};
use stackable_operator::{
    commons::platform_access::{
        condition::update_conditions,
        ownership::{OWNED_SINCE_ANNOTATION, is_owned},
    },
    k8s_openapi::{apimachinery::pkg::apis::meta::v1::Time, jiff::Timestamp},
    kube::{
        ResourceExt,
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
    condition::{TopicSyncOutcome, conditions},
    managed_config_keys::{MANAGED_CONFIG_KEYS_ANNOTATION, managed_config_keys, render},
    plan::{ObservedTopic, TopicPlan, plan},
};
use crate::{
    AGENT_NAME,
    config::{BOOTSTRAP_SERVERS_FILE, read_bootstrap_servers},
};

pub const TOPIC_CONTROLLER_NAME: &str = "kafkatopic";
pub const TOPIC_FULL_CONTROLLER_NAME: &str = concatcp!(TOPIC_CONTROLLER_NAME, ".", AGENT_NAME);
const FINALIZER: &str = "kafka.stackable.tech/kafkatopic";
const KAFKA_CLIENT_ID: &str = "stackable-kafka-agent";

/// Set by `kafka-reassign-partitions` and Cruise Control for the duration of a reassignment.
const THROTTLE_CONFIG_KEYS: [&str; 2] = [
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
];

const RESYNC_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RETRY_INTERVAL: Duration = Duration::from_secs(10);
const ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Snafu, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("the KafkaTopic is invalid"))]
    InvalidKafkaTopic {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to read the bootstrap servers"))]
    ReadBootstrapServers { source: crate::config::Error },

    #[snafu(display("failed to talk to Kafka"))]
    Kafka { source: client::Error },

    #[snafu(display("failed to annotate the KafkaTopic"))]
    Annotate {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to update the status of the KafkaTopic"))]
    UpdateStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to update the finalizers of the KafkaTopic"))]
    UpdateFinalizers {
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
    client: stackable_operator::client::Client,
    cluster_name: ClusterName,
    security: Security,
    topics: Store<DeserializeGuard<KafkaTopic>>,
    /// One client per agent, rebuilt when the bootstrap servers change
    kafka: Mutex<Option<CachedClient>>,
}

impl Ctx {
    /// `topics` is the store of the controller, used to detect conflicts between `KafkaTopic`s.
    pub fn new(
        client: stackable_operator::client::Client,
        cluster_name: ClusterName,
        security: Security,
        topics: Store<DeserializeGuard<KafkaTopic>>,
    ) -> Self {
        Self {
            client,
            cluster_name,
            security,
            topics,
            kafka: Mutex::new(None),
        }
    }
}

struct CachedClient {
    bootstrap_servers: Vec<BrokerAddress>,
    client: KafkaAdminClient,
}

pub async fn reconcile_topic(
    topic: Arc<DeserializeGuard<KafkaTopic>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    info!("Starting reconcile");

    // dereference
    let topic = topic
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaTopicSnafu)?;
    if topic.spec.cluster_name != ctx.cluster_name {
        return Ok(Action::await_change());
    }

    // finalizer (Kubernetes)
    if let Some(action) = reconcile_finalizer(topic, &ctx).await? {
        return Ok(action);
    }

    // sync (Kafka + Kubernetes)
    let (outcome, observed) = match sync(topic, &ctx).await {
        Ok(synced) => synced,
        Err(Error::Kafka { source }) => {
            let outcome = TopicSyncOutcome::from_client_error(&source);
            update_status(topic, &ctx, &outcome, None).await?;
            return Err(Error::Kafka { source });
        }
        Err(error) => return Err(error),
    };

    // update status (Kubernetes)
    update_status(topic, &ctx, &outcome, observed.as_ref()).await?;
    Ok(requeue(&outcome, observed.is_some()))
}

/// Returns `Some` if the reconcile ends here.
async fn reconcile_finalizer(topic: &KafkaTopic, ctx: &Ctx) -> Result<Option<Action>> {
    let has_finalizer = topic
        .finalizers()
        .iter()
        .any(|finalizer| finalizer == FINALIZER);
    if topic.metadata.deletion_timestamp.is_some() {
        if has_finalizer {
            cleanup(topic, ctx).await?;
            set_finalizer(topic, ctx, false).await?;
        }
        return Ok(Some(Action::await_change()));
    }

    let needs_finalizer = topic.spec.management_policy.deletes_on_cleanup();
    if needs_finalizer != has_finalizer {
        // The patch triggers another reconcile with the updated object.
        set_finalizer(topic, ctx, needs_finalizer).await?;
        return Ok(Some(Action::await_change()));
    }
    Ok(None)
}

fn requeue(outcome: &TopicSyncOutcome, observed: bool) -> Action {
    match outcome {
        TopicSyncOutcome::InvalidSpec { .. } => Action::await_change(),
        // After a create or a grow, the next reconcile fills in the topic id and partitions.
        TopicSyncOutcome::Synced { .. } | TopicSyncOutcome::UnsupportedChange { .. }
            if !observed =>
        {
            Action::requeue(RETRY_INTERVAL)
        }
        TopicSyncOutcome::ClusterNotReady => Action::requeue(RETRY_INTERVAL),
        _ => Action::requeue(RESYNC_INTERVAL),
    }
}

pub fn error_policy(
    _topic: Arc<DeserializeGuard<KafkaTopic>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidKafkaTopic { .. } => Action::await_change(),
        _ => Action::requeue(ERROR_RETRY_INTERVAL),
    }
}

/// Returns the outcome and, if the `KafkaTopic` owns the topic, the described topic. After a
/// partition grow, the topic is left out, because the description is outdated.
async fn sync(topic: &KafkaTopic, ctx: &Ctx) -> Result<(TopicSyncOutcome, Option<Topic>)> {
    let topic_name = topic.spec.topic_name.as_ref();

    // validate (no client required)
    if let Some(outcome) = validate(topic, &ctx.topics) {
        return Ok((outcome, None));
    }

    // observe (Kafka)
    let Some(bootstrap_servers) = read_bootstrap_servers(Path::new(BOOTSTRAP_SERVERS_FILE))
        .context(ReadBootstrapServersSnafu)?
    else {
        return Ok((TopicSyncOutcome::ClusterNotReady, None));
    };
    let mut cache = ctx.kafka.lock().await;
    let kafka = client_for(&mut cache, bootstrap_servers, &ctx.security);
    let observed = observe(kafka, topic_name).await?;

    // claim ownership (Kubernetes)
    let claimed = if is_owned(&topic.metadata) {
        false
    } else {
        if observed.is_some() && !topic.spec.management_policy.adopts_existing() {
            let outcome = TopicSyncOutcome::AlreadyExists {
                message: format!(
                    "the topic {topic_name:?} already exists in Kafka; set \
                     managementPolicy.adoption to Adopt to take it over"
                ),
            };
            return Ok((outcome, None));
        }
        claim_ownership(topic, ctx).await?;
        true
    };

    // plan (no client required)
    let recorded_keys = managed_config_keys(topic);
    let topic_plan = plan(topic, observed.as_ref(), &recorded_keys);
    let grows_partitions = matches!(
        &topic_plan,
        TopicPlan::Update(update) if update.grow_partitions_to.is_some()
    );

    // execute (Kafka + Kubernetes)
    let outcome = match execute(topic, ctx, kafka, topic_plan, recorded_keys).await {
        // The topic was created by someone else after it was observed.
        Err(Error::Kafka { source }) if claimed && source.is_topic_already_exists() => {
            release_ownership(topic, ctx).await?;
            return Err(Error::Kafka { source });
        }
        result => result?,
    };

    let observed = observed
        .filter(|_| !grows_partitions)
        .map(|observed| observed.topic);
    Ok((outcome, observed))
}

/// Returns why the `KafkaTopic` cannot be synced, if it cannot.
fn validate(
    topic: &KafkaTopic,
    topics: &Store<DeserializeGuard<KafkaTopic>>,
) -> Option<TopicSyncOutcome> {
    let throttle_keys = THROTTLE_CONFIG_KEYS
        .iter()
        .filter(|key| topic.spec.config.contains_key(**key))
        .copied()
        .collect::<Vec<_>>();
    if !throttle_keys.is_empty() {
        return Some(TopicSyncOutcome::InvalidSpec {
            message: format!(
                "the config keys {throttle_keys:?} are managed by partition reassignments and \
                 cannot be set"
            ),
        });
    }
    conflicting_owner(topic, topics).map(|owner| TopicSyncOutcome::Conflict {
        message: format!(
            "the topic {:?} is managed by the KafkaTopic {owner:?}",
            topic.spec.topic_name.as_ref()
        ),
    })
}

async fn observe(kafka: &mut KafkaAdminClient, topic_name: &str) -> Result<Option<ObservedTopic>> {
    let Some(topic) = kafka.describe_topic(topic_name).await.context(KafkaSnafu)? else {
        return Ok(None);
    };
    let partitions = topic
        .partitions
        .iter()
        .map(|partition| partition.index)
        .collect::<Vec<_>>();
    Ok(Some(ObservedTopic {
        configs: kafka
            .describe_topic_configs(topic_name)
            .await
            .context(KafkaSnafu)?,
        reassignments: kafka
            .list_partition_reassignments(topic_name, &partitions)
            .await
            .context(KafkaSnafu)?,
        topic,
    }))
}

async fn execute(
    topic: &KafkaTopic,
    ctx: &Ctx,
    kafka: &mut KafkaAdminClient,
    topic_plan: TopicPlan,
    mut recorded_keys: BTreeSet<String>,
) -> Result<TopicSyncOutcome> {
    let topic_name = topic.spec.topic_name.as_ref();
    let spec_keys = topic.spec.config.keys().cloned().collect::<BTreeSet<_>>();

    let changes_configs = match &topic_plan {
        TopicPlan::Create { .. } => true,
        TopicPlan::Update(update) => {
            !update.set_configs.is_empty() || !update.delete_configs.is_empty()
        }
    };
    let intended_keys = &recorded_keys | &spec_keys;
    if changes_configs && intended_keys != recorded_keys {
        record_managed_config_keys(topic, ctx, &intended_keys).await?;
        recorded_keys = intended_keys;
    }

    let outcome = match topic_plan {
        TopicPlan::Create {
            partitions,
            replication_factor,
            configs,
        } => {
            kafka
                .create_topic(topic_name, partitions, replication_factor, &configs)
                .await
                .context(KafkaSnafu)?;
            info!(
                topic = topic_name,
                partitions,
                replication_factor,
                ?configs,
                kafka_topic = %topic.name_any(),
                "Created the topic"
            );
            TopicSyncOutcome::Synced {
                partitions: partitions as usize,
                reassigning_partitions: Vec::new(),
            }
        }
        TopicPlan::Update(update) => {
            if let Some(partitions) = update.grow_partitions_to {
                kafka
                    .create_partitions(topic_name, partitions)
                    .await
                    .context(KafkaSnafu)?;
                info!(
                    topic = topic_name,
                    partitions,
                    kafka_topic = %topic.name_any(),
                    "Increased the number of partitions"
                );
            }
            if changes_configs {
                kafka
                    .incremental_alter_configs(
                        topic_name,
                        &update.set_configs,
                        &update.delete_configs,
                    )
                    .await
                    .context(KafkaSnafu)?;
                info!(
                    topic = topic_name,
                    set = ?update.set_configs,
                    reset = ?update.delete_configs,
                    kafka_topic = %topic.name_any(),
                    "Changed the topic configs"
                );
            }
            if update.unsupported.is_empty() {
                TopicSyncOutcome::Synced {
                    partitions: topic.spec.partitions as usize,
                    reassigning_partitions: update.reassigning_partitions,
                }
            } else {
                TopicSyncOutcome::UnsupportedChange {
                    messages: update.unsupported,
                }
            }
        }
    };

    if recorded_keys != spec_keys {
        record_managed_config_keys(topic, ctx, &spec_keys).await?;
    }

    Ok(outcome)
}

async fn cleanup(topic: &KafkaTopic, ctx: &Ctx) -> Result<()> {
    if topic.spec.management_policy.deletes_on_cleanup()
        && is_owned(&topic.metadata)
        && conflicting_owner(topic, &ctx.topics).is_none()
    {
        let bootstrap_servers = read_bootstrap_servers(Path::new(BOOTSTRAP_SERVERS_FILE))
            .context(ReadBootstrapServersSnafu)?
            .unwrap_or_default();
        let mut cache = ctx.kafka.lock().await;
        client_for(&mut cache, bootstrap_servers, &ctx.security)
            .delete_topic(topic.spec.topic_name.as_ref())
            .await
            .context(KafkaSnafu)?;
        info!(
            topic = topic.spec.topic_name.as_ref(),
            kafka_topic = %topic.name_any(),
            "Deleted the topic"
        );
    }
    Ok(())
}

/// Fails if the `KafkaTopic` was changed since it was read, so concurrent changes to the
/// finalizers are not lost.
async fn set_finalizer(topic: &KafkaTopic, ctx: &Ctx, present: bool) -> Result<()> {
    let mut finalizers = topic
        .finalizers()
        .iter()
        .filter(|finalizer| *finalizer != FINALIZER)
        .cloned()
        .collect::<Vec<_>>();
    if present {
        finalizers.push(FINALIZER.to_owned());
    }
    let patch = json!({
        "metadata": {
            "resourceVersion": topic.metadata.resource_version,
            "finalizers": finalizers,
        }
    });
    ctx.client
        .merge_patch(topic, patch)
        .await
        .context(UpdateFinalizersSnafu)?;
    Ok(())
}

/// Returns the cached client, rebuilt if the bootstrap servers changed.
fn client_for<'a>(
    cache: &'a mut Option<CachedClient>,
    bootstrap_servers: Vec<BrokerAddress>,
    security: &Security,
) -> &'a mut KafkaAdminClient {
    if cache
        .as_ref()
        .is_some_and(|cached| cached.bootstrap_servers != bootstrap_servers)
    {
        *cache = None;
    }
    let cached = cache.get_or_insert_with(|| {
        info!(?bootstrap_servers, "Connecting to new bootstrap servers");
        CachedClient {
            bootstrap_servers: bootstrap_servers.clone(),
            client: KafkaAdminClient::new(bootstrap_servers, security.clone(), KAFKA_CLIENT_ID),
        }
    });
    &mut cached.client
}

/// The oldest other `KafkaTopic` for the same topic of the same cluster, by creation time and
/// name. Checked against all `KafkaTopic`s in the store, so the result does not depend on the
/// reconcile order.
fn conflicting_owner(
    topic: &KafkaTopic,
    topics: &Store<DeserializeGuard<KafkaTopic>>,
) -> Option<String> {
    let key = |topic: &KafkaTopic| (topic.metadata.creation_timestamp.clone(), topic.name_any());
    topics
        .state()
        .iter()
        .filter_map(|other| other.0.as_ref().ok())
        .filter(|other| {
            other.spec.cluster_name == topic.spec.cluster_name
                && other.spec.topic_name == topic.spec.topic_name
        })
        .min_by_key(|other| key(other))
        .filter(|oldest| key(oldest) != key(topic))
        .map(ResourceExt::name_any)
}

/// Fails if the `KafkaTopic` was changed since it was read, so ownership is never claimed twice.
async fn claim_ownership(topic: &KafkaTopic, ctx: &Ctx) -> Result<()> {
    let patch = json!({
        "metadata": {
            "resourceVersion": topic.metadata.resource_version,
            "annotations": { OWNED_SINCE_ANNOTATION: Timestamp::now().to_string() },
        }
    });
    ctx.client
        .merge_patch(topic, patch)
        .await
        .context(AnnotateSnafu)?;
    Ok(())
}

async fn release_ownership(topic: &KafkaTopic, ctx: &Ctx) -> Result<()> {
    let patch = json!({
        "metadata": {
            "annotations": {
                OWNED_SINCE_ANNOTATION: null,
                MANAGED_CONFIG_KEYS_ANNOTATION: null,
            },
        }
    });
    ctx.client
        .merge_patch(topic, patch)
        .await
        .context(AnnotateSnafu)?;
    Ok(())
}

async fn record_managed_config_keys(
    topic: &KafkaTopic,
    ctx: &Ctx,
    keys: &BTreeSet<String>,
) -> Result<()> {
    let patch = json!({
        "metadata": { "annotations": { MANAGED_CONFIG_KEYS_ANNOTATION: render(keys) } }
    });
    ctx.client
        .merge_patch(topic, patch)
        .await
        .context(AnnotateSnafu)?;
    Ok(())
}

/// Patches the status only if it changed, so the agent does not trigger its own watch.
async fn update_status(
    topic: &KafkaTopic,
    ctx: &Ctx,
    outcome: &TopicSyncOutcome,
    observed: Option<&Topic>,
) -> Result<()> {
    let current = topic.status.clone().unwrap_or_default();
    let mut status = KafkaTopicStatus {
        conditions: update_conditions(
            &current.conditions,
            conditions(outcome),
            &Time(Timestamp::now()),
        ),
        ..current.clone()
    };
    match (outcome, observed) {
        (_, Some(observed)) => {
            status.topic_id = Some(observed.id.clone());
            status.partitions = observed
                .partitions
                .iter()
                .map(|partition| KafkaTopicPartitionStatus {
                    index: partition.index,
                    replicas: partition.replicas.clone(),
                })
                .collect();
        }
        (TopicSyncOutcome::AlreadyExists { .. } | TopicSyncOutcome::Conflict { .. }, None) => {
            status.topic_id = None;
            status.partitions = Vec::new();
        }
        _ => {}
    }

    if status != current {
        ctx.client
            .apply_patch_status(AGENT_NAME, topic, &status)
            .await
            .context(UpdateStatusSnafu)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        k8s_openapi::apimachinery::pkg::apis::meta::v1::Time, kube::runtime::watcher,
    };

    use super::*;
    use crate::topic::test_support::kafka_topic;

    fn created_topic(
        name: &str,
        created_at: i64,
        topic_name: &str,
    ) -> DeserializeGuard<KafkaTopic> {
        let mut topic = kafka_topic(name, topic_name);
        topic.metadata.creation_timestamp = Some(Time(
            Timestamp::from_second(created_at).expect("the timestamp should be valid"),
        ));
        DeserializeGuard(Ok(topic))
    }

    fn store(topics: Vec<DeserializeGuard<KafkaTopic>>) -> Store<DeserializeGuard<KafkaTopic>> {
        let (store, mut writer) = stackable_operator::kube::runtime::reflector::store();
        for topic in topics {
            writer.apply_watcher_event(&watcher::Event::Apply(topic));
        }
        store
    }

    fn inner(topic: &DeserializeGuard<KafkaTopic>) -> &KafkaTopic {
        topic.0.as_ref().expect("the topic should be valid")
    }

    #[test]
    fn the_oldest_kafka_topic_owns_the_topic() {
        let older = created_topic("older", 10, "orders");
        let newer = created_topic("newer", 20, "orders");
        let other_topic = created_topic("other", 5, "payments");
        let topics = store(vec![older.clone(), newer.clone(), other_topic]);

        assert_eq!(None, conflicting_owner(inner(&older), &topics));
        assert_eq!(
            Some("older".to_owned()),
            conflicting_owner(inner(&newer), &topics)
        );
    }

    #[test]
    fn the_name_decides_between_kafka_topics_of_the_same_age() {
        let first = created_topic("a", 10, "orders");
        let second = created_topic("b", 10, "orders");
        let topics = store(vec![second.clone(), first.clone()]);

        assert_eq!(None, conflicting_owner(inner(&first), &topics));
        assert_eq!(
            Some("a".to_owned()),
            conflicting_owner(inner(&second), &topics)
        );
    }

    #[rstest::rstest]
    #[case::no_throttle(&[("retention.ms", "1000")], false)]
    #[case::throttle(&[("follower.replication.throttled.replicas", "*")], true)]
    fn reject_throttle_config_keys(#[case] config: &[(&str, &str)], #[case] invalid: bool) {
        let mut topic = kafka_topic("orders", "orders");
        topic.spec.config = config
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        assert_eq!(
            invalid,
            matches!(
                validate(&topic, &store(vec![])),
                Some(TopicSyncOutcome::InvalidSpec { .. })
            )
        );
    }

    fn synced() -> TopicSyncOutcome {
        TopicSyncOutcome::Synced {
            partitions: 1,
            reassigning_partitions: vec![],
        }
    }

    #[rstest::rstest]
    #[case::invalid_spec(TopicSyncOutcome::InvalidSpec { message: String::new() }, false, Action::await_change())]
    #[case::created(synced(), false, Action::requeue(RETRY_INTERVAL))]
    #[case::cluster_not_ready(
        TopicSyncOutcome::ClusterNotReady,
        false,
        Action::requeue(RETRY_INTERVAL)
    )]
    #[case::synced(synced(), true, Action::requeue(RESYNC_INTERVAL))]
    #[case::grown_with_unsupported_change(TopicSyncOutcome::UnsupportedChange { messages: vec![] }, false, Action::requeue(RETRY_INTERVAL))]
    #[case::unsupported_change(TopicSyncOutcome::UnsupportedChange { messages: vec![] }, true, Action::requeue(RESYNC_INTERVAL))]
    fn requeue_by_outcome(
        #[case] outcome: TopicSyncOutcome,
        #[case] observed: bool,
        #[case] expected: Action,
    ) {
        assert_eq!(expected, requeue(&outcome, observed));
    }
}

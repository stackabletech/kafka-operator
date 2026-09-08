//! The kafka-agent's `KafkaTopic` reconcile surface (spike, standing reconciliation).
//!
//! Mirrors the zk-agent's znode controller: watch `KafkaTopic`, filter to this agent's single
//! cluster, finalizer-gated create/delete, and write the outcome as a readable condition. The agent
//! connects to Kafka natively via the pure-Rust [`KafkaAdmin`] client over mTLS using the mounted
//! credential — the frequent, low-latency access path (topic CRUD), split from the rare drain (see
//! [`agentrequest_controller`](crate::agentrequest_controller)).

use std::{convert::Infallible, path::PathBuf, sync::Arc, time::Duration as StdDuration};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_operator::crd::{
    KAFKA_OPERATOR_NAME,
    topic::{KafkaTopicStatus, v1alpha1},
};
use stackable_operator::{
    client::Client,
    kube::{
        ResourceExt,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller::Action, finalizer, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition, compute_conditions},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use tokio_kafka::{Error as KafkaClientError, Kafka, KafkaConfig, NewTopic};

use crate::{condition::TopicConditionBuilder, discovery::read_bootstrap_servers};

/// Context for the topic reconcile loop. Carries the connection parameters resolved once at agent
/// startup (they are identical for every topic in the agent's cluster).
pub struct Ctx {
    pub client: Client,
    /// Name of the `KafkaCluster` this agent owns. Only `KafkaTopic`s whose `spec.clusterRef` points
    /// here are reconciled; others are ignored (defence in depth on top of the namespace scoping).
    pub cluster_name: String,
    /// Directory the cluster's discovery `ConfigMap` is mounted at; the reconcile reads the `KAFKA`
    /// bootstrap key from it each run (tolerating a transient-empty value → requeue).
    pub discovery_config_dir: PathBuf,
    /// Directory the platform-access client credential (`tls.crt` / `tls.key` / `ca.crt`) is mounted
    /// at. When set, the agent connects over mTLS; `None` ⇒ plaintext (dev only).
    pub platform_access_cert_dir: Option<PathBuf>,
    /// Directory the Kafka *server's* CA (`ca.crt`) is mounted at, verified against instead of the
    /// credential's own CA (cross-CA mTLS). Falls back to the credential dir when unset.
    pub platform_access_server_ca_dir: Option<PathBuf>,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("KafkaTopic object is invalid"))]
    InvalidKafkaTopic {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to ensure the Kafka topic {topic:?} exists"))]
    EnsureTopic {
        source: KafkaClientError,
        topic: String,
    },

    #[snafu(display("failed to ensure the Kafka topic {topic:?} is deleted"))]
    DeleteTopic {
        source: KafkaClientError,
        topic: String,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("error managing finalizer"))]
    Finalizer {
        source: finalizer::Error<Infallible>,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    fn extract_finalizer_err(err: finalizer::Error<Self>) -> Self {
        match err {
            finalizer::Error::ApplyFailed(source) => source,
            finalizer::Error::CleanupFailed(source) => source,
            finalizer::Error::AddFinalizer(source) => Error::Finalizer {
                source: finalizer::Error::AddFinalizer(source),
            },
            finalizer::Error::RemoveFinalizer(source) => Error::Finalizer {
                source: finalizer::Error::RemoveFinalizer(source),
            },
            finalizer::Error::UnnamedObject => Error::Finalizer {
                source: finalizer::Error::UnnamedObject,
            },
            finalizer::Error::InvalidFinalizer => Error::Finalizer {
                source: finalizer::Error::InvalidFinalizer,
            },
        }
    }
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        None
    }
}

pub async fn reconcile_topic(
    topic: Arc<DeserializeGuard<v1alpha1::KafkaTopic>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting KafkaTopic reconcile");
    let topic = topic
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaTopicSnafu)?;

    // Ownership: only reconcile topics that target this agent's cluster. Namespace scoping already
    // restricts the watch, but a namespace can hold several KafkaClusters, so filter by name too.
    if topic.spec.cluster_ref.name.as_deref() != Some(ctx.cluster_name.as_str()) {
        tracing::debug!(
            topic = topic.name_any(),
            cluster_ref = ?topic.spec.cluster_ref.name,
            "KafkaTopic targets another cluster; skipping"
        );
        return Ok(Action::await_change());
    }

    let namespace = topic
        .metadata
        .namespace
        .clone()
        .context(ObjectHasNoNamespaceSnafu)?;
    let client = &ctx.client;

    finalizer(
        &client.get_api::<v1alpha1::KafkaTopic>(&namespace),
        &format!("{KAFKA_OPERATOR_NAME}/kafkatopic"),
        Arc::new(topic.clone()),
        |event| async {
            match event {
                finalizer::Event::Apply(topic) => {
                    let result = reconcile_apply(&ctx, &topic).await;

                    // Surface the outcome as a readable condition (best-effort: a status-write
                    // failure must not mask the real reconcile result).
                    let condition_builder = match &result {
                        Ok(_) => TopicConditionBuilder::provisioned(),
                        Err(error) => {
                            TopicConditionBuilder::degraded(error.category(), error.to_string())
                        }
                    };
                    let conditions = compute_conditions(&*topic, &[&condition_builder]);
                    if let Err(status_error) = write_conditions(client, &topic, conditions).await {
                        tracing::warn!(error = %status_error, "Failed to write KafkaTopic condition");
                    }
                    result
                }
                finalizer::Event::Cleanup(topic) => reconcile_cleanup(&ctx, &topic).await,
            }
        },
    )
    .await
    .map_err(Error::extract_finalizer_err)
}

/// Reads the current bootstrap servers from the mounted discovery ConfigMap and opens an mTLS Kafka
/// admin client. Returns `Ok(None)` when no bootstrap servers are published yet (the caller requeues,
/// since a ConfigMap volume change does not raise a reconcile event).
async fn connect(ctx: &Ctx) -> Result<Option<Kafka>, KafkaClientError> {
    let bootstrap_servers = read_bootstrap_servers(&ctx.discovery_config_dir);
    if bootstrap_servers.is_empty() {
        return Ok(None);
    }
    let kafka = Kafka::connect(KafkaConfig {
        bootstrap_servers,
        cert_dir: ctx.platform_access_cert_dir.clone(),
        server_ca_dir: ctx.platform_access_server_ca_dir.clone(),
    })
    .await?;
    Ok(Some(kafka))
}

/// Creates (or, on a live cluster, reconciles) the topic. `ensure_topic` is idempotent: an
/// already-existing topic is treated as success.
async fn reconcile_apply(ctx: &Ctx, topic: &v1alpha1::KafkaTopic) -> Result<Action> {
    let topic_name = topic.effective_topic_name();
    let new_topic = NewTopic {
        name: topic_name.clone(),
        partitions: topic.spec.partitions,
        replication_factor: topic.spec.replication_factor,
        config: topic.spec.config.clone(),
    };

    let Some(kafka) = connect(ctx).await.context(EnsureTopicSnafu {
        topic: topic_name.clone(),
    })?
    else {
        tracing::info!(
            topic = topic_name,
            "discovery ConfigMap has no bootstrap servers yet; requeuing"
        );
        return Ok(Action::requeue(StdDuration::from_secs(30)));
    };

    kafka
        .ensure_topic(&new_topic)
        .await
        .context(EnsureTopicSnafu {
            topic: topic_name.clone(),
        })?;

    tracing::info!(topic = topic_name, "KafkaTopic provisioned");
    // Re-check periodically so config drift would be noticed.
    Ok(Action::requeue(StdDuration::from_secs(300)))
}

/// Deletes the topic from Kafka on `KafkaTopic` deletion (finalizer cleanup).
async fn reconcile_cleanup(ctx: &Ctx, topic: &v1alpha1::KafkaTopic) -> Result<Action> {
    let topic_name = topic.effective_topic_name();

    let Some(kafka) = connect(ctx).await.context(DeleteTopicSnafu {
        topic: topic_name.clone(),
    })?
    else {
        // No bootstrap servers → the cluster is (being) removed; nothing to delete. Let the finalizer
        // complete rather than blocking cluster teardown forever.
        tracing::info!(
            topic = topic_name,
            "discovery ConfigMap has no bootstrap servers; skipping topic delete"
        );
        return Ok(Action::await_change());
    };

    kafka
        .delete_topic(&topic_name)
        .await
        .context(DeleteTopicSnafu {
            topic: topic_name.clone(),
        })?;

    tracing::info!(topic = topic_name, "KafkaTopic deleted from Kafka");
    Ok(Action::await_change())
}

/// Idempotent status write: only patch on an actual change so the primary watch does not loop.
async fn write_conditions(
    client: &Client,
    topic: &v1alpha1::KafkaTopic,
    conditions: Vec<ClusterCondition>,
) -> Result<()> {
    if conditions == topic.conditions() {
        return Ok(());
    }
    let status = KafkaTopicStatus { conditions };
    client
        .merge_patch_status(topic, &status)
        .await
        .context(ApplyStatusSnafu)?;
    Ok(())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::KafkaTopic>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidKafkaTopic { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

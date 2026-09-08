//! The typed admin operations — the client's public verbs (tokio-zookeeper keeps the equivalent
//! `create`/`delete`/... on `ZooKeeper` in `lib.rs`; split out here to keep `lib.rs` to the
//! connection lifecycle).
//!
//! Each op locks the reused connection, routes it to the controller (see [`crate::cluster`]) reusing
//! the `Metadata` it already needs, then issues the request. The private `*_topic` helpers own the
//! per-request wire construction.

use kafka_protocol::messages::{
    BrokerId, CreatePartitionsRequest, CreateTopicsRequest, DeleteTopicsRequest,
    IncrementalAlterConfigsRequest, TopicName, UnregisterBrokerRequest,
    create_partitions_request::CreatePartitionsTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_topics_request::DeleteTopicState,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
};
use kafka_protocol::protocol::StrBytes;

use crate::{Kafka, error::Error, proto::Connection, reassignment, types::NewTopic};

/// `IncrementalAlterConfigs` resource type for a topic (from `ConfigResource.Type`).
const CONFIG_RESOURCE_TYPE_TOPIC: i8 = 2;
/// `IncrementalAlterConfigs` operation `SET` (from `AlterConfigOp.OpType`).
const CONFIG_OP_SET: i8 = 0;

/// The admin operations the crate exposes.
///
/// A trait so callers can be exercised against a fake in their own tests, and so the concrete
/// [`Kafka`] client is swappable.
#[allow(async_fn_in_trait)]
pub trait KafkaAdmin {
    /// Idempotently ensure the topic exists with the requested partitions / replication / config.
    async fn ensure_topic(&self, spec: &NewTopic) -> Result<(), Error>;

    /// Idempotently delete the topic (a missing topic is success).
    async fn delete_topic(&self, name: &str) -> Result<(), Error>;

    /// Submit (if not already running) and poll a partition reassignment off `broker_ids`.
    async fn reassign_off(
        &self,
        broker_ids: &[i32],
    ) -> Result<reassignment::ReassignProgress, Error>;

    /// Remove the given brokers' registrations from the cluster metadata (post-drain cleanup).
    async fn unregister_brokers(&self, broker_ids: &[i32]) -> Result<(), Error>;
}

// Each public op is `op` (retrying wrapper) → `op_once` (lock the reused connection + poison it on a
// connection-fatal error) → `op_locked` (the actual work against the locked connection). The op is
// idempotent, so retrying the whole thing is safe.
impl Kafka {
    /// Idempotently ensure the topic exists with the requested partitions / replication / config.
    pub async fn ensure_topic(&self, spec: &NewTopic) -> Result<(), Error> {
        self.with_backoff(|| self.ensure_topic_once(spec)).await
    }

    async fn ensure_topic_once(&self, spec: &NewTopic) -> Result<(), Error> {
        let mut guard = self.conn.lock().await;
        let result = self.ensure_topic_locked(&mut guard, spec).await;
        self.poison_if_fatal(&mut guard, &result);
        result
    }

    async fn ensure_topic_locked(
        &self,
        guard: &mut Option<Connection>,
        spec: &NewTopic,
    ) -> Result<(), Error> {
        // Route to the controller, reusing the Metadata for the existence check below.
        let meta = self.route_to_controller(guard).await?;
        let conn = guard.as_mut().expect("connected after routing");

        let topic_name = TopicName(StrBytes::from_string(spec.name.clone()));
        let existing = meta
            .topics
            .iter()
            .find(|t| t.name.as_ref() == Some(&topic_name) && t.error_code == 0);

        match existing {
            None => {
                // Absent → CreateTopics(19) with partitions + RF + config in one shot.
                create_topic(conn, spec, &topic_name).await?;
            }
            Some(existing) => {
                // Present → grow partitions if the desired count is higher (Kafka never shrinks),
                // then reconcile config.
                let current_partitions = existing.partitions.len() as i32;
                if spec.partitions > current_partitions {
                    create_partitions(conn, spec, &topic_name).await?;
                }
                if !spec.config.is_empty() {
                    alter_configs(conn, spec, &topic_name).await?;
                }
            }
        }
        Ok(())
    }

    /// Idempotently delete the topic (a missing topic is success).
    pub async fn delete_topic(&self, name: &str) -> Result<(), Error> {
        self.with_backoff(|| self.delete_topic_once(name)).await
    }

    async fn delete_topic_once(&self, name: &str) -> Result<(), Error> {
        let mut guard = self.conn.lock().await;
        let result = self.delete_topic_locked(&mut guard, name).await;
        self.poison_if_fatal(&mut guard, &result);
        result
    }

    async fn delete_topic_locked(
        &self,
        guard: &mut Option<Connection>,
        name: &str,
    ) -> Result<(), Error> {
        self.route_to_controller(guard).await?;
        let conn = guard.as_mut().expect("connected after routing");

        let topic_name = TopicName(StrBytes::from_string(name.to_owned()));
        let version = conn.pick_version::<DeleteTopicsRequest>("DeleteTopics")?;

        // `topic_names` is the pre-v6 field; v6 moved to the `topics` struct list. kafka-protocol
        // migrates fields across versions on encode, so populating both keeps us version-agnostic.
        let req = DeleteTopicsRequest::default()
            .with_topic_names(vec![topic_name.clone()])
            .with_topics(vec![
                DeleteTopicState::default().with_name(Some(topic_name.clone())),
            ])
            .with_timeout_ms(30_000);
        let resp = conn.send(version, "DeleteTopics", req).await?;

        for result in &resp.responses {
            // Idempotent: a missing topic is success.
            if result.error_code != 0
                && result.error_code
                    != kafka_protocol::ResponseError::UnknownTopicOrPartition.code()
            {
                return Err(Error::from_code(
                    "DeleteTopics",
                    result.error_code,
                    result.error_message.as_deref(),
                ));
            }
        }
        Ok(())
    }

    /// Submit (if not already running) and poll a partition reassignment off `broker_ids`.
    ///
    /// Crash-safe: in-progress state is re-derived from the cluster each call (no stored plan).
    /// Returns the remaining count so the caller can map it to a status (`0` = done). A remaining
    /// count > 0 is a successful poll (not an error), so backoff only retries genuine failures.
    pub async fn reassign_off(
        &self,
        broker_ids: &[i32],
    ) -> Result<reassignment::ReassignProgress, Error> {
        self.with_backoff(|| self.reassign_off_once(broker_ids)).await
    }

    async fn reassign_off_once(
        &self,
        broker_ids: &[i32],
    ) -> Result<reassignment::ReassignProgress, Error> {
        let mut guard = self.conn.lock().await;
        let result = self.reassign_off_locked(&mut guard, broker_ids).await;
        self.poison_if_fatal(&mut guard, &result);
        result
    }

    async fn reassign_off_locked(
        &self,
        guard: &mut Option<Connection>,
        broker_ids: &[i32],
    ) -> Result<reassignment::ReassignProgress, Error> {
        let meta = self.route_to_controller(guard).await?;
        let draining: std::collections::HashSet<i32> = broker_ids.iter().copied().collect();
        let conn = guard.as_mut().expect("connected after routing");

        // What is already being reassigned off the draining brokers (crash-safe: re-derived from the
        // cluster each call, no stored plan). Fed into `build_plan` so a partition mid-reassignment
        // is not re-planned — while it runs, Metadata reports an inflated replica set that would
        // otherwise mis-infer the RF and wrongly report it blocked.
        let in_flight = reassignment::ongoing_off(conn, &draining).await?;
        let plan = reassignment::build_plan(&meta, &draining, &in_flight);

        // Refuse the drain if any *not-yet-started* partition can't keep its replication factor with
        // the surviving brokers — otherwise we'd remove the broker and silently leave under-
        // replication. Non-retryable (retrying won't add brokers); surfaces as AgentRequest Failed →
        // the Scaler gate holds and the broker is NOT decremented.
        if plan.blocked > 0 {
            return Err(Error::CannotDrainBelowReplicationFactor {
                blocked: plan.blocked,
            });
        }

        // Nothing to move → already drained.
        if plan.total_moving == 0 {
            return Ok(reassignment::ReassignProgress {
                total: 0,
                remaining: 0,
            });
        }

        if !plan.planned.is_empty() {
            reassignment::submit(conn, &plan.planned).await?;
        }
        // Re-derive remaining after submitting (crash-safe: no stored plan).
        let remaining = reassignment::ongoing_off(conn, &draining).await?.len() as u32;

        Ok(reassignment::ReassignProgress {
            total: plan.total_moving,
            remaining,
        })
    }

    /// Remove the given brokers' registrations from the cluster metadata via `UnregisterBroker`(64).
    ///
    /// The post-drain counterpart to [`reassign_off`](Self::reassign_off): once a drained broker's
    /// pod is gone, its registration lingers in the (KRaft) metadata as a fenced entry until it is
    /// unregistered. Idempotent — a broker that is already gone reports `BrokerIdNotRegistered`,
    /// which is treated as success, so re-running across reconciles/restarts is safe.
    pub async fn unregister_brokers(&self, broker_ids: &[i32]) -> Result<(), Error> {
        self.with_backoff(|| self.unregister_brokers_once(broker_ids))
            .await
    }

    async fn unregister_brokers_once(&self, broker_ids: &[i32]) -> Result<(), Error> {
        let mut guard = self.conn.lock().await;
        let result = self.unregister_brokers_locked(&mut guard, broker_ids).await;
        self.poison_if_fatal(&mut guard, &result);
        result
    }

    async fn unregister_brokers_locked(
        &self,
        guard: &mut Option<Connection>,
        broker_ids: &[i32],
    ) -> Result<(), Error> {
        // `UnregisterBroker` is a controller-only op — route there (KRaft forwards; ZK-mode does not,
        // hence the explicit routing). Each id is a separate single-broker request.
        self.route_to_controller(guard).await?;
        let conn = guard.as_mut().expect("connected after routing");
        for &broker_id in broker_ids {
            unregister_broker(conn, broker_id).await?;
        }
        Ok(())
    }
}

impl KafkaAdmin for Kafka {
    async fn ensure_topic(&self, spec: &NewTopic) -> Result<(), Error> {
        Kafka::ensure_topic(self, spec).await
    }

    async fn delete_topic(&self, name: &str) -> Result<(), Error> {
        Kafka::delete_topic(self, name).await
    }

    async fn reassign_off(
        &self,
        broker_ids: &[i32],
    ) -> Result<reassignment::ReassignProgress, Error> {
        Kafka::reassign_off(self, broker_ids).await
    }

    async fn unregister_brokers(&self, broker_ids: &[i32]) -> Result<(), Error> {
        Kafka::unregister_brokers(self, broker_ids).await
    }
}

/// `CreateTopics`(19). Treats `TopicAlreadyExists` as success (idempotent).
async fn create_topic(
    conn: &mut Connection,
    spec: &NewTopic,
    topic_name: &TopicName,
) -> Result<(), Error> {
    let version = conn.pick_version::<CreateTopicsRequest>("CreateTopics")?;
    let configs: Vec<CreatableTopicConfig> = spec
        .config
        .iter()
        .map(|(k, v)| {
            CreatableTopicConfig::default()
                .with_name(StrBytes::from_string(k.clone()))
                .with_value(Some(StrBytes::from_string(v.clone())))
        })
        .collect();
    let topic = CreatableTopic::default()
        .with_name(topic_name.clone())
        .with_num_partitions(spec.partitions)
        // RF is an i16 on the wire; callers carry it as i32 for ergonomics.
        .with_replication_factor(spec.replication_factor as i16)
        .with_configs(configs);
    let req = CreateTopicsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30_000);
    let resp = conn.send(version, "CreateTopics", req).await?;

    for result in &resp.topics {
        if result.error_code != 0
            && result.error_code != kafka_protocol::ResponseError::TopicAlreadyExists.code()
        {
            return Err(Error::from_code(
                "CreateTopics",
                result.error_code,
                result.error_message.as_deref(),
            ));
        }
    }
    Ok(())
}

/// `UnregisterBroker`(64) for one broker id.
///
/// Idempotent: a `BrokerIdNotRegistered` (102) reply means the registration is already gone (the
/// desired end state), so it is treated as success.
async fn unregister_broker(conn: &mut Connection, broker_id: i32) -> Result<(), Error> {
    let version = conn.pick_version::<UnregisterBrokerRequest>("UnregisterBroker")?;
    let req = UnregisterBrokerRequest::default().with_broker_id(BrokerId(broker_id));
    let resp = conn.send(version, "UnregisterBroker", req).await?;

    if resp.error_code != 0
        && resp.error_code != kafka_protocol::ResponseError::BrokerIdNotRegistered.code()
    {
        return Err(Error::from_code(
            "UnregisterBroker",
            resp.error_code,
            resp.error_message.as_deref(),
        ));
    }
    Ok(())
}

/// `CreatePartitions`(37) to grow a topic to `spec.partitions`.
async fn create_partitions(
    conn: &mut Connection,
    spec: &NewTopic,
    topic_name: &TopicName,
) -> Result<(), Error> {
    let version = conn.pick_version::<CreatePartitionsRequest>("CreatePartitions")?;
    // `count` is the *new total* partition count, not the delta. `assignments = None` lets the broker
    // place the new partitions' replicas.
    let topic = CreatePartitionsTopic::default()
        .with_name(topic_name.clone())
        .with_count(spec.partitions)
        .with_assignments(None);
    let req = CreatePartitionsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30_000);
    let resp = conn.send(version, "CreatePartitions", req).await?;

    for result in &resp.results {
        if result.error_code != 0 {
            return Err(Error::from_code(
                "CreatePartitions",
                result.error_code,
                result.error_message.as_deref(),
            ));
        }
    }
    Ok(())
}

/// `IncrementalAlterConfigs`(44) — applies `spec.config` to the topic with `SET` operations.
async fn alter_configs(
    conn: &mut Connection,
    spec: &NewTopic,
    topic_name: &TopicName,
) -> Result<(), Error> {
    let version = conn.pick_version::<IncrementalAlterConfigsRequest>("IncrementalAlterConfigs")?;
    let configs: Vec<AlterableConfig> = spec
        .config
        .iter()
        .map(|(k, v)| {
            AlterableConfig::default()
                .with_name(StrBytes::from_string(k.clone()))
                .with_config_operation(CONFIG_OP_SET)
                .with_value(Some(StrBytes::from_string(v.clone())))
        })
        .collect();
    let resource = AlterConfigsResource::default()
        .with_resource_type(CONFIG_RESOURCE_TYPE_TOPIC)
        .with_resource_name(topic_name.0.clone())
        .with_configs(configs);
    let req = IncrementalAlterConfigsRequest::default().with_resources(vec![resource]);
    let resp = conn.send(version, "IncrementalAlterConfigs", req).await?;

    for result in &resp.responses {
        if result.error_code != 0 {
            return Err(Error::from_code(
                "IncrementalAlterConfigs",
                result.error_code,
                result.error_message.as_deref(),
            ));
        }
    }
    Ok(())
}

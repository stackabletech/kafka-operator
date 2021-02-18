#![feature(backtrace)]
mod error;

use crate::error::Error;

use async_trait::async_trait;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{ListParams, Meta};
use kube::Api;
use serde_json::json;
use tracing::{info, trace, warn};

use stackable_kafka_crd::{KafkaBroker, KafkaCluster, KafkaClusterSpec};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::{finalizer, metadata, podutils};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec};

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const FINALIZER_NAME: &str = "kafka.stackable.tech/cleanup";

type KafkaReconcileResult = ReconcileResult<error::Error>;

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_spec: KafkaClusterSpec,
    zk_spec: Option<ZooKeeperClusterSpec>,
    existing_pods: Vec<Pod>,
}

impl KafkaState {
    async fn check_zookeeper_reference(&mut self) -> KafkaReconcileResult {
        let api: Api<ZooKeeperCluster> = self
            .context
            .client
            .get_namespaced_api(&self.kafka_spec.zoo_keeper_reference.namespace);
        let zk_cluster = api.get(&self.kafka_spec.zoo_keeper_reference.name).await;

        // TODO: We need to watch the ZooKeeper resource and do _something_ when it goes down or when its nodes are changed
        let zk_spec: ZooKeeperClusterSpec = match zk_cluster {
            Ok(zk) => zk.spec,
            Err(err) => {
                warn!(?err,
                    "Referencing a ZooKeeper cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                    &self.kafka_spec.zoo_keeper_reference.namespace,
                    &self.kafka_spec.zoo_keeper_reference.name
                );
                // TODO: Depending on the error either requeue or return an error (which'll requeue as well)
                // For a not found we'd like to requeue but if there was a transport error we'd like to return it.
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        };

        self.zk_spec = Some(zk_spec);

        Ok(ReconcileFunctionAction::Continue)
    }

    // TODO: Currently this only allows creation of Kafka Pods, it does not allow any updates or config changes
    async fn reconcile(&mut self) -> KafkaReconcileResult {
        let name = self.context.name();

        let zk_spec = self.zk_spec.as_ref().ok_or_else(|| error::Error::ReconcileError("zk_spec missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string()))?;

        // This retrieves all the nodes from the referenced ZooKeeper cluster and creates the
        // required connection string for Kafka
        // TODO: Port is currently hardcoded, this needs to change and in general it might make sense to move this functionality to a ZooKeeper library
        let zk_servers: String = zk_spec
            .servers
            .iter()
            .map(|server| format!("{}:2181", server.node_name))
            .collect::<Vec<String>>()
            .join(",");

        let mut labels = BTreeMap::new();
        labels.insert("kafka-name".to_string(), name.clone());

        let mut options = HashMap::new();
        options.insert("zookeeper.connect".to_string(), zk_servers);

        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars
            .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("template should work");

        for broker in &self.kafka_spec.brokers {
            trace!("Reconciling broker [{}]", broker.node_name);

            // We first check whether we already have a Pod for this Node.
            // If that's the case we'll use it and if not we create a new one.
            let pod = match self
                .existing_pods
                .iter()
                .find(|pod| podutils::is_pod_assigned_to_node(pod, &broker.node_name))
            {
                None => {
                    info!(
                        "Broker on node [{}] missing, creating now",
                        broker.node_name
                    );
                    // TODO: These names need to contain a random component, also check the maximum length of names
                    let pod_name = format!("kafka-{}-{}", name, broker.node_name);
                    let cm_name = format!("kafka-{}", pod_name);

                    // First we need to create/update the necessary pods...
                    let pod = build_pod(
                        &self.context.resource,
                        &broker,
                        &labels,
                        &pod_name,
                        &cm_name,
                    )?;
                    let pod = self.context.client.create(&pod).await?;

                    // ...then we create the data for the ConfigMap
                    // There will be one ConfigMap per Pod.
                    let config = handlebars
                        .render("conf", &json!({ "options": options }))
                        .unwrap();

                    let mut data = BTreeMap::new();
                    data.insert("server.properties".to_string(), config);

                    // And now create the actual ConfigMap
                    // TODO: Need to deal with the case where the configmaps already exists (this should only happen in unclean shutdowns or similar bad scenarios)
                    let tmp_name = cm_name.clone() + "-config"; // TODO: Create these names once and pass them around so we are consistent
                    let cm = stackable_operator::create_config_map(
                        &self.context.resource,
                        &tmp_name,
                        data,
                    )?;
                    self.context.client.create(&cm).await?;
                    pod
                }
                Some(pod) => {
                    trace!(
                        "Found existing pod [{}] for broker [{}]",
                        Meta::name(pod),
                        broker.node_name
                    );
                    pod.clone()
                }
            };

            // If the pod for this server is currently terminating (this could be for restarts or
            // upgrades) wait until it's done terminating.
            if finalizer::has_deletion_stamp(&pod) {
                info!("Waiting for Pod [{}] to terminate", Meta::name(&pod));
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }

            // At the moment we'll wait for all pods to be available and ready before we might enact any changes to existing ones.
            // TODO: Only do this next check if we want "rolling" functionality
            if !podutils::is_pod_running_and_ready(&pod) {
                info!(
                    "Waiting for Pod [{}] to be running and ready",
                    Meta::name(&pod)
                );
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    pub async fn delete_excess_pods(&self) -> KafkaReconcileResult {
        trace!("Starting to delete excess pods",);

        // We iterate over all Pods that have an OwnerReference pointing at us
        // and we'll try to find all pods that are _not_ assigned to any
        // broker from the spec, we then delete it.
        for pod in &self.existing_pods {
            if !self
                .kafka_spec
                .brokers
                .iter()
                .any(|broker| podutils::is_pod_assigned_to_node(pod, &broker.node_name))
            {
                self.context.client.delete(pod).await?;
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }
}

impl ReconciliationState for KafkaState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move {
            self.check_zookeeper_reference()
                .await?
                .then(self.reconcile())
                .await?
                .then(self.delete_excess_pods())
                .await
        })
    }
}

struct KafkaStrategy {}

impl KafkaStrategy {
    pub fn new() -> KafkaStrategy {
        KafkaStrategy {}
    }
}

#[async_trait]
impl ControllerStrategy for KafkaStrategy {
    type Item = KafkaCluster;
    type State = KafkaState;
    type Error = error::Error;

    fn finalizer_name(&self) -> String {
        FINALIZER_NAME.to_string()
    }

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context.list_pods().await?;
        trace!("Found [{}] pods", existing_pods.len());

        Ok(KafkaState {
            kafka_spec: context.resource.spec.clone(),
            context,
            zk_spec: None,
            existing_pods,
        })
    }
}

pub async fn create_controller(client: Client) {
    let kafka_api: Api<KafkaCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(kafka_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = KafkaStrategy::new();

    controller.run(client, strategy).await;
}

fn build_pod(
    resource: &KafkaCluster,
    broker: &KafkaBroker,
    labels: &BTreeMap<String, String>,
    pod_name: &str,
    cm_name: &str,
) -> Result<Pod, Error> {
    let pod = Pod {
        // Metadata
        metadata: metadata::build_metadata(pod_name.to_string(), Some(labels.clone()), resource)?,
        // Spec
        spec: Some(PodSpec {
            node_name: Some(broker.node_name.clone()),
            tolerations: Some(stackable_operator::create_tolerations()),
            containers: vec![Container {
                image: Some(format!(
                    "stackable/kafka:{}",
                    serde_json::json!(resource.spec.version).as_str().unwrap()
                )),
                name: "kafka".to_string(),
                command: Some(vec![
                    "kafka_2.12-2.6.0/bin/kafka-server-start.sh".to_string(), // TODO: Don't hardcode this
                    "{{ configroot }}/config/server.properties".to_string(),
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "config".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", cm_name)), // TODO: Create these names once and pass them around so we are consistent
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            ..PodSpec::default()
        }),
        ..Pod::default()
    };
    Ok(pod)
}

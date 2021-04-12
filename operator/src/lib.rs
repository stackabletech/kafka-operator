#![feature(backtrace)]
mod error;

use crate::error::Error;

use async_trait::async_trait;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Node, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::ListParams;
use kube::Api;
use serde_json::json;
use tracing::{debug, info, trace, warn};

use stackable_kafka_crd::{KafkaCluster, KafkaClusterSpec};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_zookeeper_crd::{ZooKeeperCluster, ZooKeeperClusterSpec};

use stackable_operator::k8s_utils::LabelOptionalValueMap;
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{k8s_utils, role_utils};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::string::ToString;
use std::time::Duration;
use strum::IntoEnumIterator;
use strum_macros::Display;
use strum_macros::EnumIter;

pub const CLUSTER_NAME_LABEL: &str = "app.kubernetes.io/instance";

pub const NODE_GROUP_LABEL: &str = "kafka.stackable.tech/node-group-name";

pub const NODE_TYPE_LABEL: &str = "kafka.stackable.tech/node-type";

type KafkaReconcileResult = ReconcileResult<error::Error>;

#[derive(EnumIter, Debug, Display, PartialEq, Eq, Hash)]
pub enum KafkaNodeType {
    Broker,
}

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_spec: KafkaClusterSpec,
    zk_spec: Option<ZooKeeperClusterSpec>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<KafkaNodeType, HashMap<String, Vec<Node>>>,
}

impl KafkaState {
    async fn check_zookeeper_reference(&mut self) -> KafkaReconcileResult {
        info!("Checking ZookeeperReference exists.");
        debug!("Checking ZookeeperReference exists.");
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

    pub fn get_full_pod_node_map(&self) -> Vec<(Vec<Node>, LabelOptionalValueMap)> {
        let mut eligible_nodes_map = vec![];
        debug!(
            "Looking for excess pods that need to be deleted for cluster [{}]",
            self.context.name()
        );
        for node_type in KafkaNodeType::iter() {
            if let Some(eligible_nodes_for_role) = self.eligible_nodes.get(&node_type) {
                for (group_name, eligible_nodes) in eligible_nodes_for_role {
                    // Create labels to identify eligible nodes
                    trace!(
                        "Adding [{}] nodes to eligible node list for role [{}] and group [{}].",
                        eligible_nodes.len(),
                        node_type,
                        group_name
                    );
                    eligible_nodes_map.push((
                        eligible_nodes.clone(),
                        get_node_and_group_labels(group_name, &node_type),
                    ))
                }
            }
        }
        eligible_nodes_map
    }

    pub fn get_deletion_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = KafkaNodeType::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(String::from(NODE_TYPE_LABEL), Some(roles));
        mandatory_labels.insert(String::from(CLUSTER_NAME_LABEL), None);
        mandatory_labels
    }

    async fn create_config_map(&self, name: &str) -> Result<(), Error> {
        match self
            .context
            .client
            .get::<ConfigMap>(name, Some(&"default".to_string()))
            .await
        {
            Ok(_) => {
                debug!("ConfigMap [{}] already exists, skipping creation!", name);
                return Ok(());
            }
            Err(e) => {
                // TODO: This is shit, but works for now. If there is an actual error in comms with
                //   K8S, it will most probably also occur further down and be properly handled
                debug!("Error getting ConfigMap [{}]: [{:?}]", name, e);
            }
        }
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
        labels.insert("kafka-name".to_string(), name);

        let mut options = HashMap::new();
        options.insert("zookeeper.connect".to_string(), zk_servers);

        let mut handlebars = Handlebars::new();
        handlebars.set_strict_mode(true);
        handlebars
            .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("template should work");

        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .unwrap();

        let mut data = BTreeMap::new();
        data.insert("server.properties".to_string(), config);

        // And now create the actual ConfigMap
        let cm =
            stackable_operator::config_map::create_config_map(&self.context.resource, &name, data)?;
        self.context.client.create(&cm).await?;
        Ok(())
    }

    async fn create_missing_pods(&mut self) -> KafkaReconcileResult {
        // The iteration happens in two stages here, to accomodate the way our operators think
        // about nodes and roles.
        // The hierarchy is:
        // - Roles (for example Datanode, Namenode, Kafka Broker)
        //   - Node groups for this role (user defined)
        //      - Individual nodes
        for node_type in KafkaNodeType::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&node_type) {
                for (role_group, nodes) in nodes_for_role {
                    // Create config map for this rolegroup
                    let pod_name =
                        format!("kafka-{}-{}-{}", self.context.name(), role_group, node_type)
                            .to_lowercase();
                    let cm_name = format!("{}-config", pod_name);
                    debug!("pod_name: [{}], cm_name: [{}]", pod_name, cm_name);

                    self.create_config_map(&cm_name).await?;
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        node_type, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        nodes.len(),
                        nodes
                            .iter()
                            .map(|node| node.metadata.name.clone().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.clone().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "labels: [{:?}]",
                        get_node_and_group_labels(role_group, &node_type)
                    );
                    let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                        nodes,
                        &self.existing_pods,
                        &get_node_and_group_labels(role_group, &node_type),
                    );

                    for node in nodes_that_need_pods {
                        let node_name = if let Some(node_name) = &node.metadata.name {
                            node_name
                        } else {
                            warn!("No name found in metadata, this should not happen! Skipping node: [{:?}]", node);
                            continue;
                        };
                        debug!(
                            "Creating pod on node [{}] for [{}] role and group [{}]",
                            node.metadata
                                .name
                                .clone()
                                .unwrap_or_else(|| String::from("<no node name found>")),
                            node_type,
                            role_group
                        );

                        let mut node_labels = BTreeMap::new();
                        node_labels.insert(String::from(NODE_TYPE_LABEL), node_type.to_string());
                        node_labels
                            .insert(String::from(NODE_GROUP_LABEL), String::from(role_group));
                        node_labels.insert(String::from(CLUSTER_NAME_LABEL), self.context.name());

                        // Create a pod for this node, role and group combination
                        let pod = build_pod(
                            &self.context.resource,
                            node_name,
                            &node_labels,
                            &pod_name,
                            &cm_name,
                        )?;
                        self.context.client.create(&pod).await?;
                    }
                }
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
        info!("========================= Starting reconciliation =========================");
        debug!("Deletion Labels: [{:?}]", &self.get_deletion_labels());

        Box::pin(async move {
            self.check_zookeeper_reference()
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_deletion_labels(),
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(
                    self.context
                        .wait_for_terminating_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(self.context.delete_excess_pods(
                    self.get_full_pod_node_map().as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods())
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

    async fn init_reconcile_state(
        &self,
        context: ReconciliationContext<Self::Item>,
    ) -> Result<Self::State, Self::Error> {
        let existing_pods = context.list_pods().await?;
        trace!("Found [{}] pods", existing_pods.len());

        let cluster_spec: KafkaClusterSpec = context.resource.spec.clone();

        let mut eligible_nodes = HashMap::new();
        eligible_nodes.insert(
            KafkaNodeType::Broker,
            role_utils::find_nodes_that_fit_selectors(
                &context.client,
                None,
                cluster_spec
                    .brokers
                    .selectors
                    .iter()
                    .map(|(group_name, selector_config)| RoleGroup {
                        name: group_name.to_string(),
                        selector: selector_config.clone().selector.unwrap(),
                    })
                    .collect(),
            )
            .await?,
        );

        Ok(KafkaState {
            kafka_spec: context.resource.spec.clone(),
            context,
            zk_spec: None,
            existing_pods,
            eligible_nodes,
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

    controller
        .run(client, strategy, Duration::from_secs(5))
        .await;
}

fn get_node_and_group_labels(group_name: &str, node_type: &KafkaNodeType) -> LabelOptionalValueMap {
    let mut node_labels = BTreeMap::new();
    node_labels.insert(String::from(NODE_TYPE_LABEL), Some(node_type.to_string()));
    node_labels.insert(
        String::from(NODE_GROUP_LABEL),
        Some(String::from(group_name)),
    );
    node_labels
}

fn build_pod(
    resource: &KafkaCluster,
    node: &str,
    labels: &BTreeMap<String, String>,
    pod_name: &str,
    cm_name: &str,
) -> Result<Pod, Error> {
    let pod = Pod {
        // Metadata
        metadata: metadata::build_metadata(
            pod_name.to_string(),
            Some(labels.clone()),
            resource,
            false,
        )?,
        // Spec
        spec: Some(PodSpec {
            node_name: Some(node.to_string()),
            tolerations: Some(stackable_operator::krustlet::create_tolerations()),
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
                    name: Some(cm_name.to_string()), // TODO: Create these names once and pass them around so we are consistent
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

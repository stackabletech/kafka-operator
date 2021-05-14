#![feature(backtrace)]
mod error;

use crate::error::Error;

use async_trait::async_trait;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, Node, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::ListParams;

use serde_json::json;
use tracing::{debug, info, trace, warn};

use stackable_kafka_crd::{KafkaCluster, KafkaClusterSpec, KafkaVersion};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::labels::{
    APP_COMPONENT_LABEL, APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL,
    APP_ROLE_GROUP_LABEL, APP_VERSION_LABEL,
};
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};

use stackable_zookeeper_crd::ZooKeeperCluster;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::Api;
use stackable_operator::error::OperatorResult;
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

type KafkaReconcileResult = ReconcileResult<error::Error>;

const FINALIZER_NAME: &str = "kafka.stackable.tech/cleanup";

const APP_NAME: &str = "kafka";
const MANAGED_BY: &str = "stackable-kafka";

#[derive(EnumIter, Debug, Display, PartialEq, Eq, Hash)]
pub enum KafkaNodeType {
    Broker,
}

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_cluster: KafkaCluster,
    zk_cluster: Option<ZooKeeperCluster>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<KafkaNodeType, HashMap<String, Vec<Node>>>,
}

impl KafkaState {
    async fn check_zookeeper_reference(&mut self) -> KafkaReconcileResult {
        debug!("Checking ZookeeperReference exists.");
        let api: Api<ZooKeeperCluster> = self
            .context
            .client
            .get_namespaced_api(&self.kafka_cluster.spec.zoo_keeper_reference.namespace);
        let zk_cluster = api
            .get(&self.kafka_cluster.spec.zoo_keeper_reference.name)
            .await;

        // TODO: We need to watch the ZooKeeper resource and do _something_ when it goes down or when its nodes are changed
        let zk_cluster: ZooKeeperCluster = match zk_cluster {
            Ok(zk) => zk,
            Err(err) => {
                warn!(?err,
                    "Referencing a ZooKeeper cluster that does not exist (or some other error while fetching it): [{}/{}], we will requeue and check again",
                    &self.kafka_cluster.spec.zoo_keeper_reference.namespace,
                    &self.kafka_cluster.spec.zoo_keeper_reference.name
                );
                // TODO: Depending on the error either requeue or return an error (which'll requeue as well)
                // For a not found we'd like to requeue but if there was a transport error we'd like to return it.
                return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
            }
        };

        self.zk_cluster = Some(zk_cluster);

        Ok(ReconcileFunctionAction::Continue)
    }

    pub fn get_full_pod_node_map(&self) -> Vec<(Vec<Node>, LabelOptionalValueMap)> {
        let mut eligible_nodes_map = vec![];
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

        mandatory_labels.insert(String::from(APP_COMPONENT_LABEL), Some(roles));
        mandatory_labels.insert(String::from(APP_INSTANCE_LABEL), None);
        mandatory_labels.insert(
            String::from(APP_NAME_LABEL),
            Some(vec![String::from(APP_NAME)]),
        );
        mandatory_labels.insert(
            String::from(APP_MANAGED_BY_LABEL),
            Some(vec![String::from(MANAGED_BY)]),
        );
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
        let zk_spec = &self.zk_cluster.as_ref().ok_or_else(|| error::Error::ReconcileError("zk_spec missing, this is a programming error and should never happen. Please report in our issue tracker.".to_string()))?.spec;

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

        // opa -> works only with adapted kafka package (https://github.com/Bisnode/opa-kafka-plugin)
        // and the opa-authorizer*.jar in the lib directory of the package (https://github.com/Bisnode/opa-kafka-plugin/releases/)
        options.insert(
            "authorizer.class.name".to_string(),
            "com.bisnode.kafka.authorization.OpaAuthorizer".to_string(),
        );
        options.insert(
            "opa.authorizer.url".to_string(),
            // TODO: make configurable
            "http://localhost:8181/v1/data/kafka/authz/allow".to_string(),
        );
        // opa end

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

    async fn delete_all_pods(&self) -> OperatorResult<ReconcileFunctionAction> {
        for pod in &self.existing_pods {
            self.context.client.delete(pod).await?;
        }
        Ok(ReconcileFunctionAction::Done)
    }

    async fn create_missing_pods(&mut self) -> KafkaReconcileResult {
        // The iteration happens in two stages here, to accommodate the way our operators think
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
                            .map(|node| node.metadata.name.as_ref().unwrap())
                            .collect::<Vec<_>>()
                    );
                    trace!(
                        "existing_pods[{}]: [{:?}]",
                        &self.existing_pods.len(),
                        &self
                            .existing_pods
                            .iter()
                            .map(|pod| pod.metadata.name.as_ref().unwrap())
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
                                .as_deref()
                                .unwrap_or("<no node name found>"),
                            node_type,
                            role_group
                        );

                        let mut node_labels = BTreeMap::new();
                        node_labels.insert(String::from(APP_NAME_LABEL), String::from(APP_NAME));
                        node_labels
                            .insert(String::from(APP_MANAGED_BY_LABEL), String::from(MANAGED_BY));
                        node_labels
                            .insert(String::from(APP_COMPONENT_LABEL), node_type.to_string());
                        node_labels
                            .insert(String::from(APP_ROLE_GROUP_LABEL), String::from(role_group));
                        node_labels.insert(String::from(APP_INSTANCE_LABEL), self.context.name());
                        let version: &KafkaVersion = &self.kafka_cluster.spec.version;
                        node_labels.insert(
                            String::from(APP_VERSION_LABEL),
                            version.fully_qualified_version(),
                        );

                        // If the node name is not part of the pod name we get duplicate names
                        // which prevents all pods from being created
                        let pod_name_with_node = format!("{}-{}", pod_name, node_name);
                        // Create a pod for this node, role and group combination
                        let pod = build_pod(
                            &self.context.resource,
                            node_name,
                            &node_labels,
                            &pod_name_with_node,
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
            self.context
                .handle_deletion(Box::pin(self.delete_all_pods()), FINALIZER_NAME, true)
                .await?
                .then(self.check_zookeeper_reference())
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
                .then(
                    self.context
                        .wait_for_running_and_ready_pods(&self.existing_pods.as_slice()),
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
        let role_groups = cluster_spec
            .brokers
            .selectors
            .iter()
            .map(|(group_name, selector_config)| RoleGroup {
                name: group_name.to_string(),
                selector: match &selector_config.selector {
                    None => LabelSelector {
                        match_expressions: None,
                        match_labels: None,
                    },
                    Some(selector) => selector.clone(),
                },
            })
            .collect::<Vec<_>>();

        eligible_nodes.insert(
            KafkaNodeType::Broker,
            role_utils::find_nodes_that_fit_selectors(
                &context.client,
                None,
                role_groups.as_slice(),
            )
            .await?,
        );

        Ok(KafkaState {
            kafka_cluster: context.resource.clone(),
            context,
            zk_cluster: None,
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
    node_labels.insert(
        String::from(APP_COMPONENT_LABEL),
        Some(node_type.to_string()),
    );
    node_labels.insert(
        String::from(APP_ROLE_GROUP_LABEL),
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
                    resource.spec.version.fully_qualified_version()
                )),
                name: "kafka".to_string(),
                command: Some(vec![
                    format!(
                        "kafka_{}/bin/kafka-server-start.sh",
                        resource.spec.version.fully_qualified_version()
                    ),
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
                    name: Some(cm_name.to_string()),
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

mod error;

use crate::error::Error;
use async_trait::async_trait;
use k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Pod};
use kube::api::ListParams;
use kube::Api;
use kube::ResourceExt;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use stackable_kafka_crd::{
    KafkaCluster, KafkaRole, APP_NAME, MANAGED_BY, METRICS_PORT, SERVER_PROPERTIES_FILE,
    ZOOKEEPER_CONNECT,
};
use stackable_opa_crd::util;
use stackable_opa_crd::util::{OpaApi, OpaApiProtocol};
use stackable_operator::builder::{
    ContainerBuilder, ContainerPortBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels, APP_COMPONENT_LABEL,
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_VERSION_LABEL,
};
use stackable_operator::name_utils;
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils::{
    find_nodes_that_fit_selectors, get_role_and_group_labels,
    list_eligible_nodes_for_role_and_group, EligibleNodesForRoleAndGroup,
};
use stackable_operator::{configmap, k8s_utils};
use stackable_zookeeper_crd::util::ZookeeperConnectionInformation;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::{debug, info, trace, warn};

type KafkaReconcileResult = ReconcileResult<error::Error>;

const FINALIZER_NAME: &str = "kafka.stackable.tech/cleanup";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";
const CONFIG_DIR: &str = "config";
const CONFIG_MAP_TYPE_CONFIG: &str = "properties";
const CONFIG_MAP_NODE_NAME_LABEL: &str = "kafka.stackable.tech/node_name";

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_cluster: KafkaCluster,
    zookeeper_info: Option<ZookeeperConnectionInformation>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
}

impl KafkaState {
    async fn get_zookeeper_connection_information(&mut self) -> KafkaReconcileResult {
        let zk_ref: &stackable_zookeeper_crd::util::ZookeeperReference =
            &self.context.resource.spec.zookeeper_reference;

        if let Some(chroot) = zk_ref.chroot.as_deref() {
            stackable_zookeeper_crd::util::is_valid_zookeeper_path(chroot)?;
        }

        let zookeeper_info =
            stackable_zookeeper_crd::util::get_zk_connection_info(&self.context.client, zk_ref)
                .await?;

        debug!(
            "Received ZooKeeper connect string: [{}]",
            &zookeeper_info.connection_string
        );

        self.zookeeper_info = Some(zookeeper_info);

        Ok(ReconcileFunctionAction::Continue)
    }

    pub fn required_pod_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = KafkaRole::iter()
            .map(|role| role.to_string())
            .collect::<Vec<_>>();
        let mut mandatory_labels = BTreeMap::new();

        mandatory_labels.insert(String::from(APP_COMPONENT_LABEL), Some(roles));
        mandatory_labels.insert(
            String::from(APP_INSTANCE_LABEL),
            Some(vec![self.context.resource.name()]),
        );
        mandatory_labels.insert(
            String::from(APP_VERSION_LABEL),
            Some(vec![self
                .context
                .resource
                .spec
                .version
                .fully_qualified_version()]),
        );
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
        //   - Role groups for this role (user defined)
        for role in KafkaRole::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&role.to_string()) {
                for (role_group, (nodes, replicas)) in nodes_for_role {
                    let role_str = &role.to_string();
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        role_str, role_group
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
                        get_role_and_group_labels(role_str, role_group)
                    );
                    let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                        nodes,
                        &self.existing_pods,
                        &get_role_and_group_labels(role_str, role_group),
                        *replicas,
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
                            role_str,
                            role_group
                        );

                        // now we have a node that needs a pod -> get validated config
                        let validated_config = config_for_role_and_group(
                            role_str,
                            role_group,
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(role_str, role_group, node_name, validated_config)
                            .await?;

                        self.create_pod(
                            role_str,
                            role_group,
                            node_name,
                            &config_maps,
                            validated_config,
                        )
                        .await?;

                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }

    /// Creates the config maps required for a kafka instance (or role, role_group combination):
    /// * The 'server.properties'
    ///
    /// The 'server.properties' properties are read from the product_config.
    ///
    /// Labels are automatically adapted from the `recommended_labels` with a type (config for
    /// 'server.properties'). Names are generated via `name_utils::build_resource_name`.
    ///
    /// Returns a map with a 'type' identifier (e.g. 'properties') as key and the corresponding
    /// ConfigMap as value. This is required to set the volume mounts in the pod later on.
    ///
    /// # Arguments
    ///
    /// - `role` - The Kafka role.
    /// - `group` - The role group.
    /// - `node_name` - The node name for this instance.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_config_maps(
        &self,
        role: &str,
        group: &str,
        node_name: &str,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<HashMap<&'static str, ConfigMap>, Error> {
        let mut config_maps = HashMap::new();

        let recommended_labels = get_recommended_labels(
            &self.context.resource,
            APP_NAME,
            &self.context.resource.spec.version.to_string(),
            role,
            group,
        );

        if let Some(config) =
            validated_config.get(&PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()))
        {
            let mut adapted_config = config.clone();

            // enhance with config map type label
            let mut cm_properties_labels = recommended_labels;
            cm_properties_labels.insert(
                configmap::CONFIGMAP_TYPE_LABEL.to_string(),
                CONFIG_MAP_TYPE_CONFIG.to_string(),
            );

            // add zookeeper reference
            if let Some(info) = &self.zookeeper_info {
                adapted_config.insert(
                    ZOOKEEPER_CONNECT.to_string(),
                    info.connection_string.clone(),
                );
            }

            // OPA reference -> works only with adapted kafka package (https://github.com/Bisnode/opa-kafka-plugin)
            // and the opa-authorizer*.jar in the lib directory of the package (https://github.com/Bisnode/opa-kafka-plugin/releases/)
            // We cannot query for the opa info as we do for zookeeper (meaning once at the start
            // of the reconcile method), since we need the node_name to potentially find matches on
            // the same machine for performance increase (which requires the node_name).
            if let Some(opa_config) = &self.kafka_cluster.spec.opa {
                // If we use opa we need to differentiate the configmaps via the node_name. We need
                // to create a configmap per node to point to the local installed opa instance if
                // available. Therefore we add an extra node name to the recommended labels here.
                cm_properties_labels.insert(
                    CONFIG_MAP_NODE_NAME_LABEL.to_string(),
                    node_name.to_string(),
                );

                let opa_api = OpaApi::Data {
                    package_path: "kafka/authz".to_string(),
                    rule: "allow".to_string(),
                };

                let connection_info = util::get_opa_connection_info(
                    &self.context.client,
                    &opa_config.reference,
                    &opa_api,
                    &OpaApiProtocol::Http,
                    Some(node_name.to_string()),
                )
                .await?;

                debug!(
                    "Found valid OPA server [{}]",
                    connection_info.connection_string
                );

                adapted_config.insert(
                    "opa.authorizer.url".to_string(),
                    connection_info.connection_string,
                );
            }

            // We need to convert from <String, String> to <String, Option<String>> to deal with
            // CLI flags etc. We can not currently represent that via operator-rs / product-config.
            // This is a preparation for that.
            let transformed_config: BTreeMap<String, Option<String>> = adapted_config
                .iter()
                .map(|(k, v)| (k.to_string(), Some(v.to_string())))
                .collect();

            let data_string =
                product_config::writer::to_java_properties_string(transformed_config.iter())?;

            let mut cm_properties_data = BTreeMap::new();
            cm_properties_data.insert(SERVER_PROPERTIES_FILE.to_string(), data_string);

            let cm_properties_name = name_utils::build_resource_name(
                APP_NAME,
                &self.context.name(),
                role,
                Some(group),
                Some(node_name),
                Some(CONFIG_MAP_TYPE_CONFIG),
            )?;

            let cm_config = configmap::build_config_map(
                &self.context.resource,
                &cm_properties_name,
                &self.context.namespace(),
                cm_properties_labels,
                cm_properties_data,
            )?;

            config_maps.insert(
                CONFIG_MAP_TYPE_CONFIG,
                configmap::create_config_map(&self.context.client, cm_config).await?,
            );
        }

        Ok(config_maps)
    }

    /// Creates the pod required for the Kafka instance.
    ///
    /// # Arguments
    ///
    /// - `role` - The Kafka role.
    /// - `group` - The role group.
    /// - `node_name` - The node name for this pod.
    /// - `config_maps` - The config maps and respective types required for this pod.
    /// - `validated_config` - The validated product config.
    ///
    async fn create_pod(
        &self,
        role: &str,
        group: &str,
        node_name: &str,
        config_maps: &HashMap<&'static str, ConfigMap>,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<Pod, Error> {
        let mut env_vars = vec![];
        let mut metrics_port: Option<u16> = None;

        let version = &self.context.resource.spec.version.fully_qualified_version();

        if let Some(config) = validated_config.get(&PropertyNameKind::Env) {
            for (property_name, property_value) in config {
                if property_name.is_empty() {
                    warn!("Received empty property_name for ENV... skipping");
                    continue;
                }

                // if a metrics port is provided (for now by user, it is not required in
                // product config to be able to not configure any monitoring / metrics)
                if property_name == METRICS_PORT {
                    // cannot panic because checked in product config
                    metrics_port = Some(property_value.parse::<u16>().unwrap());
                    env_vars.push(EnvVar {
                                name: "EXTRA_ARGS".to_string(),
                                value: Some(format!("-javaagent:{{{{packageroot}}}}/kafka_{}/stackable/lib/jmx_prometheus_javaagent-0.16.1.jar={}:{{{{packageroot}}}}/kafka_{}/stackable/conf/jmx_exporter.yaml",
                                                    version, property_value, version)),
                                ..EnvVar::default()
                            });
                    continue;
                }

                env_vars.push(EnvVar {
                    name: property_name.clone(),
                    value: Some(property_value.to_string()),
                    value_from: None,
                });
            }
        }

        let pod_name = name_utils::build_resource_name(
            APP_NAME,
            &self.context.name(),
            role,
            Some(group),
            Some(node_name),
            None,
        )?;

        let labels = get_recommended_labels(&self.context.resource, APP_NAME, version, role, group);

        let mut container_builder = ContainerBuilder::new(APP_NAME);
        container_builder.image(format!("stackable/kafka:{}", &version));
        container_builder.command(vec![
            format!("kafka_{}/bin/kafka-server-start.sh", version),
            format!(
                "{{{{configroot}}}}/{}/{}",
                CONFIG_DIR, SERVER_PROPERTIES_FILE
            ),
        ]);
        container_builder.add_env_vars(env_vars);

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_CONFIG) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_configmapvolume(name, CONFIG_DIR.to_string());
            } else {
                return Err(error::Error::MissingConfigMapNameError {
                    cm_type: CONFIG_MAP_TYPE_CONFIG,
                });
            }
        } else {
            return Err(error::Error::MissingConfigMapError {
                cm_type: CONFIG_MAP_TYPE_CONFIG,
                pod_name,
            });
        }

        let mut annotations = BTreeMap::new();
        // only add metrics container port and annotation if available
        if let Some(metrics_port) = metrics_port {
            annotations.insert(SHOULD_BE_SCRAPED.to_string(), "true".to_string());
            container_builder.add_container_port(
                ContainerPortBuilder::new(metrics_port)
                    .name("metrics")
                    .build(),
            );
        }

        let pod = PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .generate_name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(labels)
                    .with_annotations(annotations)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_stackable_agent_tolerations()
            .add_container(container_builder.build())
            .node_name(node_name)
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }
}

impl ReconciliationState for KafkaState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");
        debug!("Deletion Labels: [{:?}]", &self.required_pod_labels());

        Box::pin(async move {
            self.context
                .handle_deletion(Box::pin(self.delete_all_pods()), FINALIZER_NAME, true)
                .await?
                .then(self.get_zookeeper_connection_information())
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.required_pod_labels(),
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
                        .wait_for_running_and_ready_pods(self.existing_pods.as_slice()),
                )
                .await?
                .then(self.context.delete_excess_pods(
                    list_eligible_nodes_for_role_and_group(&self.eligible_nodes).as_slice(),
                    &self.existing_pods,
                    ContinuationStrategy::OneRequeue,
                ))
                .await?
                .then(self.create_missing_pods())
                .await
        })
    }
}

struct KafkaStrategy {
    config: Arc<ProductConfigManager>,
}

impl KafkaStrategy {
    pub fn new(config: ProductConfigManager) -> KafkaStrategy {
        KafkaStrategy {
            config: Arc::new(config),
        }
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
        let existing_pods = context
            .list_owned(build_common_labels_for_all_managed_resources(
                APP_NAME,
                &context.resource.name(),
            ))
            .await?;
        trace!("Found [{}] pods", existing_pods.len());

        let mut eligible_nodes = HashMap::new();

        eligible_nodes.insert(
            KafkaRole::Broker.to_string(),
            find_nodes_that_fit_selectors(&context.client, None, &context.resource.spec.brokers)
                .await?,
        );

        Ok(KafkaState {
            validated_role_config: validated_product_config(&context.resource, &self.config)?,
            kafka_cluster: context.resource.clone(),
            context,
            zookeeper_info: None,
            existing_pods,
            eligible_nodes,
        })
    }
}

pub fn validated_product_config(
    resource: &KafkaCluster,
    product_config: &ProductConfigManager,
) -> OperatorResult<ValidatedRoleConfigByPropertyKind> {
    let mut roles = HashMap::new();
    roles.insert(
        KafkaRole::Broker.to_string(),
        (
            vec![
                PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()),
                PropertyNameKind::Env,
            ],
            resource.spec.brokers.clone().into(),
        ),
    );

    let role_config = transform_all_roles_to_config(resource, roles);

    validate_all_roles_and_groups_config(
        resource.spec.version.kafka_version(),
        &role_config,
        product_config,
        false,
        false,
    )
}

pub async fn create_controller(client: Client, product_config_path: &str) -> OperatorResult<()> {
    let kafka_api: Api<KafkaCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(kafka_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = KafkaStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(5))
        .await;

    Ok(())
}

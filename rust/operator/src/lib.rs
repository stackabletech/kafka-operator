mod error;

use crate::error::Error;
use async_trait::async_trait;
use stackable_kafka_crd::commands::{Restart, Start, Stop};
use stackable_kafka_crd::{
    KafkaCluster, KafkaRole, APP_NAME, MANAGED_BY, METRICS_PORT, SERVER_PROPERTIES_FILE,
    ZOOKEEPER_CONNECT,
};
use stackable_opa_crd::util;
use stackable_opa_crd::util::{OpaApi, OpaApiProtocol};
use stackable_operator::builder::{ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder};
use stackable_operator::client::Client;
use stackable_operator::command::materialize_command;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::identity::{
    LabeledPodIdentityFactory, NodeIdentity, PodIdentity, PodToNodeMapping,
};
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, EnvVar, Pod};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::Api;
use stackable_operator::kube::ResourceExt;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels, APP_COMPONENT_LABEL,
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_VERSION_LABEL,
};
use stackable_operator::name_utils;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config::ProductConfigManager;
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
use stackable_operator::scheduler::{
    K8SUnboundedHistory, RoleGroupEligibleNodes, ScheduleStrategy, Scheduler, StickyScheduler,
};
use stackable_operator::status::HasClusterExecutionStatus;
use stackable_operator::status::{init_status, ClusterExecutionStatus};
use stackable_operator::versioning::{finalize_versioning, init_versioning};
use stackable_operator::{configmap, product_config};
use stackable_zookeeper_crd::discovery::ZookeeperConnectionInformation;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tracing::{debug, error, info, trace, warn};

type KafkaReconcileResult = ReconcileResult<error::Error>;

const FINALIZER_NAME: &str = "kafka.stackable.tech/cleanup";
const SHOULD_BE_SCRAPED: &str = "monitoring.stackable.tech/should_be_scraped";
const CONFIG_DIR: &str = "/stackable/config";
const CONFIG_MAP_TYPE_CONFIG: &str = "properties";
const CONFIG_MAP_NODE_NAME_LABEL: &str = "kafka.stackable.tech/node_name";
const ID_LABEL: &str = "kafka.stackable.tech/id";

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    zookeeper_info: Option<ZookeeperConnectionInformation>,
    existing_pods: Vec<Pod>,
    eligible_nodes: EligibleNodesForRoleAndGroup,
    validated_role_config: ValidatedRoleConfigByPropertyKind,
}

impl KafkaState {
    /// Will initialize the status object if it's never been set.
    async fn init_status(&mut self) -> KafkaReconcileResult {
        // init status with default values if not available yet.
        self.context.resource = init_status(&self.context.client, &self.context.resource).await?;

        let spec_version = self.context.resource.spec.version.clone();

        self.context.resource =
            init_versioning(&self.context.client, &self.context.resource, spec_version).await?;

        // set the cluster status to running
        if self.context.resource.cluster_execution_status().is_none() {
            self.context
                .client
                .merge_patch_status(
                    &self.context.resource,
                    &self
                        .context
                        .resource
                        .cluster_execution_status_patch(&ClusterExecutionStatus::Running),
                )
                .await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }

    async fn get_zookeeper_connection_information(&mut self) -> KafkaReconcileResult {
        let zk_ref: &stackable_zookeeper_crd::discovery::ZookeeperReference =
            &self.context.resource.spec.zookeeper_reference;

        if let Some(chroot) = zk_ref.chroot.as_deref() {
            stackable_zookeeper_crd::discovery::is_valid_zookeeper_path(chroot)?;
        }

        let zookeeper_info = stackable_zookeeper_crd::discovery::get_zk_connection_info(
            &self.context.client,
            zk_ref,
        )
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
                for (role_group, eligible_nodes) in nodes_for_role {
                    let role_str = &role.to_string();
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        role_str, role_group
                    );
                    trace!(
                        "candidate_nodes[{}]: [{:?}]",
                        eligible_nodes.nodes.len(),
                        eligible_nodes
                            .nodes
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

                    let mut history = match self
                        .context
                        .resource
                        .status
                        .as_ref()
                        .and_then(|status| status.history.as_ref())
                    {
                        Some(simple_history) => {
                            // we clone here because we cannot access mut self because we need it later
                            // to create config maps and pods. The `status` history will be out of sync
                            // with the cloned `simple_history` until the next reconcile.
                            // The `status` history should not be used after this method to avoid side
                            // effects.
                            K8SUnboundedHistory::new(&self.context.client, simple_history.clone())
                        }
                        None => K8SUnboundedHistory::new(
                            &self.context.client,
                            PodToNodeMapping::default(),
                        ),
                    };

                    let mut sticky_scheduler =
                        StickyScheduler::new(&mut history, ScheduleStrategy::GroupAntiAffinity);

                    let pod_id_factory = LabeledPodIdentityFactory::new(
                        APP_NAME,
                        &self.context.name(),
                        &self.eligible_nodes,
                        ID_LABEL,
                        1,
                    );

                    let state = sticky_scheduler.schedule(
                        &pod_id_factory,
                        &RoleGroupEligibleNodes::from(&self.eligible_nodes),
                        &self.existing_pods,
                    )?;

                    let mapping = state.remaining_mapping().filter(
                        APP_NAME,
                        &self.context.name(),
                        role_str,
                        role_group,
                    );

                    if let Some((pod_id, node_id)) = mapping.iter().next() {
                        // now we have a node that needs a pod -> get validated config
                        let validated_config = config_for_role_and_group(
                            pod_id.role(),
                            pod_id.group(),
                            &self.validated_role_config,
                        )?;

                        let config_maps = self
                            .create_config_maps(pod_id, node_id, validated_config)
                            .await?;

                        self.create_pod(pod_id, node_id, &config_maps, validated_config)
                            .await?;

                        history.save(&self.context.resource).await?;

                        return Ok(ReconcileFunctionAction::Requeue(Duration::from_secs(10)));
                    }
                }
            }
        }

        // If we reach here it means all pods must be running on target_version.
        // We can now set current_version to target_version (if target_version was set) and
        // target_version to None
        finalize_versioning(&self.context.client, &self.context.resource).await?;

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
        pod_id: &PodIdentity,
        node_id: &NodeIdentity,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<HashMap<&'static str, ConfigMap>, Error> {
        let mut config_maps = HashMap::new();

        let recommended_labels = get_recommended_labels(
            &self.context.resource,
            pod_id.app(),
            &self.context.resource.spec.version.to_string(),
            pod_id.role(),
            pod_id.group(),
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
            if let Some(opa_config) = &self.context.resource.spec.opa {
                // If we use opa we need to differentiate the configmaps via the node_name. We need
                // to create a configmap per node to point to the local installed opa instance if
                // available. Therefore we add an extra node name to the recommended labels here.
                cm_properties_labels
                    .insert(CONFIG_MAP_NODE_NAME_LABEL.to_string(), node_id.name.clone());

                let opa_api = OpaApi::Data {
                    package_path: "kafka/authz".to_string(),
                    rule: "allow".to_string(),
                };

                let connection_info = util::get_opa_connection_info(
                    &self.context.client,
                    &opa_config.reference,
                    &opa_api,
                    &OpaApiProtocol::Http,
                    Some(node_id.name.clone()),
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
                pod_id.app(),
                &self.context.name(),
                pod_id.role(),
                Some(pod_id.group()),
                Some(node_id.name.as_str()),
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
        pod_id: &PodIdentity,
        node_id: &NodeIdentity,
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
                                value: Some(format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/jmx_exporter.yaml",
                                                    property_value)),
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
            pod_id.app(),
            &self.context.name(),
            pod_id.role(),
            Some(pod_id.group()),
            Some(node_id.name.as_str()),
            None,
        )?;

        let mut labels = get_recommended_labels(
            &self.context.resource,
            APP_NAME,
            version,
            pod_id.role(),
            pod_id.group(),
        );
        labels.insert(String::from(ID_LABEL), String::from(pod_id.id()));

        let mut container_builder = ContainerBuilder::new(APP_NAME);
        container_builder.image(format!("kafka:{}", version));
        container_builder.command(vec![
            "bin/kafka-server-start.sh".to_string(),
            format!("{}/{}", CONFIG_DIR, SERVER_PROPERTIES_FILE),
        ]);
        container_builder.add_env_vars(env_vars);

        let mut pod_builder = PodBuilder::new();

        // One mount for the config directory
        if let Some(config_map_data) = config_maps.get(CONFIG_MAP_TYPE_CONFIG) {
            if let Some(name) = config_map_data.metadata.name.as_ref() {
                container_builder.add_volume_mount("config", CONFIG_DIR.to_string());
                pod_builder.add_volume(VolumeBuilder::new("config").with_config_map(name).build());
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
            container_builder.add_container_port("metrics", i32::from(metrics_port));
        }

        // TODO: remove if not testing locally
        container_builder.image_pull_policy("IfNotPresent");

        let pod = pod_builder
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
            .node_name(node_id.name.clone())
            // TODO: first iteration we are using host network
            .host_network(true)
            .build()?;

        Ok(self.context.client.create(&pod).await?)
    }

    pub async fn process_command(&mut self) -> KafkaReconcileResult {
        match self.context.retrieve_current_command().await? {
            // if there is no new command and the execution status is stopped we stop the
            // reconcile loop here.
            None => match self.context.resource.cluster_execution_status() {
                Some(execution_status) if execution_status == ClusterExecutionStatus::Stopped => {
                    Ok(ReconcileFunctionAction::Done)
                }
                _ => Ok(ReconcileFunctionAction::Continue),
            },
            Some(command_ref) => match command_ref.kind.as_str() {
                "Restart" => {
                    info!("Restarting cluster [{:?}]", command_ref);
                    let mut restart_command: Restart =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_restart(&mut restart_command).await?)
                }
                "Start" => {
                    info!("Starting cluster [{:?}]", command_ref);
                    let mut start_command: Start =
                        materialize_command(&self.context.client, &command_ref).await?;
                    Ok(self.context.default_start(&mut start_command).await?)
                }
                "Stop" => {
                    info!("Stopping cluster [{:?}]", command_ref);
                    let mut stop_command: Stop =
                        materialize_command(&self.context.client, &command_ref).await?;

                    Ok(self.context.default_stop(&mut stop_command).await?)
                }
                _ => {
                    error!("Got unknown type of command: [{:?}]", command_ref);
                    Ok(ReconcileFunctionAction::Done)
                }
            },
        }
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
            self.init_status()
                .await?
                .then(self.context.handle_deletion(
                    Box::pin(self.delete_all_pods()),
                    FINALIZER_NAME,
                    true,
                ))
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
                .then(self.process_command())
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
    let cmd_restart_api: Api<Restart> = client.get_all_api();
    let cmd_start_api: Api<Start> = client.get_all_api();
    let cmd_stop_api: Api<Stop> = client.get_all_api();

    let controller = Controller::new(kafka_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default())
        .owns(cmd_restart_api, ListParams::default())
        .owns(cmd_start_api, ListParams::default())
        .owns(cmd_stop_api, ListParams::default());

    let product_config = ProductConfigManager::from_yaml_file(product_config_path).unwrap();

    let strategy = KafkaStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(5))
        .await;

    Ok(())
}

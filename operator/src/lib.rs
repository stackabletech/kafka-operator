mod error;
mod pod_utils;

use crate::error::Error;
use crate::pod_utils::build_pod_name;
use async_trait::async_trait;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{ConfigMap, Node, Pod};
use kube::api::ListParams;
use kube::Api;
use kube::ResourceExt;
use product_config::types::PropertyNameKind;
use product_config::ProductConfigManager;
use serde_json::json;
use stackable_kafka_crd::{
    KafkaCluster, KafkaRole, KafkaVersion, APP_NAME, MANAGED_BY, OPA_AUTHORIZER_URL,
    SERVER_PROPERTIES_FILE, ZOOKEEPER_CONNECT,
};
use stackable_opa_crd::util;
use stackable_opa_crd::util::{OpaApi, OpaApiProtocol};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_utils;
use stackable_operator::labels::{
    build_common_labels_for_all_managed_resources, get_recommended_labels, APP_COMPONENT_LABEL,
    APP_INSTANCE_LABEL, APP_MANAGED_BY_LABEL, APP_NAME_LABEL, APP_ROLE_GROUP_LABEL,
    APP_VERSION_LABEL,
};
use stackable_operator::product_config_utils::{
    config_for_role_and_group, transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::reconcile::{
    ContinuationStrategy, ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use stackable_operator::role_utils::{
    find_nodes_that_fit_selectors, get_role_and_group_labels,
    list_eligible_nodes_for_role_and_group,
};
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

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_cluster: KafkaCluster,
    zookeeper_info: Option<ZookeeperConnectionInformation>,
    existing_pods: Vec<Pod>,
    eligible_nodes: HashMap<String, HashMap<String, Vec<Node>>>,
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

    pub fn get_required_labels(&self) -> BTreeMap<String, Option<Vec<String>>> {
        let roles = KafkaRole::iter()
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

    async fn create_config_map(&self, cm_name: &str, node_name: &str) -> Result<(), Error> {
        let mut labels = BTreeMap::new();
        labels.insert("kafka-name".to_string(), cm_name);

        let mut options = HashMap::new();
        if let Some(info) = &self.zookeeper_info {
            options.insert(
                "zookeeper.connect".to_string(),
                info.connection_string.clone(),
            );
        }

        // opa -> works only with adapted kafka package (https://github.com/Bisnode/opa-kafka-plugin)
        // and the opa-authorizer*.jar in the lib directory of the package (https://github.com/Bisnode/opa-kafka-plugin/releases/)
        // TODO: We cannot query for the opa info as we do for zookeeper (meaning once at the start
        //    of the reconcile method), since we need the node_name to potentially find matches on
        //    the same machine for performance increase (which requires the node_name).
        if let Some(opa_reference) = &self.kafka_cluster.spec.opa_reference {
            let opa_api = OpaApi::Data {
                package_path: "kafka/authz".to_string(),
                rule: "allow".to_string(),
            };

            match util::get_opa_connection_info(
                &self.context.client,
                opa_reference,
                &opa_api,
                &OpaApiProtocol::Http,
                Some(node_name.to_string()),
            )
            .await
            {
                Ok(connection_info) => {
                    debug!(
                        "Found valid OPA server [{}]",
                        connection_info.connection_string
                    );

                    options.insert(
                        "authorizer.class.name".to_string(),
                        "com.bisnode.kafka.authorization.OpaAuthorizer".to_string(),
                    );

                    options.insert(
                        "opa.authorizer.url".to_string(),
                        connection_info.connection_string,
                    );

                    // TODO: make configurable (no caching for now)
                    options.insert(
                        "opa.authorizer.cache.initial.capacity".to_string(),
                        "0".to_string(),
                    );

                    // TODO: make configurable (no caching for now)
                    options.insert(
                        "opa.authorizer.cache.maximum.size".to_string(),
                        "0".to_string(),
                    );

                    // TODO: make configurable (no caching for now)
                    // options.insert(
                    //     "opa.authorizer.cache.expire.after.seconds".to_string(),
                    //     "0".to_string(),
                    // );
                }
                Err(err) => {
                    warn!("Could not retrieve OPA connection information: {:?}", err)
                }
            }
        }
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

        let config_map = ConfigMap::default();
        //     stackable_operator::config_map::create_config_map(
        //     &self.context.resource,
        //     &cm_name,
        //     data,
        // )?;

        match self
            .context
            .client
            .get::<ConfigMap>(cm_name, Some(&self.context.namespace()))
            .await
        {
            Ok(ConfigMap {
                data: existing_config_map_data,
                ..
            }) if existing_config_map_data == config_map.data => {
                debug!(
                    "ConfigMap [{}] already exists with identical data, skipping creation!",
                    cm_name
                );
            }
            Ok(_) => {
                debug!(
                    "ConfigMap [{}] already exists, but differs, updating it!",
                    cm_name
                );
                self.context.client.update(&config_map).await?;
            }
            Err(e) => {
                // TODO: This is shit, but works for now. If there is an actual error in comes with
                //   K8S, it will most probably also occur further down and be properly handled
                debug!("Error getting ConfigMap [{}]: [{:?}]", cm_name, e);
                self.context.client.create(&config_map).await?;
            }
        }

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
        //   - Role groups for this role (user defined)
        for role in KafkaRole::iter() {
            if let Some(nodes_for_role) = self.eligible_nodes.get(&role.to_string()) {
                for (role_group, nodes) in nodes_for_role {
                    debug!(
                        "Identify missing pods for [{}] role and group [{}]",
                        role, role_group
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
                        get_role_and_group_labels(&role.to_string(), role_group)
                    );
                    let nodes_that_need_pods = k8s_utils::find_nodes_that_need_pods(
                        nodes,
                        &self.existing_pods,
                        &get_role_and_group_labels(&role.to_string(), role_group),
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
                            role,
                            role_group
                        );

                        let (pod, config_maps) = self
                            .create_pod_and_config_maps(
                                &role,
                                role_group,
                                &node_name,
                                &config_for_role_and_group(
                                    role_str,
                                    role_group,
                                    &self.validated_role_config,
                                )?,
                            )
                            .await?;

                        self.context.client.create(&pod).await?;

                        for config_map in config_maps {
                            self.create_config_map(config_map, node_name).await?;
                        }
                    }
                }
            }
        }
        Ok(ReconcileFunctionAction::Continue)
    }

    async fn create_pod_and_config_maps(
        &self,
        role: &KafkaRole,
        role_group: &str,
        node_name: &str,
        validated_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    ) -> Result<(Pod, Vec<ConfigMap>), Error> {
        let mut config_maps = vec![];
        let mut env_vars = vec![];
        let mut start_command = vec![];

        let mut cm_data = BTreeMap::new();
        let pod_name = build_pod_name(
            APP_NAME,
            &self.context.name(),
            &role.to_string(),
            role_group,
            node_name,
        );
        let cm_name = format!("{}-config", pod_name);

        for (property_name_kind, config) in validated_config {
            match property_name_kind {
                PropertyNameKind::File(file_name) => {
                    if file_name.as_str() == SERVER_PROPERTIES_FILE {
                        if let Some(zk_connect) = config.get(ZOOKEEPER_CONNECT) {
                            cm_data.insert(
                                SERVER_PROPERTIES_FILE.to_string(),
                                create_config_file(zk_connect),
                            );
                            config_maps.push(
                                ConfigMapBuilder::new()
                                    .metadata(
                                        ObjectMetaBuilder::new()
                                            .name(cm_name.clone())
                                            .ownerreference_from_resource(
                                                &self.context.resource,
                                                Some(true),
                                                Some(true),
                                            )?
                                            .namespace(&self.context.client.default_namespace)
                                            .build()?,
                                    )
                                    .data(cm_data.clone())
                                    .build()?,
                            )
                        }
                    }
                }
                PropertyNameKind::Env => {
                    for (property_name, property_value) in config {
                        if property_name.is_empty() {
                            warn!("Received empty property_name for ENV... skipping");
                            continue;
                        }

                        env_vars.push(EnvVar {
                            name: property_name.clone(),
                            value: Some(property_value.to_string()),
                            value_from: None,
                        });
                    }
                }
                PropertyNameKind::Cli => {
                    if let Some(port) = config.get(PORT) {
                        start_command = create_opa_start_command(Some(port.clone()));
                    }
                }
            }
        }

        let version = &self.context.resource.spec.version.to_string();

        let labels = get_recommended_labels(
            &self.context.resource,
            APP_NAME,
            version,
            &role.to_string(),
            role_group,
        );

        let mut container_builder = ContainerBuilder::new("opa");
        container_builder.image(format!(
            "opa:{}",
            &self.context.resource.spec.version.to_string()
        ));
        container_builder.command(start_command);
        container_builder.add_configmapvolume(cm_name, "conf".to_string());

        for env in env_vars {
            if let Some(val) = env.value {
                container_builder.add_env_var(env.name, val);
            }
        }

        let pod = PodBuilder::new()
            .metadata(
                ObjectMetaBuilder::new()
                    .name(pod_name)
                    .namespace(&self.context.client.default_namespace)
                    .with_labels(labels)
                    .ownerreference_from_resource(&self.context.resource, Some(true), Some(true))?
                    .build()?,
            )
            .add_stackable_agent_tolerations()
            .add_container(container_builder.build())
            .node_name(node_name)
            .build()?;

        Ok((pod, config_maps))
    }
}

impl ReconciliationState for KafkaState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        info!("========================= Starting reconciliation =========================");
        debug!("Deletion Labels: [{:?}]", &self.get_required_labels());

        Box::pin(async move {
            self.context
                .handle_deletion(Box::pin(self.delete_all_pods()), FINALIZER_NAME, true)
                .await?
                .then(self.get_zookeeper_connection_information())
                .await?
                .then(self.context.delete_illegal_pods(
                    self.existing_pods.as_slice(),
                    &self.get_required_labels(),
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
            vec![PropertyNameKind::File("server.properties".to_string())],
            resource.spec.brokers.clone().into_dyn(),
        ),
    );

    let role_config = transform_all_roles_to_config(resource, roles);

    validate_all_roles_and_groups_config(
        &resource.spec.version.to_string(),
        &role_config,
        &product_config,
        false,
        false,
    )
}

pub async fn create_controller(client: Client) {
    let kafka_api: Api<KafkaCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(kafka_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let product_config =
        ProductConfigManager::from_yaml_file("deploy/config-spec/properties.yaml").unwrap();

    let strategy = KafkaStrategy::new(product_config);

    controller
        .run(client, strategy, Duration::from_secs(5))
        .await;
}

// fn build_pod(
//     resource: &KafkaCluster,
//     node: &str,
//     labels: &BTreeMap<String, String>,
//     pod_name: &str,
//     cm_name: &str,
// ) -> Result<Pod, Error> {
// let pod = Pod {
//     // Metadata
//     metadata: metadata::build_metadata(
//         pod_name.to_string(),
//         Some(labels.clone()),
//         resource,
//         false,
//     )?,
//     // Spec
//     spec: Some(PodSpec {
//         node_name: Some(node.to_string()),
//         tolerations: Some(stackable_operator::krustlet::create_tolerations()),
//         containers: vec![Container {
//             image: Some(format!(
//                 "stackable/kafka:{}",
//                 resource.spec.version.fully_qualified_version()
//             )),
//             name: "kafka".to_string(),
//             command: Some(vec![
//                 format!(
//                     "kafka_{}/bin/kafka-server-start.sh",
//                     resource.spec.version.fully_qualified_version()
//                 ),
//                 "{{ configroot }}/config/server.properties".to_string(),
//             ]),
//             volume_mounts: Some(vec![VolumeMount {
//                 mount_path: "config".to_string(),
//                 name: "config-volume".to_string(),
//                 ..VolumeMount::default()
//             }]),
//             ..Container::default()
//         }],
//         volumes: Some(vec![Volume {
//             name: "config-volume".to_string(),
//             config_map: Some(ConfigMapVolumeSource {
//                 name: Some(cm_name.to_string()),
//                 ..ConfigMapVolumeSource::default()
//             }),
//             ..Volume::default()
//         }]),
//         ..PodSpec::default()
//     }),
//     ..Pod::default()
// };
// Ok(pod)
//}

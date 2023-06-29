//! Ensures that `Pod`s are configured and running for each [`KafkaCluster`]
use crate::product_logging::{
    extend_role_group_config_map, resolve_vector_aggregator_address, STACKABLE_LOG_DIR,
};
use crate::{
    discovery::{self, build_discovery_configmaps},
    pod_svc_controller,
    product_logging::{LOG4J_CONFIG_FILE, LOG_VOLUME_SIZE_IN_MIB},
    utils::{self, build_recommended_labels},
    ControllerConfig,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{
    listener::get_kafka_listener_config, security::KafkaTlsSecurity, Container, KafkaCluster,
    KafkaClusterStatus, KafkaConfig, KafkaRole, APP_NAME, DOCKER_IMAGE_BASE_NAME, KAFKA_HEAP_OPTS,
    LOG_DIRS_VOLUME_NAME, METRICS_PORT, METRICS_PORT_NAME, OPERATOR_NAME, SERVER_PROPERTIES_FILE,
    STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_TMP_DIR,
};

use stackable_operator::{
    builder::{
        resources::ResourceRequirementsBuilder, ConfigMapBuilder, ContainerBuilder,
        ObjectMetaBuilder, PodBuilder, PodSecurityContextBuilder,
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::AuthenticationClass, opa::OpaApiVersion,
        product_image_selection::ResolvedProductImage, rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort,
                EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction, ObjectFieldSelector,
                PodSpec, Probe, Service, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        api::DynamicObject,
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
/// Used as runAsUser in the pod security context. This is specified in the kafka image file
pub const KAFKA_UID: i64 = 1000;
const JAVA_HEAP_RATIO: f32 = 0.8;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub controller_config: ControllerConfig,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no name"))]
    ObjectHasNoName,
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no broker role"))]
    NoBrokerRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to apply role Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyRoleServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to find rolegroup {}", rolegroup))]
    RoleGroupNotFound {
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("invalid ServiceAccount"))]
    InvalidServiceAccount {
        source: utils::NamespaceMismatchError,
    },
    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve {}", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
        "failed to use authentication provider {} - supported methods: {:?}",
        provider,
        supported
    ))]
    AuthenticationProviderNotSupported {
        authentication_class: ObjectRef<AuthenticationClass>,
        supported: Vec<String>,
        provider: String,
    },
    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: stackable_kafka_crd::listener::KafkaListenerError,
    },
    #[snafu(display("invalid container name [{name}]"))]
    InvalidContainerName {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to initialize security context"))]
    FailedToInitializeSecurityContext {
        source: stackable_kafka_crd::security::Error,
    },
    #[snafu(display("invalid memory resource configuration"))]
    InvalidHeapConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: stackable_kafka_crd::Error },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            Error::ObjectHasNoNamespace => None,
            Error::NoBrokerRole => None,
            Error::GlobalServiceNameNotFound => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleServiceAccount { .. } => None,
            Error::ApplyRoleRoleBinding { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::GenerateProductConfig { .. } => None,
            Error::InvalidProductConfig { .. } => None,
            Error::SerializeZooCfg { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::RoleGroupNotFound { .. } => None,
            Error::InvalidServiceAccount { .. } => None,
            Error::InvalidOpaConfig { .. } => None,
            Error::AuthenticationClassRetrieval {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::AuthenticationProviderNotSupported {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::InvalidKafkaListeners { .. } => None,
            Error::InvalidContainerName { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::FailedToInitializeSecurityContext { .. } => None,
            Error::InvalidHeapConfig { .. } => None,
            Error::CreateClusterResources { .. } => None,
            Error::FailedToResolveConfig { .. } => None,
            Error::ResolveVectorAggregatorAddress { .. } => None,
            Error::InvalidLoggingConfig { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::BuildRbacResources { .. } => None,
        }
    }
}

pub async fn reconcile_kafka(kafka: Arc<KafkaCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;

    let resolved_product_image = kafka.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        KAFKA_CONTROLLER_NAME,
        &kafka.object_ref(&()),
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            kafka.as_ref(),
            [(
                KafkaRole::Broker.to_string(),
                (
                    vec![
                        PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()),
                        PropertyNameKind::Env,
                    ],
                    kafka.spec.brokers.clone().context(NoBrokerRoleSnafu)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;
    let role_broker_config = validated_config
        .get(&KafkaRole::Broker.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let kafka_security = KafkaTlsSecurity::new_from_kafka_cluster(client, &kafka)
        .await
        .context(FailedToInitializeSecurityContextSnafu)?;

    // Assemble the OPA connection string from the discovery and the given path if provided
    // Will be passed as --override parameter in the cli in the state ful set
    let opa_connect = if let Some(opa_spec) = &kafka.spec.cluster_config.authorization.opa {
        Some(
            opa_spec
                .full_document_url_from_config_map(
                    client,
                    &*kafka,
                    Some("allow"),
                    OpaApiVersion::V1,
                )
                .await
                .context(InvalidOpaConfigSnafu)?,
        )
    } else {
        None
    };

    let broker_role_service =
        build_broker_role_service(&kafka, &resolved_product_image, &kafka_security)?;

    let broker_role_service = cluster_resources
        .add(client, broker_role_service)
        .await
        .context(ApplyRoleServiceSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(&kafka, client)
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        kafka.as_ref(),
        APP_NAME,
        cluster_resources.get_required_labels(),
    )
    .context(BuildRbacResourcesSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    for (rolegroup_name, rolegroup_config) in role_broker_config.iter() {
        let rolegroup_ref = kafka.broker_rolegroup_ref(rolegroup_name);

        let merged_config = kafka
            .merged_config(&KafkaRole::Broker, rolegroup_name)
            .context(FailedToResolveConfigSnafu)?;

        let rg_service = build_broker_rolegroup_service(
            &kafka,
            &resolved_product_image,
            &kafka_security,
            &rolegroup_ref,
        )?;
        let rg_configmap = build_broker_rolegroup_config_map(
            &kafka,
            &resolved_product_image,
            &kafka_security,
            &rolegroup_ref,
            rolegroup_config,
            &merged_config,
            vector_aggregator_address.as_deref(),
        )?;
        let rg_statefulset = build_broker_rolegroup_statefulset(
            &kafka,
            &resolved_product_image,
            &rolegroup_ref,
            rolegroup_config,
            opa_connect.as_deref(),
            &kafka_security,
            &merged_config,
            &rbac_sa.name_any(),
        )?;
        cluster_resources
            .add(client, rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;
        cluster_resources
            .add(client, rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;

        ss_cond_builder.add(
            cluster_resources
                .add(client, rg_statefulset)
                .await
                .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup_ref.clone(),
                })?,
        );
    }

    for discovery_cm in build_discovery_configmaps(
        &kafka,
        &*kafka,
        &resolved_product_image,
        client,
        &kafka_security,
        &broker_role_service,
    )
    .await
    .context(BuildDiscoveryConfigSnafu)?
    {
        cluster_resources
            .add(client, discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&kafka.spec.cluster_operation);

    let status = KafkaClusterStatus {
        conditions: compute_conditions(
            kafka.as_ref(),
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    client
        .apply_patch_status(OPERATOR_NAME, &*kafka, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// The broker-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_broker_role_service(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
) -> Result<Service> {
    let role_name = KafkaRole::Broker.to_string();
    let role_svc_name = kafka
        .broker_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&role_svc_name)
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports(kafka_security)),
            selector: Some(role_selector_labels(kafka, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_broker_rolegroup_config_map(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<KafkaCluster>,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &KafkaConfig,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap> {
    let mut server_cfg = broker_config
        .get(&PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()))
        .cloned()
        .unwrap_or_default();

    server_cfg.extend(kafka_security.config_settings());

    let server_cfg = server_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(kafka)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(kafka, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    kafka,
                    KAFKA_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    "global",
                ))
                .build(),
        )
        .add_data(
            SERVER_PROPERTIES_FILE,
            to_java_properties_string(server_cfg.iter().map(|(k, v)| (k, v))).with_context(
                |_| SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?,
        );

    extend_role_group_config_map(
        rolegroup,
        vector_aggregator_address,
        &merged_config.logging,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup.object_name(),
    })?;

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_broker_rolegroup_service(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<KafkaCluster>,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports(kafka_security)),
            selector: Some(role_group_selector_labels(
                kafka,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_broker_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_broker_rolegroup_statefulset(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    merged_config: &KafkaConfig,
    sa_name: &str,
) -> Result<StatefulSet> {
    let prepare_container_name = Container::Prepare.to_string();
    let mut cb_prepare =
        ContainerBuilder::new(&prepare_container_name).context(InvalidContainerNameSnafu {
            name: prepare_container_name.clone(),
        })?;

    let get_svc_container_name = Container::GetService.to_string();
    let mut cb_get_svc =
        ContainerBuilder::new(&get_svc_container_name).context(InvalidContainerNameSnafu {
            name: get_svc_container_name.clone(),
        })?;

    let kcat_prober_container_name = Container::KcatProber.to_string();
    let mut cb_kcat_prober =
        ContainerBuilder::new(&kcat_prober_container_name).context(InvalidContainerNameSnafu {
            name: kcat_prober_container_name.clone(),
        })?;

    let kafka_container_name = Container::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let rolegroup = kafka
        .spec
        .brokers
        .as_ref()
        .context(NoBrokerRoleSnafu)?
        .role_groups
        .get(&rolegroup_ref.role_group)
        .with_context(|| RoleGroupNotFoundSnafu {
            rolegroup: rolegroup_ref.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    // Add TLS related volumes and volume mounts
    kafka_security.add_volume_and_volume_mounts(
        &mut pod_builder,
        &mut cb_prepare,
        &mut cb_kcat_prober,
        &mut cb_kafka,
    );

    cb_get_svc
        .image_from_product_image(resolved_product_image)
        .command(vec!["bash".to_string()])
        .args(vec![
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
            kafka_security.svc_container_commands(),
        ])
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .resources(merged_config.resources.clone().into());

    let mut prepare_container_args = vec![];

    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = merged_config.logging.containers.get(&Container::Prepare)
    {
        prepare_container_args.push(product_logging::framework::capture_shell_output(
            STACKABLE_LOG_DIR,
            &prepare_container_name,
            log_config,
        ));
    }

    prepare_container_args.extend(kafka_security.prepare_container_command_args());

    cb_prepare
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![prepare_container_args.join(" && ")])
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .resources(merged_config.resources.clone().into());

    let pvcs = merged_config.resources.storage.build_pvcs();

    let mut env = broker_config
        .get(&PropertyNameKind::Env)
        .into_iter()
        .flatten()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    if let Some(memory_limit) = merged_config.resources.memory.limit.as_ref() {
        let heap_size = MemoryQuantity::try_from(memory_limit)
            .context(InvalidHeapConfigSnafu)?
            .scale_to(BinaryMultiple::Mebi)
            * JAVA_HEAP_RATIO;

        env.push(EnvVar {
            name: KAFKA_HEAP_OPTS.to_string(),
            value: Some(format!(
                "-Xmx{heap}",
                heap = heap_size
                    .format_for_java()
                    .context(InvalidHeapConfigSnafu)?
            )),
            ..EnvVar::default()
        });
    }

    env.push(EnvVar {
        name: "ZOOKEEPER".to_string(),
        value_from: Some(EnvVarSource {
            config_map_key_ref: Some(ConfigMapKeySelector {
                name: Some(kafka.spec.cluster_config.zookeeper_config_map_name.clone()),
                key: "ZOOKEEPER".to_string(),
                ..ConfigMapKeySelector::default()
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "NODE".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "status.hostIP".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });
    env.push(EnvVar {
        name: "POD_NAME".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "metadata.name".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });

    let jvm_args = format!(
        "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={}:/stackable/jmx/broker.yaml",
        METRICS_PORT
    );
    let kafka_listeners =
        get_kafka_listener_config(kafka, kafka_security, &rolegroup_ref.object_name())
            .context(InvalidKafkaListenersSnafu)?;

    cb_kafka
        .image_from_product_image(resolved_product_image)
        .command(vec!["sh".to_string(), "-c".to_string()])
        .args(vec![kafka_security
            .kafka_container_commands(&kafka_listeners, opa_connect_string)
            .join(" ")])
        .add_env_var("EXTRA_ARGS", jvm_args)
        .add_env_var(
            "KAFKA_LOG4J_OPTS",
            format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}"),
        )
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .resources(merged_config.resources.clone().into());

    // Use kcat sidecar for probing container status rather than the official Kafka tools, since they incur a lot of
    // unacceptable perf overhead
    cb_kcat_prober
        .image_from_product_image(resolved_product_image)
        .command(vec!["sleep".to_string(), "infinity".to_string()])
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        )
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                // If the broker is able to get its fellow cluster members then it has at least completed basic registration at some point
                command: Some(kafka_security.kcat_prober_container_commands()),
            }),
            timeout_seconds: Some(5),
            period_seconds: Some(2),
            ..Probe::default()
        });

    if let Some(ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    }) = merged_config.logging.containers.get(&Container::Kafka)
    {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(config_map.into()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    } else {
        pod_builder.add_volume(Volume {
            name: "log-config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        });
    }

    pod_builder
        .metadata_builder(|m| {
            m.with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .with_label(pod_svc_controller::LABEL_ENABLE, "true")
        })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(cb_prepare.build())
        .add_init_container(cb_get_svc.build())
        .add_container(cb_kafka.build())
        .add_container(cb_kcat_prober.build())
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(rolegroup_ref.object_name()),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .add_volume(Volume {
            name: "tmp".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Volume::default()
        })
        .add_volume(Volume {
            name: "log".to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity(format!("{LOG_VOLUME_SIZE_IN_MIB}Mi"))),
            }),
            ..Volume::default()
        })
        .service_account_name(sa_name)
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(KAFKA_UID)
                .run_as_group(0)
                .fs_group(1000)
                .build(),
        );

    // Add vector container after kafka container to keep the defaulting into kafka container
    if merged_config.logging.enable_vector_agent {
        pod_builder.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            "config",
            "log",
            merged_config.logging.containers.get(&Container::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let mut pod_template = pod_builder.build_template();
    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: rolegroup.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    kafka,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pod_template,
            volume_claim_templates: Some(pvcs),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_obj: Arc<KafkaCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn service_ports(kafka_security: &KafkaTlsSecurity) -> Vec<ServicePort> {
    vec![
        ServicePort {
            name: Some(METRICS_PORT_NAME.to_string()),
            port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
        ServicePort {
            name: Some(kafka_security.client_port_name().to_string()),
            port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
    ]
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn container_ports(kafka_security: &KafkaTlsSecurity) -> Vec<ContainerPort> {
    vec![
        ContainerPort {
            name: Some(METRICS_PORT_NAME.to_string()),
            container_port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
        ContainerPort {
            name: Some(kafka_security.client_port_name().to_string()),
            container_port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
    ]
}

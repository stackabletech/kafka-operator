//! Ensures that `Pod`s are configured and running for each [`KafkaCluster`]
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use product_config::{
    types::PropertyNameKind,
    writer::{to_java_properties_string, PropertiesWriterError},
    ProductConfigManager,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{
    listener::get_kafka_listener_config, security::KafkaTlsSecurity, Container, KafkaCluster,
    KafkaClusterStatus, KafkaConfig, KafkaRole, APP_NAME, DOCKER_IMAGE_BASE_NAME,
    JVM_SECURITY_PROPERTIES_FILE, KAFKA_HEAP_OPTS, LOG_DIRS_VOLUME_NAME, METRICS_PORT,
    METRICS_PORT_NAME, OPERATOR_NAME, SERVER_PROPERTIES_FILE, STACKABLE_CONFIG_DIR,
    STACKABLE_DATA_DIR, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, STACKABLE_TMP_DIR,
};
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder, PodBuilder,
        },
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
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        api::DynamicObject,
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, Labels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    discovery::{self, build_discovery_configmaps},
    operations::{
        graceful_shutdown::{add_graceful_shutdown_config, graceful_shutdown_config_properties},
        pdb::add_pdbs,
    },
    pod_svc_controller,
    product_logging::{
        extend_role_group_config_map, resolve_vector_aggregator_address, LOG4J_CONFIG_FILE,
        MAX_KAFKA_LOG_FILES_SIZE,
    },
    utils::build_recommended_labels,
};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
/// Used as runAsUser in the pod security context. This is specified in the kafka image file
pub const KAFKA_UID: i64 = 1000;
const JAVA_HEAP_RATIO: f32 = 0.8;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
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

    #[snafu(display("failed to apply role Service"))]
    ApplyRoleService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyRoleServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: PropertiesWriterError,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to find rolegroup {}", rolegroup))]
    RoleGroupNotFound {
        rolegroup: RoleGroupRef<KafkaCluster>,
    },

    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("failed to retrieve {}", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::commons::opa::Error,
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
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to initialize security context"))]
    FailedToInitializeSecurityContext {
        source: stackable_kafka_crd::security::Error,
    },

    #[snafu(display("invalid memory resource configuration"))]
    InvalidHeapConfig {
        source: stackable_operator::memory::Error,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
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
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("internal operator failure"))]
    InternalOperatorError { source: stackable_kafka_crd::Error },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    JvmSecurityPoperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to add Secret Volumes and VolumeMounts"))]
    AddVolumesAndVolumeMounts {
        source: stackable_kafka_crd::security::Error,
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
            Error::InternalOperatorError { .. } => None,
            Error::JvmSecurityPoperties { .. } => None,
            Error::FailedToCreatePdb { .. } => None,
            Error::GracefulShutdown { .. } => None,
            Error::GetRequiredLabels { .. } => None,
            Error::MetadataBuild { .. } => None,
            Error::LabelBuild { .. } => None,
            Error::AddVolumesAndVolumeMounts { .. } => None,
        }
    }
}

pub async fn reconcile_kafka(kafka: Arc<KafkaCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;
    let kafka_role = KafkaRole::Broker;

    let resolved_product_image = kafka
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

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
                        PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
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
        build_bootstrap_service(&kafka, &resolved_product_image, &kafka_security)?;

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
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
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
            .merged_config(&KafkaRole::Broker, &rolegroup_ref)
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
            &kafka_role,
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

    let role_config = kafka.role_config(&kafka_role);
    if let Some(GenericRoleConfig {
        pod_disruption_budget: pdb,
    }) = role_config
    {
        add_pdbs(pdb, &kafka, &kafka_role, client, &mut cluster_resources)
            .await
            .context(FailedToCreatePdbSnafu)?;
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

/// Kafka clients will use the load-balanced bootstrap service to get a list of broker addresses and will use those to
/// transmit data to the correct broker.
pub fn build_bootstrap_service(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
) -> Result<Service> {
    let role_name = KafkaRole::Broker.to_string();
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(kafka.bootstrap_service_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &role_name,
                "global",
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports(kafka_security)),
            selector: Some(
                Labels::role_selector(kafka, APP_NAME, &role_name)
                    .context(LabelBuildSnafu)?
                    .into(),
            ),
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
    server_cfg.extend(graceful_shutdown_config_properties());

    let server_cfg = server_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();

    let jvm_sec_props: BTreeMap<String, Option<String>> = broker_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

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
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(
            SERVER_PROPERTIES_FILE,
            to_java_properties_string(server_cfg.iter().map(|(k, v)| (k, v))).with_context(
                |_| SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?,
        )
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPopertiesSnafu {
                    rolegroup: rolegroup.role_group.clone(),
                }
            })?,
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
            .name(rolegroup.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports(kafka_security)),
            selector: Some(
                Labels::role_group_selector(
                    kafka,
                    APP_NAME,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .context(LabelBuildSnafu)?
                .into(),
            ),
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
    role: &KafkaRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    merged_config: &KafkaConfig,
    sa_name: &str,
) -> Result<StatefulSet> {
    let role = kafka.role(role).context(InternalOperatorSnafu)?;
    let rolegroup = kafka
        .rolegroup(rolegroup_ref)
        .context(InternalOperatorSnafu)?;

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

    let mut pod_builder = PodBuilder::new();

    // Add TLS related volumes and volume mounts
    kafka_security
        .add_volume_and_volume_mounts(&mut pod_builder, &mut cb_kcat_prober, &mut cb_kafka)
        .context(AddVolumesAndVolumeMountsSnafu)?;

    cb_get_svc
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![kafka_security.svc_container_commands()])
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
        "-Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE} -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/broker.yaml",
    );
    let kafka_listeners =
        get_kafka_listener_config(kafka, kafka_security, &rolegroup_ref.object_name())
            .context(InvalidKafkaListenersSnafu)?;

    cb_kafka
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![kafka_security
            .kafka_container_commands(&kafka_listeners, opa_connect_string)
            .join("\n")])
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

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(build_recommended_labels(
            kafka,
            KAFKA_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(MetadataBuildSnafu)?
        .with_label(
            Label::try_from((pod_svc_controller::LABEL_ENABLE, "true")).context(LabelBuildSnafu)?,
        )
        .build();

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
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
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
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

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);

    pod_template.merge_from(role.config.pod_overrides.clone());
    pod_template.merge_from(rolegroup.config.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: rolegroup.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        kafka,
                        APP_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
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
    Action::requeue(*Duration::from_secs(5))
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

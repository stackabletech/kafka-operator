//! Ensures that `Pod`s are configured and running for each [`v1alpha1::KafkaCluster`].

use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use product_config::{
    ProductConfigManager,
    types::PropertyNameKind,
    writer::{PropertiesWriterError, to_java_properties_string},
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference, VolumeBuilder},
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        opa::OpaApiVersion,
        product_image_selection::{self, ResolvedProductImage},
        rbac::build_rbac_resources,
    },
    crd::listener,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort, EnvVar,
                EnvVarSource, ExecAction, ObjectFieldSelector, PodSpec, Probe, Service,
                ServiceAccount, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        Resource, ResourceExt,
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::{Label, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{
        ValidatedRoleConfigByPropertyKind, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::{
        self, APP_NAME, DOCKER_IMAGE_BASE_NAME, JVM_SECURITY_PROPERTIES_FILE, KAFKA_HEAP_OPTS,
        KafkaClusterStatus, LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME,
        LOG_DIRS_VOLUME_NAME, METRICS_PORT, METRICS_PORT_NAME, OPERATOR_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_DATA_DIR, STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR,
        STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
        listener::get_kafka_listener_config,
        role::{
            AnyConfig, KafkaRole,
            broker::{BROKER_PROPERTIES_FILE, BrokerConfig, BrokerContainer},
            controller::CONTROLLER_PROPERTIES_FILE,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    discovery::{self, build_discovery_configmap},
    kerberos::{self, add_kerberos_pod_config},
    operations::{
        graceful_shutdown::{add_graceful_shutdown_config, graceful_shutdown_config_properties},
        pdb::add_pdbs,
    },
    product_logging::{LOG4J_CONFIG_FILE, MAX_KAFKA_LOG_FILES_SIZE, extend_role_group_config_map},
    utils::build_recommended_labels,
};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
pub const KAFKA_FULL_CONTROLLER_NAME: &str = concatcp!(KAFKA_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("cluster object defines no '{role}' role"))]
    MissingKafkaRole {
        source: crate::crd::Error,
        role: KafkaRole,
    },

    #[snafu(display("failed to apply role Service"))]
    ApplyRoleService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to serialize config for {}", rolegroup))]
    SerializeConfig {
        source: PropertiesWriterError,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
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

    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: crate::crd::listener::KafkaListenerError,
    },

    #[snafu(display("failed to add listener volume"))]
    AddListenerVolume {
        source: stackable_operator::builder::pod::Error,
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
    FailedToInitializeSecurityContext { source: crate::crd::security::Error },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::role::Error },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

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
    AddVolumesAndVolumeMounts { source: crate::crd::security::Error },

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: kerberos::Error },

    #[snafu(display("failed to validate authentication method"))]
    FailedToValidateAuthenticationMethod { source: crate::crd::security::Error },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("KafkaCluster object is invalid"))]
    InvalidKafkaCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("KafkaCluster object is misconfigured"))]
    MisconfiguredKafkaCluster { source: crd::Error },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::crd::role::Error },

    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to parse role: {source}"))]
    ParseRole { source: strum::ParseError },

    #[snafu(display("failed to merge pod overrides"))]
    MergePodOverrides { source: crd::role::Error },

    #[snafu(display("failed to retrieve rolegroup replicas"))]
    RoleGroupReplicas { source: crd::role::Error },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::MissingSecretLifetime => None,
            Error::MissingKafkaRole { .. } => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::GenerateProductConfig { .. } => None,
            Error::InvalidProductConfig { .. } => None,
            Error::SerializeConfig { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::InvalidOpaConfig { .. } => None,
            Error::InvalidKafkaListeners { .. } => None,
            Error::AddListenerVolume { .. } => None,
            Error::InvalidContainerName { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::FailedToInitializeSecurityContext { .. } => None,
            Error::CreateClusterResources { .. } => None,
            Error::FailedToResolveConfig { .. } => None,
            Error::VectorAggregatorConfigMapMissing => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::JvmSecurityPoperties { .. } => None,
            Error::FailedToCreatePdb { .. } => None,
            Error::GracefulShutdown { .. } => None,
            Error::GetRequiredLabels { .. } => None,
            Error::MetadataBuild { .. } => None,
            Error::LabelBuild { .. } => None,
            Error::AddVolumesAndVolumeMounts { .. } => None,
            Error::ConfigureLogging { .. } => None,
            Error::AddVolume { .. } => None,
            Error::AddVolumeMount { .. } => None,
            Error::AddKerberosConfig { .. } => None,
            Error::FailedToValidateAuthenticationMethod { .. } => None,
            Error::InvalidKafkaCluster { .. } => None,
            Error::MisconfiguredKafkaCluster { .. } => None,
            Error::ConstructJvmArguments { .. } => None,
            Error::ResolveProductImage { .. } => None,
            Error::ParseRole { .. } => None,
            Error::MergePodOverrides { .. } => None,
            Error::RoleGroupReplicas { .. } => None,
        }
    }
}

pub async fn reconcile_kafka(
    kafka: Arc<DeserializeGuard<v1alpha1::KafkaCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let kafka = kafka
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidKafkaClusterSnafu)?;

    let client = &ctx.client;

    let resolved_product_image = kafka
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION)
        .context(ResolveProductImageSnafu)?;

    // check Kraft vs ZooKeeper and fail if misconfigured
    kafka
        .check_kraft_vs_zookeeper(&resolved_product_image.product_version)
        .context(MisconfiguredKafkaClusterSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        KAFKA_CONTROLLER_NAME,
        &kafka.object_ref(&()),
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let validated_config = validated_product_config(
        kafka,
        &resolved_product_image.product_version,
        &ctx.product_config,
    )?;

    let kafka_security = KafkaTlsSecurity::new_from_kafka_cluster(client, kafka)
        .await
        .context(FailedToInitializeSecurityContextSnafu)?;

    tracing::debug!(
        kerberos_enabled = kafka_security.has_kerberos_enabled(),
        kerberos_secret_class = ?kafka_security.kerberos_secret_class(),
        tls_enabled = kafka_security.tls_enabled(),
        tls_client_authentication_class = ?kafka_security.tls_client_authentication_class(),
        "The following security settings are used"
    );

    kafka_security
        .validate_authentication_methods()
        .context(FailedToValidateAuthenticationMethodSnafu)?;

    // Assemble the OPA connection string from the discovery and the given path if provided
    // Will be passed as --override parameter in the cli in the state ful set
    let opa_connect = if let Some(opa_spec) = &kafka.spec.cluster_config.authorization.opa {
        Some(
            opa_spec
                .full_document_url_from_config_map(client, kafka, Some("allow"), OpaApiVersion::V1)
                .await
                .context(InvalidOpaConfigSnafu)?,
        )
    } else {
        None
    };

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        kafka,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(GetRequiredLabelsSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut bootstrap_listeners = Vec::<listener::v1alpha1::Listener>::new();

    for (kafka_role_str, role_config) in &validated_config {
        let kafka_role = KafkaRole::from_str(kafka_role_str).context(ParseRoleSnafu)?;

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup_ref = kafka.rolegroup_ref(&kafka_role, rolegroup_name);

            let merged_config = kafka_role
                .merged_config(kafka, &rolegroup_ref.role_group)
                .context(FailedToResolveConfigSnafu)?;

            let rg_service =
                build_broker_rolegroup_service(kafka, &resolved_product_image, &rolegroup_ref)?;
            let rg_configmap = build_rolegroup_config_map(
                kafka,
                &resolved_product_image,
                &kafka_security,
                &rolegroup_ref,
                rolegroup_config,
                &merged_config,
            )?;
            let rg_statefulset = build_broker_rolegroup_statefulset(
                kafka,
                &kafka_role,
                &resolved_product_image,
                &rolegroup_ref,
                rolegroup_config,
                opa_connect.as_deref(),
                &kafka_security,
                &merged_config,
                &rbac_sa,
                &client.kubernetes_cluster_info,
            )?;

            // TODO: broker / controller?
            if let AnyConfig::Broker(broker_config) = merged_config {
                let rg_bootstrap_listener = build_broker_rolegroup_bootstrap_listener(
                    kafka,
                    &resolved_product_image,
                    &kafka_security,
                    &rolegroup_ref,
                    &broker_config,
                )?;
                bootstrap_listeners.push(
                    cluster_resources
                        .add(client, rg_bootstrap_listener)
                        .await
                        .context(ApplyRoleServiceSnafu)?,
                );
            }

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
            add_pdbs(pdb, kafka, &kafka_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    let discovery_cm = build_discovery_configmap(
        kafka,
        kafka,
        &resolved_product_image,
        &kafka_security,
        &bootstrap_listeners,
    )
    .context(BuildDiscoveryConfigSnafu)?;

    cluster_resources
        .add(client, discovery_cm)
        .await
        .context(ApplyDiscoveryConfigSnafu)?;

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&kafka.spec.cluster_operation);

    let status = KafkaClusterStatus {
        conditions: compute_conditions(kafka, &[&ss_cond_builder, &cluster_operation_cond_builder]),
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    client
        .apply_patch_status(OPERATOR_NAME, kafka, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// Kafka clients will use the load-balanced bootstrap listener to get a list of broker addresses and will use those to
/// transmit data to the correct broker.
// TODO (@NickLarsenNZ): Move shared functionality to stackable-operator
pub fn build_broker_rolegroup_bootstrap_listener(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    merged_config: &BrokerConfig,
) -> Result<listener::v1alpha1::Listener> {
    Ok(listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(kafka.bootstrap_service_name(rolegroup))
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(merged_config.bootstrap_listener_class.clone()),
            ports: Some(listener_ports(kafka_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &AnyConfig,
) -> Result<ConfigMap> {
    let kafka_config_file_name = merged_config.config_file_name();

    let mut kafka_config = rolegroup_config
        .get(&PropertyNameKind::File(kafka_config_file_name.to_string()))
        .cloned()
        .unwrap_or_default();

    kafka_config.extend(kafka_security.config_settings());
    kafka_config.extend(graceful_shutdown_config_properties());

    let kafka_config = kafka_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();

    let jvm_sec_props: BTreeMap<String, Option<String>> = rolegroup_config
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
                    &resolved_product_image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(
            kafka_config_file_name,
            to_java_properties_string(kafka_config.iter().map(|(k, v)| (k, v))).with_context(
                |_| SerializeConfigSnafu {
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

    tracing::debug!(?kafka_config, "Applied kafka config");
    tracing::debug!(?jvm_sec_props, "Applied JVM config");

    extend_role_group_config_map(rolegroup, merged_config, &mut cm_builder);

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
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
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
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
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
    kafka: &v1alpha1::KafkaCluster,
    kafka_role: &KafkaRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::KafkaCluster>,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    merged_config: &AnyConfig,
    service_account: &ServiceAccount,
    cluster_info: &KubernetesClusterInfo,
) -> Result<StatefulSet> {
    let recommended_object_labels = build_recommended_labels(
        kafka,
        KAFKA_CONTROLLER_NAME,
        &resolved_product_image.app_version_label_value,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    let recommended_labels =
        Labels::recommended(recommended_object_labels.clone()).context(LabelBuildSnafu)?;
    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels = Labels::recommended(build_recommended_labels(
        kafka,
        KAFKA_CONTROLLER_NAME,
        // A version value is required, and we do want to use the "recommended" format for the other desired labels
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .context(LabelBuildSnafu)?;

    let kcat_prober_container_name = BrokerContainer::KcatProber.to_string();
    let mut cb_kcat_prober =
        ContainerBuilder::new(&kcat_prober_container_name).context(InvalidContainerNameSnafu {
            name: kcat_prober_container_name.clone(),
        })?;

    let kafka_container_name = BrokerContainer::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .deref()
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    kafka_security
        .add_volume_and_volume_mounts(
            &mut pod_builder,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &requested_secret_lifetime,
        )
        .context(AddVolumesAndVolumeMountsSnafu)?;

    let mut pvcs = merged_config.resources().storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    pvcs.push(
        ListenerOperatorVolumeSourceBuilder::new(
            &ListenerReference::ListenerName(kafka.bootstrap_service_name(rolegroup_ref)),
            &unversioned_recommended_labels,
        )
        .build_pvc(LISTENER_BOOTSTRAP_VOLUME_NAME)
        // FIXME (@Techassi): This should either be an expect (if it can never fail) or should be
        // handled via a proper error handling
        .unwrap(),
    );

    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }

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

    if let Some(zookeeper_config_map_name) = &kafka.spec.cluster_config.zookeeper_config_map_name {
        env.push(EnvVar {
            name: "ZOOKEEPER".to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: zookeeper_config_map_name.to_string(),
                    key: "ZOOKEEPER".to_string(),
                    ..ConfigMapKeySelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        })
    };

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

    let kafka_listeners = get_kafka_listener_config(
        kafka,
        kafka_security,
        &rolegroup_ref.object_name(),
        cluster_info,
    )
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
        .args(vec![
            kafka_security
                .kafka_container_commands(
                    &kafka_listeners,
                    opa_connect_string,
                    kafka_security.has_kerberos_enabled(),
                )
                .join("\n"),
        ])
        .add_env_var(
            "EXTRA_ARGS",
            kafka_role
                .construct_non_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            KAFKA_HEAP_OPTS,
            kafka_role
                .construct_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            "KAFKA_LOG4J_OPTS",
            format!("-Dlog4j.configuration=file:{STACKABLE_LOG_CONFIG_DIR}/{LOG4J_CONFIG_FILE}"),
        )
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(
            LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(merged_config.resources().clone().into());

    // Use kcat sidecar for probing container status rather than the official Kafka tools, since they incur a lot of
    // unacceptable perf overhead
    cb_kcat_prober
        .image_from_product_image(resolved_product_image)
        .command(vec!["sleep".to_string(), "infinity".to_string()])
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
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        )
        .add_volume_mount(
            LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .context(AddVolumeMountSnafu)?
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

    if let ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    } = &*merged_config.kafka_logging()
    {
        pod_builder
            .add_volume(
                VolumeBuilder::new("log-config")
                    .with_config_map(config_map)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(
                VolumeBuilder::new("log-config")
                    .with_config_map(rolegroup_ref.object_name())
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels)
        .context(MetadataBuildSnafu)?
        .build();

    if let Some(listener_class) = merged_config.listener_class() {
        pod_builder
            .add_listener_volume_by_listener_class(
                LISTENER_BROKER_VOLUME_NAME,
                listener_class,
                &recommended_labels,
            )
            .context(AddListenerVolumeSnafu)?;
    }
    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .add_container(cb_kcat_prober.build())
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        // bootstrap volume is a persistent volume template instead, to keep addresses persistent
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    // Add vector container after kafka container to keep the defaulting into kafka container
    if merged_config.vector_logging_enabled() {
        match &kafka.spec.cluster_config.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pod_builder.add_container(
                    product_logging::framework::vector_container(
                        resolved_product_image,
                        "config",
                        "log",
                        Some(&*merged_config.vector_logging()),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);

    pod_template.merge_from(
        kafka_role
            .role_pod_overrides(kafka)
            .context(MergePodOverridesSnafu)?,
    );
    pod_template.merge_from(
        kafka_role
            .role_group_pod_overrides(kafka, &rolegroup_ref.role_group)
            .context(MergePodOverridesSnafu)?,
    );

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label_value,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: kafka_role
                .replicas(kafka, &rolegroup_ref.role_group)
                .context(RoleGroupReplicasSnafu)?
                .map(i32::from),
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
            service_name: Some(rolegroup_ref.object_name()),
            template: pod_template,
            volume_claim_templates: Some(pvcs),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::KafkaCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidKafkaCluster { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn listener_ports(kafka_security: &KafkaTlsSecurity) -> Vec<listener::v1alpha1::ListenerPort> {
    let mut ports = vec![
        listener::v1alpha1::ListenerPort {
            name: METRICS_PORT_NAME.to_string(),
            port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
        },
        listener::v1alpha1::ListenerPort {
            name: kafka_security.client_port_name().to_string(),
            port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
        },
    ];
    if kafka_security.has_kerberos_enabled() {
        ports.push(listener::v1alpha1::ListenerPort {
            name: kafka_security.bootstrap_port_name().to_string(),
            port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
        });
    }
    ports
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn container_ports(kafka_security: &KafkaTlsSecurity) -> Vec<ContainerPort> {
    let mut ports = vec![
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
    ];
    if kafka_security.has_kerberos_enabled() {
        ports.push(ContainerPort {
            name: Some(kafka_security.bootstrap_port_name().to_string()),
            container_port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        });
    }
    ports
}

/// Defines all required roles and their required configuration.
///
/// The roles and their configs are then validated and complemented by the product config.
///
/// # Arguments
/// * `resource`        - The TrinoCluster containing the role definitions.
/// * `version`         - The TrinoCluster version.
/// * `product_config`  - The product config to validate and complement the user config.
///
fn validated_product_config(
    kafka: &v1alpha1::KafkaCluster,
    product_version: &str,
    product_config: &ProductConfigManager,
) -> Result<ValidatedRoleConfigByPropertyKind, Error> {
    let mut roles = HashMap::new();

    roles.insert(
        KafkaRole::Broker.to_string(),
        (
            vec![
                PropertyNameKind::File(BROKER_PROPERTIES_FILE.to_string()),
                PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                PropertyNameKind::Env,
            ],
            kafka
                .broker_role()
                .cloned()
                .context(MissingKafkaRoleSnafu {
                    role: KafkaRole::Broker,
                })?
                .erase(),
        ),
    );

    if kafka.is_controller_configured() {
        roles.insert(
            KafkaRole::Controller.to_string(),
            (
                vec![
                    PropertyNameKind::File(CONTROLLER_PROPERTIES_FILE.to_string()),
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                    PropertyNameKind::Env,
                ],
                kafka
                    .controller_role()
                    .cloned()
                    .context(MissingKafkaRoleSnafu {
                        role: KafkaRole::Controller,
                    })?
                    .erase(),
            ),
        );
    }

    let role_config =
        transform_all_roles_to_config(kafka, roles).context(GenerateProductConfigSnafu)?;

    validate_all_roles_and_groups_config(
        product_version,
        &role_config,
        product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)
}

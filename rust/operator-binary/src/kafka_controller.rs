//! Ensures that `Pod`s are configured and running for each [`v1alpha1::KafkaCluster`].

use std::{collections::HashMap, str::FromStr, sync::Arc};

use const_format::concatcp;
use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        opa::OpaApiVersion,
        product_image_selection::{self},
        rbac::build_rbac_resources,
    },
    crd::listener,
    kube::{
        Resource,
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    product_config_utils::{
        ValidatedRoleConfigByPropertyKind, transform_all_roles_to_config,
        validate_all_roles_and_groups_config,
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::{
        self, APP_NAME, DOCKER_IMAGE_BASE_NAME, JVM_SECURITY_PROPERTIES_FILE, KafkaClusterStatus,
        OPERATOR_NAME,
        listener::get_kafka_listener_config,
        role::{
            AnyConfig, KafkaRole, broker::BROKER_PROPERTIES_FILE,
            controller::CONTROLLER_PROPERTIES_FILE,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    discovery::{self, build_discovery_configmap},
    operations::pdb::add_pdbs,
    resource::{
        configmap::build_rolegroup_config_map,
        listener::build_rolegroup_bootstrap_listener,
        service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
        statefulset::{build_broker_rolegroup_statefulset, build_controller_rolegroup_statefulset},
    },
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
    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: crate::crd::listener::KafkaListenerError,
    },

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

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("failed to validate authentication method"))]
    FailedToValidateAuthenticationMethod { source: crate::crd::security::Error },

    #[snafu(display("KafkaCluster object is invalid"))]
    InvalidKafkaCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("KafkaCluster object is misconfigured"))]
    MisconfiguredKafkaCluster { source: crd::Error },

    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to parse role: {source}"))]
    ParseRole { source: strum::ParseError },

    #[snafu(display("failed to build statefulset"))]
    BuildStatefulset {
        source: crate::resource::statefulset::Error,
    },

    #[snafu(display("failed to build configmap"))]
    BuildConfigMap {
        source: crate::resource::configmap::Error,
    },

    #[snafu(display("failed to build service"))]
    BuildService {
        source: crate::resource::service::Error,
    },

    #[snafu(display("failed to build listener"))]
    BuildListener {
        source: crate::resource::listener::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::MissingKafkaRole { .. } => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::GenerateProductConfig { .. } => None,
            Error::InvalidProductConfig { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::InvalidOpaConfig { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::FailedToInitializeSecurityContext { .. } => None,
            Error::CreateClusterResources { .. } => None,
            Error::FailedToResolveConfig { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::FailedToCreatePdb { .. } => None,
            Error::GetRequiredLabels { .. } => None,
            Error::FailedToValidateAuthenticationMethod { .. } => None,
            Error::InvalidKafkaCluster { .. } => None,
            Error::MisconfiguredKafkaCluster { .. } => None,
            Error::ResolveProductImage { .. } => None,
            Error::ParseRole { .. } => None,
            Error::BuildStatefulset { .. } => None,
            Error::BuildConfigMap { .. } => None,
            Error::BuildService { .. } => None,
            Error::BuildListener { .. } => None,
            Error::InvalidKafkaListeners { .. } => None,
            Error::BuildPodDescriptors { .. } => None,
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

            let rg_headless_service = build_rolegroup_headless_service(
                kafka,
                &resolved_product_image,
                &rolegroup_ref,
                &kafka_security,
            )
            .context(BuildServiceSnafu)?;

            let rg_metrics_service =
                build_rolegroup_metrics_service(kafka, &resolved_product_image, &rolegroup_ref)
                    .context(BuildServiceSnafu)?;

            let kafka_listeners = get_kafka_listener_config(
                kafka,
                &kafka_security,
                &rolegroup_ref,
                &client.kubernetes_cluster_info,
            )
            .context(InvalidKafkaListenersSnafu)?;

            let pod_descriptors = kafka
                .pod_descriptors(
                    None,
                    &client.kubernetes_cluster_info,
                    kafka_security.client_port(),
                )
                .context(BuildPodDescriptorsSnafu)?;

            let rg_configmap = build_rolegroup_config_map(
                kafka,
                &resolved_product_image,
                &kafka_security,
                &rolegroup_ref,
                rolegroup_config,
                &merged_config,
                &kafka_listeners,
                &pod_descriptors,
                opa_connect.as_deref(),
            )
            .context(BuildConfigMapSnafu)?;

            let rg_statefulset = match kafka_role {
                KafkaRole::Broker => build_broker_rolegroup_statefulset(
                    kafka,
                    &kafka_role,
                    &resolved_product_image,
                    &rolegroup_ref,
                    rolegroup_config,
                    &kafka_security,
                    &merged_config,
                    &rbac_sa,
                    &client.kubernetes_cluster_info,
                )
                .context(BuildStatefulsetSnafu)?,
                KafkaRole::Controller => build_controller_rolegroup_statefulset(
                    kafka,
                    &kafka_role,
                    &resolved_product_image,
                    &rolegroup_ref,
                    rolegroup_config,
                    &kafka_security,
                    &merged_config,
                    &rbac_sa,
                    &client.kubernetes_cluster_info,
                )
                .context(BuildStatefulsetSnafu)?,
            };

            let rg_bootstrap_listener = build_rolegroup_bootstrap_listener(
                kafka,
                &resolved_product_image,
                &kafka_security,
                &rolegroup_ref,
                &merged_config,
            )
            .context(BuildListenerSnafu)?;

            let rg_bootstrap_listener = cluster_resources
                .add(client, rg_bootstrap_listener)
                .await
                .context(ApplyRoleServiceSnafu)?;

            // TODO: for backwards compatibility we only add the broker bootstrap listener
            // to this list in order to not break the discovery configmap even more than it already is.
            if let AnyConfig::Broker(_) = &merged_config {
                bootstrap_listeners.push(rg_bootstrap_listener);
            }

            cluster_resources
                .add(client, rg_headless_service)
                .await
                .with_context(|_| ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup_ref.clone(),
                })?;
            cluster_resources
                .add(client, rg_metrics_service)
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

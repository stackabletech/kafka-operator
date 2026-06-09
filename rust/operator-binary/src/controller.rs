//! Ensures that `Pod`s are configured and running for each [`v1alpha1::KafkaCluster`].

use std::{collections::BTreeMap, sync::Arc};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    crd::listener,
    kube::{
        Resource,
        api::DynamicObject,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    role_utils::{GenericRoleConfig, RoleGroupRef},
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub(crate) mod build;
mod dereference;
mod validate;

use crate::{
    crd::{
        self, APP_NAME, KafkaClusterStatus, OPERATOR_NAME,
        authorization::KafkaAuthorizationConfig,
        listener::get_kafka_listener_config,
        role::{AnyConfig, KafkaRole},
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    discovery::{self, build_discovery_configmap},
    operations::pdb::add_pdbs,
    resource::{
        listener::build_broker_rolegroup_bootstrap_listener,
        service::{build_rolegroup_headless_service, build_rolegroup_metrics_service},
        statefulset::{build_broker_rolegroup_statefulset, build_controller_rolegroup_statefulset},
    },
};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
pub const KAFKA_FULL_CONTROLLER_NAME: &str = concatcp!(KAFKA_CONTROLLER_NAME, '.', OPERATOR_NAME);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub operator_environment: OperatorEnvironmentOptions,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: crate::crd::listener::KafkaListenerError,
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

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
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

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("KafkaCluster object is invalid"))]
    InvalidKafkaCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("KafkaCluster object is misconfigured"))]
    MisconfiguredKafkaCluster { source: crd::Error },

    #[snafu(display("failed to build statefulset"))]
    BuildStatefulset {
        source: crate::resource::statefulset::Error,
    },

    #[snafu(display("failed to build configmap"))]
    BuildConfigMap {
        source: crate::controller::build::config_map::Error,
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
            Error::Dereference { .. } => None,
            Error::ValidateCluster { .. } => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::CreateClusterResources { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::BuildRbacResources { .. } => None,
            Error::FailedToCreatePdb { .. } => None,
            Error::GetRequiredLabels { .. } => None,
            Error::InvalidKafkaCluster { .. } => None,
            Error::MisconfiguredKafkaCluster { .. } => None,
            Error::BuildStatefulset { .. } => None,
            Error::BuildConfigMap { .. } => None,
            Error::BuildService { .. } => None,
            Error::BuildListener { .. } => None,
            Error::InvalidKafkaListeners { .. } => None,
            Error::BuildPodDescriptors { .. } => None,
        }
    }
}

/// The validated cluster. Carries everything the build steps need, resolved once
/// here so downstream code never re-derives it or touches the raw spec.
pub struct ValidatedKafkaCluster {
    pub image: ResolvedProductImage,
    pub kafka_security: KafkaTlsSecurity,
    // DESIGN DECISION: the dereferenced authorization config is folded into the
    // validated cluster (read from here downstream). The other dereferenced input,
    // the authentication classes, is intentionally NOT stored: it is fully consumed
    // here to build `kafka_security`. Alternative: also store the resolved auth
    // classes — rejected because nothing downstream needs them beyond kafka_security.
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub role_groups: BTreeMap<KafkaRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
}

pub struct ValidatedRoleGroupConfig {
    pub merged_config: AnyConfig,
    // DESIGN DECISION: overrides are resolved into flat maps HERE rather than stored
    // as the typed KeyValueConfigOverrides and resolved in the per-file builders (the
    // hdfs-operator pattern). Reason: broker and controller use different override
    // struct types (KafkaBrokerConfigOverrides vs KafkaControllerConfigOverrides), so a
    // single typed field would require an enum. Resolving here keeps the build/properties
    // builders taking plain `BTreeMap<String,String>`. Alternative: an enum over the two
    // override types threaded to builders that call resolved_overrides() — more types for
    // no behavioural gain.
    pub config_file_overrides: BTreeMap<String, String>,
    pub jvm_security_overrides: BTreeMap<String, String>,
    pub env_overrides: BTreeMap<String, String>,
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

    // dereference (client required)
    let dereferenced_objects = dereference::dereference(client, kafka)
        .await
        .context(DereferenceSnafu)?;

    // validate (no client required)
    let validated_cluster =
        validate::validate(kafka, dereferenced_objects, &ctx.operator_environment)
            .context(ValidateClusterSnafu)?;

    let opa_connect = validated_cluster
        .authorization_config
        .as_ref()
        .map(|auth_config| auth_config.opa_connect.clone());

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        KAFKA_CONTROLLER_NAME,
        &kafka.object_ref(&()),
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
        &kafka.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    tracing::debug!(
        kerberos_enabled = validated_cluster.kafka_security.has_kerberos_enabled(),
        kerberos_secret_class = ?validated_cluster.kafka_security.kerberos_secret_class(),
        tls_enabled = validated_cluster.kafka_security.tls_enabled(),
        tls_client_authentication_class = ?validated_cluster.kafka_security.tls_client_authentication_class(),
        "The following security settings are used"
    );

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

    for (kafka_role, rg_map) in &validated_cluster.role_groups {
        for (rolegroup_name, validated_rg) in rg_map {
            let rolegroup_ref = kafka.rolegroup_ref(kafka_role, rolegroup_name);

            let rg_headless_service = build_rolegroup_headless_service(
                kafka,
                &validated_cluster.image,
                &rolegroup_ref,
                &validated_cluster.kafka_security,
            )
            .context(BuildServiceSnafu)?;

            let rg_metrics_service =
                build_rolegroup_metrics_service(kafka, &validated_cluster.image, &rolegroup_ref)
                    .context(BuildServiceSnafu)?;

            let kafka_listeners = get_kafka_listener_config(
                kafka,
                &validated_cluster.kafka_security,
                &rolegroup_ref,
                &client.kubernetes_cluster_info,
            )
            .context(InvalidKafkaListenersSnafu)?;

            let pod_descriptors = kafka
                .pod_descriptors(
                    None,
                    &client.kubernetes_cluster_info,
                    validated_cluster.kafka_security.client_port(),
                )
                .context(BuildPodDescriptorsSnafu)?;

            let rg_configmap = build::config_map::build_rolegroup_config_map(
                kafka,
                &validated_cluster,
                &rolegroup_ref,
                validated_rg,
                &kafka_listeners,
                &pod_descriptors,
                opa_connect.as_deref(),
            )
            .context(BuildConfigMapSnafu)?;

            let rg_statefulset = match kafka_role {
                KafkaRole::Broker => build_broker_rolegroup_statefulset(
                    kafka,
                    kafka_role,
                    &validated_cluster,
                    &rolegroup_ref,
                    validated_rg,
                    &rbac_sa,
                    &client.kubernetes_cluster_info,
                )
                .context(BuildStatefulsetSnafu)?,
                KafkaRole::Controller => build_controller_rolegroup_statefulset(
                    kafka,
                    kafka_role,
                    &validated_cluster,
                    &rolegroup_ref,
                    validated_rg,
                    &rbac_sa,
                    &client.kubernetes_cluster_info,
                )
                .context(BuildStatefulsetSnafu)?,
            };

            if let AnyConfig::Broker(broker_config) = &validated_rg.merged_config {
                let rg_bootstrap_listener = build_broker_rolegroup_bootstrap_listener(
                    kafka,
                    &validated_cluster,
                    &rolegroup_ref,
                    broker_config,
                )
                .context(BuildListenerSnafu)?;
                bootstrap_listeners.push(
                    cluster_resources
                        .add(client, rg_bootstrap_listener)
                        .await
                        .context(ApplyRoleServiceSnafu)?,
                );
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

            // Note: The StatefulSet needs to be applied after all ConfigMaps and Secrets it mounts
            // to prevent unnecessary Pod restarts.
            // See https://github.com/stackabletech/commons-operator/issues/111 for details.
            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup_ref.clone(),
                    })?,
            );
        }

        let role_cfg = kafka.role_config(kafka_role);
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = role_cfg
        {
            add_pdbs(pdb, kafka, kafka_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    let discovery_cm = build_discovery_configmap(kafka, validated_cluster, &bootstrap_listeners)
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

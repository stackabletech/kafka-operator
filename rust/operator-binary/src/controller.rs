//! Ensures that `Pod`s are configured and running for each [`v1alpha1::KafkaCluster`].

use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    crd::listener,
    kube::{
        Resource,
        api::{DynamicObject, ObjectMeta},
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
    v2::types::{
        kubernetes::{NamespaceName, Uid},
        operator::ClusterName,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub(crate) mod build;
mod dereference;
mod validate;

use crate::{
    crd::{
        self, APP_NAME, KafkaClusterStatus, KafkaPodDescriptor, MetadataManager, OPERATOR_NAME,
        authorization::KafkaAuthorizationConfig,
        listener::get_kafka_listener_config,
        role::{AnyConfig, AnyConfigOverrides, KafkaRole},
        security::KafkaTlsSecurity,
        v1alpha1,
    },
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
    BuildDiscoveryConfig { source: build::discovery::Error },

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
        }
    }
}

pub type RoleGroupName = String;

/// The validated cluster. Carries everything the build steps need, resolved once
/// here so downstream code never re-derives it or touches the raw spec.
///
/// The cluster identity (`name`, `namespace`, `uid`) is captured here so that owner
/// references for child objects can be built straight from this struct (via its
/// [`Resource`] impl) without threading the raw [`v1alpha1::KafkaCluster`] around.
/// This mirrors the hive-/opensearch-operator's `ValidatedCluster`.
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, so this struct can act as the
    /// owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_group_configs: BTreeMap<KafkaRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
}

impl ValidatedCluster {
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_group_configs: BTreeMap<KafkaRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
    ) -> Self {
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            image,
            cluster_config,
            role_group_configs,
        }
    }
}

/// Cluster-wide settings resolved during validation and dereferencing.
///
/// Everything the build steps need is resolved here so they never have to read the
/// raw [`v1alpha1::KafkaCluster`] spec.
pub struct ValidatedClusterConfig {
    pub kafka_security: KafkaTlsSecurity,
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub pod_descriptors: Vec<KafkaPodDescriptor>,
    pub metadata_manager: MetadataManager,
}

/// Lets [`ValidatedCluster`] act as the owner [`Resource`] for child objects, so owner
/// references are built from it (via the captured `metadata`) rather than the raw CR.
impl Resource for ValidatedCluster {
    type DynamicType = <v1alpha1::KafkaCluster as Resource>::DynamicType;
    type Scope = <v1alpha1::KafkaCluster as Resource>::Scope;

    fn kind(dt: &Self::DynamicType) -> Cow<'_, str> {
        v1alpha1::KafkaCluster::kind(dt)
    }

    fn group(dt: &Self::DynamicType) -> Cow<'_, str> {
        v1alpha1::KafkaCluster::group(dt)
    }

    fn version(dt: &Self::DynamicType) -> Cow<'_, str> {
        v1alpha1::KafkaCluster::version(dt)
    }

    fn plural(dt: &Self::DynamicType) -> Cow<'_, str> {
        v1alpha1::KafkaCluster::plural(dt)
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

/// A validated, merged Kafka role-group config.
///
/// The merged config fragment is wrapped in [`AnyConfig`] and the merged
/// `configOverrides` in [`AnyConfigOverrides`], so a single role-agnostic type
/// carries both broker and controller role groups (their concrete config and
/// override types differ). Produced via the local-`framework`
/// [`with_validated_config`](crate::framework::role_utils::with_validated_config).
pub type ValidatedRoleGroupConfig = crate::framework::role_utils::RoleGroupConfig<
    AnyConfig,
    stackable_operator::role_utils::JavaCommonConfig,
    AnyConfigOverrides,
>;

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

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        KAFKA_CONTROLLER_NAME,
        &validated_cluster.object_ref(&()),
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
        &kafka.spec.object_overrides,
    )
    .context(CreateClusterResourcesSnafu)?;

    tracing::debug!(
        kerberos_enabled = validated_cluster.cluster_config.kafka_security.has_kerberos_enabled(),
        kerberos_secret_class = ?validated_cluster.cluster_config.kafka_security.kerberos_secret_class(),
        tls_enabled = validated_cluster.cluster_config.kafka_security.tls_enabled(),
        tls_client_authentication_class = ?validated_cluster.cluster_config.kafka_security.tls_client_authentication_class(),
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

    for (kafka_role, rg_map) in &validated_cluster.role_group_configs {
        for (rolegroup_name, validated_rg) in rg_map {
            let rolegroup_ref = kafka.rolegroup_ref(kafka_role, rolegroup_name);

            let rg_headless_service = build_rolegroup_headless_service(
                &validated_cluster,
                &validated_cluster.image,
                &rolegroup_ref,
                &validated_cluster.cluster_config.kafka_security,
            )
            .context(BuildServiceSnafu)?;

            let rg_metrics_service = build_rolegroup_metrics_service(
                &validated_cluster,
                &validated_cluster.image,
                &rolegroup_ref,
            )
            .context(BuildServiceSnafu)?;

            let kafka_listeners = get_kafka_listener_config(
                kafka,
                &validated_cluster.cluster_config.kafka_security,
                &rolegroup_ref,
                &client.kubernetes_cluster_info,
            )
            .context(InvalidKafkaListenersSnafu)?;

            let rg_configmap = build::config_map::build_rolegroup_config_map(
                kafka,
                &validated_cluster,
                &rolegroup_ref,
                validated_rg,
                &kafka_listeners,
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

            if let AnyConfig::Broker(broker_config) = &validated_rg.config {
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

    let discovery_cm =
        build::discovery::build_discovery_configmap(&validated_cluster, &bootstrap_listeners)
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

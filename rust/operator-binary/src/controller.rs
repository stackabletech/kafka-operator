//! The validated cluster model, the reconcile loop, and the steps that produce it.
//!
//! [`ValidatedCluster`] carries everything the build steps need, resolved once during
//! [`validate`] (after [`dereference`]) so downstream code never re-derives it or
//! touches the raw [`v1alpha1::KafkaCluster`] spec. [`reconcile_kafka`] consumes it to
//! build (under [`build`]) and apply the child resources.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::{networking::DomainName, product_image_selection::ResolvedProductImage},
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
        policy::v1::PodDisruptionBudget,
    },
    kube::{
        Resource, ResourceExt,
        api::{DynamicObject, ObjectMeta},
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        cluster_resources::cluster_resources_new,
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{ConfigMapName, ListenerName, NamespaceName, Uid},
            operator::{ClusterName, ControllerName, OperatorName, ProductName, ProductVersion},
        },
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub(crate) mod build;
pub(crate) mod dereference;
pub(crate) mod node_id_hasher;
pub(crate) mod security;
pub(crate) mod validate;

/// The type-safe role-group name from stackable-operator. Re-exported so the rest
/// of the operator can refer to it as `controller::RoleGroupName`.
pub use stackable_operator::v2::types::operator::{RoleGroupName, RoleName};

use crate::{
    controller::{
        build::resource::rbac::{build_rbac_role_binding, build_rbac_service_account},
        node_id_hasher::node_id_hash32_offset,
        security::ValidatedKafkaSecurity,
    },
    crd::{
        APP_NAME, KafkaClusterStatus, KafkaPodDescriptor, MetadataManager, OPERATOR_NAME,
        authorization::KafkaAuthorizationConfig,
        role::{AnyConfig, AnyConfigOverrides, KafkaRole},
        v1alpha1,
    },
};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
pub const KAFKA_FULL_CONTROLLER_NAME: &str = concatcp!(KAFKA_CONTROLLER_NAME, '.', OPERATOR_NAME);

#[derive(Snafu, Debug)]
pub enum PodDescriptorsError {
    #[snafu(display(
        "the node id hash offset of role group {role}/{role_group} collides with {colliding_role}/{colliding_role_group}; node ids must be unique across the cluster"
    ))]
    KafkaNodeIdHashCollision {
        role: KafkaRole,
        role_group: RoleGroupName,
        colliding_role: KafkaRole,
        colliding_role_group: RoleGroupName,
    },
}

/// Every Kubernetes resource produced by the [`build`] step.
///
/// The discovery `ConfigMap` is not part of this: it depends on the applied bootstrap
/// [`Listener`](listener)s' status and is therefore built in [`reconcile_kafka`] after they are
/// applied.
pub struct KubernetesResources {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
}

/// The validated cluster. Carries everything the build steps need, resolved once
/// here so downstream code never re-derives it or touches the raw spec.
///
/// The cluster identity (`name`, `namespace`, `uid`) is captured here so that owner
/// references for child objects can be built straight from this struct (via its
/// [`Resource`] impl) without threading the raw [`v1alpha1::KafkaCluster`] around.
pub struct ValidatedCluster {
    /// `ObjectMeta` carrying `name`, `namespace` and `uid`, so this struct can act as the
    /// owner [`Resource`] for child objects.
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    /// The Kubernetes cluster domain (e.g. `cluster.local`), resolved from the operator's
    /// `KubernetesClusterInfo`. Used to compute pod FQDNs in [`Self::pod_descriptors`].
    pub cluster_domain: DomainName,
    pub image: ResolvedProductImage,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label. Derived from the resolved image's app version label
    /// value.
    pub product_version: ProductVersion,
    pub cluster_config: ValidatedClusterConfig,
    /// Per-role configuration (e.g. the Pod disruption budget), keyed by role.
    pub role_configs: BTreeMap<KafkaRole, ValidatedRoleConfig>,
    pub role_group_configs: BTreeMap<KafkaRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
}

impl ValidatedCluster {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        cluster_domain: DomainName,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
        role_configs: BTreeMap<KafkaRole, ValidatedRoleConfig>,
        role_group_configs: BTreeMap<KafkaRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
    ) -> Self {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        let product_version = ProductVersion::from_str(&image.app_version_label_value)
            .expect("the app version label value is a valid product version");
        Self {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some(uid.to_string()),
                ..ObjectMeta::default()
            },
            name,
            namespace,
            uid,
            cluster_domain,
            image,
            product_version,
            cluster_config,
            role_configs,
            role_group_configs,
        }
    }

    /// Predicts the pods of this cluster (or just `requested_kafka_role`'s pods, if given).
    ///
    /// Pods are predicted rather than read from the live cluster to avoid instance churn. The
    /// node-id hash offsets must be unique across the whole cluster, so collisions are detected
    /// across all role groups regardless of `requested_kafka_role`.
    ///
    /// Resource names reuse [`Self::resource_names`] (the canonical
    /// `<cluster>-<role>-<role-group>` naming) so they stay in sync with the StatefulSet and
    /// headless Service this descriptor refers to.
    pub fn pod_descriptors(
        &self,
        requested_kafka_role: Option<&KafkaRole>,
    ) -> Result<Vec<KafkaPodDescriptor>, PodDescriptorsError> {
        let client_port = self.cluster_config.kafka_security.client_port();
        let mut pod_descriptors = Vec::new();
        let mut seen_hashes = HashMap::<u32, (KafkaRole, RoleGroupName)>::new();

        for (role, role_groups) in &self.role_group_configs {
            for (role_group_name, validated_rg) in role_groups {
                let node_id_hash_offset = node_id_hash32_offset(role, role_group_name.as_ref());

                // The node id hash offset must be unique across the cluster.
                if let Some((colliding_role, colliding_role_group)) =
                    seen_hashes.get(&node_id_hash_offset)
                {
                    return KafkaNodeIdHashCollisionSnafu {
                        role: role.clone(),
                        role_group: role_group_name.clone(),
                        colliding_role: colliding_role.clone(),
                        colliding_role_group: colliding_role_group.clone(),
                    }
                    .fail();
                }
                seen_hashes.insert(node_id_hash_offset, (role.clone(), role_group_name.clone()));

                if requested_kafka_role.is_none() || Some(role) == requested_kafka_role {
                    let resource_names = self.resource_names(role, role_group_name);
                    let role_group_statefulset_name = resource_names.stateful_set_name();
                    let role_group_service_name = resource_names.headless_service_name();
                    // Pods must be predicted from a concrete count (e.g. for KRaft quorum
                    // voters), so an unset replica count falls back to 1.
                    for replica in 0..validated_rg.replicas.unwrap_or(1) {
                        pod_descriptors.push(KafkaPodDescriptor {
                            namespace: self.namespace.clone(),
                            role_group_service_name: role_group_service_name.clone(),
                            role_group_statefulset_name: role_group_statefulset_name.clone(),
                            replica,
                            cluster_domain: self.cluster_domain.clone(),
                            node_id: node_id_hash_offset + u32::from(replica),
                            role: role.clone(),
                            client_port: client_port.clone(),
                        });
                    }
                }
            }
        }

        Ok(pod_descriptors)
    }

    /// The given [`KafkaRole`] as a type-safe [`RoleName`].
    pub fn role_name(role: &KafkaRole) -> RoleName {
        RoleName::from_str(&role.to_string()).expect("a KafkaRole is a valid role name")
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn resource_names(
        &self,
        role: &KafkaRole,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(role),
            role_group_name: role_group_name.clone(),
        }
    }

    /// The name of the broker rolegroup's bootstrap [`Listener`](stackable_operator::crd::listener),
    /// `<cluster>-<role>-<role-group>-bootstrap`.
    pub fn bootstrap_listener_name(
        &self,
        role: &KafkaRole,
        role_group_name: &RoleGroupName,
    ) -> ListenerName {
        ListenerName::from_str(&format!(
            "{}-bootstrap",
            self.resource_names(role, role_group_name)
                .stateful_set_name()
        ))
        .expect("the bootstrap listener name is a valid Listener name")
    }
}

impl HasName for ValidatedCluster {
    fn to_name(&self) -> String {
        self.name.to_string()
    }
}

impl HasUid for ValidatedCluster {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl NameIsValidLabelValue for ValidatedCluster {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

/// The product name (`kafka`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'kafka' is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(KAFKA_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

/// Cluster-wide settings resolved during validation and dereferencing.
///
/// Everything the build steps need is resolved here so they never have to read the
/// raw [`v1alpha1::KafkaCluster`] spec.
pub struct ValidatedClusterConfig {
    pub kafka_security: ValidatedKafkaSecurity,
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub metadata_manager: MetadataManager,

    /// The discovery `ConfigMap` providing the ZooKeeper connection string, if the cluster
    /// is connected to a ZooKeeper ensemble. Resolved from the raw spec during validation so
    /// the build steps never have to read it.
    pub zookeeper_config_map_name: Option<ConfigMapName>,

    /// The `ConfigMap` mapping pods to broker ids, if the user supplied one. Resolved from the
    /// raw spec during validation so the build steps never have to read it.
    pub broker_id_pod_config_map_name: Option<ConfigMapName>,
}

impl ValidatedClusterConfig {
    /// Whether the cluster runs in KRaft mode (as opposed to ZooKeeper mode).
    pub fn is_kraft_mode(&self) -> bool {
        self.metadata_manager == MetadataManager::KRaft
    }

    /// Whether the operator must not generate broker ids itself, because the user supplied a
    /// `broker_id_pod_config_map_name`.
    pub fn disable_broker_id_generation(&self) -> bool {
        self.broker_id_pod_config_map_name.is_some()
    }

    /// The OPA connect string, if OPA authorization is configured.
    pub fn opa_connect(&self) -> Option<&str> {
        self.authorization_config
            .as_ref()
            .map(|auth_config| auth_config.opa_connect.as_str())
    }
}

/// Per-role configuration extracted during validation.
///
/// Resolved from the raw [`v1alpha1::KafkaCluster`] spec during validation so the reconcile loop
/// never has to read it. Kafka's `GenericRoleConfig` only carries the Pod disruption budget.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
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

/// The validated, merged per-role-group product config.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedKafkaConfig {
    pub config: AnyConfig,
    /// Validated logging configuration (derived from `config.logging` during validation).
    pub logging: validate::ValidatedLogging,
}

/// A validated, merged Kafka role-group config.
pub type ValidatedRoleGroupConfig = stackable_operator::v2::role_utils::RoleGroupConfig<
    ValidatedKafkaConfig,
    stackable_operator::v2::role_utils::JavaCommonConfig,
    AnyConfigOverrides,
>;

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

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig {
        source: build::resource::discovery::Error,
    },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
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

    #[snafu(display("failed to get required Labels"))]
    GetRequiredLabels {
        source:
            stackable_operator::kvp::KeyValuePairError<stackable_operator::kvp::LabelValueError>,
    },

    #[snafu(display("KafkaCluster object is invalid"))]
    InvalidKafkaCluster {
        source: error_boundary::InvalidObject,
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
            Error::BuildResources { .. } => None,
            Error::ApplyResource { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::DeleteOrphans { .. } => None,
            Error::ApplyServiceAccount { .. } => None,
            Error::ApplyRoleBinding { .. } => None,
            Error::ApplyStatus { .. } => None,
            Error::GetRequiredLabels { .. } => None,
            Error::InvalidKafkaCluster { .. } => None,
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

    // dereference (client required)
    let dereferenced_objects = dereference::dereference(client, kafka)
        .await
        .context(DereferenceSnafu)?;

    // validate (no client required)
    let validated_cluster =
        validate::validate(kafka, dereferenced_objects, &ctx.operator_environment)
            .context(ValidateClusterSnafu)?;

    let mut cluster_resources = cluster_resources_new(
        &product_name(),
        &operator_name(),
        &controller_name(),
        &validated_cluster.name,
        &validated_cluster.namespace,
        &validated_cluster.uid,
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
        &kafka.spec.object_overrides,
    );

    tracing::debug!(
        kerberos_enabled = validated_cluster.cluster_config.kafka_security.has_kerberos_enabled(),
        kerberos_secret_class = ?validated_cluster.cluster_config.kafka_security.kerberos_secret_class(),
        tls_enabled = validated_cluster.cluster_config.kafka_security.tls_enabled(),
        tls_client_authentication_class = ?validated_cluster.cluster_config.kafka_security.tls_client_authentication_class(),
        "The following security settings are used"
    );

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let required_labels = cluster_resources
        .get_required_labels()
        .context(GetRequiredLabelsSnafu)?;
    let rbac_sa = build_rbac_service_account(&validated_cluster, required_labels.clone());
    let rbac_rolebinding = build_rbac_role_binding(&validated_cluster, required_labels);

    let rbac_sa = cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;
    // The ServiceAccount name is deterministic, so the statefulset builders only need the name,
    // not the applied object.
    let service_account_name = rbac_sa.name_any();
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    // Build every Kubernetes resource up front (client-free). The discovery ConfigMap is not part
    // of this, as it depends on the applied bootstrap Listeners' status (see below).
    let resources =
        build::build(&validated_cluster, &service_account_name).context(BuildResourcesSnafu)?;

    // Apply order: Services, then Listeners (collecting the applied bootstrap Listeners for the
    // discovery ConfigMap), then ConfigMaps, then PodDisruptionBudgets, and finally the
    // StatefulSets. The StatefulSets must be applied after all ConfigMaps and Secrets they mount to
    // prevent unnecessary Pod restarts.
    // See https://github.com/stackabletech/commons-operator/issues/111 for details.
    for service in resources.services {
        cluster_resources
            .add(client, service)
            .await
            .context(ApplyResourceSnafu)?;
    }

    let mut bootstrap_listeners = Vec::<listener::v1alpha1::Listener>::new();
    for rg_listener in resources.listeners {
        bootstrap_listeners.push(
            cluster_resources
                .add(client, rg_listener)
                .await
                .context(ApplyResourceSnafu)?,
        );
    }

    for config_map in resources.config_maps {
        cluster_resources
            .add(client, config_map)
            .await
            .context(ApplyResourceSnafu)?;
    }

    for pdb in resources.pod_disruption_budgets {
        cluster_resources
            .add(client, pdb)
            .await
            .context(ApplyResourceSnafu)?;
    }

    for stateful_set in resources.stateful_sets {
        ss_cond_builder.add(
            cluster_resources
                .add(client, stateful_set)
                .await
                .context(ApplyResourceSnafu)?,
        );
    }

    // The discovery ConfigMap reports the bootstrap Listeners' ingress addresses, which are only
    // populated on the applied Listener objects (by the Listener operator), so it is built here
    // rather than in the client-free build() step.
    let discovery_cm = build::resource::discovery::build_discovery_configmap(
        &validated_cluster,
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

#[cfg(test)]
pub(crate) mod test_support {
    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        commons::networking::DomainName,
        utils::{cluster_info::KubernetesClusterInfo, yaml_from_str_singleton_map},
    };

    use super::{ValidatedCluster, dereference::DereferencedObjects, validate::validate};
    use crate::crd::{authentication::ResolvedAuthenticationClasses, v1alpha1};

    pub fn minimal_kafka(yaml: &str) -> v1alpha1::KafkaCluster {
        yaml_from_str_singleton_map(yaml).expect("invalid test KafkaCluster YAML")
    }

    fn cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").expect("valid domain"),
        }
    }

    fn operator_environment() -> OperatorEnvironmentOptions {
        OperatorEnvironmentOptions {
            operator_namespace: "stackable-operators".to_owned(),
            operator_service_name: "kafka-operator".to_owned(),
            image_repository: "oci.example.org".to_owned(),
        }
    }

    /// Runs the real validate step against a minimal (auth/OPA-free) fixture.
    pub fn validated_cluster(kafka: &v1alpha1::KafkaCluster) -> ValidatedCluster {
        validate(
            kafka,
            DereferencedObjects {
                authentication_classes: ResolvedAuthenticationClasses::new(Vec::new()),
                authorization_config: None,
                kubernetes_cluster_info: cluster_info(),
            },
            &operator_environment(),
        )
        .expect("validate should succeed for the test fixture")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        PodDescriptorsError,
        test_support::{minimal_kafka, validated_cluster},
    };
    use crate::crd::role::KafkaRole;

    /// Two broker role groups whose names hash to the same node-id offset must be
    /// rejected: a collision would hand two pods the same Kafka `node.id`. `rg865`
    /// and `rg1400` are a known colliding pair for the `broker` role (see
    /// [`node_id_hash32_offset`](super::node_id_hasher::node_id_hash32_offset)).
    #[test]
    fn pod_descriptors_rejects_node_id_hash_collision() {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                zookeeperConfigMapName: xyz
              brokers:
                roleGroups:
                  rg865:
                    replicas: 1
                  rg1400:
                    replicas: 1
            "#,
        );
        let validated = validated_cluster(&kafka);

        match validated.pod_descriptors(None) {
            Err(PodDescriptorsError::KafkaNodeIdHashCollision {
                role,
                colliding_role,
                ..
            }) => {
                assert_eq!(role, KafkaRole::Broker);
                assert_eq!(colliding_role, KafkaRole::Broker);
            }
            other => panic!("expected a node-id hash collision error, got {other:?}"),
        }
    }

    /// Non-colliding role groups expand to one descriptor per replica, each with a
    /// unique `node_id`.
    #[test]
    fn pod_descriptors_assigns_unique_node_ids() {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                zookeeperConfigMapName: xyz
              brokers:
                roleGroups:
                  default:
                    replicas: 2
                  other:
                    replicas: 1
            "#,
        );
        let validated = validated_cluster(&kafka);

        let descriptors = validated
            .pod_descriptors(None)
            .expect("non-colliding role groups must not error");

        assert_eq!(descriptors.len(), 3);
        let node_ids: BTreeSet<u32> = descriptors.iter().map(|d| d.node_id).collect();
        assert_eq!(node_ids.len(), 3, "node ids must be unique: {node_ids:?}");
    }
}

//! The validated cluster model, the reconcile loop, and the steps that produce it.
//!
//! [`ValidatedCluster`] carries everything the build steps need, resolved once during
//! [`validate`] (after [`dereference`]) so downstream code never re-derives it or
//! touches the raw [`v1alpha1::KafkaCluster`] spec. [`reconcile_kafka`] consumes it to
//! build (under [`build`]) and apply the child resources.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    cluster_resources::ClusterResourceApplyStrategy,
    commons::{networking::DomainName, product_image_selection::ResolvedProductImage},
    constant,
    crd::listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{
        Resource,
        api::{DynamicObject, ObjectMeta},
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        role_group_utils::ResourceNames,
        role_utils,
        types::{
            kubernetes::{ConfigMapName, ListenerName, NamespaceName, Uid},
            operator::{ClusterName, ControllerName, OperatorName, ProductName, ProductVersion},
        },
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub(crate) mod apply;
pub(crate) mod build;
pub(crate) mod dereference;
pub(crate) mod node_id_hasher;
pub(crate) mod security;
pub(crate) mod update_status;
pub(crate) mod validate;

/// The type-safe role-group name from stackable-operator. Re-exported so the rest
/// of the operator can refer to it as `controller::RoleGroupName`.
pub use stackable_operator::v2::types::operator::{RoleGroupName, RoleName};

use crate::{
    controller::{
        apply::Applier, node_id_hasher::node_id_hash32_offset, security::ValidatedKafkaSecurity,
        update_status::update_status,
    },
    crd::{
        APP_NAME, KAFKA_OPERATOR_NAME, KafkaPodDescriptor, MetadataManager,
        authorization::KafkaAuthorizationConfig,
        role::{AnyConfig, AnyConfigOverrides, KafkaRole},
        v1alpha1,
    },
};

pub const KAFKA_CONTROLLER_NAME: &str = "kafkacluster";
pub const KAFKA_FULL_CONTROLLER_NAME: &str =
    concatcp!(KAFKA_CONTROLLER_NAME, '.', KAFKA_OPERATOR_NAME);

constant!(PRODUCT_NAME: ProductName = APP_NAME);
constant!(OPERATOR_NAME: OperatorName = KAFKA_OPERATOR_NAME);
constant!(CONTROLLER_NAME: ControllerName = KAFKA_CONTROLLER_NAME);

#[derive(Snafu, Debug)]
pub enum PodDescriptorsError {
    #[snafu(display(
        "the node id hash offset of role group {role}/{role_group} collides with {colliding_role}/{colliding_role_group}; node ids must be unique across the cluster",
        role = role.as_ref(),
        colliding_role = colliding_role.as_ref(),
    ))]
    KafkaNodeIdHashCollision {
        role: KafkaRole,
        role_group: RoleGroupName,
        colliding_role: KafkaRole,
        colliding_role_group: RoleGroupName,
    },
}

/// Marker for prepared Kubernetes resources which are not applied yet.
pub struct Prepared;

/// Marker for applied Kubernetes resources.
pub struct Applied;

/// Every Kubernetes resource produced by the [`build`] step.
///
/// `T` is a marker that indicates if these resources are only [`Prepared`] or already [`Applied`].
/// The marker is useful e.g. to ensure that the cluster status is updated based on the applied
/// resources.
///
/// The discovery `ConfigMap` is part of [`Self::config_maps`]; see
/// [`build::resource::discovery::build_discovery_configmap`] for how its content depends on the
/// bootstrap [`Listener`](listener)s.
pub struct KubernetesResources<T> {
    pub stateful_sets: Vec<StatefulSet>,
    pub services: Vec<Service>,
    pub listeners: Vec<listener::v1alpha1::Listener>,
    pub config_maps: Vec<ConfigMap>,
    pub pod_disruption_budgets: Vec<PodDisruptionBudget>,
    pub service_accounts: Vec<ServiceAccount>,
    pub role_bindings: Vec<RoleBinding>,
    pub status: PhantomData<T>,
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
    /// The broker role groups' bootstrap `Listener`s as currently stored in the cluster (fetched
    /// in the dereference step), from which the discovery `ConfigMap` is built. Missing or still
    /// address-less around the first reconcile runs; the listener-operator populates the ingress
    /// addresses and the `Listener` watch triggers a new run once it does.
    pub bootstrap_listeners: Vec<listener::v1alpha1::Listener>,
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
        bootstrap_listeners: Vec<listener::v1alpha1::Listener>,
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
            bootstrap_listeners,
        }
    }

    /// Predicts the pods of this cluster (or just `requested_kafka_role`'s pods, if given).
    ///
    /// Pods are predicted rather than read from the live cluster to avoid instance churn. The
    /// node-id hash offsets must be unique across the whole cluster, so collisions are detected
    /// across all role groups regardless of `requested_kafka_role`.
    ///
    /// Resource names reuse resource names so they stay in sync with the StatefulSet and
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
                    let resource_names = self.role_group_resource_names(role, role_group_name);
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

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount shared by all
    /// Pods, its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: PRODUCT_NAME.clone(),
        }
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn role_group_resource_names(
        &self,
        role: &KafkaRole,
        role_group_name: &RoleGroupName,
    ) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: role.role_name(),
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
        build::resource::listener::bootstrap_listener_name(&self.name, role, role_group_name)
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

/// The validated, merged per-role-group config.
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
    #[snafu(display("failed to apply the Kubernetes resources"))]
    ApplyResources { source: apply::Error },

    #[snafu(display("failed to update the cluster status"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("failed to dereference resources"))]
    Dereference { source: dereference::Error },

    #[snafu(display("failed to validate cluster"))]
    ValidateCluster { source: validate::Error },

    #[snafu(display("failed to build the Kubernetes resources"))]
    BuildResources { source: build::Error },

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
            Error::ApplyResources { .. } => None,
            Error::UpdateStatus { .. } => None,
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

    tracing::debug!(
        kerberos_enabled = validated_cluster.cluster_config.kafka_security.has_kerberos_enabled(),
        kerberos_secret_class = ?validated_cluster.cluster_config.kafka_security.kerberos_secret_class(),
        tls_enabled = validated_cluster.cluster_config.kafka_security.tls_enabled(),
        tls_client_authentication_class = ?validated_cluster.cluster_config.kafka_security.tls_client_authentication_class(),
        "The following security settings are used"
    );

    // build (no client required)
    let resources = build::build(&validated_cluster).context(BuildResourcesSnafu)?;

    // apply (client required)
    let applier = Applier::new(
        client,
        &validated_cluster,
        ClusterResourceApplyStrategy::from(&kafka.spec.cluster_operation),
        &kafka.spec.object_overrides,
    );
    let applied = applier
        .apply(resources)
        .await
        .context(ApplyResourcesSnafu)?;

    // update status (client required)
    update_status(client, kafka, applied)
        .await
        .context(UpdateStatusSnafu)?;

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
    use std::collections::BTreeMap;

    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        commons::networking::DomainName,
        crd::listener,
        utils::{cluster_info::KubernetesClusterInfo, yaml_from_str_singleton_map},
    };

    use super::{ValidatedCluster, dereference::DereferencedObjects, validate::validate};
    use crate::crd::{authentication::ResolvedAuthenticationClasses, v1alpha1};

    /// The expected `app.kubernetes.io/version` label value for the given product version.
    ///
    /// The `-stackable` suffix carries the operator's own version, which is `0.0.0-dev` on main
    /// but rewritten by the release process — so tests must derive it rather than hardcode it,
    /// or they fail on release branches.
    pub fn app_version_label(product_version: &str) -> String {
        format!(
            "{product_version}-stackable{}",
            crate::built_info::PKG_VERSION
        )
    }

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

    /// A ZooKeeper-mode cluster with a single `broker` role group and default (TLS) security.
    pub fn zookeeper_mode_cluster() -> ValidatedCluster {
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
                    replicas: 1
            "#,
        );
        validated_cluster(&kafka)
    }

    /// A bootstrap `Listener` with the given ingress addresses, as stored in the cluster after
    /// the listener-operator has reconciled it.
    pub fn bootstrap_listener(
        ingress_addresses: Option<Vec<listener::v1alpha1::ListenerIngress>>,
    ) -> listener::v1alpha1::Listener {
        listener::v1alpha1::Listener {
            metadata: Default::default(),
            spec: Default::default(),
            status: Some(listener::v1alpha1::ListenerStatus {
                service_name: None,
                ingress_addresses,
                node_ports: None,
            }),
        }
    }

    /// An ingress address exposing a single named port.
    pub fn ingress_address(
        address: &str,
        port_name: &str,
        port: i32,
    ) -> listener::v1alpha1::ListenerIngress {
        listener::v1alpha1::ListenerIngress {
            address: address.to_owned(),
            address_type: listener::v1alpha1::AddressType::Hostname,
            ports: BTreeMap::from([(port_name.to_owned(), port)]),
        }
    }

    /// Runs the real validate step against a minimal (auth/OPA-free) fixture.
    pub fn validated_cluster(kafka: &v1alpha1::KafkaCluster) -> ValidatedCluster {
        validate_err(kafka).expect("validate should succeed for the test fixture")
    }

    /// Runs the real validate step against a minimal (auth/OPA-free) fixture, without unwrapping
    /// the result -- for tests asserting on a specific validation failure.
    pub fn validate_err(
        kafka: &v1alpha1::KafkaCluster,
    ) -> Result<ValidatedCluster, super::validate::Error> {
        validate(
            kafka,
            DereferencedObjects {
                authentication_classes: ResolvedAuthenticationClasses::new(Vec::new()),
                authorization_config: None,
                kubernetes_cluster_info: cluster_info(),
                bootstrap_listeners: Vec::new(),
            },
            &operator_environment(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        CONTROLLER_NAME, OPERATOR_NAME, PRODUCT_NAME, PodDescriptorsError,
        test_support::{minimal_kafka, validated_cluster},
    };
    use crate::crd::role::KafkaRole;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *PRODUCT_NAME;
        let _ = *OPERATOR_NAME;
        let _ = *CONTROLLER_NAME;
    }

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

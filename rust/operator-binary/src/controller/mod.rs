//! The validated cluster model and the steps that produce it.
//!
//! [`ValidatedCluster`] carries everything the build steps need, resolved once during
//! [`validate`] (after [`dereference`]) so downstream code never re-derives it or
//! touches the raw [`v1alpha1::KafkaCluster`] spec. The reconcile loop that consumes
//! it lives in [`crate::kafka_controller`].

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use snafu::Snafu;
use stackable_operator::{
    commons::{networking::DomainName, product_image_selection::ResolvedProductImage},
    kube::{Resource, api::ObjectMeta},
    kvp::Labels,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        kvp::label::{recommended_labels, role_group_selector},
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::{ClusterName, ControllerName, OperatorName, ProductName, ProductVersion},
        },
    },
};

pub(crate) mod build;
pub(crate) mod dereference;
pub(crate) mod validate;

/// The type-safe role-group name from stackable-operator's v2 module. Re-exported so the rest
/// of the operator can refer to it as `controller::RoleGroupName`.
pub use stackable_operator::v2::types::operator::{RoleGroupName, RoleName};

use crate::{
    config::node_id_hasher::node_id_hash32_offset,
    crd::{
        APP_NAME, KafkaPodDescriptor, MetadataManager, OPERATOR_NAME,
        authorization::KafkaAuthorizationConfig,
        role::{AnyConfig, AnyConfigOverrides, KafkaRole},
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    kafka_controller::KAFKA_CONTROLLER_NAME,
};

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
    pub role_group_configs: BTreeMap<KafkaRole, BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>,
}

impl ValidatedCluster {
    pub fn new(
        name: ClusterName,
        namespace: NamespaceName,
        uid: Uid,
        cluster_domain: DomainName,
        image: ResolvedProductImage,
        cluster_config: ValidatedClusterConfig,
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
                    let role_group_statefulset_name =
                        resource_names.stateful_set_name().to_string();
                    let role_group_service_name =
                        resource_names.headless_service_name().to_string();
                    for replica in 0..validated_rg.replicas {
                        pod_descriptors.push(KafkaPodDescriptor {
                            namespace: self.namespace.to_string(),
                            role: role.to_string(),
                            role_group_service_name: role_group_service_name.clone(),
                            role_group_statefulset_name: role_group_statefulset_name.clone(),
                            replica,
                            cluster_domain: self.cluster_domain.clone(),
                            node_id: node_id_hash_offset + u32::from(replica),
                            client_port,
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
    ) -> String {
        format!(
            "{}-bootstrap",
            self.resource_names(role, role_group_name)
                .stateful_set_name()
        )
    }

    /// Recommended labels for a role-group resource, using the given product version.
    fn recommended_labels_for(
        &self,
        product_version: &ProductVersion,
        role: &KafkaRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            &Self::role_name(role),
            role_group_name,
        )
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(&self, role: &KafkaRole, role_group_name: &RoleGroupName) -> Labels {
        self.recommended_labels_for(&self.product_version, role, role_group_name)
    }

    /// Recommended labels without a version, for PVC templates that cannot be modified once
    /// deployed.
    pub fn unversioned_recommended_labels(
        &self,
        role: &KafkaRole,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        // A version value is required, and we do want to use the "recommended" format for the
        // other desired labels.
        let none_version =
            ProductVersion::from_str("none").expect("'none' is a valid product version");
        self.recommended_labels_for(&none_version, role, role_group_name)
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(&self, role: &KafkaRole, role_group_name: &RoleGroupName) -> Labels {
        role_group_selector(
            self,
            &product_name(),
            &Self::role_name(role),
            role_group_name,
        )
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
fn product_name() -> ProductName {
    ProductName::from_str(APP_NAME).expect("'kafka' is a valid product name")
}

/// The operator name as a type-safe label value.
fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
fn controller_name() -> ControllerName {
    ControllerName::from_str(KAFKA_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

/// Cluster-wide settings resolved during validation and dereferencing.
///
/// Everything the build steps need is resolved here so they never have to read the
/// raw [`v1alpha1::KafkaCluster`] spec.
pub struct ValidatedClusterConfig {
    pub kafka_security: KafkaTlsSecurity,
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub metadata_manager: MetadataManager,

    /// Whether the operator must not generate broker ids itself, because the user
    /// supplied a `broker_id_pod_config_map_name`. Resolved from the raw spec during
    /// validation so the config-map builder never has to read it.
    pub disable_broker_id_generation: bool,
}

impl ValidatedClusterConfig {
    /// Whether the cluster runs in KRaft mode (as opposed to ZooKeeper mode).
    pub fn is_kraft_mode(&self) -> bool {
        self.metadata_manager == MetadataManager::KRaft
    }

    /// The OPA connect string, if OPA authorization is configured.
    pub fn opa_connect(&self) -> Option<&str> {
        self.authorization_config
            .as_ref()
            .map(|auth_config| auth_config.opa_connect.as_str())
    }
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
/// `configOverrides` in [`AnyConfigOverrides`], so a single role-agnostic type carries
/// both broker and controller role groups. Produced from the upstream
/// [`stackable_operator::v2::role_utils::with_validated_config`] result in
/// [`validate`](crate::controller::validate). `jvm_argument_overrides` is already merged
/// (role <- role group) at validation time and applied as-is during build.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedRoleGroupConfig {
    pub replicas: u16,
    pub config: AnyConfig,
    pub config_overrides: AnyConfigOverrides,
    pub env_overrides: stackable_operator::v2::builder::pod::container::EnvVarSet,
    pub pod_overrides: stackable_operator::k8s_openapi::api::core::v1::PodTemplateSpec,
    pub jvm_argument_overrides:
        stackable_operator::v2::jvm_argument_overrides::JvmArgumentOverrides,
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

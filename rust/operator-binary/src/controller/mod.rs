//! The validated cluster model and the steps that produce it.
//!
//! [`ValidatedCluster`] carries everything the build steps need, resolved once during
//! [`validate`] (after [`dereference`]) so downstream code never re-derives it or
//! touches the raw [`v1alpha1::KafkaCluster`] spec. The reconcile loop that consumes
//! it lives in [`crate::kafka_controller`].

use std::{borrow::Cow, collections::BTreeMap};

use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    kube::{Resource, api::ObjectMeta},
    v2::types::{
        kubernetes::{NamespaceName, Uid},
        operator::ClusterName,
    },
};

pub(crate) mod build;
pub(crate) mod dereference;
pub(crate) mod validate;

use crate::{
    crd::{
        KafkaPodDescriptor, MetadataManager,
        authorization::KafkaAuthorizationConfig,
        role::{AnyConfig, AnyConfigOverrides, KafkaRole},
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    framework::role_utils::RoleGroupConfig,
};

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
/// `configOverrides` in [`AnyConfigOverrides`], so a single role-agnostic type
/// carries both broker and controller role groups (their concrete config and
/// override types differ). Produced via the local-`framework`
/// [`with_validated_config`](crate::framework::role_utils::with_validated_config).
pub type ValidatedRoleGroupConfig = RoleGroupConfig<
    AnyConfig,
    stackable_operator::role_utils::JavaCommonConfig,
    AnyConfigOverrides,
>;

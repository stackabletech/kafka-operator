//! The validate step in the KafkaCluster controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedKafkaCluster`], consumed by the rest of `reconcile_kafka`.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{cli::OperatorEnvironmentOptions, commons::product_image_selection};

use crate::{
    controller::{
        ValidatedKafkaCluster, ValidatedRoleGroupConfig, dereference::DereferencedObjects,
    },
    crd::{
        self, CONTAINER_IMAGE_BASE_NAME,
        authentication::{self},
        role::KafkaRole,
        security::{self, KafkaTlsSecurity},
        v1alpha1,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to validate authentication classes"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to validate authentication method"))]
    FailedToValidateAuthenticationMethod { source: security::Error },

    #[snafu(display("cluster object defines no '{role}' role"))]
    MissingKafkaRole { source: crd::Error, role: KafkaRole },

    #[snafu(display("failed to resolve merged config for rolegroup"))]
    ResolveMergedConfig { source: crate::crd::role::Error },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("invalid metadata manager"))]
    InvalidMetadataManager { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Validates the cluster spec and the dereferenced inputs.
pub fn validate(
    kafka: &v1alpha1::KafkaCluster,
    dereferenced_objects: DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedKafkaCluster> {
    let image = kafka
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let authentication_classes = dereferenced_objects
        .authentication_classes
        .validate()
        .context(InvalidAuthenticationClassConfigurationSnafu)?;

    let opa_secret_class = dereferenced_objects
        .authorization_config
        .as_ref()
        .and_then(|cfg| cfg.secret_class.clone());

    let kafka_security =
        KafkaTlsSecurity::new_from_kafka_cluster(kafka, authentication_classes, opa_secret_class);

    kafka_security
        .validate_authentication_methods()
        .context(FailedToValidateAuthenticationMethodSnafu)?;

    // DESIGN DECISION: build the per-rolegroup config (merged config + resolved overrides)
    // here, so reconcile reads a fully-typed ValidatedKafkaCluster instead of re-deriving
    // merged_config in the loop and threading a product-config HashMap. Alternative: keep
    // deriving merged_config in the reconcile loop — rejected; validation is the right place
    // to prove every rolegroup resolves before any resource is built.
    let mut role_groups: BTreeMap<KafkaRole, BTreeMap<String, ValidatedRoleGroupConfig>> =
        BTreeMap::new();

    // Brokers always exist.
    let broker_role = kafka
        .broker_role()
        .cloned()
        .context(MissingKafkaRoleSnafu {
            role: KafkaRole::Broker,
        })?;

    let mut broker_groups: BTreeMap<String, ValidatedRoleGroupConfig> = BTreeMap::new();
    for rolegroup_name in broker_role.role_groups.keys() {
        let merged_config = KafkaRole::Broker
            .merged_config(kafka, rolegroup_name)
            .context(ResolveMergedConfigSnafu)?;
        let (config_file_overrides, jvm_security_overrides, env_overrides) =
            collect_broker_role_group_overrides(kafka, &broker_role, rolegroup_name);
        broker_groups.insert(
            rolegroup_name.clone(),
            ValidatedRoleGroupConfig {
                merged_config,
                config_file_overrides,
                jvm_security_overrides,
                env_overrides,
            },
        );
    }
    role_groups.insert(KafkaRole::Broker, broker_groups);

    // We need this guard because controller_role() returns an error if controllers is None,
    // which would stop reconciliation for ZooKeeper-mode clusters.
    if kafka.spec.controllers.is_some() {
        let controller_role = kafka
            .controller_role()
            .cloned()
            .context(MissingKafkaRoleSnafu {
                role: KafkaRole::Controller,
            })?;

        let mut controller_groups: BTreeMap<String, ValidatedRoleGroupConfig> = BTreeMap::new();
        for rolegroup_name in controller_role.role_groups.keys() {
            let merged_config = KafkaRole::Controller
                .merged_config(kafka, rolegroup_name)
                .context(ResolveMergedConfigSnafu)?;
            let (config_file_overrides, jvm_security_overrides, env_overrides) =
                collect_controller_role_group_overrides(kafka, &controller_role, rolegroup_name);
            controller_groups.insert(
                rolegroup_name.clone(),
                ValidatedRoleGroupConfig {
                    merged_config,
                    config_file_overrides,
                    jvm_security_overrides,
                    env_overrides,
                },
            );
        }
        role_groups.insert(KafkaRole::Controller, controller_groups);
    }

    let pod_descriptors = kafka
        .pod_descriptors(
            None,
            &dereferenced_objects.kubernetes_cluster_info,
            kafka_security.client_port(),
        )
        .context(BuildPodDescriptorsSnafu)?;

    let metadata_manager = kafka
        .effective_metadata_manager()
        .context(InvalidMetadataManagerSnafu)?;

    Ok(ValidatedKafkaCluster {
        image,
        kafka_security,
        authorization_config: dereferenced_objects.authorization_config,
        role_groups,
        pod_descriptors,
        metadata_manager,
    })
}

// DESIGN DECISION: role-group overrides are merged role-level first, then role-group
// extended on top so role-group wins — identical to the precedent product-config used.
// We read the v2 KeyValueConfigOverrides `.overrides` map and BTreeMap::extend rather
// than using its `Merge` impl, because plain extend reproduces the old behaviour exactly
// (last-writer-wins per key) and avoids depending on Merge semantics. Alternative:
// KeyValueConfigOverrides::merge — equivalent here but an unnecessary semantic dependency.
fn collect_broker_role_group_overrides(
    kafka: &v1alpha1::KafkaCluster,
    broker_role: &crate::crd::BrokerRole,
    rolegroup_name: &str,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
) {
    // --- broker.properties overrides ---
    let role_broker_overrides: BTreeMap<String, Option<String>> = broker_role
        .config
        .config_overrides
        .broker_properties
        .as_ref()
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let rg_broker_overrides: BTreeMap<String, Option<String>> = broker_role
        .role_groups
        .get(rolegroup_name)
        .and_then(|rg| rg.config.config_overrides.broker_properties.as_ref())
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let mut merged_broker = role_broker_overrides;
    merged_broker.extend(rg_broker_overrides);
    let config_file_overrides: BTreeMap<String, String> = merged_broker
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect();

    // --- security.properties overrides ---
    let role_security_overrides: BTreeMap<String, Option<String>> = broker_role
        .config
        .config_overrides
        .security_properties
        .as_ref()
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let rg_security_overrides: BTreeMap<String, Option<String>> = broker_role
        .role_groups
        .get(rolegroup_name)
        .and_then(|rg| rg.config.config_overrides.security_properties.as_ref())
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let mut merged_security = role_security_overrides;
    merged_security.extend(rg_security_overrides);
    let jvm_security_overrides: BTreeMap<String, String> = merged_security
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect();

    // --- env overrides ---
    // DESIGN DECISION: KAFKA_CLUSTER_ID is injected first, then the user env overrides
    // (role then role-group) are extended on top, so a user override of the same key wins.
    // This mirrors product-config's old merge of compute_env() output with user envOverrides.
    // Alternative: inject after user overrides (operator wins) — rejected to preserve the
    // previous precedence.
    //
    // KAFKA_CLUSTER_ID injection moved here from crd/role/broker.rs::Configuration::compute_env.
    let mut env_overrides: BTreeMap<String, String> = BTreeMap::new();
    if let Some(cluster_id) = kafka.cluster_id() {
        env_overrides.insert("KAFKA_CLUSTER_ID".to_string(), cluster_id.to_string());
    }
    let role_env: &std::collections::HashMap<String, String> = &broker_role.config.env_overrides;
    env_overrides.extend(role_env.iter().map(|(k, v)| (k.clone(), v.clone())));
    if let Some(rg) = broker_role.role_groups.get(rolegroup_name) {
        env_overrides.extend(
            rg.config
                .env_overrides
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
    }

    (config_file_overrides, jvm_security_overrides, env_overrides)
}

// DESIGN DECISION: role-group overrides are merged role-level first, then role-group
// extended on top so role-group wins — identical to the precedent product-config used.
// We read the v2 KeyValueConfigOverrides `.overrides` map and BTreeMap::extend rather
// than using its `Merge` impl, because plain extend reproduces the old behaviour exactly
// (last-writer-wins per key) and avoids depending on Merge semantics. Alternative:
// KeyValueConfigOverrides::merge — equivalent here but an unnecessary semantic dependency.
fn collect_controller_role_group_overrides(
    kafka: &v1alpha1::KafkaCluster,
    controller_role: &crate::crd::ControllerRole,
    rolegroup_name: &str,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
) {
    // --- controller.properties overrides ---
    let role_controller_overrides: BTreeMap<String, Option<String>> = controller_role
        .config
        .config_overrides
        .controller_properties
        .as_ref()
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let rg_controller_overrides: BTreeMap<String, Option<String>> = controller_role
        .role_groups
        .get(rolegroup_name)
        .and_then(|rg| rg.config.config_overrides.controller_properties.as_ref())
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let mut merged_controller = role_controller_overrides;
    merged_controller.extend(rg_controller_overrides);
    let config_file_overrides: BTreeMap<String, String> = merged_controller
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect();

    // --- security.properties overrides ---
    let role_security_overrides: BTreeMap<String, Option<String>> = controller_role
        .config
        .config_overrides
        .security_properties
        .as_ref()
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let rg_security_overrides: BTreeMap<String, Option<String>> = controller_role
        .role_groups
        .get(rolegroup_name)
        .and_then(|rg| rg.config.config_overrides.security_properties.as_ref())
        .map(|o| o.overrides.clone())
        .unwrap_or_default();
    let mut merged_security = role_security_overrides;
    merged_security.extend(rg_security_overrides);
    let jvm_security_overrides: BTreeMap<String, String> = merged_security
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v)))
        .collect();

    // --- env overrides ---
    // DESIGN DECISION: KAFKA_CLUSTER_ID is injected first, then the user env overrides
    // (role then role-group) are extended on top, so a user override of the same key wins.
    // This mirrors product-config's old merge of compute_env() output with user envOverrides.
    // Alternative: inject after user overrides (operator wins) — rejected to preserve the
    // previous precedence.
    //
    // KAFKA_CLUSTER_ID injection moved here from crd/role/controller.rs::Configuration::compute_env.
    let mut env_overrides: BTreeMap<String, String> = BTreeMap::new();
    if let Some(cluster_id) = kafka.cluster_id() {
        env_overrides.insert("KAFKA_CLUSTER_ID".to_string(), cluster_id.to_string());
    }
    let role_env: &std::collections::HashMap<String, String> =
        &controller_role.config.env_overrides;
    env_overrides.extend(role_env.iter().map(|(k, v)| (k.clone(), v.clone())));
    if let Some(rg) = controller_role.role_groups.get(rolegroup_name) {
        env_overrides.extend(
            rg.config
                .env_overrides
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
    }

    (config_file_overrides, jvm_security_overrides, env_overrides)
}

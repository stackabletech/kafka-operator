//! The validate step in the KafkaCluster controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedKafkaCluster`], consumed by the rest of `reconcile_kafka`.

use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection,
    config::merge::{Merge, merge},
    v2::config_overrides::KeyValueConfigOverrides,
};

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

/// Merge role-group overrides over the role-level overrides (role-group wins per key) via the
/// `Merge` impl derived on the override structs.
///
/// NOTE on semantics: `Merge` treats a role-group `null` value as "inherit the role-level value",
/// *not* "unset it". This differs from `main`'s product-config layering, which `.extend()`ed the
/// maps so a role-group `null` *removed* a role-level key. The `tests` module has a worked
/// example of the difference.
fn merge_role_group_overrides<O: Merge + Clone>(role: &O, role_group: Option<&O>) -> O {
    match role_group {
        Some(role_group) => merge(role_group.clone(), role),
        None => role.clone(),
    }
}

/// Flatten resolved key/value overrides into a plain map, dropping entries whose value is
/// unset (`null`).
fn flatten_overrides(overrides: KeyValueConfigOverrides) -> BTreeMap<String, String> {
    overrides
        .overrides
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}

fn collect_broker_role_group_overrides(
    kafka: &v1alpha1::KafkaCluster,
    broker_role: &crate::crd::BrokerRole,
    rolegroup_name: &str,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
) {
    let merged_overrides = merge_role_group_overrides(
        &broker_role.config.config_overrides,
        broker_role
            .role_groups
            .get(rolegroup_name)
            .map(|rg| &rg.config.config_overrides),
    );
    let config_file_overrides = flatten_overrides(merged_overrides.broker_properties);
    let jvm_security_overrides = flatten_overrides(merged_overrides.security_properties);

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

fn collect_controller_role_group_overrides(
    kafka: &v1alpha1::KafkaCluster,
    controller_role: &crate::crd::ControllerRole,
    rolegroup_name: &str,
) -> (
    BTreeMap<String, String>,
    BTreeMap<String, String>,
    BTreeMap<String, String>,
) {
    let merged_overrides = merge_role_group_overrides(
        &controller_role.config.config_overrides,
        controller_role
            .role_groups
            .get(rolegroup_name)
            .map(|rg| &rg.config.config_overrides),
    );
    let config_file_overrides = flatten_overrides(merged_overrides.controller_properties);
    let jvm_security_overrides = flatten_overrides(merged_overrides.security_properties);

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

    use super::{flatten_overrides, merge_role_group_overrides};

    /// Build a `KeyValueConfigOverrides` from `(key, value)` pairs, where a `None` value
    /// represents an explicit `null` (unset) in the CRD.
    fn overrides(pairs: &[(&str, Option<&str>)]) -> KeyValueConfigOverrides {
        KeyValueConfigOverrides {
            overrides: pairs
                .iter()
                .map(|(key, value)| (key.to_string(), value.map(str::to_string)))
                .collect(),
        }
    }

    /// Run the full role/role-group resolution (merge then flatten) for a single config file.
    fn resolve(
        role: KeyValueConfigOverrides,
        role_group: Option<KeyValueConfigOverrides>,
    ) -> BTreeMap<String, String> {
        flatten_overrides(merge_role_group_overrides(&role, role_group.as_ref()))
    }

    #[test]
    fn role_group_value_wins_over_role() {
        let role = overrides(&[("a", Some("role")), ("b", Some("role-only"))]);
        let role_group = overrides(&[("a", Some("rg"))]);

        let merged = resolve(role, Some(role_group));

        assert_eq!(
            merged,
            BTreeMap::from([
                ("a".to_string(), "rg".to_string()), // role-group wins for shared keys
                ("b".to_string(), "role-only".to_string()), // role-only keys are kept
            ])
        );
    }

    /// Illustrates the key consequence of using `Merge` (rather than `.extend()`, as `main`'s
    /// product-config did): a role-group `null` is treated as "inherit", so the role-level value
    /// is *kept* — it does NOT unset the key. Under the old `.extend()` behaviour this same input
    /// would have removed `a` entirely.
    #[test]
    fn role_group_null_inherits_role_value_rather_than_unsetting_it() {
        let role = overrides(&[("a", Some("role"))]);
        let role_group = overrides(&[("a", None)]); // explicit `null` at the more specific level

        let merged = resolve(role, Some(role_group));

        assert_eq!(
            merged,
            BTreeMap::from([("a".to_string(), "role".to_string())]),
            "a role-group `null` should inherit the role-level value under Merge semantics"
        );
    }

    #[test]
    fn without_a_role_group_role_values_are_kept_and_nulls_dropped() {
        let role = overrides(&[("a", Some("role")), ("b", None)]);

        let merged = resolve(role, None);

        assert_eq!(
            merged,
            BTreeMap::from([("a".to_string(), "role".to_string())])
        );
    }
}

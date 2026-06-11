//! The validate step in the KafkaCluster controller.
//!
//! Synchronously validates inputs that don't require a Kubernetes client. Produces
//! [`ValidatedCluster`], consumed by the rest of `reconcile_kafka`.

use std::{collections::BTreeMap, str::FromStr};

use serde::Serialize;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection,
    config::{fragment::FromFragment, merge::Merge},
    kube::ResourceExt,
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role},
    schemars::JsonSchema,
    v2::{
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::ClusterName,
        },
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedClusterConfig, ValidatedRoleGroupConfig,
        dereference::DereferencedObjects,
    },
    crd::{
        self, CONTAINER_IMAGE_BASE_NAME,
        authentication::{self},
        role::{
            AnyConfig, AnyConfigOverrides, KafkaRole, broker::BrokerConfig,
            controller::ControllerConfig,
        },
        security::{self, KafkaTlsSecurity},
        v1alpha1,
    },
    framework::role_utils::with_validated_config,
};

/// The operator-managed env var carrying the Kafka cluster id.
const KAFKA_CLUSTER_ID_ENV: &str = "KAFKA_CLUSTER_ID";

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

    #[snafu(display("failed to merge and validate the role group config"))]
    ValidateRoleGroupConfig {
        source: crate::framework::role_utils::Error,
    },

    #[snafu(display("invalid environment variable name"))]
    InvalidEnvVarName { source: container::Error },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("invalid metadata manager"))]
    InvalidMetadataManager { source: crate::crd::Error },

    #[snafu(display("invalid cluster name"))]
    InvalidClusterName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("invalid cluster namespace"))]
    InvalidNamespace {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("object has no uid"))]
    ObjectHasNoUid,

    #[snafu(display("invalid cluster uid"))]
    InvalidUid {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Validates the cluster spec and the dereferenced inputs.
pub fn validate(
    kafka: &v1alpha1::KafkaCluster,
    dereferenced_objects: DereferencedObjects,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedCluster> {
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

    let cluster_id = kafka.cluster_id();

    let mut role_group_configs: BTreeMap<
        KafkaRole,
        BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>,
    > = BTreeMap::new();

    // Brokers always exist.
    let broker_role = kafka.broker_role().context(MissingKafkaRoleSnafu {
        role: KafkaRole::Broker,
    })?;
    let broker_groups = validate_role_group_configs(
        broker_role,
        BrokerConfig::default_config(&kafka.name_any(), &KafkaRole::Broker.to_string()),
        cluster_id,
        AnyConfig::Broker,
        AnyConfigOverrides::Broker,
    )?;
    role_group_configs.insert(KafkaRole::Broker, broker_groups);

    // Controllers are optional: ZooKeeper-mode clusters have none, and `controller_role()`
    // errors when `controllers` is unset, which would stop their reconciliation.
    if let Some(controller_role) = kafka.spec.controllers.as_ref() {
        let controller_groups = validate_role_group_configs(
            controller_role,
            ControllerConfig::default_config(&kafka.name_any(), &KafkaRole::Controller.to_string()),
            cluster_id,
            AnyConfig::Controller,
            AnyConfigOverrides::Controller,
        )?;
        role_group_configs.insert(KafkaRole::Controller, controller_groups);
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

    let name = ClusterName::from_str(&kafka.name_any()).context(InvalidClusterNameSnafu)?;
    let namespace = NamespaceName::from_str(&kafka.namespace().context(ObjectHasNoNamespaceSnafu)?)
        .context(InvalidNamespaceSnafu)?;
    let uid = Uid::from_str(&kafka.uid().context(ObjectHasNoUidSnafu)?).context(InvalidUidSnafu)?;

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        ValidatedClusterConfig {
            kafka_security,
            authorization_config: dereferenced_objects.authorization_config,
            pod_descriptors,
            metadata_manager,
            disable_broker_id_generation: kafka
                .spec
                .cluster_config
                .broker_id_pod_config_map_name
                .is_some(),
        },
        role_group_configs,
    ))
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via the local-`framework`
/// [`with_validated_config`], which folds the config fragment (default <- role <-
/// role group) plus the `configOverrides`, `envOverrides`, `cliOverrides` and
/// `podOverrides` (role group wins) into a single
/// [`RoleGroupConfig`](crate::framework::role_utils::RoleGroupConfig). The concrete
/// per-role validated config and overrides are wrapped into the role-agnostic
/// [`AnyConfig`]/[`AnyConfigOverrides`] via `wrap_config`/`wrap_overrides`, and the
/// operator-managed `KAFKA_CLUSTER_ID` is injected into the env overrides.
fn validate_role_group_configs<Config, ValidatedConfig, ConfigOverrides>(
    role: &Role<Config, ConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    default_config: Config,
    cluster_id: Option<&str>,
    wrap_config: fn(ValidatedConfig) -> AnyConfig,
    wrap_overrides: fn(ConfigOverrides) -> AnyConfigOverrides,
) -> Result<BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
    ConfigOverrides: Clone + Default + JsonSchema + Merge + Serialize,
{
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let validated = with_validated_config::<
                ValidatedConfig,
                JavaCommonConfig,
                Config,
                GenericRoleConfig,
                ConfigOverrides,
            >(role_group, role, &default_config)
            .context(ValidateRoleGroupConfigSnafu)?;

            // Re-wrap the per-role validated config and overrides into the role-agnostic
            // enums; the merged env/cli/pod overrides carry over unchanged, except that
            // `KAFKA_CLUSTER_ID` is injected into the env overrides.
            let validated = ValidatedRoleGroupConfig {
                replicas: validated.replicas,
                config: wrap_config(validated.config),
                config_overrides: wrap_overrides(validated.config_overrides),
                env_overrides: inject_cluster_id(validated.env_overrides, cluster_id)?,
                cli_overrides: validated.cli_overrides,
                pod_overrides: validated.pod_overrides,
                product_specific_common_config: validated.product_specific_common_config,
            };
            Ok((role_group_name.clone(), validated))
        })
        .collect()
}

/// Injects the operator-managed `KAFKA_CLUSTER_ID` into the merged env overrides,
/// but only when the user has not already set it via `envOverrides` (user value
/// wins).
fn inject_cluster_id(env_overrides: EnvVarSet, cluster_id: Option<&str>) -> Result<EnvVarSet> {
    let Some(cluster_id) = cluster_id else {
        return Ok(env_overrides);
    };
    let name = EnvVarName::from_str(KAFKA_CLUSTER_ID_ENV).context(InvalidEnvVarNameSnafu)?;
    if env_overrides.get(&name).is_some() {
        // The user set `KAFKA_CLUSTER_ID` via envOverrides; their value wins.
        Ok(env_overrides)
    } else {
        Ok(env_overrides.with_value(&name, cluster_id))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use stackable_operator::v2::builder::pod::container::{EnvVarName, EnvVarSet};

    use super::{KAFKA_CLUSTER_ID_ENV, inject_cluster_id};

    fn cluster_id_value(env: &EnvVarSet) -> Option<String> {
        let name = EnvVarName::from_str(KAFKA_CLUSTER_ID_ENV).unwrap();
        env.get(&name).and_then(|var| var.value.clone())
    }

    #[test]
    fn injects_cluster_id_when_absent() {
        let env = inject_cluster_id(EnvVarSet::new(), Some("my-id")).unwrap();
        assert_eq!(cluster_id_value(&env), Some("my-id".to_string()));
    }

    #[test]
    fn user_cluster_id_override_wins() {
        let name = EnvVarName::from_str(KAFKA_CLUSTER_ID_ENV).unwrap();
        let env = EnvVarSet::new().with_value(&name, "user-value");

        let env = inject_cluster_id(env, Some("operator-value")).unwrap();

        assert_eq!(cluster_id_value(&env), Some("user-value".to_string()));
    }

    #[test]
    fn without_cluster_id_nothing_is_injected() {
        let env = inject_cluster_id(EnvVarSet::new(), None).unwrap();
        assert_eq!(cluster_id_value(&env), None);
    }
}

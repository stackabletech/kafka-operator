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
    product_logging::spec::Logging,
    role_utils::{GenericRoleConfig, Role},
    schemars::JsonSchema,
    v2::{
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_utils::{JavaCommonConfig, with_validated_config},
        types::kubernetes::ConfigMapName,
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedClusterConfig, ValidatedKafkaConfig,
        ValidatedRoleConfig, ValidatedRoleGroupConfig,
        dereference::DereferencedObjects,
        security::{self, ValidatedKafkaSecurity},
    },
    crd::{
        self, CONTAINER_IMAGE_BASE_NAME,
        authentication::{self},
        role::{
            AnyConfig, AnyConfigOverrides, KafkaRole,
            broker::{BrokerConfig, BrokerContainer},
            controller::{ControllerConfig, ControllerContainer},
        },
        v1alpha1,
    },
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
        source: stackable_operator::config::fragment::ValidationError,
    },

    #[snafu(display("invalid environment variable name"))]
    InvalidEnvVarName { source: container::Error },

    #[snafu(display("invalid metadata manager"))]
    InvalidMetadataManager { source: crate::crd::Error },

    #[snafu(display("failed to resolve the cluster name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve the cluster namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve the cluster uid"))]
    ResolveUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("the role group name {role_group_name:?} is invalid"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group_name: String,
    },

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector aggregator discovery ConfigMap name is required when the Vector agent is enabled"
    ))]
    MissingVectorAggregatorConfigMapName,
}

/// Validated logging configuration for a Kafka role group's Kafka and (optional) Vector
/// containers.
///
/// Produced up-front by [`validate_logging`] so that an invalid custom log `ConfigMap` name or a
/// missing Vector aggregator discovery `ConfigMap` name fails reconciliation during validation
/// rather than at resource-build time.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedLogging {
    pub kafka_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
}

/// Validates the logging configuration for a role group's Kafka and (optional) Vector container.
///
/// `vector_aggregator_config_map_name` is the discovery `ConfigMap` name of the Vector
/// aggregator; it is required (and was validated into a [`ConfigMapName`]) only when the Vector
/// agent is enabled. Generic over the role's container enum so it serves both broker and
/// controller role groups.
fn validate_logging<C>(
    logging: &Logging<C>,
    kafka_container: C,
    vector_container: C,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging>
where
    C: Clone + std::fmt::Display + Ord,
{
    let kafka_container = validate_logging_configuration_for_container(logging, &kafka_container)
        .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(logging, &vector_container)
                .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        kafka_container,
        vector_container,
    })
}

/// Validates a broker role group's logging configuration.
fn validate_broker_logging(
    config: &BrokerConfig,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    validate_logging(
        &config.logging,
        BrokerContainer::Kafka,
        BrokerContainer::Vector,
        vector_aggregator_config_map_name,
    )
}

/// Validates a controller role group's logging configuration.
fn validate_controller_logging(
    config: &ControllerConfig,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    validate_logging(
        &config.logging,
        ControllerContainer::Kafka,
        ControllerContainer::Vector,
        vector_aggregator_config_map_name,
    )
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

    let kafka_security = ValidatedKafkaSecurity::new_from_kafka_cluster(
        kafka,
        authentication_classes,
        opa_secret_class,
    );

    kafka_security
        .validate_authentication_methods()
        .context(FailedToValidateAuthenticationMethodSnafu)?;

    let cluster_id = kafka.cluster_id();

    // The Vector aggregator discovery ConfigMap name. Validity is enforced by the `ConfigMapName`
    // type on the CRD field. It is only required (per role group) when the Vector agent is
    // enabled; see [`validate_logging`].
    let vector_aggregator_config_map_name = kafka
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .clone();

    let mut role_configs: BTreeMap<KafkaRole, ValidatedRoleConfig> = BTreeMap::new();
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
        validate_broker_logging,
        &vector_aggregator_config_map_name,
    )?;
    role_configs.insert(
        KafkaRole::Broker,
        ValidatedRoleConfig {
            pdb: broker_role.role_config.pod_disruption_budget.clone(),
        },
    );
    role_group_configs.insert(KafkaRole::Broker, broker_groups);

    // Controllers are optional: ZooKeeper-mode clusters have none, in which case they are simply
    // absent from both maps and not reconciled.
    if let Some(controller_role) = kafka.spec.controllers.as_ref() {
        let controller_groups = validate_role_group_configs(
            controller_role,
            ControllerConfig::default_config(&kafka.name_any(), &KafkaRole::Controller.to_string()),
            cluster_id,
            AnyConfig::Controller,
            AnyConfigOverrides::Controller,
            validate_controller_logging,
            &vector_aggregator_config_map_name,
        )?;
        role_configs.insert(
            KafkaRole::Controller,
            ValidatedRoleConfig {
                pdb: controller_role.role_config.pod_disruption_budget.clone(),
            },
        );
        role_group_configs.insert(KafkaRole::Controller, controller_groups);
    }

    let metadata_manager = kafka
        .effective_metadata_manager()
        .context(InvalidMetadataManagerSnafu)?;

    let name = get_cluster_name(kafka).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(kafka).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(kafka).context(ResolveUidSnafu)?;
    let cluster_domain = dereferenced_objects
        .kubernetes_cluster_info
        .cluster_domain
        .clone();

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        cluster_domain,
        image,
        ValidatedClusterConfig {
            kafka_security,
            authorization_config: dereferenced_objects.authorization_config,
            metadata_manager,
            zookeeper_config_map_name: kafka.spec.cluster_config.zookeeper_config_map_name.clone(),
            broker_id_pod_config_map_name: kafka
                .spec
                .cluster_config
                .broker_id_pod_config_map_name
                .clone(),
        },
        role_configs,
        role_group_configs,
    ))
}

/// Validates every role group of a role into a map keyed by role group name.
///
/// Each role group is merged and validated via
/// [`with_validated_config`], which folds the config fragment (default <- role <-
/// role group) plus the `configOverrides`, `envOverrides`, `podOverrides` and
/// `jvmArgumentOverrides` (role group wins) into a single
/// [`ValidatedRoleGroupConfig`]. The concrete per-role validated config and overrides
/// are wrapped into the role-agnostic [`AnyConfig`]/[`AnyConfigOverrides`] via
/// `wrap_config`/`wrap_overrides`, and the operator-managed `KAFKA_CLUSTER_ID` is
/// injected into the env overrides.
fn validate_role_group_configs<Config, ValidatedConfig, ConfigOverrides>(
    role: &Role<Config, ConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    default_config: Config,
    cluster_id: Option<&str>,
    wrap_config: fn(ValidatedConfig) -> AnyConfig,
    wrap_overrides: fn(ConfigOverrides) -> AnyConfigOverrides,
    validate_logging: fn(&ValidatedConfig, &Option<ConfigMapName>) -> Result<ValidatedLogging>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, ValidatedRoleGroupConfig>>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
    ConfigOverrides: Clone + Default + JsonSchema + Merge + Serialize,
{
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = with_validated_config::<
                ValidatedConfig,
                JavaCommonConfig,
                Config,
                GenericRoleConfig,
                ConfigOverrides,
            >(role_group, role, &default_config)
            .context(ValidateRoleGroupConfigSnafu)?;

            // The merge returns env overrides as a HashMap. Convert to an
            // EnvVarSet (validating names early), then inject KAFKA_CLUSTER_ID.
            let mut env_overrides = EnvVarSet::new();
            for (name, value) in merged.config.env_overrides {
                let name = EnvVarName::from_str(&name).context(InvalidEnvVarNameSnafu)?;
                env_overrides = env_overrides.with_value(&name, value);
            }
            let env_overrides = inject_cluster_id(env_overrides, cluster_id)?;

            let logging =
                validate_logging(&merged.config.config, vector_aggregator_config_map_name)?;

            let validated = ValidatedRoleGroupConfig {
                // Passed through as-is (including `None`) so an unset replica count lets a
                // horizontal autoscaler own the StatefulSet's `.spec.replicas`.
                replicas: merged.replicas,
                config: ValidatedKafkaConfig {
                    config: wrap_config(merged.config.config),
                    logging,
                },
                config_overrides: wrap_overrides(merged.config.config_overrides),
                env_overrides,
                // Kafka does not use CLI overrides; the field is carried (and merged upstream)
                // but unused.
                cli_overrides: merged.config.cli_overrides,
                pod_overrides: merged.config.pod_overrides,
                product_specific_common_config: merged.config.product_specific_common_config,
            };
            let role_group_name = RoleGroupName::from_str(role_group_name).with_context(|_| {
                ParseRoleGroupNameSnafu {
                    role_group_name: role_group_name.clone(),
                }
            })?;
            Ok((role_group_name, validated))
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

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
        builder::pod::container::{EnvVarName, EnvVarSet},
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
        CONTAINER_IMAGE_BASE_NAME,
        authentication::{self},
        role::{
            AnyConfig, AnyConfigOverrides, KafkaRole,
            broker::{BrokerConfig, BrokerContainer},
            controller::{ControllerConfig, ControllerContainer},
        },
        tls, v1alpha1,
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

    #[snafu(display("failed to merge and validate the role group config"))]
    ValidateRoleGroupConfig {
        source: stackable_operator::config::fragment::ValidationError,
    },

    #[snafu(display("invalid environment variable name"))]
    InvalidEnvVarName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

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

    let internal_secret_class = kafka
        .spec
        .cluster_config
        .tls
        .as_ref()
        .map(|tls| tls.internal_secret_class.clone())
        .unwrap_or_else(tls::internal_tls_default);

    let kafka_security = ValidatedKafkaSecurity::new_from_kafka_cluster(
        kafka,
        internal_secret_class,
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

    // The broker role is required by the CRD.
    let broker_role = &kafka.spec.brokers;
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
        dereferenced_objects.bootstrap_listeners,
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

    use stackable_operator::v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        types::operator::RoleGroupName,
    };

    use super::{KAFKA_CLUSTER_ID_ENV, inject_cluster_id};
    use crate::{
        controller::test_support::{app_version_label, minimal_kafka, validated_cluster},
        crd::role::KafkaRole,
    };

    fn cluster_id_value(env: &EnvVarSet) -> Option<String> {
        let name = EnvVarName::from_str(KAFKA_CLUSTER_ID_ENV).unwrap();
        env.get(&name).and_then(|var| var.value.clone())
    }

    /// Locks every value the validate step itself derives from the minimal KRaft fixture — so a
    /// validation regression fails here, with a validate-shaped message, instead of surfacing as
    /// a confusing build-test failure downstream.
    ///
    /// The merged per-role-group config (resources, affinity, logging defaults, …) is produced by
    /// `with_validated_config` and the config defaults, whose contracts are tested in operator-rs;
    /// only the values this module derives on top are re-asserted here.
    #[test]
    fn validate_ok_derives_expected_values() {
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
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        let cluster = validated_cluster(&kafka);

        assert_eq!(cluster.name.to_string(), "simple-kafka");
        assert_eq!(cluster.namespace.to_string(), "default");
        assert_eq!(
            cluster.uid.to_string(),
            "12345678-1234-1234-1234-123456789012"
        );
        assert_eq!(cluster.cluster_domain.to_string(), "cluster.local");
        assert_eq!(
            cluster.image.image,
            format!("oci.example.org/kafka:{}", app_version_label("3.9.2"))
        );
        assert_eq!(cluster.image.product_version, "3.9.2");
        assert_eq!(
            cluster.product_version.to_string(),
            app_version_label("3.9.2")
        );

        // KRaft mode: no ZooKeeper ConfigMap; no user-supplied broker-id map or authorization.
        let cluster_config = &cluster.cluster_config;
        assert!(cluster_config.is_kraft_mode());
        assert_eq!(cluster_config.zookeeper_config_map_name, None);
        assert_eq!(cluster_config.broker_id_pod_config_map_name, None);
        assert!(cluster_config.authorization_config.is_none());

        // TLS defaults: server and internal both use the `tls` SecretClass; no Kerberos or OPA.
        let security = &cluster_config.kafka_security;
        assert!(security.tls_enabled());
        assert_eq!(security.tls_server_secret_class(), Some("tls"));
        assert_eq!(security.tls_internal_secret_class(), "tls");
        assert!(!security.has_kerberos_enabled());
        assert_eq!(security.opa_secret_class(), None);

        // Both roles are present, with default (enabled) PDB configs.
        let roles: Vec<_> = cluster.role_configs.keys().collect();
        assert_eq!(roles, [&KafkaRole::Broker, &KafkaRole::Controller]);
        for role_config in cluster.role_configs.values() {
            assert!(role_config.pdb.enabled);
            assert_eq!(role_config.pdb.max_unavailable, None);
        }

        // One `default` role group per role. The KRaft cluster id (derived from the cluster
        // name) is injected into every role group's env overrides.
        let default_rg = RoleGroupName::from_str("default").expect("valid role group name");
        for role in [KafkaRole::Broker, KafkaRole::Controller] {
            let role_group = &cluster.role_group_configs[&role][&default_rg];
            assert_eq!(role_group.replicas, Some(3));
            assert_eq!(
                cluster_id_value(&role_group.env_overrides),
                Some("simple-kafka".to_string())
            );
            assert_eq!(role_group.config.logging.vector_container, None);
        }
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

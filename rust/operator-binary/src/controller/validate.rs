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
    constant,
    kube::ResourceExt,
    product_logging::spec::Logging,
    role_utils::GenericRoleConfig,
    schemars::JsonSchema,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_utils::{JavaCommonConfig, Role, RoleGroup, RoleGroupConfig, with_validated_config},
        types::kubernetes::ConfigMapName,
    },
};

use crate::{
    controller::{
        BrokerRoleGroupConfig, ControllerRoleGroupConfig, RoleGroupName, ValidatedBrokerConfig,
        ValidatedCluster, ValidatedClusterConfig, ValidatedControllerConfig, ValidatedRoleConfig,
        dereference::DereferencedObjects,
        replicas_or_default,
        security::{self, ValidatedKafkaSecurity},
    },
    crd::{
        BrokerRole, CONTAINER_IMAGE_BASE_NAME, ControllerRole,
        authentication::{self},
        role::{
            KafkaRole,
            broker::{BrokerConfig, BrokerConfigFragment, BrokerContainer},
            commons::CommonConfig,
            controller::{ControllerConfig, ControllerConfigFragment, ControllerContainer},
        },
        tls, v1alpha1,
    },
};

// The operator-managed env var carrying the Kafka cluster id.
constant!(KAFKA_CLUSTER_ID: EnvVarName = "KAFKA_CLUSTER_ID");

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

    #[snafu(display(
        "the graceful shutdown timeout of {role} role group {role_group_name:?} is missing"
    ))]
    MissingGracefulShutdownTimeout {
        role: KafkaRole,
        role_group_name: String,
    },

    #[snafu(display("the secret lifetime of {role} role group {role_group_name:?} is missing"))]
    MissingSecretLifetime {
        role: KafkaRole,
        role_group_name: String,
    },

    #[snafu(display(
        "at least one KRaft controller replica is required while any broker replicas are \
         configured; a KRaft cluster with zero controllers has no metadata quorum for its \
         brokers to use. Scale brokers to 0 as well (or use `clusterOperation.stopped`) for a \
         coordinated full stop"
    ))]
    NoKraftControllerReplicas,

    #[snafu(display(
        "the `spec.controllers` role is required in KRaft mode; brokers have no metadata quorum \
         to join without it"
    ))]
    MissingKraftControllerRole,
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
    logging: &Logging<BrokerContainer>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    validate_logging(
        logging,
        BrokerContainer::Kafka,
        BrokerContainer::Vector,
        vector_aggregator_config_map_name,
    )
}

/// Validates a controller role group's logging configuration.
fn validate_controller_logging(
    logging: &Logging<ControllerContainer>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    validate_logging(
        logging,
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
            &crate::built_info::PKG_VERSION_SEMVER,
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

    // The first failure propagates, so this order decides which misconfiguration the user is told
    // about when more than one role is wrong. It is `KafkaRole`'s variant order.

    // The broker role is required by the CRD.
    let broker_role = &kafka.spec.brokers;
    let broker_role_group_configs = validate_broker_role_group_configs(
        broker_role,
        BrokerConfig::default_config(
            &kafka.name_any(),
            &KafkaRole::Broker.to_string(),
            kafka.spec.cluster_config.authorization.opa.as_ref(),
        ),
        cluster_id,
        &vector_aggregator_config_map_name,
    )?;
    let broker_config = validated_role_config(&broker_role.role_config);

    let metadata_manager = kafka
        .effective_metadata_manager()
        .context(InvalidMetadataManagerSnafu)?;

    // Controllers are optional in ZooKeeper mode only.
    // In KRaft mode they are mandatory.
    if metadata_manager == crate::crd::MetadataManager::KRaft && kafka.spec.controllers.is_none() {
        return MissingKraftControllerRoleSnafu.fail();
    }

    let (controller_config, controller_role_group_configs) = match kafka.spec.controllers.as_ref() {
        Some(controller_role) => {
            let controller_role_group_configs = validate_controller_role_group_configs(
                controller_role,
                ControllerConfig::default_config(
                    &kafka.name_any(),
                    &KafkaRole::Controller.to_string(),
                ),
                cluster_id,
                &vector_aggregator_config_map_name,
            )?;

            // A KRaft cluster with zero controller replicas *and running brokers* is a broken
            // half-state. Reject that combination here.
            //
            // Controllers *and* brokers at zero together is not rejected: that is exactly what
            // `clusterOperation.stopped` already does today.
            let controller_replicas: u16 = controller_role_group_configs
                .values()
                .map(|rg| replicas_or_default(rg.replicas))
                .sum();
            let broker_replicas: u16 = broker_role_group_configs
                .values()
                .map(|rg| replicas_or_default(rg.replicas))
                .sum();
            if metadata_manager == crate::crd::MetadataManager::KRaft
                && controller_replicas == 0
                && broker_replicas > 0
            {
                return NoKraftControllerReplicasSnafu.fail();
            }

            (
                Some(validated_role_config(&controller_role.role_config)),
                controller_role_group_configs,
            )
        }
        None => (None, BTreeMap::new()),
    };

    let name = get_cluster_name(kafka).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(kafka).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(kafka).context(ResolveUidSnafu)?;
    let cluster_domain = dereferenced_objects
        .kubernetes_cluster_info
        .cluster_domain
        .clone();

    Ok(ValidatedCluster {
        metadata: ValidatedCluster::object_meta(&name, &namespace, &uid),
        name,
        namespace,
        uid,
        cluster_domain,
        product_version: ValidatedCluster::product_version(&image),
        image,
        cluster_config: ValidatedClusterConfig {
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
        broker_config,
        broker_role_group_configs,
        controller_config,
        controller_role_group_configs,
        bootstrap_listeners: dereferenced_objects.bootstrap_listeners,
    })
}

/// The role-level config, resolved from the role's `roleConfig`.
fn validated_role_config(role_config: &GenericRoleConfig) -> ValidatedRoleConfig {
    // Destructured without `..`, so a field added upstream fails to compile here instead of
    // being silently dropped.
    let GenericRoleConfig {
        pod_disruption_budget,
    } = role_config;
    ValidatedRoleConfig {
        pdb: pod_disruption_budget.clone(),
    }
}

/// Validates every broker role group into a map keyed by role group name: merges it (see
/// [`validate_role_group_config`]), then validates its logging, then its name.
fn validate_broker_role_group_configs(
    role: &BrokerRole,
    default_config: BrokerConfigFragment,
    cluster_id: Option<&str>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, BrokerRoleGroupConfig>> {
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = validate_role_group_config(role, role_group, &default_config, cluster_id)?;
            let BrokerConfig {
                common_config:
                    CommonConfig {
                        affinity,
                        graceful_shutdown_timeout,
                        requested_secret_lifetime,
                    },
                bootstrap_listener_class,
                broker_listener_class,
                logging,
                resources,
            } = merged.config;
            let validated = BrokerRoleGroupConfig {
                replicas: merged.replicas,
                config: ValidatedBrokerConfig {
                    resources,
                    logging: validate_broker_logging(&logging, vector_aggregator_config_map_name)?,
                    affinity,
                    graceful_shutdown_timeout: graceful_shutdown_timeout.with_context(|| {
                        MissingGracefulShutdownTimeoutSnafu {
                            role: KafkaRole::Broker,
                            role_group_name: role_group_name.to_owned(),
                        }
                    })?,
                    requested_secret_lifetime: requested_secret_lifetime.with_context(|| {
                        MissingSecretLifetimeSnafu {
                            role: KafkaRole::Broker,
                            role_group_name: role_group_name.to_owned(),
                        }
                    })?,
                    bootstrap_listener_class,
                    broker_listener_class,
                },
                config_overrides: merged.config_overrides,
                env_overrides: merged.env_overrides,
                cli_overrides: merged.cli_overrides,
                pod_overrides: merged.pod_overrides,
                product_specific_common_config: merged.product_specific_common_config,
            };
            Ok((parse_role_group_name(role_group_name)?, validated))
        })
        .collect()
}

/// Validates every controller role group into a map keyed by role group name: merges it (see
/// [`validate_role_group_config`]), then validates its logging, then its name.
fn validate_controller_role_group_configs(
    role: &ControllerRole,
    default_config: ControllerConfigFragment,
    cluster_id: Option<&str>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<BTreeMap<RoleGroupName, ControllerRoleGroupConfig>> {
    role.role_groups
        .iter()
        .map(|(role_group_name, role_group)| {
            let merged = validate_role_group_config(role, role_group, &default_config, cluster_id)?;
            let ControllerConfig {
                common_config:
                    CommonConfig {
                        affinity,
                        graceful_shutdown_timeout,
                        requested_secret_lifetime,
                    },
                logging,
                resources,
            } = merged.config;
            let validated = ControllerRoleGroupConfig {
                replicas: merged.replicas,
                config: ValidatedControllerConfig {
                    resources,
                    logging: validate_controller_logging(
                        &logging,
                        vector_aggregator_config_map_name,
                    )?,
                    affinity,
                    graceful_shutdown_timeout: graceful_shutdown_timeout.with_context(|| {
                        MissingGracefulShutdownTimeoutSnafu {
                            role: KafkaRole::Controller,
                            role_group_name: role_group_name.to_owned(),
                        }
                    })?,
                    requested_secret_lifetime: requested_secret_lifetime.with_context(|| {
                        MissingSecretLifetimeSnafu {
                            role: KafkaRole::Controller,
                            role_group_name: role_group_name.to_owned(),
                        }
                    })?,
                },
                config_overrides: merged.config_overrides,
                env_overrides: merged.env_overrides,
                cli_overrides: merged.cli_overrides,
                pod_overrides: merged.pod_overrides,
                product_specific_common_config: merged.product_specific_common_config,
            };
            Ok((parse_role_group_name(role_group_name)?, validated))
        })
        .collect()
}

/// Merges and validates one role group via [`with_validated_config`], which folds the config
/// fragment (default <- role <- role group) plus the `configOverrides`, `envOverrides`,
/// `podOverrides` and `jvmArgumentOverrides` (role group wins) into a single
/// [`RoleGroupConfig`], and injects the operator-managed `KAFKA_CLUSTER_ID` into the env
/// overrides.
fn validate_role_group_config<Config, ValidatedConfig, ConfigOverrides>(
    role: &Role<Config, ConfigOverrides, GenericRoleConfig, JavaCommonConfig>,
    role_group: &RoleGroup<Config, JavaCommonConfig, ConfigOverrides>,
    default_config: &Config,
    cluster_id: Option<&str>,
) -> Result<RoleGroupConfig<ValidatedConfig, JavaCommonConfig, ConfigOverrides>>
where
    Config: Clone + Merge,
    ValidatedConfig: FromFragment<Fragment = Config>,
    ConfigOverrides: Clone + Default + JsonSchema + Merge + Serialize,
{
    let merged = with_validated_config::<
        ValidatedConfig,
        JavaCommonConfig,
        Config,
        GenericRoleConfig,
        ConfigOverrides,
    >(role_group, role, default_config)
    .context(ValidateRoleGroupConfigSnafu)?;

    // The env override names are validated on deserialization; inject the
    // operator-managed KAFKA_CLUSTER_ID (unless the user overrides it).
    let env_overrides = inject_cluster_id(merged.config.env_overrides.into(), cluster_id);

    Ok(RoleGroupConfig {
        // Passed through as is, including `None`: the StatefulSet then leaves `.spec.replicas`
        // unset, and the pod predictions count it as `DEFAULT_REPLICAS`.
        replicas: merged.replicas,
        config: merged.config.config,
        config_overrides: merged.config.config_overrides,
        env_overrides,
        // Kafka does not use CLI overrides; the field is carried (and merged upstream)
        // but unused.
        cli_overrides: merged.config.cli_overrides,
        pod_overrides: merged.config.pod_overrides,
        product_specific_common_config: merged.config.product_specific_common_config,
    })
}

/// Parses a role group name from the CRD into its type-safe form.
fn parse_role_group_name(role_group_name: &str) -> Result<RoleGroupName> {
    RoleGroupName::from_str(role_group_name).with_context(|_| ParseRoleGroupNameSnafu {
        role_group_name: role_group_name.to_owned(),
    })
}

/// Injects the operator-managed `KAFKA_CLUSTER_ID` into the merged env overrides,
/// but only when the user has not already set it via `envOverrides` (user value
/// wins).
fn inject_cluster_id(env_overrides: EnvVarSet, cluster_id: Option<&str>) -> EnvVarSet {
    let Some(cluster_id) = cluster_id else {
        return env_overrides;
    };
    if env_overrides.get(&KAFKA_CLUSTER_ID).is_some() {
        // The user set `KAFKA_CLUSTER_ID` via envOverrides; their value wins.
        env_overrides
    } else {
        env_overrides.with_value(&KAFKA_CLUSTER_ID, cluster_id)
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::v2::builder::pod::container::EnvVarSet;

    use super::{Error, KAFKA_CLUSTER_ID, inject_cluster_id};
    use crate::controller::test_support::{
        app_version_label, broker_role_group_config, controller_role_group_config, minimal_kafka,
        validated_cluster,
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *KAFKA_CLUSTER_ID;
    }

    fn cluster_id_value(env: &EnvVarSet) -> Option<String> {
        env.get(&KAFKA_CLUSTER_ID).and_then(|var| var.value.clone())
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
                metadataManager: KRaft
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
        let controller_config = cluster
            .controller_config
            .as_ref()
            .expect("the controller role is present");
        for role_config in [&cluster.broker_config, controller_config] {
            assert!(role_config.pdb.enabled);
            assert_eq!(role_config.pdb.max_unavailable, None);
        }

        // One `default` role group per role. The KRaft cluster id (derived from the cluster
        // name) is injected into every role group's env overrides.
        let broker_role_group = broker_role_group_config(&cluster, "default");
        let controller_role_group = controller_role_group_config(&cluster, "default");
        for (replicas, env_overrides, logging) in [
            (
                broker_role_group.replicas,
                &broker_role_group.env_overrides,
                &broker_role_group.config.logging,
            ),
            (
                controller_role_group.replicas,
                &controller_role_group.env_overrides,
                &controller_role_group.config.logging,
            ),
        ] {
            assert_eq!(replicas, Some(3));
            assert_eq!(
                cluster_id_value(env_overrides),
                Some("simple-kafka".to_string())
            );
            assert_eq!(logging.vector_container, None);
        }
    }

    #[test]
    fn injects_cluster_id_when_absent() {
        let env = inject_cluster_id(EnvVarSet::new(), Some("my-id"));
        assert_eq!(cluster_id_value(&env), Some("my-id".to_string()));
    }

    #[test]
    fn user_cluster_id_override_wins() {
        let env = EnvVarSet::new().with_value(&KAFKA_CLUSTER_ID, "user-value");

        let env = inject_cluster_id(env, Some("operator-value"));

        assert_eq!(cluster_id_value(&env), Some("user-value".to_string()));
    }

    #[test]
    fn without_cluster_id_nothing_is_injected() {
        let env = inject_cluster_id(EnvVarSet::new(), None);
        assert_eq!(cluster_id_value(&env), None);
    }

    #[test]
    fn kraft_mode_rejects_zero_controller_replicas_while_brokers_are_running() {
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
                metadataManager: KRaft
              controllers:
                roleGroups:
                  default:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );

        let result = crate::controller::test_support::validate_err(&kafka);
        let Err(error) = result else {
            panic!(
                "validate should reject zero controller replicas while brokers are running in KRaft mode"
            );
        };

        assert!(
            matches!(error, Error::NoKraftControllerReplicas),
            "expected NoKraftControllerReplicas, got: {error:?}"
        );
    }

    #[test]
    fn kraft_mode_rejects_missing_controller_role() {
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
                productVersion: 4.3.1
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );

        let result = crate::controller::test_support::validate_err(&kafka);
        let Err(error) = result else {
            panic!("validate should reject a KRaft cluster without a controllers role");
        };

        assert!(
            matches!(error, Error::MissingKraftControllerRole),
            "expected MissingKraftControllerRole, got: {error:?}"
        );
    }

    #[test]
    fn kraft_mode_rejects_zero_controller_replicas_summed_across_role_groups() {
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
                metadataManager: KRaft
              controllers:
                roleGroups:
                  a:
                    replicas: 0
                  b:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );

        let result = crate::controller::test_support::validate_err(&kafka);
        let Err(error) = result else {
            panic!(
                "validate should reject zero controller replicas while brokers are running in KRaft mode"
            );
        };

        assert!(
            matches!(error, Error::NoKraftControllerReplicas),
            "expected NoKraftControllerReplicas, got: {error:?}"
        );
    }

    /// Controllers *and* brokers at zero together is not rejected
    #[test]
    fn kraft_mode_allows_controllers_and_brokers_at_zero_together() {
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
                metadataManager: KRaft
              controllers:
                roleGroups:
                  default:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 0
            "#,
        );

        let _cluster = validated_cluster(&kafka);
    }

    /// A `replicas: 0` controller role group is fine in ZooKeeper mode: the check only applies
    /// to KRaft, where controllers *are* the metadata quorum.
    #[test]
    fn zookeeper_mode_allows_zero_controller_replicas() {
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
                zookeeperConfigMapName: zk-discovery
              controllers:
                roleGroups:
                  default:
                    replicas: 0
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );

        let _cluster = validated_cluster(&kafka);
    }
}

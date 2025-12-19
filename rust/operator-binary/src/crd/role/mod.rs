pub mod broker;
pub mod commons;
pub mod controller;

use std::{borrow::Cow, ops::Deref};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, Resources},
    config::{
        fragment::{self, ValidationError},
        merge::Merge,
    },
    k8s_openapi::api::core::v1::PodTemplateSpec,
    kube::{ResourceExt, runtime::reflector::ObjectRef},
    product_logging::spec::ContainerLogConfig,
    role_utils::RoleGroupRef,
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::{
    config::jvm::{construct_heap_jvm_args, construct_non_heap_jvm_args},
    crd::role::{
        broker::{BROKER_PROPERTIES_FILE, BrokerConfig, BrokerConfigFragment},
        commons::{CommonConfig, Storage},
        controller::{CONTROLLER_PROPERTIES_FILE, ControllerConfig},
    },
    v1alpha1,
};

/// Env var
pub const KAFKA_NODE_ID_OFFSET: &str = "NODE_ID_OFFSET";

/// Past versions of the operator didn't set this explicitly and allowed Kafka to generate random ids.
/// To support Kraft migration, this must be carried over to `KAFKA_NODE_ID` so the operator needs
/// to know it's value for each broker Pod.
pub const KAFKA_BROKER_ID: &str = "broker.id";

// See: https://kafka.apache.org/documentation/#brokerconfigs
/// The node ID associated with the roles this process is playing when process.roles is non-empty.
/// This is required configuration when running in KRaft mode.
pub const KAFKA_NODE_ID: &str = "node.id";

/// The roles that this process plays: 'broker', 'controller', or 'broker,controller' if it is both.
pub const KAFKA_PROCESS_ROLES: &str = "process.roles";

/// A comma-separated list of the directories where the topic data is stored.
pub const KAFKA_LOG_DIRS: &str = "log.dirs";

/// Listener List - Comma-separated list of URIs we will listen on and the listener names.
/// If the listener name is not a security protocol, listener.security.protocol.map must also be set.
pub const KAFKA_LISTENERS: &str = "listeners";

/// Specifies the listener addresses that the Kafka brokers will advertise to clients and other brokers.
/// The config is useful where the actual listener configuration 'listeners' does not represent the addresses that clients should use to connect,
/// such as in cloud environments. The addresses are published to and managed by the controller, the brokers pull these data from the controller as needed.
/// In IaaS environments, this may need to be different from the interface to which the broker binds. If this is not set, the value for 'listeners' will be used.
/// Unlike 'listeners', it is not valid to advertise the 0.0.0.0 meta-address.
/// Also unlike 'listeners', there can be duplicated ports in this property, so that one listener can be configured to advertise another listener's address.
/// This can be useful in some cases where external load balancers are used.
pub const KAFKA_ADVERTISED_LISTENERS: &str = "advertised.listeners";

/// Map between listener names and security protocols. This must be defined for the same security protocol to be usable in more than one port or IP.
/// For example, internal and external traffic can be separated even if SSL is required for both.
/// Concretely, the user could define listeners with names INTERNAL and EXTERNAL and this property as: INTERNAL:SSL,EXTERNAL:SSL
pub const KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: &str = "listener.security.protocol.map";

/// List of endpoints to use for bootstrapping the cluster metadata. The endpoints are specified in comma-separated list of {host}:{port} entries.
/// For example: localhost:9092,localhost:9093,localhost:9094.
pub const KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS: &str = "controller.quorum.bootstrap.servers";

/// Map of id/endpoint information for the set of voters in a comma-separated list of {id}@{host}:{port} entries.
/// For example: 1@localhost:9092,2@localhost:9093,3@localhost:9094
pub const KAFKA_CONTROLLER_QUORUM_VOTERS: &str = "controller.quorum.voters";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("the Kafka role [{role}] is missing from spec"))]
    MissingRole {
        source: crate::crd::Error,
        role: String,
    },

    #[snafu(display("missing role group {rolegroup:?} for role {role:?}"))]
    MissingRoleGroup { role: String, rolegroup: String },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::config::jvm::Error },
}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum KafkaRole {
    #[strum(serialize = "broker")]
    Broker,
    #[strum(serialize = "controller")]
    Controller,
}

impl KafkaRole {
    /// Return all available roles
    pub fn roles() -> Vec<KafkaRole> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role)
        }
        roles
    }

    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::KafkaCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(kafka),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// but is similar to HBase).
    pub fn kerberos_service_name(&self) -> &'static str {
        "kafka"
    }

    /// Merge the [Broker|Controller]ConfigFragment defaults, role and role group settings.
    /// The priority is: default < role config < role_group config
    pub fn merged_config(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<AnyConfig, Error> {
        match self {
            Self::Broker => {
                // Initialize the result with all default values as baseline
                let default_config =
                    BrokerConfig::default_config(&kafka.name_any(), &self.to_string());

                // Retrieve role resource config
                let role = kafka.broker_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?;

                let mut role_config = role.config.config.clone();
                // Retrieve rolegroup specific resource config
                let mut role_group_config = role
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                // Merge more specific configs into default config
                // Hierarchy is:
                // 1. RoleGroup
                // 2. Role
                // 3. Default
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyConfig::Broker(
                    fragment::validate::<BrokerConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
            Self::Controller => {
                // Initialize the result with all default values as baseline
                let default_config =
                    ControllerConfig::default_config(&kafka.name_any(), &self.to_string());

                // Retrieve role resource config
                let role = kafka.controller_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?;

                let mut role_config = role.config.config.clone();
                // Retrieve rolegroup specific resource config
                let mut role_group_config = role
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .config
                    .config
                    .clone();

                // Merge more specific configs into default config
                // Hierarchy is:
                // 1. RoleGroup
                // 2. Role
                // 3. Default
                role_config.merge(&default_config);
                role_group_config.merge(&role_config);
                Ok(AnyConfig::Controller(
                    fragment::validate::<ControllerConfig>(role_group_config)
                        .context(FragmentValidationFailureSnafu)?,
                ))
            }
        }
    }

    pub fn construct_non_heap_jvm_args(
        &self,
        merged_config: &AnyConfig,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<String, Error> {
        match self {
            Self::Broker => construct_non_heap_jvm_args::<BrokerConfigFragment>(
                merged_config,
                kafka.broker_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
            Self::Controller => construct_non_heap_jvm_args(
                merged_config,
                kafka.controller_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
        }
    }

    pub fn construct_heap_jvm_args(
        &self,
        merged_config: &AnyConfig,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<String, Error> {
        match self {
            Self::Broker => construct_heap_jvm_args::<BrokerConfigFragment>(
                merged_config,
                kafka.broker_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
            Self::Controller => construct_heap_jvm_args(
                merged_config,
                kafka.controller_role().with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?,
                rolegroup,
            )
            .context(ConstructJvmArgumentsSnafu),
        }
    }

    pub fn role_pod_overrides(
        &self,
        kafka: &v1alpha1::KafkaCluster,
    ) -> Result<PodTemplateSpec, Error> {
        let pod_overrides = match self {
            Self::Broker => kafka
                .broker_role()
                .with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
            Self::Controller => kafka
                .controller_role()
                .with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
        };

        Ok(pod_overrides)
    }

    pub fn role_group_pod_overrides(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<PodTemplateSpec, Error> {
        let pod_overrides = match self {
            Self::Broker => kafka
                .broker_role()
                .with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .role_groups
                .get(rolegroup)
                .with_context(|| MissingRoleGroupSnafu {
                    role: self.to_string(),
                    rolegroup: rolegroup.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
            Self::Controller => kafka
                .controller_role()
                .with_context(|_| MissingRoleSnafu {
                    role: self.to_string(),
                })?
                .role_groups
                .get(rolegroup)
                .with_context(|| MissingRoleGroupSnafu {
                    role: self.to_string(),
                    rolegroup: rolegroup.to_string(),
                })?
                .config
                .pod_overrides
                .clone(),
        };

        Ok(pod_overrides)
    }

    pub fn replicas(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        rolegroup: &str,
    ) -> Result<Option<u16>, Error> {
        let replicas = match self {
            Self::Broker => {
                kafka
                    .broker_role()
                    .with_context(|_| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .replicas
            }
            Self::Controller => {
                kafka
                    .controller_role()
                    .with_context(|_| MissingRoleSnafu {
                        role: self.to_string(),
                    })?
                    .role_groups
                    .get(rolegroup)
                    .with_context(|| MissingRoleGroupSnafu {
                        role: self.to_string(),
                        rolegroup: rolegroup.to_string(),
                    })?
                    .replicas
            }
        };

        Ok(replicas)
    }
}

/// Configuration for a role and rolegroup of an unknown type.
#[derive(Debug)]
pub enum AnyConfig {
    Broker(BrokerConfig),
    Controller(ControllerConfig),
}

impl Deref for AnyConfig {
    type Target = CommonConfig;

    fn deref(&self) -> &Self::Target {
        match self {
            AnyConfig::Broker(broker_config) => &broker_config.common_config,
            AnyConfig::Controller(controller_config) => &controller_config.common_config,
        }
    }
}

impl AnyConfig {
    pub fn resources(&self) -> &Resources<Storage, NoRuntimeLimits> {
        match self {
            AnyConfig::Broker(broker_config) => &broker_config.resources,
            AnyConfig::Controller(controller_config) => &controller_config.resources,
        }
    }

    // Logging config is distinct between each role, due to the different enum types,
    // so provide helpers for containers that are common between all roles.
    pub fn kafka_logging(&'_ self) -> Cow<'_, ContainerLogConfig> {
        match self {
            AnyConfig::Broker(node) => node.logging.for_container(&broker::BrokerContainer::Kafka),
            AnyConfig::Controller(node) => node
                .logging
                .for_container(&controller::ControllerContainer::Kafka),
        }
    }

    pub fn vector_logging(&'_ self) -> Cow<'_, ContainerLogConfig> {
        match &self {
            AnyConfig::Broker(broker_config) => broker_config
                .logging
                .for_container(&broker::BrokerContainer::Vector),
            AnyConfig::Controller(controller_config) => controller_config
                .logging
                .for_container(&controller::ControllerContainer::Vector),
        }
    }

    pub fn vector_logging_enabled(&self) -> bool {
        match self {
            AnyConfig::Broker(broker_config) => broker_config.logging.enable_vector_agent,
            AnyConfig::Controller(controller_config) => {
                controller_config.logging.enable_vector_agent
            }
        }
    }

    pub fn listener_class(&self) -> Option<&String> {
        match self {
            AnyConfig::Broker(broker_config) => Some(&broker_config.broker_listener_class),
            AnyConfig::Controller(_) => None,
        }
    }

    pub fn config_file_name(&self) -> &str {
        match self {
            AnyConfig::Broker(_) => BROKER_PROPERTIES_FILE,
            AnyConfig::Controller(_) => CONTROLLER_PROPERTIES_FILE,
        }
    }
}

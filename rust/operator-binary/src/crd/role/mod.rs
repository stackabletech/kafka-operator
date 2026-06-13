pub mod broker;
pub mod commons;
pub mod controller;

use std::{borrow::Cow, ops::Deref};

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, Resources},
    product_logging::spec::ContainerLogConfig,
    schemars::{self, JsonSchema},
    v2::config_overrides::KeyValueConfigOverrides,
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::{
    crd::role::{
        broker::BrokerConfig,
        commons::{CommonConfig, Storage},
        controller::ControllerConfig,
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

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
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

    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// but is similar to HBase).
    pub fn kerberos_service_name(&self) -> &'static str {
        "kafka"
    }
}

/// Configuration for a role and rolegroup of an unknown type.
#[derive(Clone, Debug, PartialEq)]
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
    /// The [`KafkaRole`] this config belongs to.
    pub fn kafka_role(&self) -> KafkaRole {
        match self {
            AnyConfig::Broker(_) => KafkaRole::Broker,
            AnyConfig::Controller(_) => KafkaRole::Controller,
        }
    }

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

    pub fn listener_class(&self) -> Option<&String> {
        match self {
            AnyConfig::Broker(broker_config) => Some(&broker_config.broker_listener_class),
            AnyConfig::Controller(_) => None,
        }
    }
}

/// Merged role/role-group `configOverrides` for a role group of an unknown type.
///
/// Mirrors [`AnyConfig`] for the override side: broker and controller use distinct
/// override structs, so this enum lets the build layer carry the typed, merged
/// overrides through a single role-agnostic `RoleGroupConfig`.
#[derive(Clone, Debug, PartialEq)]
pub enum AnyConfigOverrides {
    Broker(v1alpha1::KafkaBrokerConfigOverrides),
    Controller(v1alpha1::KafkaControllerConfigOverrides),
}

impl AnyConfigOverrides {
    /// The merged product config-file overrides (`broker.properties` for brokers,
    /// `controller.properties` for controllers).
    pub fn config_file_overrides(&self) -> &KeyValueConfigOverrides {
        match self {
            AnyConfigOverrides::Broker(o) => &o.broker_properties,
            AnyConfigOverrides::Controller(o) => &o.controller_properties,
        }
    }

    /// The merged `security.properties` overrides (shared by both roles).
    pub fn security_properties(&self) -> &KeyValueConfigOverrides {
        match self {
            AnyConfigOverrides::Broker(o) => &o.security_properties,
            AnyConfigOverrides::Controller(o) => &o.security_properties,
        }
    }
}

pub mod broker;
pub mod commons;
pub mod controller;

use std::{fmt, ops::Deref, str::FromStr};

use stackable_operator::{
    constant,
    v2::{builder::pod::container::EnvVarName, types::operator::RoleName},
};
use strum::EnumIter;

constant!(pub KAFKA_NODE_ID_OFFSET: EnvVarName = "NODE_ID_OFFSET");

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

constant!(BROKER_ROLE_NAME: RoleName = "broker");
constant!(CONTROLLER_ROLE_NAME: RoleName = "controller");

#[derive(Clone, Debug, EnumIter, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KafkaRole {
    Broker,
    Controller,
}

impl fmt::Display for KafkaRole {
    /// The role's name, e.g. `broker`.
    ///
    /// `to_string()` already worked through the [`Deref`] to [`RoleName`]; this impl exists so a
    /// role can be a `{role}` field in snafu `display` strings, which require `Display` itself.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}

impl Deref for KafkaRole {
    type Target = RoleName;

    fn deref(&self) -> &Self::Target {
        match self {
            KafkaRole::Broker => &BROKER_ROLE_NAME,
            KafkaRole::Controller => &CONTROLLER_ROLE_NAME,
        }
    }
}

impl KafkaRole {
    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// but is similar to HBase).
    pub fn kerberos_service_name(&self) -> &'static str {
        "kafka"
    }

    /// The type-safe name of this role, e.g. to build [`ResourceNames`][rn].
    ///
    /// [rn]: stackable_operator::v2::role_group_utils::ResourceNames
    pub fn role_name(&self) -> RoleName {
        RoleName::clone(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *BROKER_ROLE_NAME;
        let _ = *CONTROLLER_ROLE_NAME;
        let _ = *KAFKA_NODE_ID_OFFSET;
    }
}

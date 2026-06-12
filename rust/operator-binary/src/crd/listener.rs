use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use strum::EnumString;

pub(crate) const LISTENER_LOCAL_ADDRESS: &str = "0.0.0.0";

#[derive(strum::Display, Debug, EnumString)]
pub enum KafkaListenerProtocol {
    /// Unencrypted and unauthenticated HTTP connections
    #[strum(serialize = "PLAINTEXT")]
    Plaintext,

    /// Encrypted and server-authenticated HTTPS connections
    #[strum(serialize = "SSL")]
    Ssl,

    /// Kerberos authentication
    #[strum(serialize = "SASL_SSL")]
    SaslSsl,
}

#[derive(strum::Display, Debug, EnumString, Ord, Eq, PartialEq, PartialOrd)]
pub enum KafkaListenerName {
    /// The purpose of this listener is to handle client/broker communications.
    /// It can be configured to use the SSL or SASL_SSL (Kerberos) protocols
    /// if the brokers use TLS for communication (and possible authentication).
    /// The PLAINTEXT protocol is used when `spec.clusterConfig.tls.serverSecretClass: null`
    /// The advertised listener hosts are derived from the pod (broker) listener volume.
    #[strum(serialize = "CLIENT")]
    Client,
    /// The purpose if this listener is to handle broker internal communications.
    /// Unlike the client listener, the only protocol used here is SSL even when
    /// `spec.clusterConfig.tls.internalSecretClass: null`.
    /// The advertised listener hosts are the same as the client (broker listener host)
    /// but with a different port.
    #[strum(serialize = "INTERNAL")]
    Internal,
    /// This is almost identical with the `Client` listener with the following exceptions:
    ///
    /// - it is only defined if Kerberos is enabled
    /// - it uses a different port
    /// - the keystore associated with the listener volume uses a different CA
    ///
    /// Note: the corresponding K8S service is *always* defined, not just if Kerberos is enabled
    /// and it is published in the discovery ConfigMap for clients to consume.
    #[strum(serialize = "BOOTSTRAP")]
    Bootstrap,
    /// This listener is defined when Kraft mode is enabled.
    /// It is responsible for broker/controller as well as controller/controller communications
    /// and therefore it is present on *both* brokers and controller properties files.
    /// The only protocol used is SSL.
    /// The advertised host names are FQDN pod names of the controllers.
    ///
    /// Notes:
    ///
    /// - there is no listener for client/controller communication
    /// - this listener does not support SSL_SASL.
    #[strum(serialize = "CONTROLLER")]
    Controller,
}

impl KafkaListenerName {
    pub fn listener_ssl_keystore_location(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.keystore.location",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_keystore_password(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.keystore.password",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_keystore_type(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.keystore.type",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_truststore_location(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.truststore.location",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_truststore_password(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.truststore.password",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_truststore_type(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.truststore.type",
            listener_name = self.to_string().to_lowercase()
        )
    }

    pub fn listener_ssl_client_auth(&self) -> String {
        format!(
            "listener.name.{listener_name}.ssl.client.auth",
            listener_name = self.to_string().to_lowercase()
        )
    }
}

#[derive(Debug)]
pub struct KafkaListenerConfig {
    pub(crate) listeners: Vec<KafkaListener>,
    pub(crate) advertised_listeners: Vec<KafkaListener>,
    pub(crate) listener_security_protocol_map: BTreeMap<KafkaListenerName, KafkaListenerProtocol>,
}

impl KafkaListenerConfig {
    /// Returns the `listeners` for the Kafka `broker.properties` config.
    pub fn listeners(&self) -> String {
        self.listeners
            .iter()
            .map(|listener| listener.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Returns the `advertised.listeners` for the Kafka `broker.properties` config.
    /// May contain ENV variables and therefore should be used as cli argument
    /// like --override \"advertised.listeners=xxx\".
    pub fn advertised_listeners(&self) -> String {
        self.advertised_listeners
            .iter()
            .map(|listener| listener.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Returns the `listener.security.protocol.map` for the Kafka `broker.properties` config.
    pub fn listener_security_protocol_map(&self) -> String {
        self.listener_security_protocol_map
            .iter()
            .map(|(name, protocol)| format!("{name}:{protocol}"))
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Returns the `listener.security.protocol.map` for the Kraft controller.
    /// This map must include the internal broker listener too.
    pub fn listener_security_protocol_map_for_controller(&self) -> String {
        self.listener_security_protocol_map
            .iter()
            .filter(|(name, _)| {
                *name == &KafkaListenerName::Internal || *name == &KafkaListenerName::Controller
            })
            .map(|(name, protocol)| format!("{name}:{protocol}"))
            .collect::<Vec<String>>()
            .join(",")
    }
}

#[derive(Debug)]
pub(crate) struct KafkaListener {
    pub(crate) name: KafkaListenerName,
    pub(crate) host: String,
    pub(crate) port: String,
}

impl Display for KafkaListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.name, self.host, self.port)
    }
}

pub fn node_address_cmd_env(directory: &str) -> String {
    format!("$(cat {directory}/default-address/address)")
}

pub fn node_port_cmd_env(directory: &str, port_name: &str) -> String {
    format!("$(cat {directory}/default-address/ports/{port_name})")
}

pub fn node_address_cmd(directory: &str) -> String {
    format!("${{file:UTF-8:{directory}/default-address/address}}")
}

pub fn node_port_cmd(directory: &str, port_name: &str) -> String {
    format!("${{file:UTF-8:{directory}/default-address/ports/{port_name}}}")
}

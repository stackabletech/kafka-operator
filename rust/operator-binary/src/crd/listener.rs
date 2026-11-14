use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use strum::EnumString;

pub(crate) const LISTENER_LOCAL_ADDRESS: &str = "0.0.0.0";

// Layout of the listener volume mounted by the listener operator: the default address is
// exposed under `<dir>/default-address/address` and per-port values under
// `<dir>/default-address/ports/<port-name>`.
const LISTENER_DEFAULT_ADDRESS_DIR: &str = "default-address";
const LISTENER_ADDRESS_FILE: &str = "address";
const LISTENER_PORTS_DIR: &str = "ports";

#[derive(strum::Display, Debug, EnumString)]
pub enum KafkaListenerProtocol {
    /// Unencrypted and unauthenticated Kafka protocol connections
    #[strum(serialize = "PLAINTEXT")]
    Plaintext,

    /// TLS-encrypted Kafka protocol connections, with optional client certificate authentication
    /// (`ssl.client.auth`)
    #[strum(serialize = "SSL")]
    Ssl,

    /// SASL authentication over TLS. The operator only configures the GSSAPI (Kerberos) mechanism.
    #[strum(serialize = "SASL_SSL")]
    SaslSsl,
}

#[derive(strum::Display, Debug, EnumString, Ord, Eq, PartialEq, PartialOrd)]
pub enum KafkaListenerName {
    /// Handles client/broker communication. The protocol is:
    ///
    /// - SASL_SSL if Kerberos is enabled
    /// - SSL if `spec.clusterConfig.tls.serverSecretClass` or a TLS AuthenticationClass is set
    ///   (client certificates are required with the AuthenticationClass)
    /// - PLAINTEXT otherwise
    ///
    /// The advertised host and port are read from the broker listener volume.
    #[strum(serialize = "CLIENT")]
    Client,
    /// Handles broker/broker communication (`inter.broker.listener.name`).
    /// The protocol is always SSL with client certificates required, using the pod certificates
    /// of `spec.clusterConfig.tls.internalSecretClass` (default `tls`), also when Kerberos is
    /// enabled.
    /// The advertised host is the FQDN of the broker pod in the headless service.
    #[strum(serialize = "INTERNAL")]
    Internal,
    /// Like the `Client` listener with SASL_SSL, with the following differences:
    ///
    /// - it is only defined if Kerberos is enabled
    /// - it uses a different port
    /// - the Kerberos principal is derived from the bootstrap listener address instead of the
    ///   broker listener address
    ///
    /// It uses the same TLS keystore as the `Client` listener, whose certificate covers both the
    /// broker and the bootstrap listener addresses.
    ///
    /// Note: the bootstrap `Listener` is created for every broker role group, not only if
    /// Kerberos is enabled. Its addresses are published in the discovery ConfigMap, with the
    /// bootstrap port if Kerberos is enabled and the client port otherwise.
    #[strum(serialize = "BOOTSTRAP")]
    Bootstrap,
    /// Handles broker/controller and controller/controller communication in KRaft mode.
    /// Only the controllers bind this listener; brokers connect to it as clients, so it appears
    /// in the `controller.listener.names` and the security protocol map of both properties files.
    /// The protocol is always SSL with client certificates required, using the pod certificates
    /// of the internal `SecretClass`, also when Kerberos is enabled.
    /// The controllers are addressed by their pod FQDNs (`controller.quorum.bootstrap.servers`).
    ///
    /// Note: there is no listener for client/controller communication. The only other clients
    /// are the quorum management admin tools running inside the controller pods, which use the
    /// same pod certificates.
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
    format!("$(cat {directory}/{LISTENER_DEFAULT_ADDRESS_DIR}/{LISTENER_ADDRESS_FILE})")
}

pub fn node_port_cmd_env(directory: &str, port_name: &str) -> String {
    format!("$(cat {directory}/{LISTENER_DEFAULT_ADDRESS_DIR}/{LISTENER_PORTS_DIR}/{port_name})")
}

pub fn node_address_cmd(directory: &str) -> String {
    format!("${{file:UTF-8:{directory}/{LISTENER_DEFAULT_ADDRESS_DIR}/{LISTENER_ADDRESS_FILE}}}")
}

pub fn node_port_cmd(directory: &str, port_name: &str) -> String {
    format!(
        "${{file:UTF-8:{directory}/{LISTENER_DEFAULT_ADDRESS_DIR}/{LISTENER_PORTS_DIR}/{port_name}}}"
    )
}

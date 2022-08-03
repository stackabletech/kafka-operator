use crate::{
    KafkaCluster, CLIENT_PORT, CLIENT_PORT_NAME, INTERNAL_PORT, SECURE_CLIENT_PORT,
    SECURE_CLIENT_PORT_NAME, SECURE_INTERNAL_PORT, STACKABLE_TMP_DIR,
};
use snafu::{OptionExt, Snafu};
use stackable_operator::kube::ResourceExt;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use strum::{EnumDiscriminants, EnumString};

const LISTENER_LOCAL_ADDRESS: &str = "0.0.0.0";
const LISTENER_NODE_ADDRESS: &str = "$NODE";

#[derive(Snafu, Debug, EnumDiscriminants)]
pub enum KafkaListenerError {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
}

#[derive(strum::Display, Debug, EnumString)]
pub enum KafkaListenerProtocol {
    /// Unencrypted and authenticated HTTP connections
    #[strum(serialize = "PLAINTEXT")]
    Plaintext,
    /// Encrypted and server-authenticated HTTPS connections
    #[strum(serialize = "SSL")]
    Ssl,
}

#[derive(strum::Display, Debug, EnumString, Ord, Eq, PartialEq, PartialOrd)]
pub enum KafkaListenerName {
    #[strum(serialize = "CLIENT")]
    Client,
    #[strum(serialize = "CLIENT_AUTH")]
    ClientAuth,
    #[strum(serialize = "INTERNAL")]
    Internal,
}

#[derive(Debug)]
pub struct KafkaListenerConfig {
    listeners: Vec<KafkaListener>,
    advertised_listeners: Vec<KafkaListener>,
    listener_security_protocol_map: BTreeMap<KafkaListenerName, KafkaListenerProtocol>,
}

impl KafkaListenerConfig {
    /// Returns the `listeners` for the Kafka `server.properties` config.
    pub fn listeners(&self) -> String {
        self.listeners
            .iter()
            .map(|listener| listener.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Returns the `advertised.listeners` for the Kafka `server.properties` config.
    /// May contain ENV variables and therefore should be used as cli argument
    /// like --override \"advertised.listeners=xxx\".
    pub fn advertised_listeners(&self) -> String {
        self.advertised_listeners
            .iter()
            .map(|listener| listener.to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    /// Returns the `listener.security.protocol.map` for the Kafka `server.properties` config.
    pub fn listener_security_protocol_map(&self) -> String {
        self.listener_security_protocol_map
            .iter()
            .map(|(name, protocol)| format!("{}:{}", name, protocol))
            .collect::<Vec<String>>()
            .join(",")
    }
}

#[derive(Debug)]
struct KafkaListener {
    name: KafkaListenerName,
    host: String,
    port: String,
}

impl Display for KafkaListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.name, self.host, self.port)
    }
}

pub fn get_kafka_listener_config(
    kafka: &KafkaCluster,
    object_name: &str,
) -> Result<KafkaListenerConfig, KafkaListenerError> {
    let pod_fqdn = pod_fqdn(kafka, object_name)?;
    let mut listeners = vec![];
    let mut advertised_listeners = vec![];
    let mut listener_security_protocol_map = BTreeMap::new();

    if kafka.client_authentication_class().is_some() {
        // 1) If client authentication required, we expose only CLIENT_AUTH connection with SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::ClientAuth,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: SECURE_CLIENT_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::ClientAuth,
            host: LISTENER_NODE_ADDRESS.to_string(),
            port: node_port_cmd(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::ClientAuth, KafkaListenerProtocol::Ssl);
    } else if kafka.client_tls_secret_class().is_some() {
        // 2) If no client authentication but tls is required we expose CLIENT with SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: SECURE_CLIENT_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_NODE_ADDRESS.to_string(),
            port: node_port_cmd(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Client, KafkaListenerProtocol::Ssl);
    } else {
        // 3) If no client auth or tls is required we expose CLIENT with PLAINTEXT
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: CLIENT_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_NODE_ADDRESS.to_string(),
            port: node_port_cmd(STACKABLE_TMP_DIR, CLIENT_PORT_NAME),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Client, KafkaListenerProtocol::Plaintext);
    }

    if kafka.internal_tls_secret_class().is_some() {
        // 4) If internal tls is required we expose INTERNAL as SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: SECURE_INTERNAL_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn,
            port: SECURE_INTERNAL_PORT.to_string(),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
    } else {
        // 5) If no internal tls is required we expose INTERNAL as PLAINTEXT
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: INTERNAL_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn,
            port: INTERNAL_PORT.to_string(),
        });
        listener_security_protocol_map.insert(
            KafkaListenerName::Internal,
            KafkaListenerProtocol::Plaintext,
        );
    }

    Ok(KafkaListenerConfig {
        listeners,
        advertised_listeners,
        listener_security_protocol_map,
    })
}

fn node_port_cmd(directory: &str, port_name: &str) -> String {
    format!("$(cat {}/{}_nodeport)", directory, port_name)
}

fn pod_fqdn(kafka: &KafkaCluster, object_name: &str) -> Result<String, KafkaListenerError> {
    Ok(format!(
        "$POD_NAME.{}.{}.svc.cluster.local",
        object_name,
        kafka.namespace().context(ObjectHasNoNamespaceSnafu)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_kafka_listeners_config() {
        let object_name = "simple-kafka-broker-default";

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls:
              secretClass: tls
            clientAuthentication:
              authenticationClass: kafka-client-tls
            internalTls:
              secretClass: internalTls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let config = get_kafka_listener_config(&kafka, object_name).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::ClientAuth,
                host = LISTENER_LOCAL_ADDRESS,
                port = SECURE_CLIENT_PORT,
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = SECURE_INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::ClientAuth,
                host = LISTENER_NODE_ADDRESS,
                port = node_port_cmd(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name).unwrap(),
                internal_port = SECURE_INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol}",
                name = KafkaListenerName::ClientAuth,
                protocol = KafkaListenerProtocol::Ssl,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Ssl
            )
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls:
              secretClass: tls
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let config = get_kafka_listener_config(&kafka, object_name).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_LOCAL_ADDRESS,
                port = SECURE_CLIENT_PORT,
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_NODE_ADDRESS,
                port = node_port_cmd(STACKABLE_TMP_DIR, SECURE_CLIENT_PORT_NAME),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name).unwrap(),
                internal_port = INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol}",
                name = KafkaListenerName::Client,
                protocol = KafkaListenerProtocol::Ssl,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Plaintext
            )
        );

        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          version: abc
          zookeeperConfigMapName: xyz
          config:
            tls: null
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let config = get_kafka_listener_config(&kafka, object_name).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_LOCAL_ADDRESS,
                port = CLIENT_PORT,
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_NODE_ADDRESS,
                port = node_port_cmd(STACKABLE_TMP_DIR, CLIENT_PORT_NAME),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name).unwrap(),
                internal_port = INTERNAL_PORT,
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol}",
                name = KafkaListenerName::Client,
                protocol = KafkaListenerProtocol::Plaintext,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Plaintext
            )
        );
    }
}

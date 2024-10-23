use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use snafu::{OptionExt, Snafu};
use stackable_operator::kube::ResourceExt;
use stackable_operator::utils::cluster_info::KubernetesClusterInfo;
use strum::{EnumDiscriminants, EnumString};

use crate::security::KafkaTlsSecurity;
use crate::{KafkaCluster, STACKABLE_LISTENER_BROKER_DIR};

const LISTENER_LOCAL_ADDRESS: &str = "0.0.0.0";

#[derive(Snafu, Debug, EnumDiscriminants)]
pub enum KafkaListenerError {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
}

#[derive(strum::Display, Debug, EnumString)]
pub enum KafkaListenerProtocol {
    /// Unencrypted and unauthenticated HTTP connections
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
            .map(|(name, protocol)| format!("{name}:{protocol}"))
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
    kafka_security: &KafkaTlsSecurity,
    object_name: &str,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KafkaListenerConfig, KafkaListenerError> {
    let pod_fqdn = pod_fqdn(kafka, object_name, cluster_info)?;
    let mut listeners = vec![];
    let mut advertised_listeners = vec![];
    let mut listener_security_protocol_map = BTreeMap::new();

    if kafka_security.tls_client_authentication_class().is_some() {
        // 1) If client authentication required, we expose only CLIENT_AUTH connection with SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::ClientAuth,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.client_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::ClientAuth,
            host: node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
            port: node_port_cmd(
                STACKABLE_LISTENER_BROKER_DIR,
                kafka_security.client_port_name(),
            ),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::ClientAuth, KafkaListenerProtocol::Ssl);
    } else if kafka_security.tls_server_secret_class().is_some() {
        // 2) If no client authentication but tls is required we expose CLIENT with SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.client_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
            port: node_port_cmd(
                STACKABLE_LISTENER_BROKER_DIR,
                kafka_security.client_port_name(),
            ),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Client, KafkaListenerProtocol::Ssl);
    } else {
        // 3) If no client auth or tls is required we expose CLIENT with PLAINTEXT
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: KafkaTlsSecurity::CLIENT_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
            port: node_port_cmd(
                STACKABLE_LISTENER_BROKER_DIR,
                kafka_security.client_port_name(),
            ),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Client, KafkaListenerProtocol::Plaintext);
    }

    if kafka_security.tls_internal_secret_class().is_some() {
        // 4) If internal tls is required we expose INTERNAL as SSL
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.internal_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn,
            port: kafka_security.internal_port().to_string(),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
    } else {
        // 5) If no internal tls is required we expose INTERNAL as PLAINTEXT
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.internal_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn,
            port: kafka_security.internal_port().to_string(),
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

fn node_address_cmd(directory: &str) -> String {
    format!("$(cat {directory}/default-address/address)")
}

fn node_port_cmd(directory: &str, port_name: &str) -> String {
    format!("$(cat {directory}/default-address/ports/{port_name})")
}

fn pod_fqdn(
    kafka: &KafkaCluster,
    object_name: &str,
    cluster_info: &KubernetesClusterInfo,
) -> Result<String, KafkaListenerError> {
    // We need to init the variable first in tests
    let cluster_domain = &cluster_info.cluster_domain;
    Ok(format!(
        "$POD_NAME.{object_name}.{namespace}.svc.{cluster_domain}",
        object_name = object_name,
        namespace = kafka.namespace().context(ObjectHasNoNamespaceSnafu)?,
        cluster_domain = cluster_domain
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::ResolvedAuthenticationClasses;

    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        commons::{
            authentication::{
                tls::AuthenticationProvider, AuthenticationClass, AuthenticationClassProvider,
                AuthenticationClassSpec,
            },
            networking::DomainName,
        },
    };

    fn default_cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }

    #[test]
    fn test_get_kafka_listeners_config() {
        let object_name = "simple-kafka-broker-default";
        let cluster_info = default_cluster_info();

        let kafka_cluster = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          image:
            productVersion: 3.7.1
          clusterConfig:
            authentication:
              - authenticationClass: kafka-client-tls
            tls:
              internalSecretClass: internalTls
              serverSecretClass: tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(kafka_cluster).expect("illegal test input");
        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![AuthenticationClass {
                metadata: ObjectMetaBuilder::new().name("auth-class").build(),
                spec: AuthenticationClassSpec {
                    provider: AuthenticationClassProvider::Tls(AuthenticationProvider {
                        client_cert_secret_class: Some("client-auth-secret-class".to_string()),
                    }),
                },
            }]),
            "internalTls".to_string(),
            Some("tls".to_string()),
        );
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, object_name, &cluster_info).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::ClientAuth,
                host = LISTENER_LOCAL_ADDRESS,
                port = kafka_security.client_port(),
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::ClientAuth,
                host = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
                port = node_port_cmd(
                    STACKABLE_LISTENER_BROKER_DIR,
                    kafka_security.client_port_name()
                ),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name, &cluster_info).unwrap(),
                internal_port = kafka_security.internal_port(),
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
          image:
            productVersion: 3.7.1
          clusterConfig:
            tls:
              serverSecretClass: tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "tls".to_string(),
            Some("tls".to_string()),
        );
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, object_name, &cluster_info).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_LOCAL_ADDRESS,
                port = kafka_security.client_port(),
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
                port = node_port_cmd(
                    STACKABLE_LISTENER_BROKER_DIR,
                    kafka_security.client_port_name()
                ),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name, &cluster_info).unwrap(),
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol}",
                name = KafkaListenerName::Client,
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
          image:
            productVersion: 3.7.1
          zookeeperConfigMapName: xyz
          clusterConfig:
            tls:
              internalSecretClass: null
              serverSecretClass: null
            zookeeperConfigMapName: xyz
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "".to_string(),
            None,
        );
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, object_name, &cluster_info).unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_LOCAL_ADDRESS,
                port = kafka_security.client_port(),
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port}",
                name = KafkaListenerName::Client,
                host = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
                port = node_port_cmd(
                    STACKABLE_LISTENER_BROKER_DIR,
                    kafka_security.client_port_name()
                ),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(&kafka, object_name, &cluster_info).unwrap(),
                internal_port = kafka_security.internal_port(),
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

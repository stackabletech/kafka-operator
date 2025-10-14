use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

use snafu::{OptionExt, Snafu};
use stackable_operator::{
    kube::ResourceExt, role_utils::RoleGroupRef, utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, EnumString};

use crate::crd::{STACKABLE_LISTENER_BROKER_DIR, security::KafkaTlsSecurity, v1alpha1};

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

    /// Kerberos authentication
    #[strum(serialize = "SASL_SSL")]
    SaslSsl,
}

#[derive(strum::Display, Debug, EnumString, Ord, Eq, PartialEq, PartialOrd)]
pub enum KafkaListenerName {
    #[strum(serialize = "CLIENT")]
    Client,
    #[strum(serialize = "CLIENT_AUTH")]
    ClientAuth,
    #[strum(serialize = "INTERNAL")]
    Internal,
    #[strum(serialize = "BOOTSTRAP")]
    Bootstrap,
    #[strum(serialize = "CONTROLLER")]
    Controller,
    #[strum(serialize = "CONTROLLER_AUTH")]
    ControllerAuth,
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

    pub fn listener_gssapi_sasl_jaas_config(&self) -> String {
        format!(
            "listener.name.{listener_name}.gssapi.sasl.jaas.config",
            listener_name = self.to_string().to_lowercase()
        )
    }
}

#[derive(Debug)]
pub struct KafkaListenerConfig {
    listeners: Vec<KafkaListener>,
    advertised_listeners: Vec<KafkaListener>,
    listener_security_protocol_map: BTreeMap<KafkaListenerName, KafkaListenerProtocol>,
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

    /// Returns the `listener.security.protocol.map` for the Kafka `broker.properties` config.
    pub fn listener_security_protocol_map_for_listener(
        &self,
        listener_name: &KafkaListenerName,
    ) -> Option<String> {
        self.listener_security_protocol_map
            .get(listener_name)
            .map(|protocol| format!("{listener_name}:{protocol}"))
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
    kafka: &v1alpha1::KafkaCluster,
    kafka_security: &KafkaTlsSecurity,
    rolegroup_ref: &RoleGroupRef<v1alpha1::KafkaCluster>,
    cluster_info: &KubernetesClusterInfo,
) -> Result<KafkaListenerConfig, KafkaListenerError> {
    let pod_fqdn = pod_fqdn(
        kafka,
        &rolegroup_ref.rolegroup_headless_service_name(),
        cluster_info,
    )?;
    let mut listeners = vec![];
    let mut advertised_listeners = vec![];
    let mut listener_security_protocol_map: BTreeMap<KafkaListenerName, KafkaListenerProtocol> =
        BTreeMap::new();

    // CLIENT
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
        listener_security_protocol_map.insert(
            KafkaListenerName::ControllerAuth,
            KafkaListenerProtocol::Ssl,
        );
    } else if kafka_security.has_kerberos_enabled() {
        // 2) Kerberos and TLS authentication classes are mutually exclusive
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: KafkaTlsSecurity::SECURE_CLIENT_PORT.to_string(),
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
            .insert(KafkaListenerName::Client, KafkaListenerProtocol::SaslSsl);
        listener_security_protocol_map.insert(
            KafkaListenerName::Controller,
            KafkaListenerProtocol::SaslSsl,
        );
    } else if kafka_security.tls_server_secret_class().is_some() {
        // 3) If no client authentication but tls is required we expose CLIENT with SSL
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
        // 4) If no client auth or tls is required we expose CLIENT with PLAINTEXT
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

    // INTERNAL / CONTROLLER
    if kafka_security.has_kerberos_enabled() || kafka_security.tls_internal_secret_class().is_some()
    {
        // 5) & 6) Kerberos and TLS authentication classes are mutually exclusive but both require internal tls to be used
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: KafkaTlsSecurity::SECURE_INTERNAL_PORT.to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn.to_string(),
            port: KafkaTlsSecurity::SECURE_INTERNAL_PORT.to_string(),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
        listener_security_protocol_map
            .insert(KafkaListenerName::Controller, KafkaListenerProtocol::Ssl);
    } else {
        // 7) If no internal tls is required we expose INTERNAL as PLAINTEXT
        listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.internal_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Internal,
            host: pod_fqdn.to_string(),
            port: kafka_security.internal_port().to_string(),
        });
        listener_security_protocol_map.insert(
            KafkaListenerName::Internal,
            KafkaListenerProtocol::Plaintext,
        );
        listener_security_protocol_map.insert(
            KafkaListenerName::Controller,
            KafkaListenerProtocol::Plaintext,
        );
    }

    // BOOTSTRAP
    if kafka_security.has_kerberos_enabled() {
        listeners.push(KafkaListener {
            name: KafkaListenerName::Bootstrap,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: kafka_security.bootstrap_port().to_string(),
        });
        advertised_listeners.push(KafkaListener {
            name: KafkaListenerName::Bootstrap,
            host: node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
            port: node_port_cmd(
                STACKABLE_LISTENER_BROKER_DIR,
                kafka_security.client_port_name(),
            ),
        });
        listener_security_protocol_map
            .insert(KafkaListenerName::Bootstrap, KafkaListenerProtocol::SaslSsl);
    }

    Ok(KafkaListenerConfig {
        listeners,
        advertised_listeners,
        listener_security_protocol_map,
    })
}

pub fn node_address_cmd(directory: &str) -> String {
    format!("$(cat {directory}/default-address/address)")
}

pub fn node_port_cmd(directory: &str, port_name: &str) -> String {
    format!("$(cat {directory}/default-address/ports/{port_name})")
}

pub fn pod_fqdn(
    kafka: &v1alpha1::KafkaCluster,
    sts_service_name: &str,
    cluster_info: &KubernetesClusterInfo,
) -> Result<String, KafkaListenerError> {
    Ok(format!(
        "$POD_NAME.{sts_service_name}.{namespace}.svc.{cluster_domain}",
        namespace = kafka.namespace().context(ObjectHasNoNamespaceSnafu)?,
        cluster_domain = cluster_info.cluster_domain
    ))
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        commons::networking::DomainName,
        crd::authentication::{core, kerberos, tls},
    };

    use super::*;
    use crate::crd::{authentication::ResolvedAuthenticationClasses, role::KafkaRole};

    fn default_cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }

    #[test]
    fn test_get_kafka_listeners_config() {
        let kafka_cluster = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            authentication:
              - authenticationClass: kafka-client-tls
            tls:
              internalSecretClass: internalTls
              serverSecretClass: tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(kafka_cluster).expect("illegal test input");
        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![core::v1alpha1::AuthenticationClass {
                metadata: ObjectMetaBuilder::new().name("auth-class").build(),
                spec: core::v1alpha1::AuthenticationClassSpec {
                    provider: core::v1alpha1::AuthenticationClassProvider::Tls(
                        tls::v1alpha1::AuthenticationProvider {
                            client_cert_secret_class: Some("client-auth-secret-class".to_string()),
                        },
                    ),
                },
            }]),
            "internalTls".to_string(),
            Some("tls".to_string()),
        );
        let cluster_info = default_cluster_info();
        // "simple-kafka-broker-default"
        let rolegroup_ref = kafka.rolegroup_ref(&KafkaRole::Broker, "default");
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, &rolegroup_ref, &cluster_info)
                .unwrap();

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
                internal_host = pod_fqdn(
                    &kafka,
                    &rolegroup_ref.rolegroup_headless_service_name(),
                    &cluster_info
                )
                .unwrap(),
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol},{controller_name}:{controller_protocol},{controller_auth_name}:{controller_auth_protocol}",
                name = KafkaListenerName::ClientAuth,
                protocol = KafkaListenerProtocol::Ssl,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Ssl,
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Ssl,
                controller_auth_name = KafkaListenerName::ControllerAuth,
                controller_auth_protocol = KafkaListenerProtocol::Ssl,
            )
        );

        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "tls".to_string(),
            Some("tls".to_string()),
        );
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, &rolegroup_ref, &cluster_info)
                .unwrap();

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
                internal_host = pod_fqdn(
                    &kafka,
                    &rolegroup_ref.rolegroup_headless_service_name(),
                    &cluster_info
                )
                .unwrap(),
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol},{controller_name}:{controller_protocol}",
                name = KafkaListenerName::Client,
                protocol = KafkaListenerProtocol::Ssl,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Ssl,
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Ssl,
            )
        );

        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "".to_string(),
            None,
        );

        let config =
            get_kafka_listener_config(&kafka, &kafka_security, &rolegroup_ref, &cluster_info)
                .unwrap();

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
                internal_host = pod_fqdn(
                    &kafka,
                    &rolegroup_ref.rolegroup_headless_service_name(),
                    &cluster_info
                )
                .unwrap(),
                internal_port = kafka_security.internal_port(),
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol},{controller_name}:{controller_protocol}",
                name = KafkaListenerName::Client,
                protocol = KafkaListenerProtocol::Plaintext,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Plaintext,
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Plaintext,
            )
        );
    }

    #[test]
    fn test_get_kafka_kerberos_listeners_config() {
        let kafka_cluster = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
        spec:
          image:
            productVersion: 3.9.1
          clusterConfig:
            authentication:
              - authenticationClass: kafka-kerberos
            tls:
              serverSecretClass: tls
            zookeeperConfigMapName: xyz
        "#;
        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(kafka_cluster).expect("illegal test input");
        let kafka_security = KafkaTlsSecurity::new(
            ResolvedAuthenticationClasses::new(vec![core::v1alpha1::AuthenticationClass {
                metadata: ObjectMetaBuilder::new().name("auth-class").build(),
                spec: core::v1alpha1::AuthenticationClassSpec {
                    provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                        kerberos::v1alpha1::AuthenticationProvider {
                            kerberos_secret_class: "kerberos-secret-class".to_string(),
                        },
                    ),
                },
            }]),
            "tls".to_string(),
            Some("tls".to_string()),
        );
        let cluster_info = default_cluster_info();
        // "simple-kafka-broker-default"
        let rolegroup_ref = kafka.rolegroup_ref(&KafkaRole::Broker, "default");
        let config =
            get_kafka_listener_config(&kafka, &kafka_security, &rolegroup_ref, &cluster_info)
                .unwrap();

        assert_eq!(
            config.listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port},{bootstrap_name}://{bootstrap_host}:{bootstrap_port}",
                name = KafkaListenerName::Client,
                host = LISTENER_LOCAL_ADDRESS,
                port = kafka_security.client_port(),
                internal_name = KafkaListenerName::Internal,
                internal_host = LISTENER_LOCAL_ADDRESS,
                internal_port = kafka_security.internal_port(),
                bootstrap_name = KafkaListenerName::Bootstrap,
                bootstrap_host = LISTENER_LOCAL_ADDRESS,
                bootstrap_port = kafka_security.bootstrap_port(),
            )
        );

        assert_eq!(
            config.advertised_listeners(),
            format!(
                "{name}://{host}:{port},{internal_name}://{internal_host}:{internal_port},{bootstrap_name}://{bootstrap_host}:{bootstrap_port}",
                name = KafkaListenerName::Client,
                host = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
                port = node_port_cmd(
                    STACKABLE_LISTENER_BROKER_DIR,
                    kafka_security.client_port_name()
                ),
                internal_name = KafkaListenerName::Internal,
                internal_host = pod_fqdn(
                    &kafka,
                    &rolegroup_ref.rolegroup_headless_service_name(),
                    &cluster_info
                )
                .unwrap(),
                internal_port = kafka_security.internal_port(),
                bootstrap_name = KafkaListenerName::Bootstrap,
                bootstrap_host = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
                bootstrap_port = node_port_cmd(
                    STACKABLE_LISTENER_BROKER_DIR,
                    kafka_security.client_port_name()
                ),
            )
        );

        assert_eq!(
            config.listener_security_protocol_map(),
            format!(
                "{name}:{protocol},{internal_name}:{internal_protocol},{bootstrap_name}:{bootstrap_protocol},{controller_name}:{controller_protocol}",
                name = KafkaListenerName::Client,
                protocol = KafkaListenerProtocol::SaslSsl,
                internal_name = KafkaListenerName::Internal,
                internal_protocol = KafkaListenerProtocol::Ssl,
                bootstrap_name = KafkaListenerName::Bootstrap,
                bootstrap_protocol = KafkaListenerProtocol::SaslSsl,
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Ssl,
            )
        );
    }
}

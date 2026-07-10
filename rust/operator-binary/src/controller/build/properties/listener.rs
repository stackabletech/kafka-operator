//! Builds the Kafka listener configuration (`listeners` / `advertised.listeners` /
//! `listener.security.protocol.map`) for a role group's broker/controller properties.
//!
//! Consumes the [`ValidatedCluster`] (for the namespace and role-group resource names) instead of
//! the raw CRD.

use std::collections::BTreeMap;

use stackable_operator::{commons::networking::DomainName, v2::types::kubernetes::NamespaceName};

use crate::{
    controller::{RoleGroupName, ValidatedCluster, security::ValidatedKafkaSecurity},
    crd::{
        STACKABLE_LISTENER_BROKER_DIR,
        listener::{
            KafkaListener, KafkaListenerConfig, KafkaListenerName, KafkaListenerProtocol,
            LISTENER_LOCAL_ADDRESS, node_address_cmd, node_port_cmd,
        },
        role::KafkaRole,
    },
};

pub fn get_kafka_listener_config(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
    role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> KafkaListenerConfig {
    let headless_service_name = validated_cluster
        .resource_names(role, role_group_name)
        .headless_service_name();
    let pod_fqdn = pod_fqdn(
        &validated_cluster.namespace,
        headless_service_name.as_ref(),
        &validated_cluster.cluster_domain,
    );
    let mut listeners = vec![];
    let mut advertised_listeners = vec![];
    let mut listener_security_protocol_map: BTreeMap<KafkaListenerName, KafkaListenerProtocol> =
        BTreeMap::new();

    // CLIENT
    if kafka_security.has_kerberos_enabled() {
        // 1) Kerberos and TLS authentication classes are mutually exclusive
        listeners.push(KafkaListener {
            name: KafkaListenerName::Client,
            host: LISTENER_LOCAL_ADDRESS.to_string(),
            port: ValidatedKafkaSecurity::SECURE_CLIENT_PORT.to_string(),
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
    } else if kafka_security.tls_client_authentication_class().is_some()
        || kafka_security.tls_server_secret_class().is_some()
    {
        // 2) Client listener uses TLS (possibly with authentication)
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
            port: ValidatedKafkaSecurity::CLIENT_PORT.to_string(),
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
    // 5) & 6) Kerberos and TLS authentication classes are mutually exclusive but both require internal tls to be used
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
    listener_security_protocol_map.insert(KafkaListenerName::Internal, KafkaListenerProtocol::Ssl);
    listener_security_protocol_map
        .insert(KafkaListenerName::Controller, KafkaListenerProtocol::Ssl);

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

    KafkaListenerConfig {
        listeners,
        advertised_listeners,
        listener_security_protocol_map,
    }
}

pub(crate) fn pod_fqdn(
    namespace: &NamespaceName,
    sts_service_name: &str,
    cluster_domain: &DomainName,
) -> String {
    format!("${{env:POD_NAME}}.{sts_service_name}.{namespace}.svc.{cluster_domain}")
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        commons::networking::DomainName,
        crd::authentication::{core, kerberos, tls},
        utils::cluster_info::KubernetesClusterInfo,
    };

    use super::*;
    use crate::{
        controller::test_support::{minimal_kafka, validated_cluster},
        crd::authentication::ResolvedAuthenticationClasses,
    };

    fn default_cluster_info() -> KubernetesClusterInfo {
        KubernetesClusterInfo {
            cluster_domain: DomainName::try_from("cluster.local").unwrap(),
        }
    }

    #[test]
    fn test_get_kafka_listeners_config() {
        // The fixture only needs to resolve a `ValidatedCluster` (for the namespace and the
        // `<cluster>-<role>-<role-group>` resource names); the listener output is driven by the
        // explicit `kafka_security` below, so no authentication/TLS is configured here.
        let kafka_cluster = r#"
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
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let kafka = minimal_kafka(kafka_cluster);
        let validated = validated_cluster(&kafka);
        let kafka_security = ValidatedKafkaSecurity::new(
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
            "internal-tls".parse().unwrap(),
            Some("tls".parse().unwrap()),
            None,
        );
        let cluster_info = default_cluster_info();
        let role_group_name: RoleGroupName = "default".parse().unwrap();
        let config = get_kafka_listener_config(
            &validated,
            &kafka_security,
            &KafkaRole::Broker,
            &role_group_name,
        );

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
                    &validated.namespace,
                    validated
                        .resource_names(&KafkaRole::Broker, &role_group_name)
                        .headless_service_name()
                        .as_ref(),
                    &cluster_info.cluster_domain
                ),
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

        let kafka_security = ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "tls".parse().unwrap(),
            Some("tls".parse().unwrap()),
            None,
        );
        let config = get_kafka_listener_config(
            &validated,
            &kafka_security,
            &KafkaRole::Broker,
            &role_group_name,
        );

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
                    &validated.namespace,
                    validated
                        .resource_names(&KafkaRole::Broker, &role_group_name)
                        .headless_service_name()
                        .as_ref(),
                    &cluster_info.cluster_domain
                ),
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

        let kafka_security = ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![]),
            "tls".parse().unwrap(),
            None,
            None,
        );

        let config = get_kafka_listener_config(
            &validated,
            &kafka_security,
            &KafkaRole::Broker,
            &role_group_name,
        );

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
                    &validated.namespace,
                    validated
                        .resource_names(&KafkaRole::Broker, &role_group_name)
                        .headless_service_name()
                        .as_ref(),
                    &cluster_info.cluster_domain
                ),
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
                internal_protocol = KafkaListenerProtocol::Ssl,
                controller_name = KafkaListenerName::Controller,
                controller_protocol = KafkaListenerProtocol::Ssl,
            )
        );
    }

    #[test]
    fn test_get_kafka_kerberos_listeners_config() {
        // See the comment in `test_get_kafka_listeners_config`: the fixture only resolves a
        // `ValidatedCluster`; Kerberos is configured via the explicit `kafka_security` below.
        let kafka_cluster = r#"
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
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let kafka = minimal_kafka(kafka_cluster);
        let validated = validated_cluster(&kafka);
        let kafka_security = ValidatedKafkaSecurity::new(
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
            "tls".parse().unwrap(),
            Some("tls".parse().unwrap()),
            None,
        );
        let cluster_info = default_cluster_info();
        let role_group_name: RoleGroupName = "default".parse().unwrap();
        let config = get_kafka_listener_config(
            &validated,
            &kafka_security,
            &KafkaRole::Broker,
            &role_group_name,
        );

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
                    &validated.namespace,
                    validated
                        .resource_names(&KafkaRole::Broker, &role_group_name)
                        .headless_service_name()
                        .as_ref(),
                    &cluster_info.cluster_domain
                ),
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

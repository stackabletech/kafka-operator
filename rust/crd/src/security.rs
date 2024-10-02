//! A helper module to process Apache Kafka security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos
use std::collections::BTreeMap;

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            container::ContainerBuilder,
            volume::{SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
            PodBuilder,
        },
    },
    client::Client,
    commons::authentication::{AuthenticationClass, AuthenticationClassProvider},
    k8s_openapi::api::core::v1::Volume,
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};

use crate::STACKABLE_LOG_DIR;
use crate::{
    authentication::{self, ResolvedAuthenticationClasses},
    listener::{self, KafkaListenerConfig},
    tls, KafkaCluster, SERVER_PROPERTIES_FILE, STACKABLE_CONFIG_DIR, STACKABLE_TMP_DIR,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to process authentication class"))]
    InvalidAuthenticationClassConfiguration { source: authentication::Error },

    #[snafu(display("failed to build the secret operator Volume"))]
    SecretVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

/// Helper struct combining TLS settings for server and internal with the resolved AuthenticationClasses
pub struct KafkaTlsSecurity<'a> {
    kafka: &'a KafkaCluster,
    resolved_authentication_classes: ResolvedAuthenticationClasses,
    internal_secret_class: String,
    server_secret_class: Option<String>,
}

impl<'a> KafkaTlsSecurity<'a> {
    // ports
    pub const CLIENT_PORT_NAME: &'static str = "kafka";
    pub const CLIENT_PORT: u16 = 9092;
    pub const SECURE_CLIENT_PORT_NAME: &'static str = "kafka-tls";
    pub const SECURE_CLIENT_PORT: u16 = 9093;
    pub const INTERNAL_PORT: u16 = 19092;
    pub const SECURE_INTERNAL_PORT: u16 = 19093;
    // - TLS global
    const SSL_STORE_PASSWORD: &'static str = "";
    // - TLS client
    const CLIENT_SSL_KEYSTORE_LOCATION: &'static str = "listener.name.client.ssl.keystore.location";
    const CLIENT_SSL_KEYSTORE_PASSWORD: &'static str = "listener.name.client.ssl.keystore.password";
    const CLIENT_SSL_KEYSTORE_TYPE: &'static str = "listener.name.client.ssl.keystore.type";
    const CLIENT_SSL_TRUSTSTORE_LOCATION: &'static str =
        "listener.name.client.ssl.truststore.location";
    const CLIENT_SSL_TRUSTSTORE_PASSWORD: &'static str =
        "listener.name.client.ssl.truststore.password";
    const CLIENT_SSL_TRUSTSTORE_TYPE: &'static str = "listener.name.client.ssl.truststore.type";
    // - TLS client authentication
    const CLIENT_AUTH_SSL_KEYSTORE_LOCATION: &'static str =
        "listener.name.client_auth.ssl.keystore.location";
    const CLIENT_AUTH_SSL_KEYSTORE_PASSWORD: &'static str =
        "listener.name.client_auth.ssl.keystore.password";
    const CLIENT_AUTH_SSL_KEYSTORE_TYPE: &'static str =
        "listener.name.client_auth.ssl.keystore.type";
    const CLIENT_AUTH_SSL_TRUSTSTORE_LOCATION: &'static str =
        "listener.name.client_auth.ssl.truststore.location";
    const CLIENT_AUTH_SSL_TRUSTSTORE_PASSWORD: &'static str =
        "listener.name.client_auth.ssl.truststore.password";
    const CLIENT_AUTH_SSL_TRUSTSTORE_TYPE: &'static str =
        "listener.name.client_auth.ssl.truststore.type";
    const CLIENT_AUTH_SSL_CLIENT_AUTH: &'static str = "listener.name.client_auth.ssl.client.auth";
    // - TLS internal
    const INTER_BROKER_LISTENER_NAME: &'static str = "inter.broker.listener.name";
    const INTER_SSL_KEYSTORE_LOCATION: &'static str =
        "listener.name.internal.ssl.keystore.location";
    const INTER_SSL_KEYSTORE_PASSWORD: &'static str =
        "listener.name.internal.ssl.keystore.password";
    const INTER_SSL_KEYSTORE_TYPE: &'static str = "listener.name.internal.ssl.keystore.type";
    const INTER_SSL_TRUSTSTORE_LOCATION: &'static str =
        "listener.name.internal.ssl.truststore.location";
    const INTER_SSL_TRUSTSTORE_PASSWORD: &'static str =
        "listener.name.internal.ssl.truststore.password";
    const INTER_SSL_TRUSTSTORE_TYPE: &'static str = "listener.name.internal.ssl.truststore.type";
    const INTER_SSL_CLIENT_AUTH: &'static str = "listener.name.internal.ssl.client.auth";
    // directories
    // for kcat container
    const STACKABLE_TLS_CERT_SERVER_DIR: &'static str = "/stackable/tls_cert_server_mount";
    const STACKABLE_TLS_CERT_SERVER_DIR_NAME: &'static str = "tls-cert-server-mount";
    // kafka container
    const STACKABLE_TLS_KEYSTORE_SERVER_DIR: &'static str = "/stackable/tls_keystore_server";
    const STACKABLE_TLS_KEYSTORE_SERVER_DIR_NAME: &'static str = "tls-keystore-server";
    const STACKABLE_TLS_KEYSTORE_INTERNAL_DIR: &'static str = "/stackable/tls_keystore_internal";
    const STACKABLE_TLS_KEYSTORE_INTERNAL_DIR_NAME: &'static str = "tls-keystore-internal";

    #[cfg(test)]
    pub fn new(
        kafka: &'a KafkaCluster,
        resolved_authentication_classes: ResolvedAuthenticationClasses,
        internal_secret_class: String,
        server_secret_class: Option<String>,
    ) -> Self {
        Self {
            kafka,
            resolved_authentication_classes,
            internal_secret_class,
            server_secret_class,
        }
    }

    /// Create a `KafkaSecurity` struct from the Kafka custom resource and resolve
    /// all provided `AuthenticationClass` references.
    pub async fn new_from_kafka_cluster(
        client: &Client,
        kafka: &'a KafkaCluster,
    ) -> Result<Self, Error> {
        Ok(KafkaTlsSecurity {
            kafka,
            resolved_authentication_classes: ResolvedAuthenticationClasses::from_references(
                client,
                &kafka.spec.cluster_config.authentication,
            )
            .await
            .context(InvalidAuthenticationClassConfigurationSnafu)?,
            internal_secret_class: kafka
                .spec
                .cluster_config
                .tls
                .as_ref()
                .map(|tls| tls.internal_secret_class.clone())
                .unwrap_or_else(tls::internal_tls_default),
            server_secret_class: kafka
                .spec
                .cluster_config
                .tls
                .as_ref()
                .and_then(|tls| tls.server_secret_class.clone()),
        })
    }

    /// Check if TLS encryption is enabled. This could be due to:
    /// - A provided server `SecretClass`
    /// - A provided client `AuthenticationClass`
    ///
    /// This affects init container commands, Kafka configuration, volume mounts and
    /// the Kafka client port
    pub fn tls_enabled(&self) -> bool {
        // TODO: This must be adapted if other authentication methods are supported and require TLS
        self.tls_client_authentication_class().is_some() || self.tls_server_secret_class().is_some()
    }

    /// Retrieve an optional TLS secret class for external client -> server communications.
    pub fn tls_server_secret_class(&self) -> Option<&str> {
        self.server_secret_class.as_deref()
    }

    /// Retrieve an optional TLS `AuthenticationClass`.
    pub fn tls_client_authentication_class(&self) -> Option<&AuthenticationClass> {
        self.resolved_authentication_classes
            .get_tls_authentication_class()
    }

    /// Retrieve the mandatory internal `SecretClass`.
    pub fn tls_internal_secret_class(&self) -> Option<&str> {
        if !self.internal_secret_class.is_empty() {
            Some(self.internal_secret_class.as_str())
        } else {
            None
        }
    }

    /// Return the Kafka (secure) client port depending on tls or authentication settings.
    pub fn client_port(&self) -> u16 {
        if self.tls_enabled() {
            Self::SECURE_CLIENT_PORT
        } else {
            Self::CLIENT_PORT
        }
    }

    /// Return the Kafka (secure) client port name depending on tls or authentication settings.
    pub fn client_port_name(&self) -> &str {
        if self.tls_enabled() {
            Self::SECURE_CLIENT_PORT_NAME
        } else {
            Self::CLIENT_PORT_NAME
        }
    }

    /// Return the Kafka (secure) internal port depending on tls settings.
    pub fn internal_port(&self) -> u16 {
        if self.tls_internal_secret_class().is_some() {
            Self::SECURE_INTERNAL_PORT
        } else {
            Self::INTERNAL_PORT
        }
    }

    /// Returns SVC container command to retrieve the node port service port.
    pub fn svc_container_commands(&self) -> String {
        let port_name = self.client_port_name();
        // Extract the nodeport from the nodeport service
        format!("kubectl get service \"$POD_NAME\" -o jsonpath='{{.spec.ports[?(@.name==\"{port_name}\")].nodePort}}' | tee {STACKABLE_TMP_DIR}/{port_name}_nodeport")
    }

    /// Returns the commands for the kcat readiness probe.
    pub fn kcat_prober_container_commands(&self) -> Vec<String> {
        let mut args = vec!["/stackable/kcat".to_string()];
        let port = self.client_port();

        if self.tls_client_authentication_class().is_some() {
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
            args.extend(Self::kcat_client_auth_ssl(
                Self::STACKABLE_TLS_CERT_SERVER_DIR,
            ));
        } else if self.tls_server_secret_class().is_some() {
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
            args.extend(Self::kcat_client_ssl(Self::STACKABLE_TLS_CERT_SERVER_DIR));
        } else {
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
        }

        args.push("-L".to_string());
        args
    }

    /// Returns the commands to start the main Kafka container
    pub fn kafka_container_commands(
        &self,
        kafka_listeners: &KafkaListenerConfig,
        opa_connect_string: Option<&str>,
    ) -> Vec<String> {
        vec![formatdoc! {"
            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            bin/kafka-server-start.sh {STACKABLE_CONFIG_DIR}/{SERVER_PROPERTIES_FILE} --override \"zookeeper.connect=$ZOOKEEPER\" --override \"listeners={listeners}\" --override \"advertised.listeners={advertised_listeners}\" --override \"listener.security.protocol.map={listener_security_protocol_map}\"{opa_config} &
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
        remove_vector_shutdown_file_command =
            remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        create_vector_shutdown_file_command =
            create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            listeners = kafka_listeners.listeners(),
            advertised_listeners = kafka_listeners.advertised_listeners(),
            listener_security_protocol_map = kafka_listeners.listener_security_protocol_map(),
            opa_config = match opa_connect_string {
                None => "".to_string(),
                Some(opa_connect_string) => format!(" --override \"opa.authorizer.url={opa_connect_string}\""),
            }
        }]
    }

    /// Adds required volumes and volume mounts to the pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_volume_and_volume_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        cb_kcat_prober: &mut ContainerBuilder,
        cb_kafka: &mut ContainerBuilder,
    ) -> Result<(), Error> {
        // add tls (server or client authentication volumes) if required
        if let Some(tls_server_secret_class) = self.get_tls_secret_class() {
            // We have to mount tls pem files for kcat (the mount can be used directly)
            pod_builder
                .add_volume(Self::create_tls_volume(
                    &self.kafka.bootstrap_service_name(),
                    Self::STACKABLE_TLS_CERT_SERVER_DIR_NAME,
                    tls_server_secret_class,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kcat_prober
                .add_volume_mount(
                    Self::STACKABLE_TLS_CERT_SERVER_DIR_NAME,
                    Self::STACKABLE_TLS_CERT_SERVER_DIR,
                )
                .context(AddVolumeMountSnafu)?;
            // Keystores fore the kafka container
            pod_builder
                .add_volume(Self::create_tls_keystore_volume(
                    &self.kafka.bootstrap_service_name(),
                    Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR_NAME,
                    tls_server_secret_class,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kafka
                .add_volume_mount(
                    Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR_NAME,
                    Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR,
                )
                .context(AddVolumeMountSnafu)?;
        }

        if let Some(tls_internal_secret_class) = self.tls_internal_secret_class() {
            pod_builder
                .add_volume(Self::create_tls_keystore_volume(
                    &self.kafka.bootstrap_service_name(),
                    Self::STACKABLE_TLS_KEYSTORE_INTERNAL_DIR_NAME,
                    tls_internal_secret_class,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kafka
                .add_volume_mount(
                    Self::STACKABLE_TLS_KEYSTORE_INTERNAL_DIR_NAME,
                    Self::STACKABLE_TLS_KEYSTORE_INTERNAL_DIR,
                )
                .context(AddVolumeMountSnafu)?;
        }

        Ok(())
    }

    /// Returns required Kafka configuration settings for the `server.properties` file
    /// depending on the tls and authentication settings.
    pub fn config_settings(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        // We set either client tls with authentication or client tls without authentication
        // If authentication is explicitly required we do not want to have any other CAs to
        // be trusted.
        if self.tls_client_authentication_class().is_some() {
            config.insert(
                Self::CLIENT_AUTH_SSL_KEYSTORE_LOCATION.to_string(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR),
            );
            config.insert(
                Self::CLIENT_AUTH_SSL_KEYSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::CLIENT_AUTH_SSL_KEYSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            config.insert(
                Self::CLIENT_AUTH_SSL_TRUSTSTORE_LOCATION.to_string(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR),
            );
            config.insert(
                Self::CLIENT_AUTH_SSL_TRUSTSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::CLIENT_AUTH_SSL_TRUSTSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            // client auth required
            config.insert(
                Self::CLIENT_AUTH_SSL_CLIENT_AUTH.to_string(),
                "required".to_string(),
            );
        } else if self.tls_server_secret_class().is_some() {
            config.insert(
                Self::CLIENT_SSL_KEYSTORE_LOCATION.to_string(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR),
            );
            config.insert(
                Self::CLIENT_SSL_KEYSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::CLIENT_SSL_KEYSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            config.insert(
                Self::CLIENT_SSL_TRUSTSTORE_LOCATION.to_string(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KEYSTORE_SERVER_DIR),
            );
            config.insert(
                Self::CLIENT_SSL_TRUSTSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::CLIENT_SSL_TRUSTSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
        }

        // Internal TLS
        if self.tls_internal_secret_class().is_some() {
            config.insert(
                Self::INTER_SSL_KEYSTORE_LOCATION.to_string(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KEYSTORE_INTERNAL_DIR),
            );
            config.insert(
                Self::INTER_SSL_KEYSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::INTER_SSL_KEYSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            config.insert(
                Self::INTER_SSL_TRUSTSTORE_LOCATION.to_string(),
                format!(
                    "{}/truststore.p12",
                    Self::STACKABLE_TLS_KEYSTORE_INTERNAL_DIR
                ),
            );
            config.insert(
                Self::INTER_SSL_TRUSTSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::INTER_SSL_TRUSTSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            config.insert(
                Self::INTER_SSL_CLIENT_AUTH.to_string(),
                "required".to_string(),
            );
        }

        // common
        config.insert(
            Self::INTER_BROKER_LISTENER_NAME.to_string(),
            listener::KafkaListenerName::Internal.to_string(),
        );

        config
    }

    /// Returns the `SecretClass` provided in a `AuthenticationClass` for TLS.
    fn get_tls_secret_class(&self) -> Option<&String> {
        self.resolved_authentication_classes
            .get_tls_authentication_class()
            .and_then(|auth_class| match &auth_class.spec.provider {
                AuthenticationClassProvider::Tls(tls) => tls.client_cert_secret_class.as_ref(),
                _ => None,
            })
            .or(self.server_secret_class.as_ref())
    }

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods
    fn create_tls_volume(
        kafka_bootstrap_service_name: &str,
        volume_name: &str,
        secret_class_name: &str,
    ) -> Result<Volume, Error> {
        Ok(VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_node_scope()
                    .with_service_scope(kafka_bootstrap_service_name)
                    .build()
                    .context(SecretVolumeBuildSnafu)?,
            )
            .build())
    }

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods as keystores
    fn create_tls_keystore_volume(
        kafka_bootstrap_service_name: &str,
        volume_name: &str,
        secret_class_name: &str,
    ) -> Result<Volume, Error> {
        Ok(VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_node_scope()
                    .with_service_scope(kafka_bootstrap_service_name)
                    .with_format(SecretFormat::TlsPkcs12)
                    .build()
                    .context(SecretVolumeBuildSnafu)?,
            )
            .build())
    }

    fn kcat_client_auth_ssl(cert_directory: &str) -> Vec<String> {
        vec![
            "-X".to_string(),
            "security.protocol=SSL".to_string(),
            "-X".to_string(),
            format!("ssl.key.location={cert_directory}/tls.key"),
            "-X".to_string(),
            format!("ssl.certificate.location={cert_directory}/tls.crt"),
            "-X".to_string(),
            format!("ssl.ca.location={cert_directory}/ca.crt"),
        ]
    }

    fn kcat_client_ssl(cert_directory: &str) -> Vec<String> {
        vec![
            "-X".to_string(),
            "security.protocol=SSL".to_string(),
            "-X".to_string(),
            format!("ssl.ca.location={cert_directory}/ca.crt"),
        ]
    }
}

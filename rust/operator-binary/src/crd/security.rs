//! A helper module to process Apache Kafka security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos
use std::collections::BTreeMap;

use indoc::formatdoc;
use snafu::{ensure, ResultExt, Snafu};
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
    time::Duration,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};

use super::listener::node_port_cmd;
use crate::crd::{
    authentication::{self, ResolvedAuthenticationClasses},
    listener::{self, node_address_cmd, KafkaListenerConfig},
    tls, v1alpha1, KafkaRole, LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME,
    SERVER_PROPERTIES_FILE, STACKABLE_CONFIG_DIR, STACKABLE_KERBEROS_KRB5_PATH,
    STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR, STACKABLE_LOG_DIR,
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

    #[snafu(display("kerberos enablement requires TLS activation"))]
    KerberosRequiresTls,
}

/// Helper struct combining TLS settings for server and internal with the resolved AuthenticationClasses
pub struct KafkaTlsSecurity {
    resolved_authentication_classes: ResolvedAuthenticationClasses,
    internal_secret_class: String,
    server_secret_class: Option<String>,
}

impl KafkaTlsSecurity {
    // ports
    pub const CLIENT_PORT_NAME: &'static str = "kafka";
    pub const CLIENT_PORT: u16 = 9092;
    pub const SECURE_CLIENT_PORT_NAME: &'static str = "kafka-tls";
    pub const SECURE_CLIENT_PORT: u16 = 9093;
    // bootstrap: we will have a single named port with different values for
    // secure (9095) and insecure (9094). The bootstrap listener is needed to
    // be able to expose principals for both the broker and bootstrap in the
    // JAAS configuration, so that clients can use both.
    pub const BOOTSTRAP_PORT_NAME: &'static str = "bootstrap";
    pub const BOOTSTRAP_PORT: u16 = 9094;
    pub const SECURE_BOOTSTRAP_PORT: u16 = 9095;
    // internal
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
    // - Bootstrapper
    const BOOTSTRAP_SSL_KEYSTORE_LOCATION: &'static str =
        "listener.name.bootstrap.ssl.keystore.location";
    const BOOTSTRAP_SSL_KEYSTORE_PASSWORD: &'static str =
        "listener.name.bootstrap.ssl.keystore.password";
    const BOOTSTRAP_SSL_KEYSTORE_TYPE: &'static str = "listener.name.bootstrap.ssl.keystore.type";
    const BOOTSTRAP_SSL_TRUSTSTORE_LOCATION: &'static str =
        "listener.name.bootstrap.ssl.truststore.location";
    const BOOTSTRAP_SSL_TRUSTSTORE_PASSWORD: &'static str =
        "listener.name.bootstrap.ssl.truststore.password";
    const BOOTSTRAP_SSL_TRUSTSTORE_TYPE: &'static str =
        "listener.name.bootstrap.ssl.truststore.type";
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
    const STACKABLE_TLS_KCAT_DIR: &'static str = "/stackable/tls-kcat";
    const STACKABLE_TLS_KCAT_VOLUME_NAME: &'static str = "tls-kcat";
    const STACKABLE_TLS_KAFKA_SERVER_DIR: &'static str = "/stackable/tls-kafka-server";
    const STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME: &'static str = "tls-kafka-server";
    const STACKABLE_TLS_KAFKA_INTERNAL_DIR: &'static str = "/stackable/tls-kafka-internal";
    const STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME: &'static str = "tls-kafka-internal";

    #[cfg(test)]
    pub fn new(
        resolved_authentication_classes: ResolvedAuthenticationClasses,
        internal_secret_class: String,
        server_secret_class: Option<String>,
    ) -> Self {
        Self {
            resolved_authentication_classes,
            internal_secret_class,
            server_secret_class,
        }
    }

    /// Create a `KafkaSecurity` struct from the Kafka custom resource and resolve
    /// all provided `AuthenticationClass` references.
    pub async fn new_from_kafka_cluster(
        client: &Client,
        kafka: &v1alpha1::KafkaCluster,
    ) -> Result<Self, Error> {
        Ok(KafkaTlsSecurity {
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

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_secret_class().is_some()
    }

    pub fn kerberos_secret_class(&self) -> Option<String> {
        if let Some(kerberos) = self
            .resolved_authentication_classes
            .get_kerberos_authentication_class()
        {
            match &kerberos.spec.provider {
                AuthenticationClassProvider::Kerberos(kerberos) => {
                    Some(kerberos.kerberos_secret_class.clone())
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn validate_authentication_methods(&self) -> Result<(), Error> {
        // Client TLS authentication and Kerberos authentication are mutually
        // exclusive, but this has already been checked when checking the
        // authentication classes. When users enable Kerberos we require them
        // to also enable TLS for a) maximum security and b) to limit the
        // number of combinations we need to support.
        if self.has_kerberos_enabled() {
            ensure!(self.server_secret_class.is_some(), KerberosRequiresTlsSnafu);
        }

        Ok(())
    }

    /// Return the Kafka (secure) client port depending on tls or authentication settings.
    pub fn client_port(&self) -> u16 {
        if self.tls_enabled() {
            Self::SECURE_CLIENT_PORT
        } else {
            Self::CLIENT_PORT
        }
    }

    pub fn bootstrap_port(&self) -> u16 {
        if self.tls_enabled() {
            Self::SECURE_BOOTSTRAP_PORT
        } else {
            Self::BOOTSTRAP_PORT
        }
    }

    pub fn bootstrap_port_name(&self) -> &str {
        Self::BOOTSTRAP_PORT_NAME
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

    /// Returns the commands for the kcat readiness probe.
    pub fn kcat_prober_container_commands(&self) -> Vec<String> {
        let mut args = vec![];
        let port = self.client_port();

        if self.tls_client_authentication_class().is_some() {
            args.push("/stackable/kcat".to_string());
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
            args.extend(Self::kcat_client_auth_ssl(Self::STACKABLE_TLS_KCAT_DIR));
            args.push("-L".to_string());
        } else if self.has_kerberos_enabled() {
            let service_name = KafkaRole::Broker.kerberos_service_name();
            let broker_port = node_port_cmd(STACKABLE_LISTENER_BROKER_DIR, self.client_port_name());
            // here we need to specify a shell so that variable substitution will work
            // see e.g. https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1ExecAction.md
            args.push("/bin/bash".to_string());
            args.push("-x".to_string());
            args.push("-euo".to_string());
            args.push("pipefail".to_string());
            args.push("-c".to_string());

            // the entire command needs to be subject to the -c directive
            // to prevent short-circuiting
            let mut bash_args = vec![];
            bash_args.push(
                format!(
                    "export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {});",
                    STACKABLE_KERBEROS_KRB5_PATH
                )
                .to_string(),
            );
            bash_args.push(
                format!(
                    "export POD_BROKER_LISTENER_ADDRESS={};",
                    node_address_cmd(STACKABLE_LISTENER_BROKER_DIR)
                )
                .to_string(),
            );
            bash_args.push("/stackable/kcat".to_string());
            bash_args.push("-b".to_string());
            bash_args.push(format!("$POD_BROKER_LISTENER_ADDRESS:{broker_port}"));
            bash_args.extend(Self::kcat_client_sasl_ssl(
                Self::STACKABLE_TLS_KCAT_DIR,
                service_name,
            ));
            bash_args.push("-L".to_string());

            args.push(bash_args.join(" "));
        } else if self.tls_server_secret_class().is_some() {
            args.push("/stackable/kcat".to_string());
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
            args.extend(Self::kcat_client_ssl(Self::STACKABLE_TLS_KCAT_DIR));
            args.push("-L".to_string());
        } else {
            args.push("/stackable/kcat".to_string());
            args.push("-b".to_string());
            args.push(format!("localhost:{}", port));
            args.push("-L".to_string());
        }

        args
    }

    /// Returns the commands to start the main Kafka container
    pub fn kafka_container_commands(
        &self,
        kafka_listeners: &KafkaListenerConfig,
        opa_connect_string: Option<&str>,
        kerberos_enabled: bool,
    ) -> Vec<String> {
        vec![formatdoc! {"
            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
            {set_realm_env}
            bin/kafka-server-start.sh {STACKABLE_CONFIG_DIR}/{SERVER_PROPERTIES_FILE} --override \"zookeeper.connect=$ZOOKEEPER\" --override \"listeners={listeners}\" --override \"advertised.listeners={advertised_listeners}\" --override \"listener.security.protocol.map={listener_security_protocol_map}\"{opa_config}{jaas_config} &
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
        remove_vector_shutdown_file_command =
            remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
        create_vector_shutdown_file_command =
            create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            set_realm_env = match kerberos_enabled {
                true => format!("export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {})", STACKABLE_KERBEROS_KRB5_PATH),
                false => "".to_string(),
            },
            listeners = kafka_listeners.listeners(),
            advertised_listeners = kafka_listeners.advertised_listeners(),
            listener_security_protocol_map = kafka_listeners.listener_security_protocol_map(),
            opa_config = match opa_connect_string {
                None => "".to_string(),
                Some(opa_connect_string) => format!(" --override \"opa.authorizer.url={opa_connect_string}\""),
            },
            jaas_config = match kerberos_enabled {
                true => {
                    let service_name = KafkaRole::Broker.kerberos_service_name();
                    let broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR);
                    let bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR);
                    // TODO replace client and bootstrap below with constants
                    format!(" --override \"listener.name.client.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{broker_address}@$KERBEROS_REALM\\\";\" --override \"listener.name.bootstrap.gssapi.sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true isInitiator=false keyTab=\\\"/stackable/kerberos/keytab\\\" principal=\\\"{service_name}/{bootstrap_address}@$KERBEROS_REALM\\\";\"").to_string()},
                false => "".to_string(),
            },
        }]
    }

    /// Adds required volumes and volume mounts to the pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_volume_and_volume_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        cb_kcat_prober: &mut ContainerBuilder,
        cb_kafka: &mut ContainerBuilder,
        requested_secret_lifetime: &Duration,
    ) -> Result<(), Error> {
        // add tls (server or client authentication volumes) if required
        if let Some(tls_server_secret_class) = self.get_tls_secret_class() {
            // We have to mount tls pem files for kcat (the mount can be used directly)
            pod_builder
                .add_volume(Self::create_kcat_tls_volume(
                    Self::STACKABLE_TLS_KCAT_VOLUME_NAME,
                    tls_server_secret_class,
                    requested_secret_lifetime,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kcat_prober
                .add_volume_mount(
                    Self::STACKABLE_TLS_KCAT_VOLUME_NAME,
                    Self::STACKABLE_TLS_KCAT_DIR,
                )
                .context(AddVolumeMountSnafu)?;
            // Keystores fore the kafka container
            pod_builder
                .add_volume(Self::create_tls_keystore_volume(
                    Self::STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
                    tls_server_secret_class,
                    requested_secret_lifetime,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kafka
                .add_volume_mount(
                    Self::STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR,
                )
                .context(AddVolumeMountSnafu)?;
        }

        if let Some(tls_internal_secret_class) = self.tls_internal_secret_class() {
            pod_builder
                .add_volume(Self::create_tls_keystore_volume(
                    Self::STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                    tls_internal_secret_class,
                    requested_secret_lifetime,
                )?)
                .context(AddVolumeSnafu)?;
            cb_kafka
                .add_volume_mount(
                    Self::STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                    Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR,
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
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
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
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
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
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
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
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
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

        if self.has_kerberos_enabled() {
            // Bootstrap
            config.insert(
                Self::BOOTSTRAP_SSL_KEYSTORE_LOCATION.to_string(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                Self::BOOTSTRAP_SSL_KEYSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::BOOTSTRAP_SSL_KEYSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
            config.insert(
                Self::BOOTSTRAP_SSL_TRUSTSTORE_LOCATION.to_string(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                Self::BOOTSTRAP_SSL_TRUSTSTORE_PASSWORD.to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                Self::BOOTSTRAP_SSL_TRUSTSTORE_TYPE.to_string(),
                "PKCS12".to_string(),
            );
        }

        // Internal TLS
        if self.tls_internal_secret_class().is_some() {
            config.insert(
                Self::INTER_SSL_KEYSTORE_LOCATION.to_string(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
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
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
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

        // Kerberos
        if self.has_kerberos_enabled() {
            config.insert("sasl.enabled.mechanisms".to_string(), "GSSAPI".to_string());
            config.insert(
                "sasl.kerberos.service.name".to_string(),
                KafkaRole::Broker.kerberos_service_name().to_string(),
            );
            config.insert(
                "sasl.mechanism.inter.broker.protocol".to_string(),
                "GSSAPI".to_string(),
            );
            tracing::debug!("Kerberos configs added: [{:#?}]", config);
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

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods for kcat client
    fn create_kcat_tls_volume(
        volume_name: &str,
        secret_class_name: &str,
        requested_secret_lifetime: &Duration,
    ) -> Result<Volume, Error> {
        Ok(VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_format(SecretFormat::TlsPem)
                    .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
                    .build()
                    .context(SecretVolumeBuildSnafu)?,
            )
            .build())
    }

    /// Creates ephemeral volumes to mount the `SecretClass` into the Pods as keystores
    fn create_tls_keystore_volume(
        volume_name: &str,
        secret_class_name: &str,
        requested_secret_lifetime: &Duration,
    ) -> Result<Volume, Error> {
        Ok(VolumeBuilder::new(volume_name)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                    .with_pod_scope()
                    .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
                    .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME)
                    .with_format(SecretFormat::TlsPkcs12)
                    .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
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

    fn kcat_client_sasl_ssl(cert_directory: &str, service_name: &str) -> Vec<String> {
        vec![
            "-X".to_string(),
            "security.protocol=SASL_SSL".to_string(),
            "-X".to_string(),
            format!("ssl.ca.location={cert_directory}/ca.crt"),
            "-X".to_string(),
            "sasl.kerberos.keytab=/stackable/kerberos/keytab".to_string(),
            "-X".to_string(),
            "sasl.mechanism=GSSAPI".to_string(),
            "-X".to_string(),
            format!("sasl.kerberos.service.name={service_name}"),
            "-X".to_string(),
            format!("sasl.kerberos.principal={service_name}/$POD_BROKER_LISTENER_ADDRESS@$KERBEROS_REALM"),
        ]
    }
}

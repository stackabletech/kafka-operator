//! A helper module to process Apache Kafka security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos
use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu, ensure};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
        },
    },
    client::Client,
    crd::authentication::core,
    k8s_openapi::api::core::v1::Volume,
    shared::time::Duration,
};

use super::listener::KafkaListenerProtocol;
use crate::crd::{
    LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, STACKABLE_KERBEROS_KRB5_PATH,
    STACKABLE_LISTENER_BROKER_DIR,
    authentication::{self, ResolvedAuthenticationClasses},
    listener::{self, KafkaListenerName, node_address_cmd_env, node_port_cmd_env},
    role::KafkaRole,
    tls, v1alpha1,
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

    #[snafu(display("failed to build OPA TLS certificate volume"))]
    OpaTlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },
}

/// Helper struct combining TLS settings for server and internal with the resolved AuthenticationClasses
pub struct KafkaTlsSecurity {
    resolved_authentication_classes: ResolvedAuthenticationClasses,
    internal_secret_class: String,
    server_secret_class: Option<String>,
    opa_secret_class: Option<String>,
}

impl KafkaTlsSecurity {
    pub const BOOTSTRAP_PORT: u16 = 9094;
    // bootstrap: we will have a single named port with different values for
    // secure (9095) and insecure (9094). The bootstrap listener is needed to
    // be able to expose principals for both the broker and bootstrap in the
    // JAAS configuration, so that clients can use both.
    pub const BOOTSTRAP_PORT_NAME: &'static str = "bootstrap";
    pub const CLIENT_PORT: u16 = 9092;
    // ports
    pub const CLIENT_PORT_NAME: &'static str = "kafka";
    // internal
    pub const INTERNAL_PORT: u16 = 19092;
    // - TLS internal
    const INTER_BROKER_LISTENER_NAME: &'static str = "inter.broker.listener.name";
    const OPA_TLS_MOUNT_PATH: &str = "/stackable/tls-opa";
    // opa
    const OPA_TLS_VOLUME_NAME: &str = "tls-opa";
    pub const SECURE_BOOTSTRAP_PORT: u16 = 9095;
    pub const SECURE_CLIENT_PORT: u16 = 9093;
    pub const SECURE_CLIENT_PORT_NAME: &'static str = "kafka-tls";
    pub const SECURE_INTERNAL_PORT: u16 = 19093;
    // - TLS global
    const SSL_STORE_PASSWORD: &'static str = "";
    const STACKABLE_TLS_KAFKA_INTERNAL_DIR: &'static str = "/stackable/tls-kafka-internal";
    const STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME: &'static str = "tls-kafka-internal";
    const STACKABLE_TLS_KAFKA_SERVER_DIR: &'static str = "/stackable/tls-kafka-server";
    const STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME: &'static str = "tls-kafka-server";
    // directories
    const STACKABLE_TLS_KCAT_DIR: &'static str = "/stackable/tls-kcat";
    const STACKABLE_TLS_KCAT_VOLUME_NAME: &'static str = "tls-kcat";

    #[cfg(test)]
    pub fn new(
        resolved_authentication_classes: ResolvedAuthenticationClasses,
        internal_secret_class: String,
        server_secret_class: Option<String>,
        opa_secret_class: Option<String>,
    ) -> Self {
        Self {
            resolved_authentication_classes,
            internal_secret_class,
            server_secret_class,
            opa_secret_class,
        }
    }

    /// Create a `KafkaSecurity` struct from the Kafka custom resource and resolve
    /// all provided `AuthenticationClass` references.
    pub async fn new_from_kafka_cluster(
        client: &Client,
        kafka: &v1alpha1::KafkaCluster,
        opa_secret_class: Option<String>,
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
            opa_secret_class,
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
    pub fn tls_client_authentication_class(&self) -> Option<&core::v1alpha1::AuthenticationClass> {
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

    fn has_opa_tls_enabled(&self) -> bool {
        self.opa_secret_class.is_some()
    }

    pub fn copy_opa_tls_cert_command(&self) -> String {
        match self.has_opa_tls_enabled() {
            true => format!(
                "keytool -importcert -file {opa_mount_path}/ca.crt -keystore {tls_dir}/truststore.p12 -storepass '{tls_password}' -alias opa-ca -noprompt",
                opa_mount_path = Self::OPA_TLS_MOUNT_PATH,
                tls_dir = Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR,
                tls_password = Self::SSL_STORE_PASSWORD,
            ),
            false => "".to_string(),
        }
    }

    pub fn kerberos_secret_class(&self) -> Option<String> {
        if let Some(kerberos) = self
            .resolved_authentication_classes
            .get_kerberos_authentication_class()
        {
            match &kerberos.spec.provider {
                core::v1alpha1::AuthenticationClassProvider::Kerberos(kerberos) => {
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
                    node_address_cmd_env(STACKABLE_LISTENER_BROKER_DIR)
                )
                .to_string(),
            );
            bash_args.push(
                format!(
                    "export POD_BROKER_LISTENER_PORT={};",
                    node_port_cmd_env(STACKABLE_LISTENER_BROKER_DIR, self.client_port_name())
                )
                .to_string(),
            );
            bash_args.push("/stackable/kcat".to_string());
            bash_args.push("-b".to_string());
            bash_args.push("$POD_BROKER_LISTENER_ADDRESS:$POD_BROKER_LISTENER_PORT".to_string());
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

    /// Returns a configuration file that can be used by Kafka clients running inside the
    /// Kubernetes cluster to connect to the Kafka servers.
    pub fn client_properties(&self) -> Vec<(String, Option<String>)> {
        let mut props = vec![];

        if self.tls_client_authentication_class().is_some() {
            props.push((
                "security.protocol".to_string(),
                Some(KafkaListenerProtocol::Ssl.to_string()),
            ));
            props.push(("ssl.client.auth".to_string(), Some("required".to_string())));
            props.push(("ssl.keystore.type".to_string(), Some("PKCS12".to_string())));
            props.push((
                "ssl.keystore.location".to_string(),
                Some(format!(
                    "{}/keystore.p12",
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR
                )),
            ));
            props.push((
                "ssl.keystore.password".to_string(),
                Some(Self::SSL_STORE_PASSWORD.to_string()),
            ));
            props.push((
                "ssl.truststore.type".to_string(),
                Some("PKCS12".to_string()),
            ));
            props.push((
                "ssl.truststore.location".to_string(),
                Some(format!(
                    "{}/truststore.p12",
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR
                )),
            ));
            props.push((
                "ssl.truststore.password".to_string(),
                Some(Self::SSL_STORE_PASSWORD.to_string()),
            ));
        } else if self.has_kerberos_enabled() {
            // TODO: to make this configuration file usable out of the box the operator needs to be
            // refactored to write out Java jaas files instead of passing command line parameters
            // to the Kafka daemon scripts.
            // This will simplify the code and the command lines lot.
            // It will also make the jaas files reusable by the Kafka shell scripts.
            props.push((
                "security.protocol".to_string(),
                Some(KafkaListenerProtocol::SaslSsl.to_string()),
            ));
            props.push(("ssl.keystore.type".to_string(), Some("PKCS12".to_string())));
            props.push((
                "ssl.keystore.location".to_string(),
                Some(format!(
                    "{}/keystore.p12",
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR
                )),
            ));
            props.push((
                "ssl.keystore.password".to_string(),
                Some(Self::SSL_STORE_PASSWORD.to_string()),
            ));
            props.push((
                "ssl.truststore.type".to_string(),
                Some("PKCS12".to_string()),
            ));
            props.push((
                "ssl.truststore.location".to_string(),
                Some(format!(
                    "{}/truststore.p12",
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR
                )),
            ));
            props.push((
                "ssl.truststore.password".to_string(),
                Some(Self::SSL_STORE_PASSWORD.to_string()),
            ));
            props.push((
                "sasl.enabled.mechanisms".to_string(),
                Some("GSSAPI".to_string()),
            ));
            props.push((
                "sasl.kerberos.service.name".to_string(),
                Some(KafkaRole::Broker.kerberos_service_name().to_string()),
            ));
            props.push((
                "sasl.mechanism.inter.broker.protocol".to_string(),
                Some("GSSAPI".to_string()),
            ));
            props.push((
                "sasl.jaas.config".to_string(),
                Some(format!("com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"{keytab}\" principal=\"{service}/{pod}@{realm}\"",
                    keytab="/stackable/kerberos/keytab",
                    service=KafkaRole::Broker.kerberos_service_name(),
                    pod="todo",
                    realm="$KERBEROS_REALM"))));
        } else if self.tls_server_secret_class().is_some() {
            props.push((
                "security.protocol".to_string(),
                Some(KafkaListenerProtocol::Ssl.to_string()),
            ));
            props.push((
                "ssl.truststore.type".to_string(),
                Some("PKCS12".to_string()),
            ));
            props.push((
                "ssl.truststore.location".to_string(),
                Some(format!(
                    "{}/truststore.p12",
                    Self::STACKABLE_TLS_KAFKA_SERVER_DIR
                )),
            ));
            props.push((
                "ssl.truststore.password".to_string(),
                Some(Self::SSL_STORE_PASSWORD.to_string()),
            ));
        } else {
            props.push((
                "security.protocol".to_string(),
                Some(KafkaListenerProtocol::Plaintext.to_string()),
            ));
        }

        props
    }

    /// Adds required volumes and volume mounts to the broker pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_broker_volume_and_volume_mounts(
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

        if let Some(secret_class) = &self.opa_secret_class {
            cb_kafka
                .add_volume_mount(Self::OPA_TLS_VOLUME_NAME, Self::OPA_TLS_MOUNT_PATH)
                .context(AddVolumeMountSnafu)?;

            pod_builder
                .add_volume(
                    VolumeBuilder::new(Self::OPA_TLS_VOLUME_NAME)
                        .ephemeral(
                            SecretOperatorVolumeSourceBuilder::new(secret_class)
                                .build()
                                .context(OpaTlsCertSecretClassVolumeBuildSnafu)?,
                        )
                        .build(),
                )
                .context(AddVolumeSnafu)?;
        }

        Ok(())
    }

    /// Adds required volumes and volume mounts to the controller pod and container builders
    /// depending on the tls and authentication settings.
    pub fn add_controller_volume_and_volume_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        cb_kafka: &mut ContainerBuilder,
        requested_secret_lifetime: &Duration,
    ) -> Result<(), Error> {
        if let Some(tls_internal_secret_class) = self.tls_internal_secret_class() {
            pod_builder
                .add_volume(
                    VolumeBuilder::new(Self::STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME)
                        .ephemeral(
                            SecretOperatorVolumeSourceBuilder::new(tls_internal_secret_class)
                                .with_pod_scope()
                                .with_format(SecretFormat::TlsPkcs12)
                                .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
                                .build()
                                .context(SecretVolumeBuildSnafu)?,
                        )
                        .build(),
                )
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

    /// Returns required Kafka configuration settings for the `broker.properties` file
    /// depending on the tls and authentication settings.
    pub fn broker_config_settings(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        // We set either client tls with authentication or client tls without authentication
        // If authentication is explicitly required we do not want to have any other CAs to
        // be trusted.
        if self.tls_client_authentication_class().is_some()
            || self.tls_server_secret_class().is_some()
        {
            config.insert(
                KafkaListenerName::Client.listener_ssl_keystore_location(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                KafkaListenerName::Client.listener_ssl_keystore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Client.listener_ssl_keystore_type(),
                "PKCS12".to_string(),
            );
            config.insert(
                KafkaListenerName::Client.listener_ssl_truststore_location(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                KafkaListenerName::Client.listener_ssl_truststore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Client.listener_ssl_truststore_type(),
                "PKCS12".to_string(),
            );
            if self.tls_client_authentication_class().is_some() {
                // client auth required
                config.insert(
                    KafkaListenerName::Client.listener_ssl_client_auth(),
                    "required".to_string(),
                );
            }
        }

        if self.has_kerberos_enabled() {
            // Bootstrap
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_keystore_location(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_keystore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_keystore_type(),
                "PKCS12".to_string(),
            );
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_truststore_location(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_SERVER_DIR),
            );
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_truststore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Bootstrap.listener_ssl_truststore_type(),
                "PKCS12".to_string(),
            );
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

        // Internal TLS
        if self.tls_internal_secret_class().is_some() {
            // BROKERS
            config.insert(
                KafkaListenerName::Internal.listener_ssl_keystore_location(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Internal.listener_ssl_keystore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Internal.listener_ssl_keystore_type(),
                "PKCS12".to_string(),
            );
            config.insert(
                KafkaListenerName::Internal.listener_ssl_truststore_location(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Internal.listener_ssl_truststore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Internal.listener_ssl_truststore_type(),
                "PKCS12".to_string(),
            );
            // CONTROLLERS
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_location(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_type(),
                "PKCS12".to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_location(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_type(),
                "PKCS12".to_string(),
            );
            // client auth required
            config.insert(
                KafkaListenerName::Internal.listener_ssl_client_auth(),
                "required".to_string(),
            );
        }

        //OPA Tls
        if self.opa_secret_class.is_some() {
            config.insert(
                "opa.authorizer.truststore.path".to_string(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                "opa.authorizer.truststore.password".to_string(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
        }
        config.insert(
            "opa.authorizer.truststore.type".to_string(),
            "PKCS12".to_string(),
        );

        // common
        config.insert(
            Self::INTER_BROKER_LISTENER_NAME.to_string(),
            listener::KafkaListenerName::Internal.to_string(),
        );

        config
    }

    /// Returns required Kafka configuration settings for the `controller.properties` file
    /// depending on the tls and authentication settings.
    pub fn controller_config_settings(&self) -> BTreeMap<String, String> {
        let mut config = BTreeMap::new();

        if self.tls_client_authentication_class().is_some()
            || self.tls_internal_secret_class().is_some()
        {
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_location(),
                format!("{}/keystore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_keystore_type(),
                "PKCS12".to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_location(),
                format!("{}/truststore.p12", Self::STACKABLE_TLS_KAFKA_INTERNAL_DIR),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_password(),
                Self::SSL_STORE_PASSWORD.to_string(),
            );
            config.insert(
                KafkaListenerName::Controller.listener_ssl_truststore_type(),
                "PKCS12".to_string(),
            );
            // We set either client tls with authentication or client tls without authentication
            // If authentication is explicitly required we do not want to have any other CAs to
            // be trusted.
            if self.tls_client_authentication_class().is_some() {
                // client auth required
                config.insert(
                    KafkaListenerName::Controller.listener_ssl_client_auth(),
                    "required".to_string(),
                );
            }
        }

        // Kerberos
        if self.has_kerberos_enabled() {
            config.insert("sasl.enabled.mechanisms".to_string(), "GSSAPI".to_string());
            config.insert(
                "sasl.kerberos.service.name".to_string(),
                KafkaRole::Controller.kerberos_service_name().to_string(),
            );
            config.insert(
                "sasl.mechanism.inter.broker.protocol".to_string(),
                "GSSAPI".to_string(),
            );
            tracing::debug!("Kerberos configs added: [{:#?}]", config);
        }

        config
    }

    /// Returns the `SecretClass` provided in a `AuthenticationClass` for TLS.
    fn get_tls_secret_class(&self) -> Option<&String> {
        self.resolved_authentication_classes
            .get_tls_authentication_class()
            .and_then(|auth_class| match &auth_class.spec.provider {
                core::v1alpha1::AuthenticationClassProvider::Tls(tls) => {
                    tls.client_cert_secret_class.as_ref()
                }
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
            format!(
                "sasl.kerberos.principal={service_name}/$POD_BROKER_LISTENER_ADDRESS@$KERBEROS_REALM"
            ),
        ]
    }
}

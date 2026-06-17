//! A helper module to process Apache Kafka security configuration
//!
//! This module merges the `tls` and `authentication` module and offers better accessibility
//! and helper functions
//!
//! This is required due to overlaps between TLS encryption and e.g. mTLS authentication or Kerberos
use snafu::{Snafu, ensure};
use stackable_operator::{
    crd::authentication::core,
    v2::types::{common::Port, kubernetes::SecretClassName},
};

use crate::crd::{authentication::ResolvedAuthenticationClasses, tls, v1alpha1};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("kerberos enablement requires TLS activation"))]
    KerberosRequiresTls,
}

/// Helper struct combining TLS settings for server and internal with the resolved AuthenticationClasses
pub struct ValidatedKafkaSecurity {
    resolved_authentication_classes: ResolvedAuthenticationClasses,
    internal_secret_class: Option<SecretClassName>,
    server_secret_class: Option<SecretClassName>,
    opa_secret_class: Option<SecretClassName>,
}

impl ValidatedKafkaSecurity {
    pub const BOOTSTRAP_PORT: Port = Port(9094);
    // bootstrap: we will have a single named port with different values for
    // secure (9095) and insecure (9094). The bootstrap listener is needed to
    // be able to expose principals for both the broker and bootstrap in the
    // JAAS configuration, so that clients can use both.
    pub const BOOTSTRAP_PORT_NAME: &'static str = "bootstrap";
    pub const CLIENT_PORT: Port = Port(9092);
    // ports
    pub const CLIENT_PORT_NAME: &'static str = "kafka";
    // internal
    pub const INTERNAL_PORT: Port = Port(19092);
    pub const SECURE_BOOTSTRAP_PORT: Port = Port(9095);
    pub const SECURE_CLIENT_PORT: Port = Port(9093);
    pub const SECURE_CLIENT_PORT_NAME: &'static str = "kafka-tls";
    pub const SECURE_INTERNAL_PORT: Port = Port(19093);

    #[cfg(test)]
    pub fn new(
        resolved_authentication_classes: ResolvedAuthenticationClasses,
        internal_secret_class: Option<SecretClassName>,
        server_secret_class: Option<SecretClassName>,
        opa_secret_class: Option<SecretClassName>,
    ) -> Self {
        Self {
            resolved_authentication_classes,
            internal_secret_class,
            server_secret_class,
            opa_secret_class,
        }
    }

    /// Build a [`ValidatedKafkaSecurity`] from already-resolved authentication classes.
    ///
    /// The async retrieval of [`ResolvedAuthenticationClasses`] now happens in the dereference
    /// step of the controller; this constructor only reads TLS settings from the spec.
    pub fn new_from_kafka_cluster(
        kafka: &v1alpha1::KafkaCluster,
        resolved_authentication_classes: ResolvedAuthenticationClasses,
        opa_secret_class: Option<SecretClassName>,
    ) -> Self {
        ValidatedKafkaSecurity {
            resolved_authentication_classes,
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
        }
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
        self.server_secret_class.as_ref().map(|s| s.as_ref())
    }

    /// Retrieve an optional TLS `AuthenticationClass`.
    pub fn tls_client_authentication_class(&self) -> Option<&core::v1alpha1::AuthenticationClass> {
        self.resolved_authentication_classes
            .get_tls_authentication_class()
    }

    /// Retrieve the optional internal `SecretClass`.
    ///
    /// Returns `None` when internal TLS is disabled (a plaintext cluster).
    pub fn tls_internal_secret_class(&self) -> Option<&str> {
        self.internal_secret_class.as_ref().map(|s| s.as_ref())
    }

    pub fn has_kerberos_enabled(&self) -> bool {
        self.kerberos_secret_class().is_some()
    }

    /// Retrieve the optional OPA TLS `SecretClass`.
    pub fn opa_secret_class(&self) -> Option<&SecretClassName> {
        self.opa_secret_class.as_ref()
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
    pub fn client_port(&self) -> Port {
        if self.tls_enabled() {
            Self::SECURE_CLIENT_PORT
        } else {
            Self::CLIENT_PORT
        }
    }

    pub fn bootstrap_port(&self) -> Port {
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
    pub fn internal_port(&self) -> Port {
        if self.tls_internal_secret_class().is_some() {
            Self::SECURE_INTERNAL_PORT
        } else {
            Self::INTERNAL_PORT
        }
    }
}

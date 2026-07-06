//! Build-step helpers that turn a [`ValidatedKafkaSecurity`] into Kafka configuration settings,
//! client properties, container commands and TLS volumes/mounts.
//!
//! These consume the validated security inputs and produce build artifacts; they must not perform
//! any validation themselves.
use std::collections::BTreeMap;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder},
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    crd::authentication::core,
    k8s_openapi::api::core::v1::Volume,
    shared::time::Duration,
};

use crate::{
    controller::security::ValidatedKafkaSecurity,
    crd::{
        LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, STACKABLE_KERBEROS_KRB5_PATH,
        STACKABLE_LISTENER_BROKER_DIR,
        listener::{
            self, KafkaListenerName, KafkaListenerProtocol, node_address_cmd_env, node_port_cmd_env,
        },
        role::KafkaRole,
    },
};

// - TLS internal
const INTER_BROKER_LISTENER_NAME: &str = "inter.broker.listener.name";
// - TLS global
const KEYSTORE_P12_FILE_NAME: &str = "keystore.p12";
const OPA_TLS_MOUNT_PATH: &str = "/stackable/tls-opa";
// opa
const OPA_TLS_VOLUME_NAME: &str = "tls-opa";
const SSL_STORE_PASSWORD: &str = "";
const SSL_STORE_TYPE_PKCS12: &str = "PKCS12";
const SSL_CLIENT_AUTH_REQUIRED: &str = "required";
const SASL_MECHANISM_GSSAPI: &str = "GSSAPI";
const STACKABLE_KCAT_BINARY: &str = "/stackable/kcat";
const PROPERTY_SECURITY_PROTOCOL: &str = "security.protocol";
const PROPERTY_SASL_ENABLED_MECHANISMS: &str = "sasl.enabled.mechanisms";
const PROPERTY_SASL_KERBEROS_SERVICE_NAME: &str = "sasl.kerberos.service.name";
const PROPERTY_SASL_INTER_BROKER_MECHANISM: &str = "sasl.mechanism.inter.broker.protocol";
const STACKABLE_TLS_KAFKA_INTERNAL_DIR: &str = "/stackable/tls-kafka-internal";
const STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME: &str = "tls-kafka-internal";
const STACKABLE_TLS_KAFKA_SERVER_DIR: &str = "/stackable/tls-kafka-server";
const STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME: &str = "tls-kafka-server";
// directories
const STACKABLE_TLS_KCAT_DIR: &str = "/stackable/tls-kcat";
const STACKABLE_TLS_KCAT_VOLUME_NAME: &str = "tls-kcat";
const TRUSTSTORE_P12_FILE_NAME: &str = "truststore.p12";

#[derive(Snafu, Debug)]
pub enum Error {
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

    #[snafu(display("failed to build OPA TLS certificate volume"))]
    OpaTlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },
}

pub fn copy_opa_tls_cert_command(security: &ValidatedKafkaSecurity) -> String {
    match security.opa_secret_class().is_some() {
        true => format!(
            "keytool -importcert -file {opa_mount_path}/ca.crt -keystore {tls_dir}/{truststore} -storepass '{tls_password}' -alias opa-ca -noprompt",
            opa_mount_path = OPA_TLS_MOUNT_PATH,
            tls_dir = STACKABLE_TLS_KAFKA_INTERNAL_DIR,
            truststore = TRUSTSTORE_P12_FILE_NAME,
            tls_password = SSL_STORE_PASSWORD,
        ),
        false => "".to_string(),
    }
}

/// Returns the commands for the kcat readiness probe.
pub fn kcat_prober_container_commands(security: &ValidatedKafkaSecurity) -> Vec<String> {
    let mut args = vec![];
    let port = security.client_port();

    if security.tls_client_authentication_class().is_some() {
        args.push(STACKABLE_KCAT_BINARY.to_string());
        args.push("-b".to_string());
        args.push(format!("localhost:{}", port));
        args.extend(kcat_client_auth_ssl(STACKABLE_TLS_KCAT_DIR));
        args.push("-L".to_string());
    } else if security.has_kerberos_enabled() {
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
                node_port_cmd_env(STACKABLE_LISTENER_BROKER_DIR, security.client_port_name())
            )
            .to_string(),
        );
        bash_args.push(STACKABLE_KCAT_BINARY.to_string());
        bash_args.push("-b".to_string());
        bash_args.push("$POD_BROKER_LISTENER_ADDRESS:$POD_BROKER_LISTENER_PORT".to_string());
        bash_args.extend(kcat_client_sasl_ssl(STACKABLE_TLS_KCAT_DIR, service_name));
        bash_args.push("-L".to_string());

        args.push(bash_args.join(" "));
    } else if security.tls_server_secret_class().is_some() {
        args.push(STACKABLE_KCAT_BINARY.to_string());
        args.push("-b".to_string());
        args.push(format!("localhost:{}", port));
        args.extend(kcat_client_ssl(STACKABLE_TLS_KCAT_DIR));
        args.push("-L".to_string());
    } else {
        args.push(STACKABLE_KCAT_BINARY.to_string());
        args.push("-b".to_string());
        args.push(format!("localhost:{}", port));
        args.push("-L".to_string());
    }

    args
}

/// Returns a configuration file that can be used by Kafka clients running inside the
/// Kubernetes cluster to connect to the Kafka servers.
pub fn client_properties(security: &ValidatedKafkaSecurity) -> Vec<(String, Option<String>)> {
    let mut props = vec![];

    if security.tls_client_authentication_class().is_some() {
        props.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::Ssl.to_string()),
        ));
        props.push((
            "ssl.client.auth".to_string(),
            Some(SSL_CLIENT_AUTH_REQUIRED.to_string()),
        ));
        push_client_ssl_stores(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
    } else if security.has_kerberos_enabled() {
        // TODO: to make this configuration file usable out of the box the operator needs to be
        // refactored to write out Java jaas files instead of passing command line parameters
        // to the Kafka daemon scripts.
        // This will simplify the code and the command lines lot.
        // It will also make the jaas files reusable by the Kafka shell scripts.
        props.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::SaslSsl.to_string()),
        ));
        push_client_ssl_stores(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
        props.push((
            PROPERTY_SASL_ENABLED_MECHANISMS.to_string(),
            Some(SASL_MECHANISM_GSSAPI.to_string()),
        ));
        props.push((
            PROPERTY_SASL_KERBEROS_SERVICE_NAME.to_string(),
            Some(KafkaRole::Broker.kerberos_service_name().to_string()),
        ));
        props.push((
            PROPERTY_SASL_INTER_BROKER_MECHANISM.to_string(),
            Some(SASL_MECHANISM_GSSAPI.to_string()),
        ));
        props.push((
            "sasl.jaas.config".to_string(),
            Some(format!("com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"{keytab}\" principal=\"{service}/{pod}@{realm}\"",
                keytab="/stackable/kerberos/keytab",
                service=KafkaRole::Broker.kerberos_service_name(),
                pod="todo",
                realm="$KERBEROS_REALM"))));
    } else if security.tls_server_secret_class().is_some() {
        props.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::Ssl.to_string()),
        ));
        push_client_ssl_truststore(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
    } else {
        props.push((
            PROPERTY_SECURITY_PROTOCOL.to_string(),
            Some(KafkaListenerProtocol::Plaintext.to_string()),
        ));
    }

    props
}

/// Adds required volumes and volume mounts to the broker pod and container builders
/// depending on the tls and authentication settings.
pub fn add_broker_volume_and_volume_mounts(
    security: &ValidatedKafkaSecurity,
    pod_builder: &mut PodBuilder,
    cb_kcat_prober: &mut ContainerBuilder,
    cb_kafka: &mut ContainerBuilder,
    requested_secret_lifetime: &Duration,
) -> Result<(), Error> {
    // add tls (server or client authentication volumes) if required
    if let Some(tls_server_secret_class) = tls_secret_class(security) {
        // We have to mount tls pem files for kcat (the mount can be used directly)
        pod_builder
            .add_volume(create_kcat_tls_volume(
                STACKABLE_TLS_KCAT_VOLUME_NAME,
                tls_server_secret_class,
                requested_secret_lifetime,
            )?)
            .context(AddVolumeSnafu)?;
        cb_kcat_prober
            .add_volume_mount(STACKABLE_TLS_KCAT_VOLUME_NAME, STACKABLE_TLS_KCAT_DIR)
            .context(AddVolumeMountSnafu)?;
        // Keystores fore the kafka container
        pod_builder
            .add_volume(create_tls_keystore_volume(
                STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
                tls_server_secret_class,
                requested_secret_lifetime,
            )?)
            .context(AddVolumeSnafu)?;
        cb_kafka
            .add_volume_mount(
                STACKABLE_TLS_KAFKA_SERVER_VOLUME_NAME,
                STACKABLE_TLS_KAFKA_SERVER_DIR,
            )
            .context(AddVolumeMountSnafu)?;
    }

    pod_builder
        .add_volume(create_tls_keystore_volume(
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            security.tls_internal_secret_class(),
            requested_secret_lifetime,
        )?)
        .context(AddVolumeSnafu)?;
    cb_kafka
        .add_volume_mount(
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            STACKABLE_TLS_KAFKA_INTERNAL_DIR,
        )
        .context(AddVolumeMountSnafu)?;

    if let Some(secret_class) = security.opa_secret_class() {
        cb_kafka
            .add_volume_mount(OPA_TLS_VOLUME_NAME, OPA_TLS_MOUNT_PATH)
            .context(AddVolumeMountSnafu)?;

        pod_builder
            .add_volume(
                VolumeBuilder::new(OPA_TLS_VOLUME_NAME)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            secret_class,
                            // Only the truststore is required to connect to OPA.
                            SecretClassVolumeProvisionParts::Public,
                        )
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
    security: &ValidatedKafkaSecurity,
    pod_builder: &mut PodBuilder,
    cb_kafka: &mut ContainerBuilder,
    requested_secret_lifetime: &Duration,
) -> Result<(), Error> {
    pod_builder
        .add_volume(
            VolumeBuilder::new(STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        security.tls_internal_secret_class(),
                        // Kafka needs both the public certificate and the private key for
                        // the internal communication.
                        SecretClassVolumeProvisionParts::PublicPrivate,
                    )
                    .with_pod_scope()
                    .with_format(SecretFormat::TlsPkcs12)
                    .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
                    .with_auto_tls_cert_domain_components_in_subject_dn(true)
                    .build()
                    .context(SecretVolumeBuildSnafu)?,
                )
                .build(),
        )
        .context(AddVolumeSnafu)?;
    cb_kafka
        .add_volume_mount(
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            STACKABLE_TLS_KAFKA_INTERNAL_DIR,
        )
        .context(AddVolumeMountSnafu)?;

    Ok(())
}

/// Inserts the `listener.<name>.ssl.{keystore,truststore}.{location,password,type}`
/// settings for `listener`, pointing at the PKCS12 stores under `dir`.
fn insert_listener_ssl_stores(
    config: &mut BTreeMap<String, String>,
    listener: &KafkaListenerName,
    dir: &str,
) {
    config.insert(
        listener.listener_ssl_keystore_location(),
        format!("{dir}/{}", KEYSTORE_P12_FILE_NAME),
    );
    config.insert(
        listener.listener_ssl_keystore_password(),
        SSL_STORE_PASSWORD.to_string(),
    );
    config.insert(
        listener.listener_ssl_keystore_type(),
        SSL_STORE_TYPE_PKCS12.to_string(),
    );
    config.insert(
        listener.listener_ssl_truststore_location(),
        format!("{dir}/{}", TRUSTSTORE_P12_FILE_NAME),
    );
    config.insert(
        listener.listener_ssl_truststore_password(),
        SSL_STORE_PASSWORD.to_string(),
    );
    config.insert(
        listener.listener_ssl_truststore_type(),
        SSL_STORE_TYPE_PKCS12.to_string(),
    );
}

/// Pushes the client-side `ssl.keystore.*` and `ssl.truststore.*` properties, pointing
/// at the PKCS12 stores under `dir`.
fn push_client_ssl_stores(props: &mut Vec<(String, Option<String>)>, dir: &str) {
    props.push((
        "ssl.keystore.type".to_string(),
        Some(SSL_STORE_TYPE_PKCS12.to_string()),
    ));
    props.push((
        "ssl.keystore.location".to_string(),
        Some(format!("{dir}/{}", KEYSTORE_P12_FILE_NAME)),
    ));
    props.push((
        "ssl.keystore.password".to_string(),
        Some(SSL_STORE_PASSWORD.to_string()),
    ));
    push_client_ssl_truststore(props, dir);
}

/// Pushes the client-side `ssl.truststore.*` properties, pointing at the PKCS12
/// truststore under `dir`.
fn push_client_ssl_truststore(props: &mut Vec<(String, Option<String>)>, dir: &str) {
    props.push((
        "ssl.truststore.type".to_string(),
        Some(SSL_STORE_TYPE_PKCS12.to_string()),
    ));
    props.push((
        "ssl.truststore.location".to_string(),
        Some(format!("{dir}/{}", TRUSTSTORE_P12_FILE_NAME)),
    ));
    props.push((
        "ssl.truststore.password".to_string(),
        Some(SSL_STORE_PASSWORD.to_string()),
    ));
}

/// Returns required Kafka configuration settings for the `broker.properties` file
/// depending on the tls and authentication settings.
pub fn broker_config_settings(security: &ValidatedKafkaSecurity) -> BTreeMap<String, String> {
    let mut config = BTreeMap::new();

    // We set either client tls with authentication or client tls without authentication
    // If authentication is explicitly required we do not want to have any other CAs to
    // be trusted.
    if security.tls_client_authentication_class().is_some()
        || security.tls_server_secret_class().is_some()
    {
        insert_listener_ssl_stores(
            &mut config,
            &KafkaListenerName::Client,
            STACKABLE_TLS_KAFKA_SERVER_DIR,
        );
        if security.tls_client_authentication_class().is_some() {
            // client auth required
            config.insert(
                KafkaListenerName::Client.listener_ssl_client_auth(),
                SSL_CLIENT_AUTH_REQUIRED.to_string(),
            );
        }
    }

    if security.has_kerberos_enabled() {
        // Bootstrap
        insert_listener_ssl_stores(
            &mut config,
            &KafkaListenerName::Bootstrap,
            STACKABLE_TLS_KAFKA_SERVER_DIR,
        );
        config.insert(
            PROPERTY_SASL_ENABLED_MECHANISMS.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
        config.insert(
            PROPERTY_SASL_KERBEROS_SERVICE_NAME.to_string(),
            KafkaRole::Broker.kerberos_service_name().to_string(),
        );
        config.insert(
            PROPERTY_SASL_INTER_BROKER_MECHANISM.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
        tracing::debug!("Kerberos configs added: [{:#?}]", config);
    }

    // Internal TLS
    // BROKERS
    insert_listener_ssl_stores(
        &mut config,
        &KafkaListenerName::Internal,
        STACKABLE_TLS_KAFKA_INTERNAL_DIR,
    );
    // CONTROLLERS
    insert_listener_ssl_stores(
        &mut config,
        &KafkaListenerName::Controller,
        STACKABLE_TLS_KAFKA_INTERNAL_DIR,
    );
    // client auth required
    config.insert(
        KafkaListenerName::Internal.listener_ssl_client_auth(),
        SSL_CLIENT_AUTH_REQUIRED.to_string(),
    );

    //OPA Tls
    if security.opa_secret_class().is_some() {
        config.insert(
            "opa.authorizer.truststore.path".to_string(),
            format!(
                "{}/{}",
                STACKABLE_TLS_KAFKA_INTERNAL_DIR, TRUSTSTORE_P12_FILE_NAME
            ),
        );
        config.insert(
            "opa.authorizer.truststore.password".to_string(),
            SSL_STORE_PASSWORD.to_string(),
        );
        config.insert(
            "opa.authorizer.truststore.type".to_string(),
            SSL_STORE_TYPE_PKCS12.to_string(),
        );
    }

    // common
    config.insert(
        INTER_BROKER_LISTENER_NAME.to_string(),
        listener::KafkaListenerName::Internal.to_string(),
    );

    config
}

/// Returns required Kafka configuration settings for the `controller.properties` file
/// depending on the tls and authentication settings.
pub fn controller_config_settings(security: &ValidatedKafkaSecurity) -> BTreeMap<String, String> {
    let mut config = BTreeMap::new();

    insert_listener_ssl_stores(
        &mut config,
        &KafkaListenerName::Controller,
        STACKABLE_TLS_KAFKA_INTERNAL_DIR,
    );

    // The TLS properties for the internal broker listener are needed by the Kraft controllers
    // too during metadata migration from ZooKeeper to Kraft mode.
    insert_listener_ssl_stores(
        &mut config,
        &KafkaListenerName::Internal,
        STACKABLE_TLS_KAFKA_INTERNAL_DIR,
    );
    // We set either client tls with authentication or client tls without authentication
    // If authentication is explicitly required we do not want to have any other CAs to
    // be trusted.
    if security.tls_client_authentication_class().is_some() {
        // client auth required
        config.insert(
            KafkaListenerName::Controller.listener_ssl_client_auth(),
            SSL_CLIENT_AUTH_REQUIRED.to_string(),
        );
    }

    // Kerberos
    if security.has_kerberos_enabled() {
        config.insert(
            PROPERTY_SASL_ENABLED_MECHANISMS.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
        config.insert(
            PROPERTY_SASL_KERBEROS_SERVICE_NAME.to_string(),
            KafkaRole::Controller.kerberos_service_name().to_string(),
        );
        config.insert(
            PROPERTY_SASL_INTER_BROKER_MECHANISM.to_string(),
            SASL_MECHANISM_GSSAPI.to_string(),
        );
        tracing::debug!("Kerberos configs added: [{:#?}]", config);
    }

    config
}

/// Returns the `SecretClass` provided in a `AuthenticationClass` for TLS.
fn tls_secret_class(security: &ValidatedKafkaSecurity) -> Option<&str> {
    security
        .tls_client_authentication_class()
        .and_then(|auth_class| match &auth_class.spec.provider {
            core::v1alpha1::AuthenticationClassProvider::Tls(tls) => {
                tls.client_cert_secret_class.as_deref()
            }
            _ => None,
        })
        .or_else(|| security.tls_server_secret_class())
}

/// Creates ephemeral volumes to mount the `SecretClass` into the Pods for kcat client
fn create_kcat_tls_volume(
    volume_name: &str,
    secret_class_name: &str,
    requested_secret_lifetime: &Duration,
) -> Result<Volume, Error> {
    Ok(VolumeBuilder::new(volume_name)
        .ephemeral(
            SecretOperatorVolumeSourceBuilder::new(
                secret_class_name,
                // Both the public certificate and the private key are required for the kcat
                // client authentication.
                SecretClassVolumeProvisionParts::PublicPrivate,
            )
            .with_pod_scope()
            .with_format(SecretFormat::TlsPem)
            .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
            .with_auto_tls_cert_domain_components_in_subject_dn(true)
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
            SecretOperatorVolumeSourceBuilder::new(
                secret_class_name,
                // Both the keystore and truststore are required for keystore volume.
                SecretClassVolumeProvisionParts::PublicPrivate,
            )
            .with_pod_scope()
            .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
            .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME)
            .with_format(SecretFormat::TlsPkcs12)
            .with_auto_tls_cert_lifetime(*requested_secret_lifetime)
            .with_auto_tls_cert_domain_components_in_subject_dn(true)
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
        format!("sasl.mechanism={SASL_MECHANISM_GSSAPI}"),
        "-X".to_string(),
        format!("sasl.kerberos.service.name={service_name}"),
        "-X".to_string(),
        format!(
            "sasl.kerberos.principal={service_name}/$POD_BROKER_LISTENER_ADDRESS@$KERBEROS_REALM"
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        builder::meta::ObjectMetaBuilder,
        crd::authentication::{core, kerberos, tls},
        v2::types::kubernetes::SecretClassName,
    };

    use super::*;
    use crate::crd::authentication::ResolvedAuthenticationClasses;

    fn tls_auth_class() -> core::v1alpha1::AuthenticationClass {
        core::v1alpha1::AuthenticationClass {
            metadata: ObjectMetaBuilder::new().name("tls-auth").build(),
            spec: core::v1alpha1::AuthenticationClassSpec {
                provider: core::v1alpha1::AuthenticationClassProvider::Tls(
                    tls::v1alpha1::AuthenticationProvider {
                        client_cert_secret_class: Some("client-auth-secret-class".to_string()),
                    },
                ),
            },
        }
    }

    fn kerberos_auth_class() -> core::v1alpha1::AuthenticationClass {
        core::v1alpha1::AuthenticationClass {
            metadata: ObjectMetaBuilder::new().name("kerberos-auth").build(),
            spec: core::v1alpha1::AuthenticationClassSpec {
                provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                    kerberos::v1alpha1::AuthenticationProvider {
                        kerberos_secret_class: "kerberos-secret-class".to_string(),
                    },
                ),
            },
        }
    }

    fn no_auth() -> ResolvedAuthenticationClasses {
        ResolvedAuthenticationClasses::new(vec![])
    }

    /// Plaintext: no TLS, no authentication, no OPA.
    fn plaintext() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            no_auth(),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            None,
            None,
        )
    }

    /// Server TLS only (encryption without client authentication).
    fn server_tls() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            no_auth(),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    /// Mutual TLS (client-certificate authentication).
    fn client_auth_tls() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![tls_auth_class()]),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    /// Kerberos, which also requires server and internal TLS.
    fn kerberos() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![kerberos_auth_class()]),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    /// Internal TLS only (broker/controller encryption without client TLS).
    fn internal_tls() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            no_auth(),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            None,
            None,
        )
    }

    /// OPA authorization with a TLS truststore (internal + server TLS also enabled).
    fn opa() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            no_auth(),
            SecretClassName::from_str("tls").expect("tls secret class name is valid"),
            Some("tls".parse().unwrap()),
            Some("opa-tls".parse().unwrap()),
        )
    }

    fn as_str_vec(commands: &[String]) -> Vec<&str> {
        commands.iter().map(String::as_str).collect()
    }

    fn as_map(props: Vec<(String, Option<String>)>) -> BTreeMap<String, Option<String>> {
        props.into_iter().collect()
    }

    // ---- kcat_prober_container_commands ----

    #[test]
    fn kcat_prober_plaintext_targets_insecure_client_port() {
        let commands = kcat_prober_container_commands(&plaintext());
        assert_eq!(
            as_str_vec(&commands),
            vec!["/stackable/kcat", "-b", "localhost:9092", "-L"]
        );
    }

    #[test]
    fn kcat_prober_server_tls_trusts_ca_without_client_key() {
        let commands = kcat_prober_container_commands(&server_tls());
        let commands = as_str_vec(&commands);

        assert_eq!(commands[0], "/stackable/kcat");
        assert_eq!(commands[2], "localhost:9093");
        assert_eq!(commands[commands.len() - 1], "-L");
        assert!(
            commands
                .iter()
                .any(|a| a.contains("ssl.ca.location=/stackable/tls-kcat/ca.crt"))
        );
        assert!(!commands.iter().any(|a| a.contains("ssl.key.location")));
    }

    #[test]
    fn kcat_prober_client_auth_presents_client_key() {
        let commands = kcat_prober_container_commands(&client_auth_tls());
        let commands = as_str_vec(&commands);

        assert_eq!(commands[0], "/stackable/kcat");
        assert_eq!(commands[2], "localhost:9093");
        assert!(
            commands
                .iter()
                .any(|a| a.contains("ssl.key.location=/stackable/tls-kcat/tls.key"))
        );
    }

    #[test]
    fn kcat_prober_kerberos_wraps_command_in_bash() {
        let commands = kcat_prober_container_commands(&kerberos());
        let commands = as_str_vec(&commands);

        assert_eq!(commands[0], "/bin/bash");
        assert_eq!(commands[4], "-c");
        assert_eq!(commands.len(), 6);

        let script = commands[5];
        assert!(script.contains("KERBEROS_REALM"));
        assert!(script.contains("/stackable/kcat"));
        assert!(script.contains("sasl.mechanism=GSSAPI"));
        assert!(script.contains("sasl.kerberos.service.name=kafka"));
    }

    // ---- copy_opa_tls_cert_command ----

    #[test]
    fn copy_opa_tls_cert_command_imports_ca_when_opa_enabled() {
        let command = copy_opa_tls_cert_command(&opa());
        assert!(command.contains("keytool -importcert"));
        assert!(command.contains("-alias opa-ca"));
        assert!(command.contains("/stackable/tls-opa/ca.crt"));
    }

    #[test]
    fn copy_opa_tls_cert_command_empty_without_opa() {
        assert_eq!(copy_opa_tls_cert_command(&plaintext()), "");
    }

    // ---- client_properties ----

    #[test]
    fn client_properties_plaintext() {
        let props = as_map(client_properties(&plaintext()));
        assert_eq!(
            props.get("security.protocol"),
            Some(&Some("PLAINTEXT".to_string()))
        );
        assert_eq!(props.len(), 1);
    }

    #[test]
    fn client_properties_server_tls_uses_truststore_only() {
        let props = as_map(client_properties(&server_tls()));
        assert_eq!(
            props.get("security.protocol"),
            Some(&Some("SSL".to_string()))
        );
        assert!(props.contains_key("ssl.truststore.location"));
        assert!(!props.contains_key("ssl.keystore.location"));
    }

    #[test]
    fn client_properties_client_auth_requires_keystore_and_client_auth() {
        let props = as_map(client_properties(&client_auth_tls()));
        assert_eq!(
            props.get("security.protocol"),
            Some(&Some("SSL".to_string()))
        );
        assert_eq!(
            props.get("ssl.client.auth"),
            Some(&Some("required".to_string()))
        );
        assert!(props.contains_key("ssl.keystore.location"));
    }

    #[test]
    fn client_properties_kerberos_uses_sasl_ssl() {
        let props = as_map(client_properties(&kerberos()));
        assert_eq!(
            props.get("security.protocol"),
            Some(&Some("SASL_SSL".to_string()))
        );
        assert_eq!(
            props.get("sasl.enabled.mechanisms"),
            Some(&Some("GSSAPI".to_string()))
        );
        assert_eq!(
            props.get("sasl.kerberos.service.name"),
            Some(&Some("kafka".to_string()))
        );
        assert!(props.contains_key("sasl.jaas.config"));
    }

    // ---- broker_config_settings ----

    #[test]
    fn broker_config_plaintext_still_configures_internal_tls() {
        // Internal (inter-broker) TLS is mandatory, so even a plaintext cluster (no client TLS or
        // authentication) still carries the internal + controller SSL stores and the inter-broker
        // listener — while the CLIENT listener stays plaintext.
        let config = broker_config_settings(&plaintext());
        assert_eq!(
            config.get(INTER_BROKER_LISTENER_NAME),
            Some(&"INTERNAL".to_string())
        );
        assert!(config.contains_key(&KafkaListenerName::Internal.listener_ssl_keystore_location()));
        assert!(config.contains_key(&KafkaListenerName::Internal.listener_ssl_client_auth()));
        assert!(
            config.contains_key(&KafkaListenerName::Controller.listener_ssl_keystore_location())
        );
        assert!(!config.contains_key(&KafkaListenerName::Client.listener_ssl_keystore_location()));
    }

    #[test]
    fn broker_config_client_auth_requires_client_auth() {
        let config = broker_config_settings(&client_auth_tls());
        assert_eq!(
            config.get("listener.name.client.ssl.client.auth"),
            Some(&"required".to_string())
        );
        assert!(config.contains_key("listener.name.client.ssl.keystore.location"));
    }

    #[test]
    fn broker_config_kerberos_adds_sasl_and_bootstrap_stores() {
        let config = broker_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.enabled.mechanisms"),
            Some(&"GSSAPI".to_string())
        );
        assert_eq!(
            config.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
        assert_eq!(
            config.get("sasl.mechanism.inter.broker.protocol"),
            Some(&"GSSAPI".to_string())
        );
        assert!(config.contains_key("listener.name.bootstrap.ssl.keystore.location"));
    }

    #[test]
    fn broker_config_internal_tls_covers_internal_and_controller() {
        let config = broker_config_settings(&internal_tls());
        assert!(config.contains_key("listener.name.internal.ssl.keystore.location"));
        assert!(config.contains_key("listener.name.controller.ssl.keystore.location"));
        assert_eq!(
            config.get("listener.name.internal.ssl.client.auth"),
            Some(&"required".to_string())
        );
    }

    #[test]
    fn broker_config_opa_adds_authorizer_truststore() {
        let config = broker_config_settings(&opa());
        assert!(config.contains_key("opa.authorizer.truststore.path"));
        assert!(config.contains_key("opa.authorizer.truststore.password"));
        assert!(config.contains_key("opa.authorizer.truststore.type"));
    }

    // ---- controller_config_settings ----

    #[test]
    fn controller_config_plaintext_has_internal_tls() {
        // Internal TLS is mandatory, so the controller config is non-empty even for a plaintext
        // cluster: it carries the controller + internal SSL stores, but no client-cert auth
        // (there is no client authentication class).
        let config = controller_config_settings(&plaintext());
        assert!(
            config.contains_key(&KafkaListenerName::Controller.listener_ssl_keystore_location())
        );
        assert!(config.contains_key(&KafkaListenerName::Internal.listener_ssl_keystore_location()));
        assert!(!config.contains_key(&KafkaListenerName::Controller.listener_ssl_client_auth()));
    }

    #[test]
    fn controller_config_internal_tls_covers_controller_and_internal() {
        let config = controller_config_settings(&internal_tls());
        assert!(config.contains_key("listener.name.controller.ssl.keystore.location"));
        assert!(config.contains_key("listener.name.internal.ssl.keystore.location"));
    }

    #[test]
    fn controller_config_kerberos_adds_sasl() {
        let config = controller_config_settings(&kerberos());
        assert_eq!(
            config.get("sasl.enabled.mechanisms"),
            Some(&"GSSAPI".to_string())
        );
        assert_eq!(
            config.get("sasl.kerberos.service.name"),
            Some(&"kafka".to_string())
        );
    }
}

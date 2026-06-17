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
        args.push("/stackable/kcat".to_string());
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
        bash_args.push("/stackable/kcat".to_string());
        bash_args.push("-b".to_string());
        bash_args.push("$POD_BROKER_LISTENER_ADDRESS:$POD_BROKER_LISTENER_PORT".to_string());
        bash_args.extend(kcat_client_sasl_ssl(STACKABLE_TLS_KCAT_DIR, service_name));
        bash_args.push("-L".to_string());

        args.push(bash_args.join(" "));
    } else if security.tls_server_secret_class().is_some() {
        args.push("/stackable/kcat".to_string());
        args.push("-b".to_string());
        args.push(format!("localhost:{}", port));
        args.extend(kcat_client_ssl(STACKABLE_TLS_KCAT_DIR));
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
pub fn client_properties(security: &ValidatedKafkaSecurity) -> Vec<(String, Option<String>)> {
    let mut props = vec![];

    if security.tls_client_authentication_class().is_some() {
        props.push((
            "security.protocol".to_string(),
            Some(KafkaListenerProtocol::Ssl.to_string()),
        ));
        props.push(("ssl.client.auth".to_string(), Some("required".to_string())));
        push_client_ssl_stores(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
    } else if security.has_kerberos_enabled() {
        // TODO: to make this configuration file usable out of the box the operator needs to be
        // refactored to write out Java jaas files instead of passing command line parameters
        // to the Kafka daemon scripts.
        // This will simplify the code and the command lines lot.
        // It will also make the jaas files reusable by the Kafka shell scripts.
        props.push((
            "security.protocol".to_string(),
            Some(KafkaListenerProtocol::SaslSsl.to_string()),
        ));
        push_client_ssl_stores(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
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
    } else if security.tls_server_secret_class().is_some() {
        props.push((
            "security.protocol".to_string(),
            Some(KafkaListenerProtocol::Ssl.to_string()),
        ));
        push_client_ssl_truststore(&mut props, STACKABLE_TLS_KAFKA_SERVER_DIR);
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

    if let Some(tls_internal_secret_class) = security.tls_internal_secret_class() {
        pod_builder
            .add_volume(create_tls_keystore_volume(
                STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                tls_internal_secret_class,
                requested_secret_lifetime,
            )?)
            .context(AddVolumeSnafu)?;
        cb_kafka
            .add_volume_mount(
                STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                STACKABLE_TLS_KAFKA_INTERNAL_DIR,
            )
            .context(AddVolumeMountSnafu)?;
    }

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
    if let Some(tls_internal_secret_class) = security.tls_internal_secret_class() {
        pod_builder
            .add_volume(
                VolumeBuilder::new(STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME)
                    .ephemeral(
                        SecretOperatorVolumeSourceBuilder::new(
                            tls_internal_secret_class,
                            // Kafka needs both the public certificate and the private key for
                            // the internal communication.
                            SecretClassVolumeProvisionParts::PublicPrivate,
                        )
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
                STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                STACKABLE_TLS_KAFKA_INTERNAL_DIR,
            )
            .context(AddVolumeMountSnafu)?;
    }

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
                "required".to_string(),
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
    if security.tls_internal_secret_class().is_some() {
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
            "required".to_string(),
        );
    }

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

    if security.tls_client_authentication_class().is_some()
        || security.tls_internal_secret_class().is_some()
    {
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
                "required".to_string(),
            );
        }
    }

    // Kerberos
    if security.has_kerberos_enabled() {
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

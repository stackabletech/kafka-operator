use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::{
        container::ContainerBuilder,
        volume::{
            SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
            VolumeBuilder,
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constant,
    k8s_openapi::api::core::v1::Volume,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        types::kubernetes::VolumeName,
    },
};

use crate::{
    controller::security::ValidatedKafkaSecurity,
    crd::{
        LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, STACKABLE_KERBEROS_DIR,
        STACKABLE_KERBEROS_KRB5_PATH, role::KafkaRole,
    },
};

constant!(pub KERBEROS_VOLUME_NAME: VolumeName = "kerberos");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add Kerberos secret volume"))]
    KerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },
}

/// Mounts the Kerberos keytab and `krb5.conf` volume into a broker's `kafka` container and
/// returns the pod volume, when Kerberos is enabled; `None` otherwise.
///
/// Brokers are exposed through listener-operator `Listener` volumes (the broker and bootstrap
/// listeners), so the keytab's principals must cover both.
///
/// Only brokers get a keytab: Kerberos is used on the client-facing listeners only. The Kafka
/// nodes authenticate each other with their internal pod certificates, so KRaft controllers
/// have no Kerberos configuration at all.
///
/// # Panics
///
/// Panics if the volume mount cannot be added to the container builder. Only call this on a
/// container builder whose mount paths are still distinct from the ones added here.
pub fn mount_broker_kerberos_volume(
    kafka_security: &ValidatedKafkaSecurity,
    cb_kafka: &mut ContainerBuilder,
) -> Result<Option<Volume>, Error> {
    let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() else {
        return Ok(None);
    };
    let kerberos_secret_operator_volume = SecretOperatorVolumeSourceBuilder::new(
        &kerberos_secret_class,
        // We need both public (krb5.conf) and private (keytab) parts.
        SecretClassVolumeProvisionParts::PublicPrivate,
    )
    .with_listener_volume_scope(&*LISTENER_BROKER_VOLUME_NAME)
    .with_listener_volume_scope(&*LISTENER_BOOTSTRAP_VOLUME_NAME)
    .with_kerberos_service_name(KafkaRole::Broker.kerberos_service_name())
    .build()
    .context(KerberosSecretVolumeSnafu)?;
    cb_kafka
        .add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.");

    Ok(Some(
        VolumeBuilder::new(&*KERBEROS_VOLUME_NAME)
            .ephemeral(kerberos_secret_operator_volume)
            .build(),
    ))
}

constant!(pub KRB5_CONFIG: EnvVarName = "KRB5_CONFIG");
constant!(pub KAFKA_OPTS: EnvVarName = "KAFKA_OPTS");

/// The environment variables the Kerberos configuration requires on the Kafka container, or an
/// empty set when Kerberos is disabled.
///
/// Returned as an [`EnvVarSet`] (rather than added to the containers directly) so the callers
/// can merge the user's `envOverrides` on top, letting an override win on a name collision.
pub fn kerberos_env_vars(kafka_security: &ValidatedKafkaSecurity) -> EnvVarSet {
    if !kafka_security.has_kerberos_enabled() {
        return EnvVarSet::new();
    }
    EnvVarSet::new()
        .with_value(&KRB5_CONFIG, STACKABLE_KERBEROS_KRB5_PATH)
        .with_value(
            &KAFKA_OPTS,
            format!(
                "-Djava.security.auth.login.config=/tmp/jaas.properties -Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}"
            ),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *KRB5_CONFIG;
        let _ = *KAFKA_OPTS;
        let _ = *KERBEROS_VOLUME_NAME;
    }
}

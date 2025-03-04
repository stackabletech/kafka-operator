use snafu::{ResultExt, Snafu};
use stackable_operator::builder::{
    self,
    pod::{
        container::ContainerBuilder,
        volume::{
            SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
            VolumeBuilder,
        },
        PodBuilder,
    },
};

use crate::crd::{
    security::KafkaTlsSecurity, KafkaRole, LISTENER_BOOTSTRAP_VOLUME_NAME,
    LISTENER_BROKER_VOLUME_NAME, STACKABLE_KERBEROS_DIR, STACKABLE_KERBEROS_KRB5_PATH,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add Kerberos secret volume"))]
    KerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },
}

pub fn add_kerberos_pod_config(
    kafka_security: &KafkaTlsSecurity,
    role: &KafkaRole,
    cb_kcat_prober: &mut ContainerBuilder,
    cb_kafka: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() {
        // Mount keytab
        let kerberos_secret_operator_volume =
            SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class)
                .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
                .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME)
                // The pod scope is required for the kcat-prober.
                .with_pod_scope()
                .with_kerberos_service_name(role.kerberos_service_name())
                .build()
                .context(KerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        for cb in [cb_kafka, cb_kcat_prober] {
            cb.add_volume_mount("kerberos", STACKABLE_KERBEROS_DIR)
                .context(AddVolumeMountSnafu)?;
            cb.add_env_var("KRB5_CONFIG", STACKABLE_KERBEROS_KRB5_PATH);
            cb.add_env_var(
                "KAFKA_OPTS",
                format!("-Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}",),
            );
        }
    }

    Ok(())
}

use snafu::{ResultExt, Snafu};
use stackable_operator::builder::{
    self,
    pod::{
        PodBuilder,
        container::ContainerBuilder,
        volume::{
            SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
            VolumeBuilder,
        },
    },
};

use crate::crd::{
    LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, LISTENER_HEADLESS_VOLUME_NAME,
    STACKABLE_KERBEROS_DIR, STACKABLE_KERBEROS_KRB5_PATH, role::KafkaRole,
    security::KafkaTlsSecurity,
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
    container_builders: &mut [&mut ContainerBuilder],
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() {
        for cb in container_builders.iter_mut() {
            cb.add_volume_mount("kerberos", STACKABLE_KERBEROS_DIR)
                .context(AddVolumeMountSnafu)?;
            cb.add_env_var("KRB5_CONFIG", STACKABLE_KERBEROS_KRB5_PATH);
            cb.add_env_var(
                "KAFKA_OPTS",
                format!("-Djava.security.auth.login.config=/tmp/jaas.properties -Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}",),
            );
        }
        match role {
            KafkaRole::Broker => {
                // Mount keytab
                let kerberos_secret_operator_volume =
                    SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class)
                        .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
                        .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME)
                        .with_kerberos_service_name(role.kerberos_service_name())
                        .build()
                        .context(KerberosSecretVolumeSnafu)?;
                pb.add_volume(
                    VolumeBuilder::new("kerberos")
                        .ephemeral(kerberos_secret_operator_volume)
                        .build(),
                )
                .context(AddVolumeSnafu)?;
            }
            KafkaRole::Controller => {
                // Mount keytab
                let kerberos_secret_operator_volume =
                    SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class)
                        .with_kerberos_service_name(role.kerberos_service_name())
                        .with_listener_volume_scope(LISTENER_HEADLESS_VOLUME_NAME)
                        .build()
                        .context(KerberosSecretVolumeSnafu)?;
                pb.add_volume(
                    VolumeBuilder::new("kerberos")
                        .ephemeral(kerberos_secret_operator_volume)
                        .build(),
                )
                .context(AddVolumeSnafu)?;
            }
        }
    }
    Ok(())
}

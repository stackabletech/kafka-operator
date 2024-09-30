use snafu::{ResultExt, Snafu};
use stackable_kafka_crd::{security::KafkaTlsSecurity, KafkaCluster, KafkaRole};
use stackable_operator::{
    builder::{
        self,
        pod::{
            container::ContainerBuilder,
            volume::{
                SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
                VolumeBuilder,
            },
            PodBuilder,
        },
    },
    kube::ResourceExt,
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
    kafka: &KafkaCluster,
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
                .with_service_scope(kafka.name_any())
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
        cb_kcat_prober
            .add_volume_mount("kerberos", "/stackable/kerberos")
            .context(AddVolumeMountSnafu)?;
        cb_kcat_prober.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
        cb_kcat_prober.add_env_var(
            "KAFKA_OPTS",
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf",
        );

        cb_kafka
            .add_volume_mount("kerberos", "/stackable/kerberos")
            .context(AddVolumeMountSnafu)?;
        cb_kafka.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
        cb_kafka.add_env_var(
            "KAFKA_OPTS",
            "-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf",
        );
    }

    Ok(())
}

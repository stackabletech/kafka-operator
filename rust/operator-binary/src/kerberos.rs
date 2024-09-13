use snafu::{ResultExt, Snafu};
use stackable_kafka_crd::{KafkaCluster, KafkaRole};
use stackable_operator::{
    builder::pod::{
        container::ContainerBuilder,
        volume::{
            SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
            VolumeBuilder,
        },
        PodBuilder,
    },
    kube::ResourceExt,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add Kerberos secret volume"))]
    AddKerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },
}

pub fn add_kerberos_pod_config(
    kafka: &KafkaCluster,
    role: &KafkaRole,
    cb_kcat_prober: &mut ContainerBuilder,
    cb_kafka: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka.kerberos_secret_class() {
        // Mount keytab
        let kerberos_secret_operator_volume =
            SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class)
                .with_service_scope(kafka.name_any())
                .with_pod_scope()
                .with_kerberos_service_name(role.kerberos_service_name())
                .build()
                .context(AddKerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        );
        cb_kcat_prober.add_volume_mount("kerberos", "/stackable/kerberos");
        cb_kcat_prober.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");

        cb_kafka.add_volume_mount("kerberos", "/stackable/kerberos");
        cb_kafka.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
    }

    Ok(())
}

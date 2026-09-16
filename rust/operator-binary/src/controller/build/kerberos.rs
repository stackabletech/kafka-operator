use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{
                SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
                VolumeBuilder,
            },
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constant,
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

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },
}

/// Adds the Kerberos keytab and `krb5.conf` volume to the pod builder and mounts it into the
/// Kafka and kcat-prober containers, when Kerberos is enabled.
///
/// # Panics
///
/// Panics if the volume mounts cannot be added to the container builders. Only call this on
/// container builders whose mount paths are still distinct from the ones added here.
pub fn add_kerberos_pod_config(
    kafka_security: &ValidatedKafkaSecurity,
    role: &KafkaRole,
    cb_kafka: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() {
        // Mount keytab
        let mut volume_builder = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        );
        match role {
            // Brokers are exposed through listener-operator `Listener` volumes (the broker
            // and bootstrap listeners), so the keytab principal must cover both.
            KafkaRole::Broker => {
                volume_builder
                    .with_listener_volume_scope(&*LISTENER_BROKER_VOLUME_NAME)
                    .with_listener_volume_scope(&*LISTENER_BOOTSTRAP_VOLUME_NAME);
            }
            // KRaft controllers have no listener-operator `Listener` volume: they are only
            // reachable through their own StatefulSet pod DNS name, so the keytab must be
            // pod-scoped, matching how the controller's internal TLS cert is provisioned in
            // `add_controller_volume_and_volume_mounts`.
            KafkaRole::Controller => {
                volume_builder.with_pod_scope();
            }
        }
        let kerberos_secret_operator_volume = volume_builder
            .with_kerberos_service_name(role.kerberos_service_name())
            .build()
            .context(KerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new(&*KERBEROS_VOLUME_NAME)
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        cb_kafka
            .add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
            .expect("The mount paths are statically defined and there should be no duplicates.");
    }

    Ok(())
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
    use std::collections::BTreeMap;

    use stackable_operator::builder::pod::container::ContainerBuilder;

    use super::*;
    use crate::controller::build::security::tests::kerberos;

    /// Reads the `secrets.stackable.tech/*` annotations off the `kerberos` ephemeral volume.
    fn kerberos_volume_annotations(pb: &mut PodBuilder) -> BTreeMap<String, String> {
        pb.build_template()
            .spec
            .as_ref()
            .and_then(|spec| spec.volumes.as_ref())
            .and_then(|volumes| {
                volumes
                    .iter()
                    .find(|v| v.name == KERBEROS_VOLUME_NAME.to_string())
            })
            .expect("kerberos volume must be present")
            .ephemeral
            .as_ref()
            .expect("kerberos volume must be an ephemeral secret-operator volume")
            .volume_claim_template
            .as_ref()
            .and_then(|t| t.metadata.as_ref())
            .and_then(|m| m.annotations.clone())
            .expect("volume claim template must carry secrets.stackable.tech annotations")
    }

    fn kerberos_volume_annotations_for(role: &KafkaRole) -> BTreeMap<String, String> {
        let mut pb = PodBuilder::new();
        let mut cb_kafka = ContainerBuilder::new("kafka").expect("valid container name");

        add_kerberos_pod_config(&kerberos(), role, &mut cb_kafka, &mut pb)
            .expect("kerberos pod config");

        kerberos_volume_annotations(&mut pb)
    }

    #[test]
    fn controller_keytab_is_pod_scoped() {
        let annotations = kerberos_volume_annotations_for(&KafkaRole::Controller);

        // Controllers have no listener-operator Listener volume, so the keytab must be
        // scoped to the pod's own DNS name, matching how their internal TLS cert is
        // provisioned in `add_controller_volume_and_volume_mounts`.
        assert_eq!(
            annotations
                .get("secrets.stackable.tech/scope")
                .map(String::as_str),
            Some("pod"),
            "controller keytab must be pod-scoped, got: {annotations:?}"
        );
        assert_eq!(
            annotations
                .get("secrets.stackable.tech/kerberos.service.names")
                .map(String::as_str),
            Some("kafka")
        );
    }

    #[test]
    fn broker_keytab_stays_listener_scoped() {
        let annotations = kerberos_volume_annotations_for(&KafkaRole::Broker);

        let scope = annotations
            .get("secrets.stackable.tech/scope")
            .expect("scope annotation must be present");
        assert!(
            scope.contains("listener-volume=listener-broker")
                && scope.contains("listener-volume=listener-bootstrap"),
            "broker keytab must stay listener-volume-scoped, got: {scope}"
        );
        assert!(
            !scope.split(',').any(|s| s == "pod"),
            "broker keytab must not be pod-scoped, got: {scope}"
        );
    }

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *KRB5_CONFIG;
        let _ = *KAFKA_OPTS;
        let _ = *KERBEROS_VOLUME_NAME;
    }
}

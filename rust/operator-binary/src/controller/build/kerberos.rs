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
};

use crate::{
    controller::security::ValidatedKafkaSecurity,
    crd::{
        LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, STACKABLE_KERBEROS_DIR,
        STACKABLE_KERBEROS_KRB5_PATH, role::KafkaRole,
    },
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
    kafka_security: &ValidatedKafkaSecurity,
    role: &KafkaRole,
    cb_kcat_prober: Option<&mut ContainerBuilder>,
    cb_kafka: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = kafka_security.kerberos_secret_class() {
        let mut volume_builder = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        );
        match role {
            // Brokers are exposed through listener-operator `Listener` volumes (the client
            // and bootstrap listeners); the keytab principal must cover both.
            KafkaRole::Broker => {
                volume_builder
                    .with_listener_volume_scope(LISTENER_BROKER_VOLUME_NAME)
                    .with_listener_volume_scope(LISTENER_BOOTSTRAP_VOLUME_NAME);
            }
            // KRaft controllers have no listener-operator `Listener` volume (see
            // `controller/build/mod.rs`, "Only broker role groups get a bootstrap Listener"):
            // they're only reachable through their own StatefulSet pod DNS name, so the keytab
            // must be pod-scoped, matching how the controller's internal TLS cert is provisioned
            // in `add_controller_volume_and_volume_mounts`.
            KafkaRole::Controller => {
                volume_builder.with_pod_scope();
            }
        };
        let kerberos_secret_operator_volume = volume_builder
            .with_kerberos_service_name(role.kerberos_service_name())
            .build()
            .context(KerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;

        let mut containers: Vec<&mut ContainerBuilder> = vec![cb_kafka];
        if let Some(cb_kcat_prober) = cb_kcat_prober {
            containers.push(cb_kcat_prober);
        }
        for cb in containers {
            cb.add_volume_mount("kerberos", STACKABLE_KERBEROS_DIR)
                .context(AddVolumeMountSnafu)?;
            cb.add_env_var("KRB5_CONFIG", STACKABLE_KERBEROS_KRB5_PATH);
            cb.add_env_var(
                "KAFKA_OPTS",
                format!("-Djava.security.auth.login.config=/tmp/jaas.properties -Djava.security.krb5.conf={STACKABLE_KERBEROS_KRB5_PATH}",),
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        builder::{meta::ObjectMetaBuilder, pod::container::ContainerBuilder},
        crd::authentication::{core, kerberos},
    };

    use super::*;
    use crate::crd::authentication::ResolvedAuthenticationClasses;

    fn kerberos_security() -> ValidatedKafkaSecurity {
        ValidatedKafkaSecurity::new(
            ResolvedAuthenticationClasses::new(vec![core::v1alpha1::AuthenticationClass {
                metadata: ObjectMetaBuilder::new().name("kerberos-auth").build(),
                spec: core::v1alpha1::AuthenticationClassSpec {
                    provider: core::v1alpha1::AuthenticationClassProvider::Kerberos(
                        kerberos::v1alpha1::AuthenticationProvider {
                            kerberos_secret_class: "kerberos-secret-class".to_string(),
                        },
                    ),
                },
            }]),
            "tls".parse().unwrap(),
            Some("tls".parse().unwrap()),
            None,
        )
    }

    #[test]
    fn controller_role_mounts_pod_scoped_keytab_without_kcat_container() {
        let mut pb = PodBuilder::new();
        let mut cb_kafka = ContainerBuilder::new("kafka").expect("valid container name");

        add_kerberos_pod_config(
            &kerberos_security(),
            &KafkaRole::Controller,
            None,
            &mut cb_kafka,
            &mut pb,
        )
        .expect("kerberos pod config for controller role");

        let pod = pb.build_template();
        let kerberos_volume = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.volumes.as_ref())
            .and_then(|volumes| volumes.iter().find(|v| v.name == "kerberos"))
            .expect("kerberos volume must be present");
        let ephemeral = kerberos_volume
            .ephemeral
            .as_ref()
            .expect("kerberos volume must be an ephemeral (secret-operator) volume");
        let annotations = ephemeral
            .volume_claim_template
            .as_ref()
            .and_then(|t| t.metadata.as_ref())
            .and_then(|m| m.annotations.as_ref())
            .expect("volume claim template must carry secrets.stackable.tech annotations");
        // Pod-scoping (`with_pod_scope()`) is expressed as a `secrets.stackable.tech/scope: pod`
        // annotation (same as the controller's internal TLS cert, see
        // `add_controller_volume_and_volume_mounts`) -- it must not mention a listener volume.
        assert_eq!(
            annotations
                .get("secrets.stackable.tech/scope")
                .map(String::as_str),
            Some("pod"),
            "controller keytab must be pod-scoped only, not listener-volume-scoped: {annotations:?}"
        );

        let kafka_container = cb_kafka.build();
        let env_names: Vec<_> = kafka_container
            .env
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(env_names.contains(&"KRB5_CONFIG".to_string()));
        assert!(env_names.contains(&"KAFKA_OPTS".to_string()));
    }
}

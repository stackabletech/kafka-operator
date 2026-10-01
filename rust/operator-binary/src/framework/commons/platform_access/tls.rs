use serde::{Deserialize, Serialize};
use stackable_operator::{
    builder::pod::{
        PodBuilder,
        container::ContainerBuilder,
        volume::{
            SecretFormat, SecretOperatorVolumeSourceBuilder, VolumeBuilder, VolumeMountBuilder,
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    k8s_openapi::api::core::v1::{SecretVolumeSource, Volume, VolumeMount},
    schemars::{self, JsonSchema},
    v2::types::kubernetes::{SecretClassName, SecretName},
};

use crate::framework::constants::secret::SECRET_BASE_PATH;

/// Source of a TLS client certificate: a secret-operator SecretClass or a static Secret.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum TlsClientCredential {
    /// An AutoTLS SecretClass used to provision the certificate.
    SecretClass(SecretClassName),

    /// A static Secret holding the certificate in the keys `tls.crt` and `tls.key` (PEM), such as a
    /// Secret of type `kubernetes.io/tls`.
    Secret(SecretName),
}

impl TlsClientCredential {
    /// Adds the certificate volume to the Pod and mounts it into all given containers.
    pub fn add_volumes_and_mounts(
        &self,
        pod_builder: &mut PodBuilder,
        container_builders: Vec<&mut ContainerBuilder>,
    ) {
        let (volumes, mounts) = self.volumes_and_mounts();
        pod_builder
            .add_volumes(volumes)
            .expect("The volume name is derived from the credential and should not collide.");
        for container_builder in container_builders {
            container_builder
                .add_volume_mounts(mounts.clone())
                .expect("The mount path is derived from the credential and should not collide.");
        }
    }

    fn volumes_and_mounts(&self) -> (Vec<Volume>, Vec<VolumeMount>) {
        let volume_name = self.volume_name();
        let volume = match self {
            Self::SecretClass(secret_class) => VolumeBuilder::new(&volume_name)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        secret_class,
                        SecretClassVolumeProvisionParts::PublicPrivate,
                    )
                    .with_pod_scope()
                    .with_format(SecretFormat::TlsPem)
                    .build()
                    .expect("the annotations are built from a valid SecretClass name"),
                )
                .build(),
            Self::Secret(secret) => Volume {
                name: volume_name.clone(),
                secret: Some(SecretVolumeSource {
                    secret_name: Some(secret.to_string()),
                    ..SecretVolumeSource::default()
                }),
                ..Volume::default()
            },
        };
        let mount = VolumeMountBuilder::new(&volume_name, self.mount_path()).build();
        (vec![volume], vec![mount])
    }

    /// The directory containing `tls.crt`, `tls.key` and `ca.crt` (PEM).
    pub fn mount_path(&self) -> String {
        format!("{SECRET_BASE_PATH}/{}", self.volume_name())
    }

    fn volume_name(&self) -> String {
        match self {
            Self::SecretClass(secret_class) => format!("{secret_class}-tls-cert"),
            Self::Secret(secret) => format!("{secret}-tls-cert"),
        }
    }
}

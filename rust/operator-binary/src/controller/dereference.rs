//! The dereference step in the KafkaCluster controller.
//!
//! Fetches all Kubernetes objects referenced by the [`v1alpha1::KafkaCluster`] spec and returns
//! them in [`DereferencedObjects`]. This step only performs I/O; validation of the fetched
//! objects (constraints on which auth class providers are supported, kerberos + TLS
//! compatibility, etc.) happens in the validate step.
//!
//! `KafkaAuthorization::get_opa_config` is a pure fetch + URL assembly (no validation to peel off)
//! and stays here as-is.

use snafu::{ResultExt, Snafu};
use stackable_operator::{client::Client, utils::cluster_info::KubernetesClusterInfo};

use crate::crd::{
    authentication::{self, ResolvedAuthenticationClasses},
    authorization::{self, KafkaAuthorizationConfig},
    v1alpha1,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to fetch authentication classes"))]
    FetchAuthenticationClasses { source: authentication::Error },

    #[snafu(display("failed to get OPA config"))]
    GetOpaConfig { source: authorization::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Kubernetes objects referenced from the [`v1alpha1::KafkaCluster`] spec, already fetched but
/// not yet validated.
pub struct DereferencedObjects {
    pub authentication_classes: ResolvedAuthenticationClasses,
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub kubernetes_cluster_info: KubernetesClusterInfo,
}

/// Fetches all Kubernetes objects referenced from the [`v1alpha1::KafkaCluster`] spec.
pub async fn dereference(
    client: &Client,
    kafka: &v1alpha1::KafkaCluster,
) -> Result<DereferencedObjects> {
    let authentication_classes = ResolvedAuthenticationClasses::fetch_references(
        client,
        &kafka.spec.cluster_config.authentication,
    )
    .await
    .context(FetchAuthenticationClassesSnafu)?;

    let authorization_config = kafka
        .spec
        .cluster_config
        .authorization
        .clone()
        .get_opa_config(client, kafka)
        .await
        .context(GetOpaConfigSnafu)?;

    Ok(DereferencedObjects {
        authentication_classes,
        authorization_config,
        kubernetes_cluster_info: client.kubernetes_cluster_info.clone(),
    })
}

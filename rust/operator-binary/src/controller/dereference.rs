//! The dereference step in the KafkaCluster controller.
//!
//! Fetches the Kubernetes objects the later steps need and returns them in
//! [`DereferencedObjects`]. Most of them are referenced from the [`v1alpha1::KafkaCluster`]
//! spec (e.g. the AuthenticationClasses). The broker role groups' bootstrap `Listener`s are the
//! exception: they are not referenced from the spec but created by this operator itself in a
//! previous reconcile run, and are fetched back because the discovery `ConfigMap` is built from
//! their ingress addresses, which only the listener-operator writes. `Listener`s that do not
//! exist yet (e.g. around the first reconcile runs) are simply absent.
//!
//! Validation of the fetched objects (constraints on which auth class providers are supported,
//! kerberos + TLS compatibility, etc.) happens in the validate step, not here.
//!
//! `KafkaAuthorization::get_opa_config` is a pure fetch + URL assembly (no validation to peel off)
//! and stays here as-is.

use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    crd::listener,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        controller_utils::{get_cluster_name, get_namespace},
        types::kubernetes::ListenerName,
    },
};

use crate::{
    controller::{RoleGroupName, build::resource::listener::bootstrap_listener_name},
    crd::{
        authentication::{self, ResolvedAuthenticationClasses},
        authorization::{self, KafkaAuthorizationConfig},
        role::KafkaRole,
        v1alpha1,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to fetch authentication classes"))]
    FetchAuthenticationClasses { source: authentication::Error },

    #[snafu(display("failed to get OPA config"))]
    GetOpaConfig { source: authorization::Error },

    #[snafu(display("failed to resolve the cluster name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve the cluster namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("the role group name {role_group_name:?} is invalid"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group_name: String,
    },

    #[snafu(display("failed to fetch bootstrap Listener {listener_name}"))]
    FetchBootstrapListener {
        source: stackable_operator::client::Error,
        listener_name: ListenerName,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DereferencedObjects {
    pub authentication_classes: ResolvedAuthenticationClasses,
    pub authorization_config: Option<KafkaAuthorizationConfig>,
    pub kubernetes_cluster_info: KubernetesClusterInfo,
    pub bootstrap_listeners: Vec<listener::v1alpha1::Listener>,
}

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

    let cluster_name = get_cluster_name(kafka).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(kafka).context(ResolveNamespaceSnafu)?;

    // Only broker role groups get a bootstrap Listener, so only their names are looked up.
    let mut bootstrap_listeners = Vec::new();
    for role_group_name in kafka.spec.brokers.role_groups.keys() {
        let role_group_name =
            RoleGroupName::from_str(role_group_name).with_context(|_| ParseRoleGroupNameSnafu {
                role_group_name: role_group_name.clone(),
            })?;
        let listener_name =
            bootstrap_listener_name(&cluster_name, &KafkaRole::Broker, &role_group_name);

        if let Some(bootstrap_listener) = client
            .get_opt::<listener::v1alpha1::Listener>(listener_name.as_ref(), namespace.as_ref())
            .await
            .with_context(|_| FetchBootstrapListenerSnafu {
                listener_name: listener_name.clone(),
            })?
        {
            bootstrap_listeners.push(bootstrap_listener);
        }
    }

    Ok(DereferencedObjects {
        authentication_classes,
        authorization_config,
        kubernetes_cluster_info: client.kubernetes_cluster_info.clone(),
        bootstrap_listeners,
    })
}

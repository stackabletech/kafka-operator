//! Building the Kubernetes resources of one role group.
//!
//! Each role's module ([`broker`], [`controller`]) fills in a [`RoleGroupBuilder`] for one role
//! group. It builds the containers only that role runs (its `kafka` container, and for
//! controllers the `quorum-manager` sidecar) together with the pod volumes they need, and renders
//! the role group's `ConfigMap` entries. Kafka's roles differ in their main container (probes,
//! `preStop`, command and environment) and in the sidecars they run, which is why the role
//! modules build those containers themselves.
//!
//! [`RoleGroupBuilder`]'s `build_*` methods then emit the Kubernetes objects. They add what every
//! role's pod has (the `config` and `log` volumes, the Vector sidecar and the pod-level settings)
//! and never look at which role they are building.

pub(crate) mod broker;
pub(crate) mod controller;

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    k8s_openapi::api::{
        apps::v1::{StatefulSet, StatefulSetUpdateStrategy},
        core::v1::{ConfigMap, Container, PersistentVolumeClaim, PodTemplateSpec, Service, Volume},
    },
    v2::{config_file_writer::PropertiesWriterError, types::kubernetes::ContainerName},
};

use super::{jvm, kerberos, properties::ConfigFileName, resource, security};
use crate::{
    controller::{
        PodDescriptorsError, RoleGroupName, ValidatedCluster, validate::ValidatedLogging,
    },
    crd::{
        STACKABLE_KERBEROS_KEYTAB_PATH,
        role::{KafkaRole, commons::CommonConfig},
    },
};

/// Why building one role group failed. The caller adds which role and role group it was.
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    ConfigMap { source: resource::config_map::Error },

    #[snafu(display("failed to build StatefulSet"))]
    StatefulSet {
        source: resource::statefulset::Error,
    },
}

/// Everything one role group's Kubernetes objects are built from.
///
/// The role's module fills this in with the role's containers already built and the `ConfigMap`
/// entries already rendered, so the `build_*` methods read straight through it.
pub(crate) struct RoleGroupBuilder<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    /// Used for the names and labels of the role group's objects.
    pub(crate) role: KafkaRole,
    pub(crate) role_group_name: RoleGroupName,
    /// The role group's merged config that is common to every role; read for the pod's affinity
    /// and its graceful shutdown timeout.
    pub(crate) common: CommonConfig,
    /// The role group's validated logging; read for the Vector sidecar, which runs only when the
    /// Vector agent is enabled.
    pub(crate) logging: ValidatedLogging,
    /// The name of the Vector sidecar, from the role's container enum.
    pub(crate) vector_container_name: &'static ContainerName,
    /// The containers only this role runs, in pod order: the `kafka` container, and for
    /// controllers the `quorum-manager` sidecar. The Vector sidecar comes after them.
    pub(crate) role_containers: Vec<Container>,
    /// The pod volumes only this role has, in the order the pod lists them, including the
    /// `log-config` volume, which sits between them. The `config` and `log` volumes come after
    /// them.
    pub(crate) role_pod_volumes: Vec<Volume>,
    /// The `StatefulSet`'s persistent volume claim templates. For brokers these include the
    /// bootstrap listener claim template.
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    pub(crate) pod_management_policy: &'static str,
    /// `None` leaves the Kubernetes default in place.
    pub(crate) update_strategy: Option<StatefulSetUpdateStrategy>,
    /// The role group's replica count, passed to the StatefulSet as is; `None` makes Kubernetes
    /// run one pod.
    pub(crate) replicas: Option<u16>,
    pub(crate) pod_overrides: PodTemplateSpec,
    /// Every `ConfigMap` entry, as (file name, content).
    pub(crate) config_map_data: Vec<(String, String)>,
}

impl RoleGroupBuilder<'_> {
    /// The headless and metrics `Service`s.
    pub(crate) fn build_services(&self) -> Vec<Service> {
        vec![
            resource::service::build_rolegroup_headless_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
                &self.cluster.cluster_config.kafka_security,
            ),
            resource::service::build_rolegroup_metrics_service(
                self.cluster,
                &self.role,
                &self.role_group_name,
            ),
        ]
    }

    pub(crate) fn build_config_map(&self) -> Result<ConfigMap, Error> {
        resource::config_map::build_config_map(self).context(ConfigMapSnafu)
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        resource::statefulset::build_statefulset(self).context(StatefulSetSnafu)
    }
}

/// The JAAS section for the CONTROLLER listener, which both roles render, authenticating as
/// `kafka/<controller_principal_address>`.
///
/// Unlike the broker's bootstrap and client sections, this context is used for BOTH sides of
/// every CONTROLLER-listener connection: brokers connect out to controllers, and controllers
/// connect to each other for Raft. This is the only listener in this operator where the process
/// must act as a GSSAPI initiator as well as an acceptor, so `isInitiator` is intentionally left
/// at its default (`true`).
fn controller_jaas_section(controller_principal_address: &str) -> String {
    formatdoc! {"
        controller.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            keyTab=\"{STACKABLE_KERBEROS_KEYTAB_PATH}\"
            principal=\"kafka/{controller_principal_address}@${{env:KERBEROS_REALM}}\";
        }};
    "}
}

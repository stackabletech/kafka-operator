//! Building the Kubernetes resources of one role group.
//!
//! Two phases, so neither has to reach into the other. First each role's module ([`broker`],
//! [`controller`]) gathers its [`RoleGroupInputs`]: it builds the containers only that role runs
//! (its `kafka` container, and for controllers the `quorum-manager` sidecar) together with the
//! pod volumes they need, and renders the `ConfigMap` entries only that role has.
//! [`RoleGroupBuilder::new`] adds the `ConfigMap` entries every role has. Then
//! [`RoleGroupBuilder`]'s `build_*` methods emit the Kubernetes objects, adding what every role's
//! pod has (the `config` and `log` volumes, the Vector sidecar and the pod-level settings).
//!
//! The emit phase is identical for every role, so it lives here once. Everything that differs
//! between roles happens in the two role modules' `build` functions. Kafka's roles differ in
//! their main container (probes, `preStop`, command and environment) and in the sidecars they
//! run, so the role modules build those containers themselves.

mod broker;
mod controller;

use std::collections::BTreeMap;

pub(crate) use broker::build as build_broker_role_group;
#[cfg(test)]
pub(crate) use controller::QUORUM_MANAGER_CONTAINER_NAME;
pub(crate) use controller::build as build_controller_role_group;
use indoc::formatdoc;
use snafu::Snafu;
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::{StatefulSet, StatefulSetUpdateStrategy},
        core::v1::{ConfigMap, Container, PersistentVolumeClaim, PodTemplateSpec, Service, Volume},
    },
    product_logging::spec::ContainerLogConfig,
    v2::{config_file_writer::PropertiesWriterError, types::kubernetes::ContainerName},
};

use super::{properties::ConfigFileName, resource};
use crate::{
    controller::{
        PodDescriptorsError, RoleGroupName, ValidatedCluster, validate::ValidatedLogging,
    },
    crd::{
        STACKABLE_KERBEROS_KEYTAB_PATH,
        role::{KafkaRole, commons::CommonConfig},
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role {role} role group {role_group}", role = &**role))]
    ConfigMap {
        source: resource::config_map::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build StatefulSet for role {role} role group {role_group}", role = &**role))]
    StatefulSet {
        source: resource::statefulset::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to serialize [{file}] for role {role} role group {role_group}", role = &**role))]
    SerializeConfigFile {
        source: PropertiesWriterError,
        file: ConfigFileName,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build pod descriptors for role {role} role group {role_group}", role = &**role))]
    BuildPodDescriptors {
        source: PodDescriptorsError,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("missing secret lifetime for role {role} role group {role_group}", role = &**role))]
    MissingSecretLifetime {
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to add Secret Volumes and VolumeMounts for role {role} role group {role_group}", role = &**role))]
    AddVolumesAndVolumeMounts {
        source: super::security::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to add kerberos config for role {role} role group {role_group}", role = &**role))]
    AddKerberosConfig {
        source: super::kerberos::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build listener volume for role {role} role group {role_group}", role = &**role))]
    AddListenerVolume {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build container probe for role {role} role group {role_group}", role = &**role))]
    BuildProbe {
        source: resource::probes::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to construct JVM arguments for role {role} role group {role_group}", role = &**role))]
    ConstructJvmArguments {
        source: super::jvm::Error,
        role: KafkaRole,
        role_group: RoleGroupName,
    },
}

/// One role group's validated configuration, as its role's module reads it off the CRD, with
/// the parts only that role has already built.
///
/// [`RoleGroupBuilder::new`] consumes this: every field is either resolved into the builder's
/// `ConfigMap` entries, or carried over to the emit phase.
pub(crate) struct RoleGroupInputs<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) role: KafkaRole,
    pub(crate) role_group_name: RoleGroupName,
    /// The role group's merged config that is common to every role; carried for the pod's
    /// affinity and its graceful shutdown timeout.
    pub(crate) common: CommonConfig,
    /// The validated log configs of the `kafka` container and the Vector sidecar; the sidecar
    /// runs only when the Vector agent is enabled for this role group.
    pub(crate) logging: ValidatedLogging,
    /// The name of the role's `kafka` container, under which its log config is rendered.
    pub(crate) kafka_container_name: &'static ContainerName,
    /// The log config of the role's `kafka` container, rendered into the `ConfigMap`'s log4j or
    /// log4j2 entries.
    pub(crate) kafka_log_config: ContainerLogConfig,
    /// The name of the role's Vector sidecar container.
    pub(crate) vector_container_name: &'static ContainerName,
    /// The containers only this role runs, in pod order: the `kafka` container, and for
    /// controllers the `quorum-manager` sidecar. The emit phase adds the Vector sidecar after
    /// them.
    pub(crate) role_containers: Vec<Container>,
    /// The pod volumes only this role has, in the order the pod lists them; this includes the
    /// `log-config` volume, which sits between role volumes. The emit phase adds the `config`
    /// and `log` volumes after them.
    pub(crate) role_pod_volumes: Vec<Volume>,
    /// The `StatefulSet`'s persistent volume claim templates. For brokers these include the
    /// bootstrap listener claim template.
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    pub(crate) pod_management_policy: &'static str,
    /// `None` leaves the Kubernetes default in place.
    pub(crate) update_strategy: Option<StatefulSetUpdateStrategy>,
    /// The `ConfigMap` entries only this role has, as (file name, content): the role's
    /// properties file rendered with its overrides, its JAAS config, and for controllers
    /// `admin-client.properties`.
    pub(crate) role_config_map_data: Vec<(String, String)>,
    /// The role group's `security.properties` overrides.
    pub(crate) security_properties_overrides: BTreeMap<String, String>,
    /// The role group's replica count; `None` when unset, which counts as one replica.
    pub(crate) replicas: Option<u16>,
    pub(crate) pod_overrides: PodTemplateSpec,
}

/// Everything one role group's Kubernetes objects are built from, already resolved.
///
/// These fields are outputs rather than configuration: the role's containers are built and the
/// `ConfigMap` entries rendered. The `build_*` methods therefore read straight through, and
/// nothing about which role this is reaches them.
pub(crate) struct RoleGroupBuilder<'a> {
    pub(crate) cluster: &'a ValidatedCluster,
    pub(crate) role: KafkaRole,
    pub(crate) role_group_name: RoleGroupName,
    /// Carried for the pod's affinity and its graceful shutdown timeout.
    pub(crate) common: CommonConfig,
    /// Carried for the Vector sidecar.
    pub(crate) logging: ValidatedLogging,
    pub(crate) vector_container_name: &'static ContainerName,
    pub(crate) role_containers: Vec<Container>,
    pub(crate) role_pod_volumes: Vec<Volume>,
    pub(crate) volume_claim_templates: Vec<PersistentVolumeClaim>,
    pub(crate) pod_management_policy: &'static str,
    pub(crate) update_strategy: Option<StatefulSetUpdateStrategy>,
    pub(crate) replicas: Option<u16>,
    pub(crate) pod_overrides: PodTemplateSpec,
    /// The rendered `ConfigMap` entries, as (file name, content).
    pub(crate) config_map_data: Vec<(String, String)>,
}

impl<'a> RoleGroupBuilder<'a> {
    /// Resolves one role group: renders the `ConfigMap` entries every role has and adds them to
    /// the ones the role rendered.
    pub(crate) fn new(inputs: RoleGroupInputs<'a>) -> Result<Self, Error> {
        let mut config_map_data = inputs.role_config_map_data;
        config_map_data.extend(
            resource::config_map::common_config_map_data(
                inputs.cluster,
                inputs.security_properties_overrides,
                inputs.kafka_container_name,
                &inputs.kafka_log_config,
                &inputs.logging,
            )
            .map_err(|source| Error::ConfigMap {
                source,
                role: inputs.role.clone(),
                role_group: inputs.role_group_name.clone(),
            })?,
        );

        Ok(Self {
            cluster: inputs.cluster,
            role: inputs.role,
            role_group_name: inputs.role_group_name,
            common: inputs.common,
            logging: inputs.logging,
            vector_container_name: inputs.vector_container_name,
            role_containers: inputs.role_containers,
            role_pod_volumes: inputs.role_pod_volumes,
            volume_claim_templates: inputs.volume_claim_templates,
            pod_management_policy: inputs.pod_management_policy,
            update_strategy: inputs.update_strategy,
            replicas: inputs.replicas,
            pod_overrides: inputs.pod_overrides,
            config_map_data,
        })
    }

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
        resource::config_map::build_config_map(self).map_err(|source| Error::ConfigMap {
            source,
            role: self.role.clone(),
            role_group: self.role_group_name.clone(),
        })
    }

    pub(crate) fn build_statefulset(&self) -> Result<StatefulSet, Error> {
        resource::statefulset::build_statefulset(self).map_err(|source| Error::StatefulSet {
            source,
            role: self.role.clone(),
            role_group: self.role_group_name.clone(),
        })
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

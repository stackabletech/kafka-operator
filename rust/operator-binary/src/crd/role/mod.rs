pub mod broker;
pub mod commons;
pub mod controller;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    kube::runtime::reflector::ObjectRef,
    role_utils::RoleGroupRef,
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

use crate::crd::v1alpha1;

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    Hash,
    JsonSchema,
    PartialEq,
    Serialize,
    EnumString,
)]
pub enum KafkaRole {
    #[strum(serialize = "broker")]
    Broker,
    #[strum(serialize = "controller")]
    Controller,
}

impl KafkaRole {
    /// Metadata about a rolegroup
    pub fn rolegroup_ref(
        &self,
        kafka: &v1alpha1::KafkaCluster,
        group_name: impl Into<String>,
    ) -> RoleGroupRef<v1alpha1::KafkaCluster> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(kafka),
            role: self.to_string(),
            role_group: group_name.into(),
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }

    /// A Kerberos principal has three parts, with the form username/fully.qualified.domain.name@YOUR-REALM.COM.
    /// We only have one role and will use "kafka" everywhere (which e.g. differs from the current hdfs implementation,
    /// but is similar to HBase).
    pub fn kerberos_service_name(&self) -> &'static str {
        "kafka"
    }

    RoleConfigByPropertyKind
}

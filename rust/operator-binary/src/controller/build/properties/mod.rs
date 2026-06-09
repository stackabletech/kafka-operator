//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;

use crate::crd::{KafkaPodDescriptor, role::KafkaRole};

pub(crate) fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Vec<String> {
    pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller.to_string())
        .map(|desc| {
            format!(
                "{fqdn}:{client_port}",
                fqdn = desc.fqdn(),
                client_port = desc.client_port
            )
        })
        .collect::<Vec<String>>()
}

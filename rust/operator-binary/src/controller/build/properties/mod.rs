//! Property-file builders for Kafka rolegroup ConfigMaps.

pub mod broker_properties;
pub mod controller_properties;

use snafu::Snafu;

use crate::crd::{KafkaPodDescriptor, role::KafkaRole};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("no Kraft controllers found to build"))]
    NoKraftControllersFound,
}

pub(crate) fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Option<String> {
    let result = pod_descriptors
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
        .join(",");

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

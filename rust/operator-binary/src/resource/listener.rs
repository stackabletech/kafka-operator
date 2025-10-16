use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder, commons::product_image_selection::ResolvedProductImage,
    crd::listener, role_utils::RoleGroupRef,
};

use crate::{
    crd::{role::broker::BrokerConfig, security::KafkaTlsSecurity, v1alpha1},
    kafka_controller::KAFKA_CONTROLLER_NAME,
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },
}

/// Kafka clients will use the load-balanced bootstrap listener to get a list of broker addresses and will use those to
/// transmit data to the correct broker.
// TODO (@NickLarsenNZ): Move shared functionality to stackable-operator
pub fn build_broker_rolegroup_bootstrap_listener(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    merged_config: &BrokerConfig,
) -> Result<listener::v1alpha1::Listener, Error> {
    Ok(listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(kafka.bootstrap_service_name(rolegroup))
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(merged_config.bootstrap_listener_class.clone()),
            ports: Some(bootstrap_listener_ports(kafka_security)),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    })
}

fn bootstrap_listener_ports(
    kafka_security: &KafkaTlsSecurity,
) -> Vec<listener::v1alpha1::ListenerPort> {
    vec![if kafka_security.has_kerberos_enabled() {
        listener::v1alpha1::ListenerPort {
            name: kafka_security.bootstrap_port_name().to_string(),
            port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
        }
    } else {
        listener::v1alpha1::ListenerPort {
            name: kafka_security.client_port_name().to_string(),
            port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
        }
    }]
}

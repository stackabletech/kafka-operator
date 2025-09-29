use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{Service, ServiceSpec},
    kvp::{Label, Labels},
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{APP_NAME, v1alpha1},
    kafka_controller::KAFKA_CONTROLLER_NAME,
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_service(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(rolegroup.object_name())
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
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(
                Labels::role_group_selector(
                    kafka,
                    APP_NAME,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .context(LabelBuildSnafu)?
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

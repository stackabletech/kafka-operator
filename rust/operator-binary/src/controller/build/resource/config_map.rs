use std::{borrow::Cow, collections::BTreeMap};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::{framework::VECTOR_CONFIG_FILE, spec::ContainerLogConfig},
    v2::{
        builder::meta::ownerreference_from_resource,
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        types::kubernetes::ContainerName,
    },
};

use crate::controller::{
    ValidatedCluster,
    build::{
        properties::{
            ConfigFileName,
            product_logging::{role_group_config_map_data, vector_config_file_content},
        },
        recommended_labels_for_role_group_resources,
        role_group::RoleGroupBuilder,
        security::client_properties,
    },
    validate::ValidatedLogging,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap"))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to serialize [{}]", ConfigFileName::Security))]
    JvmSecurityProperties { source: PropertiesWriterError },

    #[snafu(display("failed to serialize [{}]", ConfigFileName::Client))]
    ClientProperties { source: PropertiesWriterError },
}

/// The rolegroup [`ConfigMap`] entries every role has: `security.properties` with the role
/// group's `security_overrides`, `client.properties`, the log config files of the role's `kafka`
/// container and, if the Vector agent is enabled, `vector.yaml`.
pub(crate) fn common_config_map_data(
    validated_cluster: &ValidatedCluster,
    security_overrides: BTreeMap<String, String>,
    kafka_container_name: &ContainerName,
    kafka_log_config: &ContainerLogConfig,
    logging: &ValidatedLogging,
) -> Result<Vec<(String, String)>, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;

    // The `networkaddress.cache.*` defaults are always emitted (with user `security.properties`
    // overrides winning); see `security_properties::build`.
    let jvm_sec_props =
        crate::controller::build::properties::security_properties::build(security_overrides);

    let mut data = vec![
        (
            ConfigFileName::Security.to_string(),
            to_java_properties_string(jvm_sec_props.iter()).context(JvmSecurityPropertiesSnafu)?,
        ),
        (
            ConfigFileName::Client.to_string(),
            to_java_properties_string(
                client_properties(kafka_security)
                    .iter()
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
            )
            .context(ClientPropertiesSnafu)?,
        ),
    ];

    tracing::debug!(?jvm_sec_props, "Applied JVM config");

    let log_config_data = role_group_config_map_data(
        &validated_cluster.image.product_version,
        kafka_container_name.as_ref(),
        Cow::Borrowed(kafka_log_config),
    );
    for (file_name, data_entry) in log_config_data {
        if let Some(data_entry) = data_entry {
            data.push((file_name, data_entry));
        }
    }

    // The Vector agent config is the static `vector.yaml`, added only when the Vector agent is
    // enabled (resolved during validation).
    if logging.vector_container.is_some() {
        data.push((VECTOR_CONFIG_FILE.to_string(), vector_config_file_content()));
    }

    Ok(data)
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the
/// administrator; its entries are already rendered ([`RoleGroupBuilder::config_map_data`]).
pub fn build_config_map(builder: &RoleGroupBuilder) -> Result<ConfigMap, Error> {
    let validated_cluster = builder.cluster;

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder.metadata(
        ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(
                validated_cluster
                    .role_group_resource_names(&builder.role, &builder.role_group_name)
                    .role_group_config_map()
                    .to_string(),
            )
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(recommended_labels_for_role_group_resources(
                validated_cluster,
                &builder.role,
                &builder.role_group_name,
            ))
            .build(),
    );

    for (file_name, data) in &builder.config_map_data {
        cm_builder.add_data(file_name, data);
    }

    cm_builder.build().context(BuildRoleGroupConfigSnafu)
}

use std::collections::BTreeMap;

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
    v2::config_file_writer::{PropertiesWriterError, to_java_properties_string},
};

use crate::{
    controller::KAFKA_CONTROLLER_NAME,
    crd::{
        JVM_SECURITY_PROPERTIES_FILE, KafkaPodDescriptor, MetadataManager,
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR,
        listener::{KafkaListenerConfig, node_address_cmd},
        role::AnyConfig,
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    product_logging::extend_role_group_config_map,
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid metadata manager"))]
    InvalidMetadataManager { source: crate::crd::Error },

    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        rolegroup: String,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to serialize config for {rolegroup}"))]
    SerializeConfig {
        source: PropertiesWriterError,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to build properties for {rolegroup}"))]
    BuildProperties {
        source: crate::controller::build::properties::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("failed to build jaas configuration file for {rolegroup}"))]
    BuildJaasConfig { rolegroup: String },
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
pub fn build_rolegroup_config_map(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    config_file_overrides: BTreeMap<String, String>,
    jvm_security_overrides: BTreeMap<String, String>,
    merged_config: &AnyConfig,
    listener_config: &KafkaListenerConfig,
    pod_descriptors: &[KafkaPodDescriptor],
    opa_connect_string: Option<&str>,
) -> Result<ConfigMap, Error> {
    let kafka_config_file_name = merged_config.config_file_name();

    let metadata_manager = kafka
        .effective_metadata_manager()
        .context(InvalidMetadataManagerSnafu)?;

    let kafka_config = match merged_config {
        AnyConfig::Broker(_) => crate::controller::build::properties::broker_properties::build(
            kafka_security,
            listener_config,
            pod_descriptors,
            opa_connect_string,
            metadata_manager == MetadataManager::KRaft,
            kafka
                .spec
                .cluster_config
                .broker_id_pod_config_map_name
                .is_some(),
            config_file_overrides,
        ),
        AnyConfig::Controller(_) => {
            crate::controller::build::properties::controller_properties::build(
                kafka_security,
                listener_config,
                pod_descriptors,
                metadata_manager == MetadataManager::KRaft,
                config_file_overrides,
            )
        }
    }
    .with_context(|_| BuildPropertiesSnafu {
        rolegroup: rolegroup.clone(),
    })?;

    let kafka_config = kafka_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();

    let jvm_sec_props: BTreeMap<String, Option<String>> = jvm_security_overrides
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect();

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(kafka)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(kafka, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    kafka,
                    KAFKA_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(
            kafka_config_file_name,
            to_java_properties_string(kafka_config.iter().map(|(k, v)| (k, v))).with_context(
                |_| SerializeConfigSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?,
        )
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    rolegroup: rolegroup.role_group.clone(),
                }
            })?,
        )
        .add_data(
            "client.properties",
            to_java_properties_string(
                kafka_security
                    .client_properties()
                    .iter()
                    .map(|(k, v)| (k, v)),
            )
            .with_context(|_| JvmSecurityPropertiesSnafu {
                rolegroup: rolegroup.role_group.clone(),
            })?,
        )
        // This file contains the JAAS configuration for Kerberos authentication
        // It has the ".properties" extension but is not a Java properties file.
        // It is processed by `config-utils` to substitute "env:" and "file:" variables
        // and this tool currently doesn't support the JAAS login configuration format.
        .add_data(
            "jaas.properties",
            jaas_config_file(kafka_security.has_kerberos_enabled()),
        );

    tracing::debug!(?kafka_config, "Applied kafka config");
    tracing::debug!(?jvm_sec_props, "Applied JVM config");

    extend_role_group_config_map(
        &resolved_product_image.product_version,
        rolegroup,
        merged_config,
        &mut cm_builder,
    );

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool) -> String {
    match is_kerberos_enabled {
        false => String::new(),
        true => formatdoc! {"
        bootstrap.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            isInitiator=false
            keyTab=\"/stackable/kerberos/keytab\"
            principal=\"kafka/{bootstrap_address}@${{env:KERBEROS_REALM}}\";
        }};

        client.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            isInitiator=false
            keyTab=\"/stackable/kerberos/keytab\"
            principal=\"kafka/{broker_address}@${{env:KERBEROS_REALM}}\";
        }};

    ",
        bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR),
        broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
        },
    }
}

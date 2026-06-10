use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
    v2::config_file_writer::{PropertiesWriterError, to_java_properties_string},
};

use crate::{
    controller::{KAFKA_CONTROLLER_NAME, ValidatedKafkaCluster, ValidatedRoleGroupConfig},
    crd::{
        JVM_SECURITY_PROPERTIES_FILE, MetadataManager, STACKABLE_LISTENER_BOOTSTRAP_DIR,
        STACKABLE_LISTENER_BROKER_DIR,
        listener::{KafkaListenerConfig, node_address_cmd},
        role::AnyConfig,
        v1alpha1,
    },
    product_logging::role_group_config_map_data,
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

    #[snafu(display("failed to build jaas configuration file for {rolegroup}"))]
    BuildJaasConfig { rolegroup: String },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("no Kraft controllers found to build"))]
    NoKraftControllersFound,
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
pub fn build_rolegroup_config_map(
    kafka: &v1alpha1::KafkaCluster,
    validated_cluster: &ValidatedKafkaCluster,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    validated_rg: &ValidatedRoleGroupConfig,
    listener_config: &KafkaListenerConfig,
) -> Result<ConfigMap, Error> {
    let kafka_security = &validated_cluster.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let kafka_config_file_name = validated_rg.merged_config.config_file_name();
    let config_overrides = validated_rg.config_file_overrides.clone();

    let opa_connect = validated_cluster
        .authorization_config
        .as_ref()
        .map(|auth_config| auth_config.opa_connect.clone());

    let kraft_mode = validated_cluster.metadata_manager == MetadataManager::KRaft;

    if kraft_mode && validated_cluster.pod_descriptors.is_empty() {
        return NoKraftControllersFoundSnafu.fail();
    }

    let kafka_config = match &validated_rg.merged_config {
        AnyConfig::Broker(_) => crate::controller::build::properties::broker_properties::build(
            kafka_security,
            listener_config,
            &validated_cluster.pod_descriptors,
            opa_connect.as_deref(),
            kraft_mode,
            kafka
                .spec
                .cluster_config
                .broker_id_pod_config_map_name
                .is_some(),
            config_overrides,
        ),
        AnyConfig::Controller(_) => {
            crate::controller::build::properties::controller_properties::build(
                kafka_security,
                listener_config,
                &validated_cluster.pod_descriptors,
                kraft_mode,
                config_overrides,
            )
        }
    };

    let jvm_sec_props = &validated_rg.jvm_security_overrides;

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
            to_java_properties_string(kafka_config.iter()).with_context(|_| {
                SerializeConfigSnafu {
                    rolegroup: rolegroup.clone(),
                }
            })?,
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
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
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

    let config_data = role_group_config_map_data(
        &resolved_product_image.product_version,
        rolegroup,
        &validated_rg.merged_config,
    );
    for (file_name, data) in config_data {
        if let Some(data) = data {
            cm_builder.add_data(file_name, data);
        }
    }

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

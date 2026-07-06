use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    v2::{
        builder::meta::ownerreference_from_resource,
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            labels,
            properties::{
                ConfigFileName, config_file_name, product_logging::role_group_config_map_data,
            },
            security::client_properties,
        },
    },
    crd::{
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR,
        listener::{KafkaListenerConfig, node_address_cmd},
        role::AnyConfig,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for role group {role_group}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display(
        "failed to serialize [{}] for role group {role_group}",
        ConfigFileName::Security
    ))]
    JvmSecurityProperties {
        source: PropertiesWriterError,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to serialize config for role group {role_group}"))]
    SerializeConfig {
        source: PropertiesWriterError,
        role_group: RoleGroupName,
    },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors {
        source: crate::controller::PodDescriptorsError,
    },

    #[snafu(display("no Kraft controllers found to build"))]
    NoKraftControllersFound,
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator.
///
/// `vector_config` is the static Vector agent config (`vector.yaml`) added by the caller; it is
/// `None` when the Vector agent is disabled. Resource naming and labels use the role (derived
/// from `validated_rg.config`) and the typed `role_group_name`.
pub fn build_rolegroup_config_map(
    validated_cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    validated_rg: &ValidatedRoleGroupConfig,
    listener_config: &KafkaListenerConfig,
    vector_config: Option<String>,
) -> Result<ConfigMap, Error> {
    let role = validated_rg.config.config.kafka_role();
    let cluster_config = &validated_cluster.cluster_config;
    let kafka_security = &cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let kafka_config_file_name = config_file_name(&validated_rg.config.config).to_string();
    let config_overrides = validated_rg
        .config_overrides
        .config_file_overrides()
        .overrides
        .clone();

    let pod_descriptors = validated_cluster
        .pod_descriptors(None)
        .context(BuildPodDescriptorsSnafu)?;

    if cluster_config.is_kraft_mode() && pod_descriptors.is_empty() {
        return NoKraftControllersFoundSnafu.fail();
    }

    let kafka_config = match &validated_rg.config.config {
        AnyConfig::Broker(_) => crate::controller::build::properties::broker_properties::build(
            cluster_config,
            listener_config,
            &pod_descriptors,
            config_overrides,
        ),
        AnyConfig::Controller(_) => {
            crate::controller::build::properties::controller_properties::build(
                cluster_config,
                listener_config,
                &pod_descriptors,
                config_overrides,
            )
        }
    };

    // The `networkaddress.cache.*` defaults are always emitted (with user `security.properties`
    // overrides winning); see `security_properties::build`.
    let jvm_sec_props = crate::controller::build::properties::security_properties::build(
        validated_rg
            .config_overrides
            .security_properties()
            .overrides
            .clone(),
    );

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(validated_cluster)
                .name(
                    validated_cluster
                        .resource_names(&role, role_group_name)
                        .role_group_config_map()
                        .to_string(),
                )
                .ownerreference(ownerreference_from_resource(
                    validated_cluster,
                    None,
                    Some(true),
                ))
                .with_labels(labels::recommended_labels(
                    validated_cluster,
                    &role,
                    role_group_name,
                ))
                .build(),
        )
        .add_data(
            kafka_config_file_name,
            to_java_properties_string(kafka_config.iter()).with_context(|_| {
                SerializeConfigSnafu {
                    role_group: role_group_name.clone(),
                }
            })?,
        )
        .add_data(
            ConfigFileName::Security.to_string(),
            to_java_properties_string(jvm_sec_props.iter()).with_context(|_| {
                JvmSecurityPropertiesSnafu {
                    role_group: role_group_name.clone(),
                }
            })?,
        )
        .add_data(
            ConfigFileName::Client.to_string(),
            to_java_properties_string(
                client_properties(kafka_security)
                    .iter()
                    .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
            )
            .with_context(|_| JvmSecurityPropertiesSnafu {
                role_group: role_group_name.clone(),
            })?,
        )
        // This file contains the JAAS configuration for Kerberos authentication
        // It has the ".properties" extension but is not a Java properties file.
        // It is processed by `config-utils` to substitute "env:" and "file:" variables
        // and this tool currently doesn't support the JAAS login configuration format.
        .add_data(
            ConfigFileName::Jaas.to_string(),
            jaas_config_file(kafka_security.has_kerberos_enabled()),
        );

    tracing::debug!(?kafka_config, "Applied kafka config");
    tracing::debug!(?jvm_sec_props, "Applied JVM config");

    let config_data = role_group_config_map_data(
        &resolved_product_image.product_version,
        &validated_rg.config.config,
    );
    for (file_name, data) in config_data {
        if let Some(data) = data {
            cm_builder.add_data(file_name, data);
        }
    }

    if let Some(vector_config) = vector_config {
        cm_builder.add_data(VECTOR_CONFIG_FILE, vector_config);
    }

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            role_group: role_group_name.clone(),
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

#[cfg(test)]
mod tests {
    use super::jaas_config_file;

    #[test]
    fn jaas_config_file_empty_without_kerberos() {
        assert_eq!(jaas_config_file(false), "");
    }

    #[test]
    fn jaas_config_file_renders_bootstrap_and_client_sections_with_kerberos() {
        let jaas = jaas_config_file(true);
        assert!(jaas.contains("bootstrap.KafkaServer"));
        assert!(jaas.contains("client.KafkaServer"));
        assert!(jaas.contains("Krb5LoginModule"));
        assert!(jaas.contains("${env:KERBEROS_REALM}"));
        // The bootstrap and client principals embed distinct listener addresses.
        assert!(jaas.contains("/stackable/listener-bootstrap"));
        assert!(jaas.contains("/stackable/listener-broker"));
    }
}

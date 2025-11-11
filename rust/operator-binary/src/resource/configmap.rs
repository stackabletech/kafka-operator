use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use indoc::formatdoc;
use product_config::{types::PropertyNameKind, writer::to_java_properties_string};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{
        JVM_SECURITY_PROPERTIES_FILE, KafkaPodDescriptor, STACKABLE_LISTENER_BOOTSTRAP_DIR,
        STACKABLE_LISTENER_BROKER_DIR,
        listener::{KafkaListenerConfig, KafkaListenerName, node_address_cmd},
        role::{
            AnyConfig, KAFKA_ADVERTISED_LISTENERS, KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS,
            KAFKA_CONTROLLER_QUORUM_VOTERS, KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, KAFKA_LISTENERS,
            KAFKA_LOG_DIRS, KAFKA_NODE_ID, KAFKA_PROCESS_ROLES, KafkaRole,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    kafka_controller::KAFKA_CONTROLLER_NAME,
    operations::graceful_shutdown::graceful_shutdown_config_properties,
    product_logging::extend_role_group_config_map,
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display(
        "failed to serialize [{JVM_SECURITY_PROPERTIES_FILE}] for {}",
        rolegroup
    ))]
    JvmSecurityPoperties {
        source: product_config::writer::PropertiesWriterError,
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
        source: product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<v1alpha1::KafkaCluster>,
    },

    #[snafu(display("no Kraft controllers found to build"))]
    NoKraftControllersFound,

    #[snafu(display("unknown Kafka role [{name}]"))]
    UnknownKafkaRole {
        source: strum::ParseError,
        name: String,
    },

    #[snafu(display("failed to build jaas configuration file for {}", rolegroup))]
    BuildJaasConfig { rolegroup: String },
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
pub fn build_rolegroup_config_map(
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &KafkaTlsSecurity,
    rolegroup: &RoleGroupRef<v1alpha1::KafkaCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    merged_config: &AnyConfig,
    listener_config: &KafkaListenerConfig,
    pod_descriptors: &[KafkaPodDescriptor],
    opa_connect_string: Option<&str>,
) -> Result<ConfigMap, Error> {
    let kafka_config_file_name = merged_config.config_file_name();

    let mut kafka_config = server_properties_file(
        kafka.is_controller_configured(),
        &rolegroup.role,
        pod_descriptors,
        listener_config,
        opa_connect_string,
        resolved_product_image.product_version.starts_with("3.7"), // needs_quorum_voters
    )?;

    // Need to call this to get configOverrides :(
    kafka_config.extend(
        rolegroup_config
            .get(&PropertyNameKind::File(kafka_config_file_name.to_string()))
            .cloned()
            .unwrap_or_default(),
    );

    match merged_config {
        AnyConfig::Broker(_) => kafka_config.extend(kafka_security.broker_config_settings()),
        AnyConfig::Controller(_) => {
            kafka_config.extend(kafka_security.controller_config_settings())
        }
    }

    kafka_config.extend(graceful_shutdown_config_properties());

    let kafka_config = kafka_config
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();

    let jvm_sec_props: BTreeMap<String, Option<String>> = rolegroup_config
        .get(&PropertyNameKind::File(
            JVM_SECURITY_PROPERTIES_FILE.to_string(),
        ))
        .cloned()
        .unwrap_or_default()
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
                .with_recommended_labels(build_recommended_labels(
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
                JvmSecurityPopertiesSnafu {
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
            .with_context(|_| JvmSecurityPopertiesSnafu {
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

// Generate the content of both server.properties and controller.properties files.
fn server_properties_file(
    kraft_mode: bool,
    role: &str,
    pod_descriptors: &[KafkaPodDescriptor],
    listener_config: &KafkaListenerConfig,
    opa_connect_string: Option<&str>,
    needs_quorum_voters: bool,
) -> Result<BTreeMap<String, String>, Error> {
    let kraft_controllers = kraft_controllers(pod_descriptors);

    let role = KafkaRole::from_str(role).context(UnknownKafkaRoleSnafu {
        name: role.to_string(),
    })?;

    match role {
        KafkaRole::Controller => {
            let kraft_controllers = kraft_controllers.context(NoKraftControllersFoundSnafu)?;

            let mut result = BTreeMap::from([
            (
                KAFKA_LOG_DIRS.to_string(),
                "/stackable/data/kraft".to_string(),
            ),
            (KAFKA_PROCESS_ROLES.to_string(), role.to_string()),
            (
                "controller.listener.names".to_string(),
                KafkaListenerName::Controller.to_string(),
            ),
            (
                KAFKA_NODE_ID.to_string(),
                "${env:REPLICA_ID}".to_string(),
            ),
            (
                KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS.to_string(),
                kraft_controllers.clone(),
            ),
            (
                KAFKA_LISTENERS.to_string(),
                "CONTROLLER://${env:POD_NAME}.${env:ROLEGROUP_HEADLESS_SERVICE_NAME}.${env:NAMESPACE}.svc.${env:CLUSTER_DOMAIN}:${env:KAFKA_CLIENT_PORT}".to_string(),
            ),
            (
                KAFKA_LISTENER_SECURITY_PROTOCOL_MAP.to_string(),
                listener_config
                    .listener_security_protocol_map_for_listener(&KafkaListenerName::Controller)
                    .unwrap_or("".to_string())),
            ]);

            if needs_quorum_voters {
                let kraft_voters =
                    kraft_voters(pod_descriptors).context(NoKraftControllersFoundSnafu)?;

                result.extend([(KAFKA_CONTROLLER_QUORUM_VOTERS.to_string(), kraft_voters)]);
            }

            Ok(result)
        }
        KafkaRole::Broker => {
            let mut result = BTreeMap::from([
                (
                    KAFKA_LOG_DIRS.to_string(),
                    "/stackable/data/topicdata".to_string(),
                ),
                (KAFKA_LISTENERS.to_string(), listener_config.listeners()),
                (
                    KAFKA_ADVERTISED_LISTENERS.to_string(),
                    listener_config.advertised_listeners(),
                ),
                (
                    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP.to_string(),
                    listener_config.listener_security_protocol_map(),
                ),
            ]);

            if kraft_mode {
                let kraft_controllers = kraft_controllers.context(NoKraftControllersFoundSnafu)?;

                // Running in KRaft mode
                result.extend([
                    (KAFKA_NODE_ID.to_string(), "${env:REPLICA_ID}".to_string()),
                    (
                        KAFKA_PROCESS_ROLES.to_string(),
                        KafkaRole::Broker.to_string(),
                    ),
                    (
                        "controller.listener.names".to_string(),
                        KafkaListenerName::Controller.to_string(),
                    ),
                    (
                        KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS.to_string(),
                        kraft_controllers.clone(),
                    ),
                ]);

                if needs_quorum_voters {
                    let kraft_voters =
                        kraft_voters(pod_descriptors).context(NoKraftControllersFoundSnafu)?;

                    result.extend([(KAFKA_CONTROLLER_QUORUM_VOTERS.to_string(), kraft_voters)]);
                }
            } else {
                // Running with ZooKeeper enabled
                result.extend([(
                    "zookeeper.connect".to_string(),
                    "${env:ZOOKEEPER}".to_string(),
                )]);
            }

            // Enable OPA authorization
            if opa_connect_string.is_some() {
                result.extend([
                    (
                        "authorizer.class.name".to_string(),
                        "org.openpolicyagent.kafka.OpaAuthorizer".to_string(),
                    ),
                    (
                        "opa.authorizer.metrics.enabled".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "opa.authorizer.url".to_string(),
                        opa_connect_string.unwrap_or_default().to_string(),
                    ),
                ]);
            }

            Ok(result)
        }
    }
}

fn kraft_controllers(pod_descriptors: &[KafkaPodDescriptor]) -> Option<String> {
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

fn kraft_voters(pod_descriptors: &[KafkaPodDescriptor]) -> Option<String> {
    let result = pod_descriptors
        .iter()
        .filter(|pd| pd.role == KafkaRole::Controller.to_string())
        .map(|desc| desc.as_quorum_voter())
        .collect::<Vec<String>>()
        .join(",");

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
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

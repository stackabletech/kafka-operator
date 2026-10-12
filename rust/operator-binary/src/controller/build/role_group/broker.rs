//! Gathering one broker role group.

use indoc::formatdoc;
use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::pod::volume::{
        ListenerOperatorVolumeSourceBuilder, ListenerReference as ListenerOperatorReference,
        VolumeBuilder,
    },
    k8s_openapi::api::core::v1::{Container, EnvVar, PersistentVolumeClaim, Volume},
    v2::{
        builder::pod::{
            container::new_container_builder,
            volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        config_file_writer::to_java_properties_string,
        product_logging::framework::STACKABLE_LOG_DIR,
    },
};

use super::{
    AddKerberosConfigSnafu, AddListenerVolumeSnafu, AddVolumesAndVolumeMountsSnafu,
    BuildPodDescriptorsSnafu, BuildProbeSnafu, ConstructJvmArgumentsSnafu, Error,
    MissingSecretLifetimeSnafu, RoleGroupBuilder, RoleGroupInputs, SerializeConfigFileSnafu,
    controller_jaas_section,
};
use crate::{
    controller::{
        BrokerRoleGroupConfig, RoleGroupName, ValidatedCluster,
        build::{
            command::broker_kafka_container_commands,
            container::{
                common_kafka_env, common_operator_env_vars, container_ports, log_config_volume,
            },
            kerberos::{add_kerberos_pod_config, kerberos_env_vars},
            properties::{ConfigFileName, broker_properties, listener::get_kafka_listener_config},
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources,
            resource::{listener::build_broker_rolegroup_bootstrap_listener, probes},
            security::add_broker_volume_and_volume_mounts,
        },
    },
    crd::{
        BROKER_ID_POD_MAP_DIR, BROKER_ID_POD_MAP_DIR_NAME, LISTENER_BOOTSTRAP_VOLUME_NAME,
        LISTENER_BROKER_VOLUME_NAME, LOG_DIRS_VOLUME_NAME, METRICS_PORT, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_DIR_NAME, STACKABLE_DATA_DIR, STACKABLE_KERBEROS_KEYTAB_PATH,
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR, STACKABLE_LOG_CONFIG_DIR,
        STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME,
        listener::node_address_cmd,
        role::{
            KafkaRole,
            broker::{BrokerConfig, BrokerContainer},
        },
    },
    listener,
};

const ROLE: KafkaRole = KafkaRole::Broker;

const POD_MANAGEMENT_POLICY_PARALLEL: &str = "Parallel";

/// Gathers one broker role group.
///
/// Brokers are the only role with a bootstrap `Listener`, returned alongside the builder, and
/// the only role exposed through listener-operator volumes: the bootstrap listener as a
/// persistent claim template and the broker listener as an ephemeral pod volume.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &BrokerRoleGroupConfig,
) -> Result<(RoleGroupBuilder<'a>, listener::v1alpha1::Listener), Error> {
    let config = &rg_config.config.config;

    let role_config_map_data = role_config_map_data(cluster, role_group_name, rg_config)?;
    let (role_containers, role_pod_volumes) = role_pod(cluster, role_group_name, rg_config)?;

    let inputs = RoleGroupInputs {
        cluster,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        common: config.common_config.clone(),
        logging: rg_config.config.logging.clone(),
        kafka_container_name: BrokerContainer::Kafka.name(),
        kafka_log_config: config
            .logging
            .for_container(&BrokerContainer::Kafka)
            .into_owned(),
        vector_container_name: BrokerContainer::Vector.name(),
        role_containers,
        role_pod_volumes,
        volume_claim_templates: volume_claim_templates(cluster, role_group_name, config),
        pod_management_policy: POD_MANAGEMENT_POLICY_PARALLEL,
        update_strategy: None,
        role_config_map_data,
        security_properties_overrides: rg_config
            .config_overrides
            .security_properties
            .overrides
            .clone(),
        replicas: rg_config.replicas,
        pod_overrides: rg_config.pod_overrides.clone(),
    };

    let bootstrap_listener =
        build_broker_rolegroup_bootstrap_listener(cluster, &ROLE, role_group_name, config);

    Ok((RoleGroupBuilder::new(inputs)?, bootstrap_listener))
}

/// `broker.properties` rendered with the role group's overrides, and the broker's JAAS config.
fn role_config_map_data(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &BrokerRoleGroupConfig,
) -> Result<Vec<(String, String)>, Error> {
    let kafka_security = &cluster.cluster_config.kafka_security;

    let listener_config =
        get_kafka_listener_config(cluster, kafka_security, &ROLE, role_group_name);

    let pod_descriptors =
        cluster
            .pod_descriptors(None)
            .with_context(|_| BuildPodDescriptorsSnafu {
                role: ROLE,
                role_group: role_group_name.clone(),
            })?;

    let kafka_config = broker_properties::build(
        &cluster.cluster_config,
        &listener_config,
        &pod_descriptors,
        rg_config
            .config_overrides
            .broker_properties
            .overrides
            .clone(),
    );

    tracing::debug!(?kafka_config, "Applied kafka config");

    Ok(vec![
        (
            ConfigFileName::BrokerProperties.to_string(),
            to_java_properties_string(kafka_config.iter()).with_context(|_| {
                SerializeConfigFileSnafu {
                    file: ConfigFileName::BrokerProperties,
                    role: ROLE,
                    role_group: role_group_name.clone(),
                }
            })?,
        ),
        // This file contains the JAAS configuration for Kerberos authentication
        // It has the ".properties" extension but is not a Java properties file.
        // It is processed by `config-utils` to substitute "env:" and "file:" variables
        // and this tool currently doesn't support the JAAS login configuration format.
        (
            ConfigFileName::Jaas.to_string(),
            jaas_config_file(kafka_security.has_kerberos_enabled()),
        ),
    ])
}

/// The `StatefulSet`'s volume claim templates: the storage volumes and the bootstrap listener.
fn volume_claim_templates(
    validated_cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    merged_config: &BrokerConfig,
) -> Vec<PersistentVolumeClaim> {
    let kafka_role = &ROLE;
    // Used for PVC templates, which cannot be modified once they are deployed. The version label
    // is omitted so the labels stay stable across version upgrades.
    let unversioned_recommended_labels = recommended_labels_for_unversioned_role_group_resources(
        validated_cluster,
        kafka_role,
        role_group_name,
    );

    let mut pvcs = merged_config.resources.storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    let bootstrap_listener_name =
        validated_cluster.bootstrap_listener_name(kafka_role, role_group_name);
    pvcs.push(listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(bootstrap_listener_name),
        &unversioned_recommended_labels,
        &LISTENER_BOOTSTRAP_VOLUME_NAME,
    ));

    pvcs
}

/// The broker pod's `kafka` container and the volumes only brokers have, in pod order.
fn role_pod(
    validated_cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    validated_rg: &BrokerRoleGroupConfig,
) -> Result<(Vec<Container>, Vec<Volume>), Error> {
    let kafka_role = &ROLE;
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config.config;
    let resource_names = validated_cluster.role_group_resource_names(kafka_role, role_group_name);
    let recommended_labels =
        recommended_labels_for_role_group_resources(validated_cluster, kafka_role, role_group_name);
    let mut cb_kafka = new_container_builder(BrokerContainer::Kafka.name());

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .common_config
        .requested_secret_lifetime
        .with_context(|| MissingSecretLifetimeSnafu {
            role: ROLE,
            role_group: role_group_name.clone(),
        })?;
    let mut pod_volumes = add_broker_volume_and_volume_mounts(
        kafka_security,
        &mut cb_kafka,
        &requested_secret_lifetime,
    )
    .with_context(|_| AddVolumesAndVolumeMountsSnafu {
        role: ROLE,
        role_group: role_group_name.clone(),
    })?;

    if kafka_security.has_kerberos_enabled() {
        pod_volumes.extend(
            add_kerberos_pod_config(kafka_security, kafka_role, &mut cb_kafka).with_context(
                |_| AddKerberosConfigSnafu {
                    role: ROLE,
                    role_group: role_group_name.clone(),
                },
            )?,
        );
    }

    // Operator-set env vars first; the user's `envOverrides` are merged on top last and win.
    let env: Vec<EnvVar> = common_operator_env_vars(validated_cluster, kafka_security)
        .merge(
            common_kafka_env(
                &merged_config.resources,
                &validated_rg
                    .product_specific_common_config
                    .jvm_argument_overrides,
                resolved_product_image,
                kafka_role,
                role_group_name,
            )
            .with_context(|_| ConstructJvmArgumentsSnafu {
                role: ROLE,
                role_group: role_group_name.clone(),
            })?,
        )
        .merge(kerberos_env_vars(kafka_security))
        .merge(validated_rg.env_overrides.clone())
        .into();

    let probe_context = || BuildProbeSnafu {
        role: ROLE,
        role_group: role_group_name.clone(),
    };
    // The client port can accept connections before the broker has replayed its log and
    // reached the JMX `RUNNING` state, so the startupProbe waits for both, giving it up to
    // 5 minutes (60 * 5s) before the livenessProbe is allowed to start counting failures.
    let broker_startup_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 5, 5, 60)
            .with_context(|_| probe_context())?;
    let broker_liveness_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 10, 30, 20)
            .with_context(|_| probe_context())?;
    let broker_readiness_probe =
        probes::broker_kcat_readiness_probe(kafka_security).with_context(|_| probe_context())?;

    cb_kafka
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![broker_kafka_container_commands(
            validated_cluster.cluster_config.is_kraft_mode(),
            kafka_security,
        )]);

    cb_kafka
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(&*LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(
            &*LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .resources(merged_config.resources.clone().into())
        .startup_probe(broker_startup_probe)
        .liveness_probe(broker_liveness_probe)
        .readiness_probe(broker_readiness_probe);

    pod_volumes.push(log_config_volume(
        &validated_rg.config.logging,
        &resource_names,
    ));

    pod_volumes.push(Volume {
        name: LISTENER_BROKER_VOLUME_NAME.to_string(),
        ephemeral: Some(
            ListenerOperatorVolumeSourceBuilder::new(
                &ListenerOperatorReference::ListenerClass(
                    merged_config.broker_listener_class.to_string(),
                ),
                &recommended_labels,
            )
            .build_ephemeral()
            .with_context(|_| AddListenerVolumeSnafu {
                role: ROLE,
                role_group: role_group_name.clone(),
            })?,
        ),
        ..Volume::default()
    });

    if let Some(broker_id_config_map_name) = &validated_cluster
        .cluster_config
        .broker_id_pod_config_map_name
    {
        pod_volumes.push(
            VolumeBuilder::new(&*BROKER_ID_POD_MAP_DIR_NAME)
                .with_config_map(broker_id_config_map_name)
                .build(),
        );
        cb_kafka
            .add_volume_mount(&*BROKER_ID_POD_MAP_DIR_NAME, BROKER_ID_POD_MAP_DIR)
            .expect("The mount paths are statically defined and there should be no duplicates.");
    }

    Ok((vec![cb_kafka.build()], pod_volumes))
}

// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool) -> String {
    if !is_kerberos_enabled {
        return String::new();
    }

    // Broker pods reach the CONTROLLER listener as SASL clients; the only principals in
    // their keytab (see `add_kerberos_pod_config`) are for the broker and bootstrap listener
    // addresses, so their CONTROLLER section must reuse the broker address.
    let controller_section =
        controller_jaas_section(&node_address_cmd(STACKABLE_LISTENER_BROKER_DIR));

    formatdoc! {"
        bootstrap.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            isInitiator=false
            keyTab=\"{STACKABLE_KERBEROS_KEYTAB_PATH}\"
            principal=\"kafka/{bootstrap_address}@${{env:KERBEROS_REALM}}\";
        }};

        client.KafkaServer {{
            com.sun.security.auth.module.Krb5LoginModule required
            useKeyTab=true
            storeKey=true
            isInitiator=false
            keyTab=\"{STACKABLE_KERBEROS_KEYTAB_PATH}\"
            principal=\"kafka/{broker_address}@${{env:KERBEROS_REALM}}\";
        }};

        {controller_section}
    ",
    bootstrap_address = node_address_cmd(STACKABLE_LISTENER_BOOTSTRAP_DIR),
    broker_address = node_address_cmd(STACKABLE_LISTENER_BROKER_DIR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::{
        build::container::CONTAINERDEBUG_LOG_DIRECTORY,
        test_support::{broker_role_group_config, minimal_kafka, validated_cluster},
    };

    /// The user-supplied `envOverrides` must be merged in after all operator-set environment
    /// variables, so that they can override any of them. `CONTAINERDEBUG_LOG_DIRECTORY` is used
    /// as the example here because it is set unconditionally by the operator. The controller
    /// builder merges the same [`EnvVarSet`] the same way.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let kafka = minimal_kafka(
            r#"
            apiVersion: kafka.stackable.tech/v1alpha1
            kind: KafkaCluster
            metadata:
              name: simple-kafka
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              image:
                productVersion: 3.9.2
              clusterConfig:
                zookeeperConfigMapName: xyz
              brokers:
                roleGroups:
                  default:
                    replicas: 1
            "#,
        );
        let cluster = validated_cluster(&kafka);
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");
        let mut validated_rg = broker_role_group_config(&cluster, "default").clone();
        validated_rg.env_overrides = validated_rg
            .env_overrides
            .with_value(&CONTAINERDEBUG_LOG_DIRECTORY, "/custom/log/dir");

        let (builder, _) =
            build(&cluster, &role_group_name, &validated_rg).expect("the role group builds");
        let stateful_set = builder.build_statefulset().expect("the StatefulSet builds");

        let env = stateful_set
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
            .into_iter()
            .find(|container| container.name == "kafka")
            .expect("the kafka container exists")
            .env
            .expect("the kafka container has env vars");

        let containerdebug: Vec<_> = env
            .iter()
            .filter(|env_var| env_var.name == "CONTAINERDEBUG_LOG_DIRECTORY")
            .collect();
        assert_eq!(
            containerdebug.len(),
            1,
            "the override must replace the operator-set value, not duplicate it"
        );
        assert_eq!(containerdebug[0].value.as_deref(), Some("/custom/log/dir"));
    }

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

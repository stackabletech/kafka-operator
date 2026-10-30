//! Building one broker role group.

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pod::volume::{
        ListenerOperatorVolumeSourceBuilder, ListenerOperatorVolumeSourceBuilderError,
        ListenerReference as ListenerOperatorReference, VolumeBuilder,
    },
    k8s_openapi::api::core::v1::{Container, EnvVar, PersistentVolumeClaim, Volume},
    v2::{
        builder::pod::{
            container::new_container_builder,
            volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        product_logging::framework::STACKABLE_LOG_DIR,
    },
};

use super::{RoleGroupBuilder, controller_jaas_section};
use crate::{
    controller::{
        BrokerRoleGroupConfig, PodDescriptorsError, RoleGroupName, ValidatedBrokerConfig,
        ValidatedCluster,
        build::{
            command::broker_kafka_container_commands,
            container::{
                common_kafka_env, common_operator_env_vars, container_ports, log_config_volume,
            },
            jvm,
            kerberos::{self, kerberos_env_vars, mount_broker_kerberos_volume},
            properties::{ConfigFileName, broker_properties, listener::get_kafka_listener_config},
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources,
            resource::{
                config_map::{self, common_config_map_data},
                listener::build_broker_rolegroup_bootstrap_listener,
                probes,
            },
            security::{self, mount_broker_tls_volumes},
        },
    },
    crd::{
        BROKER_ID_POD_MAP_DIR, BROKER_ID_POD_MAP_DIR_NAME, LISTENER_BOOTSTRAP_VOLUME_NAME,
        LISTENER_BROKER_VOLUME_NAME, LOG_DIRS_VOLUME_NAME, METRICS_PORT, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_DIR_NAME, STACKABLE_DATA_DIR, STACKABLE_KERBEROS_KEYTAB_PATH,
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR, STACKABLE_LOG_CONFIG_DIR,
        STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME,
        listener::node_address_cmd,
        role::{KafkaRole, broker::BrokerContainer},
    },
    listener,
};

const ROLE: KafkaRole = KafkaRole::Broker;

const POD_MANAGEMENT_POLICY_PARALLEL: &str = "Parallel";

/// Why gathering one broker role group's inputs failed. The caller adds which role group it was.
#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to render the ConfigMap entries every role has"))]
    CommonConfigMapData { source: config_map::Error },

    #[snafu(display("failed to serialize [{file}]"))]
    SerializeConfigFile {
        source: PropertiesWriterError,
        file: ConfigFileName,
    },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: PodDescriptorsError },

    #[snafu(display("failed to build the TLS volumes"))]
    BuildTlsVolumes { source: security::Error },

    #[snafu(display("failed to build the Kerberos volume"))]
    BuildKerberosVolume { source: kerberos::Error },

    #[snafu(display("failed to build the broker listener volume"))]
    BuildListenerVolume {
        source: ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build container probe"))]
    BuildProbe { source: probes::Error },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: jvm::Error },
}

/// Builds one broker role group: the [`RoleGroupBuilder`] for its Services, ConfigMap and
/// StatefulSet, and its bootstrap `Listener`.
///
/// Brokers are the only role with a bootstrap `Listener` and the only role exposed through
/// listener-operator volumes: the bootstrap listener as a persistent claim template and the
/// broker listener as an ephemeral pod volume.
pub(crate) fn build<'a>(
    cluster: &'a ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &BrokerRoleGroupConfig,
) -> Result<(RoleGroupBuilder<'a>, listener::v1alpha1::Listener), Error> {
    let config = &rg_config.config;
    let logging = &rg_config.config.logging;

    let mut config_map_data = role_config_map_data(cluster, role_group_name, rg_config)?;
    config_map_data.extend(
        common_config_map_data(
            cluster,
            BrokerContainer::Kafka.name(),
            logging,
            rg_config
                .config_overrides
                .security_properties
                .overrides
                .clone(),
        )
        .context(CommonConfigMapDataSnafu)?,
    );

    let (role_containers, role_pod_volumes) = role_pod(cluster, role_group_name, rg_config)?;

    let builder = RoleGroupBuilder {
        cluster,
        role: ROLE,
        role_group_name: role_group_name.clone(),
        affinity: config.affinity.clone(),
        graceful_shutdown_timeout: config.graceful_shutdown_timeout,
        logging: logging.clone(),
        vector_container_name: BrokerContainer::Vector.name(),
        role_containers,
        role_pod_volumes,
        volume_claim_templates: volume_claim_templates(cluster, role_group_name, config),
        pod_management_policy: POD_MANAGEMENT_POLICY_PARALLEL,
        replicas: rg_config.replicas,
        pod_overrides: rg_config.pod_overrides.clone(),
        config_map_data,
    };

    let bootstrap_listener =
        build_broker_rolegroup_bootstrap_listener(cluster, &ROLE, role_group_name, config);

    Ok((builder, bootstrap_listener))
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

    let pod_descriptors = cluster
        .pod_descriptors(None)
        .context(BuildPodDescriptorsSnafu)?;

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
            to_java_properties_string(kafka_config.iter()).context(SerializeConfigFileSnafu {
                file: ConfigFileName::BrokerProperties,
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
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    config: &ValidatedBrokerConfig,
) -> Vec<PersistentVolumeClaim> {
    // Used for PVC templates, which cannot be modified once they are deployed. The version label
    // is omitted so the labels stay stable across version upgrades.
    let unversioned_recommended_labels =
        recommended_labels_for_unversioned_role_group_resources(cluster, &ROLE, role_group_name);

    let mut pvcs = config.resources.storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    let bootstrap_listener_name = cluster.bootstrap_listener_name(&ROLE, role_group_name);
    pvcs.push(listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(bootstrap_listener_name),
        &unversioned_recommended_labels,
        &LISTENER_BOOTSTRAP_VOLUME_NAME,
    ));

    pvcs
}

/// The broker pod's `kafka` container and the volumes only brokers have, in pod order.
fn role_pod(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg_config: &BrokerRoleGroupConfig,
) -> Result<(Vec<Container>, Vec<Volume>), Error> {
    let config = &rg_config.config;
    let logging = &rg_config.config.logging;
    let kafka_security = &cluster.cluster_config.kafka_security;
    let image = &cluster.image;
    let resource_names = cluster.role_group_resource_names(&ROLE, role_group_name);

    let mut cb_kafka = new_container_builder(BrokerContainer::Kafka.name());

    // The push order below (TLS, Kerberos, `log-config`, the broker listener, the broker-id
    // ConfigMap) is part of the pod template: a pod's `volumes` is an ordered list, so changing
    // the order changes the template of every deployed StatefulSet and restarts its pods. The
    // order is pinned by `pod_volumes_keep_their_order` in `build/mod.rs`.

    // The TLS certificates are requested with the role group's secret lifetime.
    let mut pod_volumes = mount_broker_tls_volumes(
        kafka_security,
        &mut cb_kafka,
        &config.requested_secret_lifetime,
    )
    .context(BuildTlsVolumesSnafu)?;

    if let Some(kerberos_volume) = mount_broker_kerberos_volume(kafka_security, &mut cb_kafka)
        .context(BuildKerberosVolumeSnafu)?
    {
        pod_volumes.push(kerberos_volume);
    }

    // Operator-set env vars first; the user's `envOverrides` are merged on top last and win.
    let env: Vec<EnvVar> = common_operator_env_vars(cluster, kafka_security)
        .merge(
            common_kafka_env(
                &config.resources,
                &rg_config
                    .product_specific_common_config
                    .jvm_argument_overrides,
                image,
                &ROLE,
                role_group_name,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        .merge(kerberos_env_vars(kafka_security))
        .merge(rg_config.env_overrides.clone())
        .into();

    // The client port can accept connections before the broker has replayed its log and
    // reached the JMX `RUNNING` state, so the startupProbe waits for both, giving it up to
    // 5 minutes (60 * 5s) before the livenessProbe is allowed to start counting failures.
    let startup_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 5, 5, 60)
            .context(BuildProbeSnafu)?;
    let liveness_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 10, 30, 20)
            .context(BuildProbeSnafu)?;
    let readiness_probe =
        probes::broker_kcat_readiness_probe(kafka_security).context(BuildProbeSnafu)?;

    cb_kafka
        .image_from_product_image(image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![broker_kafka_container_commands(
            cluster.cluster_config.is_kraft_mode(),
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
        .resources(config.resources.clone().into())
        .startup_probe(startup_probe)
        .liveness_probe(liveness_probe)
        .readiness_probe(readiness_probe);

    pod_volumes.push(log_config_volume(logging, &resource_names));
    pod_volumes.push(broker_listener_volume(cluster, role_group_name, config)?);

    if let Some(broker_id_config_map_name) = &cluster.cluster_config.broker_id_pod_config_map_name {
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

/// The ephemeral listener volume through which the broker is reached directly, from the role
/// group's `brokerListenerClass`. Unlike the bootstrap listener it is not persistent, so it is a
/// pod volume rather than a claim template.
///
/// Built with the v1 [`ListenerOperatorVolumeSourceBuilder`], whose reference type is imported as
/// `ListenerOperatorReference` to keep it apart from the v2 `ListenerReference` the bootstrap
/// claim template uses: operator-rs has no v2 helper for the ephemeral form, only for the claim.
fn broker_listener_volume(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    config: &ValidatedBrokerConfig,
) -> Result<Volume, Error> {
    let recommended_labels =
        recommended_labels_for_role_group_resources(cluster, &ROLE, role_group_name);
    let listener_volume_source = ListenerOperatorVolumeSourceBuilder::new(
        &ListenerOperatorReference::ListenerClass(config.broker_listener_class.to_string()),
        &recommended_labels,
    )
    .build_ephemeral()
    .context(BuildListenerVolumeSnafu)?;

    Ok(Volume {
        name: LISTENER_BROKER_VOLUME_NAME.to_string(),
        ephemeral: Some(listener_volume_source),
        ..Volume::default()
    })
}

// Generate JAAS configuration file for Kerberos authentication
// or an empty string if Kerberos is not enabled.
// See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html
fn jaas_config_file(is_kerberos_enabled: bool) -> String {
    if !is_kerberos_enabled {
        return String::new();
    }

    // Broker pods reach the CONTROLLER listener as SASL clients; the only principals in
    // their keytab (see `mount_broker_kerberos_volume`) are for the broker and bootstrap listener
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

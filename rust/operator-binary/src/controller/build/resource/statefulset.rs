use std::{ops::Deref, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::{ContainerBuilder, FieldPathEnvVar},
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::VolumeBuilder,
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy},
            core::v1::{
                ConfigMapVolumeSource, ContainerPort, EnvVar, ExecAction, LifecycleHandler, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    product_logging,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::{
                container::{EnvVarName, EnvVarSet},
                volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
            },
        },
        jvm_argument_overrides::JvmArgumentOverrides,
        product_logging::framework::{
            STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice, vector_container,
        },
        role_group_utils::ResourceNames,
        types::kubernetes::{ConfigMapKey, ContainerName, PersistentVolumeClaimName, VolumeName},
    },
};

use super::probes;
use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            command::{
                KAFKA_LOG4J_OPTS, broker_kafka_container_commands,
                controller_kafka_container_command, controller_remove_self_pre_stop_command,
                kafka_log_opts, quorum_manager_container_command,
            },
            graceful_shutdown::add_graceful_shutdown_config,
            kerberos::{add_kerberos_pod_config, kerberos_env_vars},
            properties::product_logging::MAX_KAFKA_LOG_FILES_SIZE,
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources, role_group_selector,
            security::{
                STACKABLE_TLS_KAFKA_INTERNAL_DIR, STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
                add_broker_volume_and_volume_mounts, add_controller_volume_and_volume_mounts,
            },
        },
        node_id_hasher::node_id_hash32_offset,
        security::ValidatedKafkaSecurity,
        validate::ValidatedLogging,
    },
    crd::{
        BROKER_ID_POD_MAP_DIR, BROKER_ID_POD_MAP_DIR_NAME, KAFKA_HEAP_OPTS,
        LISTENER_BOOTSTRAP_VOLUME_NAME, LISTENER_BROKER_VOLUME_NAME, LOG_DIRS_VOLUME_NAME,
        METRICS_PORT, METRICS_PORT_NAME, STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_DIR_NAME,
        STACKABLE_DATA_DIR, STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR,
        STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME,
        role::{
            AnyConfig, KAFKA_NODE_ID_OFFSET, KafkaRole, broker::BrokerContainer,
            controller::ControllerContainer,
        },
    },
};

// The Vector container reads its `vector.yaml` from the `config` volume (the rolegroup
// ConfigMap) and tails product logs from the `log` volume.
stackable_operator::constant!(VECTOR_CONFIG_VOLUME_NAME: VolumeName = "config");
stackable_operator::constant!(VECTOR_LOG_VOLUME_NAME: VolumeName = "log");

// Env vars the operator sets on the Kafka containers.
stackable_operator::constant!(POD_NAME: EnvVarName = "POD_NAME");
stackable_operator::constant!(KAFKA_CLIENT_PORT: EnvVarName = "KAFKA_CLIENT_PORT");
stackable_operator::constant!(NAMESPACE: EnvVarName = "NAMESPACE");
stackable_operator::constant!(ROLEGROUP_HEADLESS_SERVICE_NAME: EnvVarName = "ROLEGROUP_HEADLESS_SERVICE_NAME");
stackable_operator::constant!(CLUSTER_DOMAIN: EnvVarName = "CLUSTER_DOMAIN");
stackable_operator::constant!(EXTRA_ARGS: EnvVarName = "EXTRA_ARGS");
// Needed for the `containerdebug` process to log its tracing information to.
stackable_operator::constant!(CONTAINERDEBUG_LOG_DIRECTORY: EnvVarName = "CONTAINERDEBUG_LOG_DIRECTORY");

// The env var and the ZooKeeper discovery ConfigMap key holding the ZooKeeper connection
// string (the same string, as the env var name is used as the ConfigMap key).
stackable_operator::constant!(ZOOKEEPER: EnvVarName = "ZOOKEEPER");
stackable_operator::constant!(ZOOKEEPER_CONFIG_MAP_KEY: ConfigMapKey = "ZOOKEEPER");

/// Environment variables the operator sets on the Kafka container that are common to broker and
/// controller role groups.
///
/// These form the base; the caller merges the user's `envOverrides` on top (see
/// [`build_broker_rolegroup_statefulset`] and [`build_controller_rolegroup_statefulset`]), so a
/// user override wins on a name collision. Using an [`EnvVarSet`] (a name-keyed map) makes that
/// precedence explicit and de-duplicates by name, rather than relying on append order.
fn common_operator_env_vars(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
) -> EnvVarSet {
    let mut env = EnvVarSet::new()
        .with_field_path(&POD_NAME, &FieldPathEnvVar::Name)
        .with_value(&KAFKA_CLIENT_PORT, kafka_security.client_port().to_string());

    // Present in ZooKeeper mode only: brokers use it to connect, controllers for migration.
    if let Some(zookeeper_config_map_name) =
        &validated_cluster.cluster_config.zookeeper_config_map_name
    {
        env = env.with_config_map_key_ref(
            &ZOOKEEPER,
            zookeeper_config_map_name,
            &ZOOKEEPER_CONFIG_MAP_KEY,
        );
    }

    env
}

/// Environment variables the operator sets that are common to *every* container in a
/// **controller** pod: today that's the `kafka` server process and, when present, the
/// `quorum-manager` sidecar.
///
/// The caller merges the user's `envOverrides` on top (so a user override wins on a name
/// collision); the `quorum-manager` sidecar additionally gets its own container-specific env
/// vars layered on top (see [`KAFKA_NODE_ID_OFFSET`]).
fn controller_pod_shared_env_vars(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
    resource_names: &ResourceNames,
) -> EnvVarSet {
    common_operator_env_vars(validated_cluster, kafka_security)
        .with_field_path(&NAMESPACE, &FieldPathEnvVar::Namespace)
        .with_value(
            &ROLEGROUP_HEADLESS_SERVICE_NAME,
            resource_names.headless_service_name().to_string(),
        )
        .with_value(
            &CLUSTER_DOMAIN,
            validated_cluster.cluster_domain.to_string(),
        )
}

const POD_MANAGEMENT_POLICY_PARALLEL: &str = "Parallel";
const POD_MANAGEMENT_POLICY_ORDERED_READY: &str = "OrderedReady";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig {
        source: crate::controller::build::kerberos::Error,
    },

    #[snafu(display("failed to add listener volume"))]
    AddListenerVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add Secret Volumes and VolumeMounts"))]
    AddVolumesAndVolumeMounts {
        source: crate::controller::build::security::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors {
        source: crate::controller::PodDescriptorsError,
    },

    #[snafu(display("failed to build container probe"))]
    BuildProbe { source: probes::Error },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments {
        source: crate::controller::build::jvm::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::controller::build::graceful_shutdown::Error,
    },

    #[snafu(display("invalid Container name [{name}]"))]
    InvalidContainerName {
        name: String,
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,
}

/// The broker rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) from [`build_rolegroup_headless_service`](`crate::controller::build::resource::service::build_rolegroup_headless_service`).
pub fn build_broker_rolegroup_statefulset(
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
    validated_cluster: &ValidatedCluster,
    validated_rg: &ValidatedRoleGroupConfig,
) -> Result<StatefulSet, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config.config;
    let resource_names = validated_cluster.role_group_resource_names(kafka_role, role_group_name);
    let recommended_labels =
        recommended_labels_for_role_group_resources(validated_cluster, kafka_role, role_group_name);
    // Used for PVC templates, which cannot be modified once they are deployed. The version label
    // is omitted so the labels stay stable across version upgrades.
    let unversioned_recommended_labels = recommended_labels_for_unversioned_role_group_resources(
        validated_cluster,
        kafka_role,
        role_group_name,
    );

    let kafka_container_name = BrokerContainer::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .deref()
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    add_broker_volume_and_volume_mounts(
        kafka_security,
        &mut pod_builder,
        &mut cb_kafka,
        &requested_secret_lifetime,
    )
    .context(AddVolumesAndVolumeMountsSnafu)?;

    let mut pvcs = merged_config.resources().storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    let bootstrap_listener_name =
        validated_cluster.bootstrap_listener_name(kafka_role, role_group_name);
    let bootstrap_pvc_name = PersistentVolumeClaimName::from_str(LISTENER_BOOTSTRAP_VOLUME_NAME)
        .expect("the bootstrap listener volume name is a valid PVC name");
    pvcs.push(listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(bootstrap_listener_name),
        &unversioned_recommended_labels,
        &bootstrap_pvc_name,
    ));

    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(kafka_security, kafka_role, &mut cb_kafka, &mut pod_builder)
            .context(AddKerberosConfigSnafu)?;
    }

    // Operator-set env vars first; the user's `envOverrides` are merged on top last and win.
    let env: Vec<EnvVar> = common_operator_env_vars(validated_cluster, kafka_security)
        .merge(common_kafka_env(
            merged_config,
            &validated_rg
                .product_specific_common_config
                .jvm_argument_overrides,
            resolved_product_image,
            kafka_role,
            role_group_name,
        )?)
        .merge(kerberos_env_vars(kafka_security))
        .merge(validated_rg.env_overrides.clone())
        .into();

    // The client port can accept connections before the broker has replayed its log and
    // reached the JMX `RUNNING` state, so the startupProbe waits for both, giving it up to
    // 5 minutes (60 * 5s) before the livenessProbe is allowed to start counting failures.
    let broker_startup_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 5, 5, 60)
            .context(BuildProbeSnafu)?;
    let broker_liveness_probe =
        probes::broker_running_probe(kafka_security.client_port(), METRICS_PORT, 10, 30, 20)
            .context(BuildProbeSnafu)?;
    let broker_readiness_probe =
        probes::broker_kcat_readiness_probe(kafka_security).context(BuildProbeSnafu)?;

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
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(
            LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(merged_config.resources().clone().into())
        .startup_probe(broker_startup_probe)
        .liveness_probe(broker_liveness_probe)
        .readiness_probe(broker_readiness_probe);

    add_log_config_volume(
        &mut pod_builder,
        &validated_rg.config.logging,
        &resource_names,
    )?;

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    if let Some(listener_class) = merged_config.listener_class() {
        pod_builder
            .add_listener_volume_by_listener_class(
                LISTENER_BROKER_VOLUME_NAME,
                listener_class.as_ref(),
                &recommended_labels,
            )
            .context(AddListenerVolumeSnafu)?;
    }

    if let Some(broker_id_config_map_name) = &validated_cluster
        .cluster_config
        .broker_id_pod_config_map_name
    {
        pod_builder
            .add_volume(
                VolumeBuilder::new(BROKER_ID_POD_MAP_DIR_NAME)
                    .with_config_map(broker_id_config_map_name)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
        cb_kafka
            .add_volume_mount(BROKER_ID_POD_MAP_DIR_NAME, BROKER_ID_POD_MAP_DIR)
            .context(AddVolumeMountSnafu)?;
    }

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .affinity(&merged_config.affinity);

    add_common_pod_config(
        &mut pod_builder,
        &resource_names,
        validated_cluster
            .cluster_resource_names()
            .service_account_name()
            .as_ref(),
    )?;

    add_vector_container(
        &mut pod_builder,
        &container_name(BrokerContainer::Vector),
        &validated_rg.config.logging,
        resolved_product_image,
        &resource_names,
    );

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    // Pod overrides were already merged (role <- role group) during validation.
    pod_template.merge_from(validated_rg.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(resource_names.stateful_set_name().to_string())
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(recommended_labels.clone())
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some(POD_MANAGEMENT_POLICY_PARALLEL.to_string()),
            replicas: validated_rg.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    role_group_selector(validated_cluster, kafka_role, role_group_name).into(),
                ),
                ..LabelSelector::default()
            },
            service_name: Some(resource_names.headless_service_name().to_string()),
            template: pod_template,
            volume_claim_templates: Some(pvcs),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

/// The controller rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
pub fn build_controller_rolegroup_statefulset(
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
    validated_cluster: &ValidatedCluster,
    validated_rg: &ValidatedRoleGroupConfig,
) -> Result<StatefulSet, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config.config;
    let resource_names = validated_cluster.role_group_resource_names(kafka_role, role_group_name);
    let recommended_labels =
        recommended_labels_for_role_group_resources(validated_cluster, kafka_role, role_group_name);

    let kafka_container_name = ControllerContainer::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    let node_id_offset = node_id_hash32_offset(kafka_role, role_group_name.as_ref()).to_string();

    // Operator-set env vars first (common + controller-specific); the user's `envOverrides`
    // are merged on top and win. Shared between the `kafka` container and the
    // `quorum-manager` sidecar (see `controller_pod_shared_env_vars`) so they can't drift
    // apart; each container then layers its own additions on top.
    let controller_shared_env =
        controller_pod_shared_env_vars(validated_cluster, kafka_security, &resource_names);

    let env: Vec<EnvVar> = controller_shared_env
        .clone()
        .merge(common_kafka_env(
            merged_config,
            &validated_rg
                .product_specific_common_config
                .jvm_argument_overrides,
            resolved_product_image,
            kafka_role,
            role_group_name,
        )?)
        .merge(validated_rg.env_overrides.clone())
        .into();

    let quorum_manager_env: Vec<EnvVar> = controller_shared_env
        .with_value(&KAFKA_NODE_ID_OFFSET, &node_id_offset)
        .merge(validated_rg.env_overrides.clone())
        .into();

    let controller_pod_descriptors = validated_cluster
        .pod_descriptors(Some(kafka_role))
        .context(BuildPodDescriptorsSnafu)?;

    // The controller listener socket only opens once the KRaft node has finished replaying
    // its metadata log, which can take a while on a slow first boot or after a long outage.
    // The startupProbe gives it up to 5 minutes (60 * 5s) before the liveness probe is
    // allowed to start counting failures at all, so a slow (but progressing) boot is never
    // mistaken for a stuck process.
    let controller_startup_probe =
        probes::controller_tcp_probe(kafka_security.client_port(), 5, 5, 60)
            .context(BuildProbeSnafu)?;
    // See `probes::controller_stuck_unattached_liveness_probe`'s doc comment for why this is no
    // longer a plain TCP check.
    let controller_liveness_probe = probes::controller_stuck_unattached_liveness_probe(
        kafka_security.client_port(),
        METRICS_PORT,
        10,
        30,
        20,
    )
    .context(BuildProbeSnafu)?;
    let controller_readiness_probe =
        probes::controller_raft_state_probe(METRICS_PORT, 10, 10, 6).context(BuildProbeSnafu)?;

    cb_kafka
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![controller_kafka_container_command(
            controller_pod_descriptors,
        )]);

    cb_kafka
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(merged_config.resources().clone().into())
        .startup_probe(controller_startup_probe)
        .liveness_probe(controller_liveness_probe)
        .readiness_probe(controller_readiness_probe);
    // Skipped when Kerberos is enabled, matching `build_quorum_manager_container`'s own
    // gating — `admin-client.properties` (the file this removal call relies on) only covers
    // the TLS/SSL case.
    if !kafka_security.has_kerberos_enabled() {
        cb_kafka.lifecycle_pre_stop(LifecycleHandler {
            exec: Some(ExecAction {
                command: Some(vec![
                    "/bin/bash".to_string(),
                    "-c".to_string(),
                    controller_remove_self_pre_stop_command(
                        merged_config.graceful_shutdown_timeout,
                    ),
                ]),
            }),
            ..LifecycleHandler::default()
        });
    }

    add_log_config_volume(
        &mut pod_builder,
        &validated_rg.config.logging,
        &resource_names,
    )?;

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .deref()
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    add_controller_volume_and_volume_mounts(
        kafka_security,
        &mut pod_builder,
        &mut cb_kafka,
        &requested_secret_lifetime,
    )
    .context(AddVolumesAndVolumeMountsSnafu)?;

    let kafka_container = cb_kafka.build();

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(kafka_container)
        .affinity(&merged_config.affinity);

    if let Some(quorum_manager_container) =
        build_quorum_manager_container(resolved_product_image, kafka_security, quorum_manager_env)?
    {
        pod_builder.add_container(quorum_manager_container);
    }

    add_common_pod_config(
        &mut pod_builder,
        &resource_names,
        validated_cluster
            .cluster_resource_names()
            .service_account_name()
            .as_ref(),
    )?;

    add_vector_container(
        &mut pod_builder,
        &container_name(ControllerContainer::Vector),
        &validated_rg.config.logging,
        resolved_product_image,
        &resource_names,
    );

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    // Pod overrides were already merged (role <- role group) during validation.
    pod_template.merge_from(validated_rg.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(validated_cluster)
            .name(resource_names.stateful_set_name().to_string())
            .ownerreference(ownerreference_from_resource(
                validated_cluster,
                None,
                Some(true),
            ))
            .with_labels(recommended_labels.clone())
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some(POD_MANAGEMENT_POLICY_ORDERED_READY.to_string()),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..StatefulSetUpdateStrategy::default()
            }),
            replicas: validated_rg.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    role_group_selector(validated_cluster, kafka_role, role_group_name).into(),
                ),
                ..LabelSelector::default()
            },
            service_name: Some(resource_names.headless_service_name().to_string()),
            template: pod_template,
            volume_claim_templates: Some(merged_config.resources().storage.build_pvcs()),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn container_ports(kafka_security: &ValidatedKafkaSecurity) -> Vec<ContainerPort> {
    let mut ports = vec![
        ContainerPort {
            name: Some(METRICS_PORT_NAME.to_string()),
            container_port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
        ContainerPort {
            name: Some(kafka_security.client_port_name().to_string()),
            container_port: kafka_security.client_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        },
    ];
    if kafka_security.has_kerberos_enabled() {
        ports.push(ContainerPort {
            name: Some(kafka_security.bootstrap_port_name().to_string()),
            container_port: kafka_security.bootstrap_port().into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        });
    }
    ports
}

/// Environment variables the operator sets on the Kafka container of both roles, on top of
/// [`common_operator_env_vars`].
///
/// Returned as an [`EnvVarSet`] so the callers can merge the user's `envOverrides` on top,
/// letting an override win on a name collision.
fn common_kafka_env(
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
    resolved_product_image: &ResolvedProductImage,
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Result<EnvVarSet, Error> {
    Ok(EnvVarSet::new()
        .with_value(
            &EXTRA_ARGS,
            crate::controller::build::jvm::construct_non_heap_jvm_args(
                merged_config,
                jvm_argument_overrides,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        .with_value(
            &KAFKA_HEAP_OPTS,
            crate::controller::build::jvm::construct_heap_jvm_args(
                merged_config,
                jvm_argument_overrides,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        .with_value(
            &KAFKA_LOG4J_OPTS,
            kafka_log_opts(&resolved_product_image.product_version),
        )
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY,
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .with_value(
            &KAFKA_NODE_ID_OFFSET,
            node_id_hash32_offset(kafka_role, role_group_name.as_ref()).to_string(),
        ))
}

/// Adds the `log-config` volume, sourced either from the user-supplied custom log config
/// `ConfigMap` or the rolegroup `ConfigMap` (which carries the operator-generated config).
/// Branches on the *validated* Kafka-container logging choice.
fn add_log_config_volume(
    pod_builder: &mut PodBuilder,
    logging: &ValidatedLogging,
    resource_names: &ResourceNames,
) -> Result<(), Error> {
    let config_map = match &logging.kafka_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    pod_builder
        .add_volume(
            VolumeBuilder::new(STACKABLE_LOG_CONFIG_DIR_NAME)
                .with_config_map(config_map)
                .build(),
        )
        .context(AddVolumeSnafu)?;
    Ok(())
}

/// Adds the `config` volume, the `log` emptyDir, the service account and the pod security
/// context that the broker and controller pods share.
fn add_common_pod_config(
    pod_builder: &mut PodBuilder,
    resource_names: &ResourceNames,
    service_account_name: &str,
) -> Result<(), Error> {
    pod_builder
        .add_volume(Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            STACKABLE_LOG_DIR_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account_name)
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );
    Ok(())
}

/// Adds the Vector log-aggregation sidecar container, when the Vector agent is enabled.
///
/// Whether Vector is enabled, the per-container log config and the (validated) aggregator
/// discovery `ConfigMap` name are resolved up-front in
/// [`ValidatedLogging`]. The container mounts the
/// static `vector.yaml` from the `config` volume and is driven by the env vars the
/// [`vector_container`] sets.
/// The [`ContainerName`] for a role container, derived from its `Display` name so the
/// Vector sidecar's container name always matches that container's logging-config key.
fn container_name(container: impl std::fmt::Display) -> ContainerName {
    ContainerName::from_str(&container.to_string())
        .expect("a container enum variant is always a valid ContainerName")
}

/// Name of the controller's `quorum-manager` sidecar container.
const QUORUM_MANAGER_CONTAINER_NAME: &str = "quorum-manager";

/// Builds the `quorum-manager` sidecar for a controller pod. Returns `None` when Kerberos is
/// enabled (the sidecar's admin-client properties file only covers the TLS/SSL case).
///
/// `env` is expected to be [`controller_pod_shared_env_vars`] (plus `NODE_ID_OFFSET` and the
/// rolegroup's `envOverrides`) — the same base the `kafka` container in this pod gets — so
/// this sidecar's `controller.properties` render has every env var it references. See
/// [`controller_pod_shared_env_vars`] for why that matters.
fn build_quorum_manager_container(
    resolved_product_image: &ResolvedProductImage,
    kafka_security: &ValidatedKafkaSecurity,
    env: Vec<EnvVar>,
) -> Result<Option<stackable_operator::k8s_openapi::api::core::v1::Container>, Error> {
    if kafka_security.has_kerberos_enabled() {
        return Ok(None);
    }

    let mut cb = ContainerBuilder::new(QUORUM_MANAGER_CONTAINER_NAME).context(
        InvalidContainerNameSnafu {
            name: QUORUM_MANAGER_CONTAINER_NAME,
        },
    )?;

    cb.image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-c".to_string(),
            quorum_manager_container_command(),
        ])
        // `kafka-metadata-quorum.sh` goes through `kafka-run-class.sh`, which defaults
        // `KAFKA_HEAP_OPTS` to `-Xmx256M` when unset. Set an explicit, modest heap so the
        // JVM's max heap plus its base/metaspace/SSL-buffer overhead stays comfortably
        // under the container's memory limit below.
        .add_env_var(KAFKA_HEAP_OPTS.to_string(), "-Xmx128M")
        .add_env_vars(env)
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                // A JVM cold start plus an SSL handshake and an admin-client round-trip all
                // need to happen inside this sidecar's existing budgets
                .with_cpu_limit("500m")
                .with_memory_request("512Mi")
                .with_memory_limit("512Mi")
                .build(),
        )
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        // `controller_admin_client_properties` always points its keystore/truststore
        // at this directory, so the sidecar's admin-client calls need it mounted
        // here too, not just on the `kafka` container.
        .add_volume_mount(
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            STACKABLE_TLS_KAFKA_INTERNAL_DIR,
        )
        .context(AddVolumeMountSnafu)?
        // `add-controller` reads this controller's own on-disk `meta.properties` (its
        // `node.id`/`directory.id`, written by `kafka-storage.sh format`) from `log.dirs` in
        // the merged config it connects with - without this mount, every
        // `add-controller` attempt failed with "Unable to read meta.properties from
        // /stackable/data/kraft", since that path doesn't exist in this container's
        // filesystem at all without it. This mounts the *same* per-pod PVC the `kafka`
        // container itself writes `meta.properties` into, read-write for parity with it
        // (the CLI tool doesn't document a read-only requirement, and this repo has no
        // read-only-mount helper to reach for).
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?;

    Ok(Some(cb.build()))
}

fn add_vector_container(
    pod_builder: &mut PodBuilder,
    vector_container_name: &ContainerName,
    logging: &ValidatedLogging,
    resolved_product_image: &ResolvedProductImage,
    resource_names: &ResourceNames,
) {
    // Add vector container after kafka container to keep the defaulting into kafka container
    if let Some(vector_container_log_config) = &logging.vector_container {
        pod_builder.add_container(vector_container(
            vector_container_name,
            resolved_product_image,
            vector_container_log_config,
            resource_names,
            &VECTOR_CONFIG_VOLUME_NAME,
            &VECTOR_LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

    use super::*;
    use crate::controller::test_support::{minimal_kafka, validated_cluster};

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *VECTOR_CONFIG_VOLUME_NAME;
        let _ = *VECTOR_LOG_VOLUME_NAME;
        let _ = *POD_NAME;
        let _ = *KAFKA_CLIENT_PORT;
        let _ = *NAMESPACE;
        let _ = *ROLEGROUP_HEADLESS_SERVICE_NAME;
        let _ = *CLUSTER_DOMAIN;
        let _ = *EXTRA_ARGS;
        let _ = *CONTAINERDEBUG_LOG_DIRECTORY;
        let _ = *ZOOKEEPER;
        let _ = *ZOOKEEPER_CONFIG_MAP_KEY;
    }

    /// The user-supplied `envOverrides` must be merged in after all operator-set environment
    /// variables, so that they can override any of them. `CONTAINERDEBUG_LOG_DIRECTORY` is used
    /// as the example here because it is set unconditionally by the operator.
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
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");
        let mut validated_rg =
            cluster.role_group_configs[&KafkaRole::Broker][&role_group_name].clone();
        validated_rg.env_overrides = validated_rg
            .env_overrides
            .with_value(&CONTAINERDEBUG_LOG_DIRECTORY, "/custom/log/dir");

        let stateful_set = build_broker_rolegroup_statefulset(
            &KafkaRole::Broker,
            &role_group_name,
            &cluster,
            &validated_rg,
        )
        .expect("the StatefulSet builds");

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

    /// A minimal KRaft cluster with one controller role group, resolved through the real
    /// validate step (mirroring the fixtures in `build/mod.rs`'s own tests), since
    /// `ValidatedCluster` carries several resolved types that are impractical to construct by
    /// hand.
    fn kraft_mode_cluster() -> crate::controller::ValidatedCluster {
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
                metadataManager: kraft
              controllers:
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        validated_cluster(&kafka)
    }

    /// Same guarantee for the controller role, whose env vars are assembled by a separate
    /// builder ([`build_controller_rolegroup_statefulset`]).
    #[test]
    fn controller_env_overrides_override_operator_set_env_vars() {
        let cluster = kraft_mode_cluster();
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");
        let mut validated_rg =
            cluster.role_group_configs[&KafkaRole::Controller][&role_group_name].clone();
        validated_rg.env_overrides = validated_rg
            .env_overrides
            .with_value(&CONTAINERDEBUG_LOG_DIRECTORY, "/custom/log/dir");

        let stateful_set = build_controller_rolegroup_statefulset(
            &KafkaRole::Controller,
            &role_group_name,
            &cluster,
            &validated_rg,
        )
        .expect("the StatefulSet builds");

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
    fn controller_statefulset_uses_ordered_ready_pod_management() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet is built");

        assert_eq!(
            sts.spec
                .expect("the StatefulSet has a spec")
                .pod_management_policy,
            Some("OrderedReady".to_string())
        );
    }

    #[test]
    fn broker_statefulset_still_uses_parallel_pod_management() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");

        assert_eq!(
            sts.spec
                .expect("the StatefulSet has a spec")
                .pod_management_policy,
            Some("Parallel".to_string())
        );
    }

    /// End-to-end regression covering the whole point of removing `--initial-controllers`
    /// (and the sidecar's own baked-in bootstrap-servers literal) from the controller pod
    /// template: scaling an existing controller role group's replica count must not change
    /// either container's `command`, or Kubernetes will roll every already-existing
    /// controller pod on every scale-up/down, not just the ones actually being added or
    /// removed. Confirmed live: before this fix, both the `kafka` container's format command
    /// and the `quorum-manager` sidecar's bootstrap-servers literal changed with replica
    /// count, forcing a full rolling restart on every scale operation.
    #[test]
    fn controller_pod_template_is_stable_across_replica_count_changes() {
        let three_replicas = kraft_mode_cluster();
        let five_replicas = crate::controller::test_support::validated_cluster(
            &crate::controller::test_support::minimal_kafka(
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
                    metadataManager: kraft
                  controllers:
                    roleGroups:
                      default:
                        replicas: 5
                  brokers:
                    roleGroups:
                      default:
                        replicas: 3
                "#,
            ),
        );

        let three_containers = controller_containers(&three_replicas);
        let five_containers = controller_containers(&five_replicas);

        for name in ["kafka", QUORUM_MANAGER_CONTAINER_NAME] {
            let three_command = three_containers
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("the {name} container is built (3 replicas)"))
                .command
                .clone();
            let five_command = five_containers
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("the {name} container is built (5 replicas)"))
                .command
                .clone();
            assert_eq!(
                three_command, five_command,
                "the {name} container's command must not change when only the replica count \
                 of an existing controller role group changes"
            );
        }
    }

    fn controller_containers(
        cluster: &crate::controller::ValidatedCluster,
    ) -> Vec<stackable_operator::k8s_openapi::api::core::v1::Container> {
        let resources = crate::controller::build::build(cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller StatefulSet is built");
        sts.spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
    }

    #[test]
    fn controller_pods_get_a_quorum_manager_sidecar_on_supported_versions() {
        let cluster = kraft_mode_cluster();
        let containers = controller_containers(&cluster);

        assert!(
            containers
                .iter()
                .any(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME),
            "expected a quorum-manager sidecar, got containers: {:?}",
            containers.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn quorum_manager_sidecar_targets_bootstrap_servers_in_its_command() {
        let cluster = kraft_mode_cluster();
        let containers = controller_containers(&cluster);
        let sidecar = containers
            .iter()
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME)
            .expect("the quorum-manager sidecar is built");

        let command = sidecar
            .command
            .as_ref()
            .expect("the sidecar has a command")
            .join(" ");
        assert!(command.contains("add-controller"));

        // The sidecar only ever joins the quorum now — it has no `preStop` hook of its own.
        // See `controller_kafka_container_has_a_remove_self_pre_stop_hook` for why the
        // removal-on-departure half moved to the `kafka` container instead.
        assert!(sidecar.lifecycle.is_none());
    }

    /// `remove-controller` must run as the `kafka` container's own `preStop` hook, not the
    /// `quorum-manager` sidecar's — `preStop` only delays *that same container's* `SIGTERM`,
    /// and it's the `kafka` container's own Raft process (the thing actually leaving the
    /// voter set) that needs to stay alive while removal is attempted. See
    /// `controller_remove_self_pre_stop_command`'s doc comment for the full rationale.
    #[test]
    fn controller_kafka_container_has_a_remove_self_pre_stop_hook() {
        let cluster = kraft_mode_cluster();
        let container = controller_kafka_container(&cluster);

        let pre_stop_command = container
            .lifecycle
            .as_ref()
            .and_then(|l| l.pre_stop.as_ref())
            .and_then(|h| h.exec.as_ref())
            .and_then(|e| e.command.as_ref())
            .expect("the kafka container has a preStop exec hook")
            .join(" ");
        assert!(pre_stop_command.contains("remove-controller"));
        assert!(pre_stop_command.trim_end().ends_with("exit 0"));
    }

    /// Every `${env:NAME}` placeholder found in a rendered Java properties (or similar)
    /// string, in first-seen order, de-duplicated.
    ///
    /// The Java properties writer used to serialize the rendered `controller.properties`
    /// escapes `:` as `\:` (`:` otherwise separates a properties key from its value), so a
    /// placeholder actually appears as `${env\:NAME}` in the rendered ConfigMap content —
    /// this accepts either form.
    fn extract_env_placeholders(rendered: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut rest = rendered;
        while let Some(start) = rest.find("${env") {
            rest = &rest[start + "${env".len()..];
            rest = rest.strip_prefix('\\').unwrap_or(rest);
            let Some(rest_after_colon) = rest.strip_prefix(':') else {
                continue;
            };
            rest = rest_after_colon;
            let Some(end) = rest.find('}') else {
                break;
            };
            let name = rest[..end].to_string();
            if !result.contains(&name) {
                result.push(name);
            }
            rest = &rest[end + 1..];
        }
        result
    }

    /// Regression test for a real bug found in review: `build_quorum_manager_container` once
    /// set only `POD_NAME`/`NODE_ID_OFFSET` on the sidecar, while its own
    /// `controller.properties` render (used to build the `add-controller` config, see
    /// `command.rs`) needs `POD_NAME`, `ROLEGROUP_HEADLESS_SERVICE_NAME`, `NAMESPACE`,
    /// `CLUSTER_DOMAIN` and `KAFKA_CLIENT_PORT` — so the rendered `listeners` value was most
    /// likely broken (unresolved `${env:...}` placeholders). This asserts, from the actual
    /// rendered `controller.properties` content, that every placeholder it references has a
    /// matching env var on the sidecar container.
    #[test]
    fn quorum_manager_sidecar_has_every_env_var_controller_properties_rendering_references() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");

        let controller_properties = resources
            .config_maps
            .iter()
            .find(|cm| cm.metadata.name.as_deref() == Some("simple-kafka-controller-default"))
            .expect("the controller rolegroup ConfigMap is built")
            .data
            .as_ref()
            .expect("the ConfigMap carries data")
            .get("controller.properties")
            .expect("controller.properties is rendered into the ConfigMap")
            .clone();

        let placeholders = extract_env_placeholders(&controller_properties);
        assert!(
            placeholders.len() > 1,
            "sanity check failed: expected multiple ${{env:...}} placeholders in the rendered \
             controller.properties, got: {placeholders:?}"
        );

        let containers = controller_containers(&cluster);
        let sidecar = containers
            .iter()
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME)
            .expect("the quorum-manager sidecar is built");
        let sidecar_env_names: Vec<&str> = sidecar
            .env
            .as_ref()
            .expect("the sidecar has env vars")
            .iter()
            .map(|e| e.name.as_str())
            .collect();

        for placeholder in &placeholders {
            // REPLICA_ID is not a Kubernetes-injected env var: both the `kafka` container's
            // entrypoint and this sidecar's main-loop script derive and `export` it
            // themselves from `$POD_NAME`/`$NODE_ID_OFFSET` before rendering the template
            // (see `command.rs`), so it's expected to be absent from the container spec's
            // `env` list.
            if placeholder == "REPLICA_ID" {
                continue;
            }
            assert!(
                sidecar_env_names.contains(&placeholder.as_str()),
                "quorum-manager sidecar is missing env var {placeholder:?}, which is \
                 referenced by controller.properties's rendering; sidecar env vars: \
                 {sidecar_env_names:?}"
            );
        }

        // Targeted assertion (rather than relying on it only showing up incidentally among
        // `placeholders` above): NODE_ID_OFFSET is consumed directly by the sidecar's own
        // `EXPORT_REPLICA_ID` bash logic under `set -u` (see `command.rs`), so a regression
        // here would break the sidecar's `add-controller` main loop silently (an unset
        // variable under `set -u` aborts the script).
        let node_id_offset_name = KAFKA_NODE_ID_OFFSET.to_string();
        assert!(
            sidecar_env_names.contains(&node_id_offset_name.as_str()),
            "quorum-manager sidecar is missing the {node_id_offset_name} env var, needed by \
             its EXPORT_REPLICA_ID derivation under `set -u`; sidecar env vars: \
             {sidecar_env_names:?}"
        );
    }

    #[test]
    fn controller_pods_get_no_quorum_manager_sidecar_when_kerberos_is_enabled() {
        // This is a Global Constraint (see the plan header): the sidecar's admin-client
        // properties file only covers the TLS/SSL case, so it must never be added when
        // Kerberos is enabled, even on an otherwise-supported Kafka version.
        //
        // Rather than building a full CRD-level Kerberos fixture (which needs a resolved
        // AuthenticationClass threaded through `DereferencedObjects`, more than this test
        // needs), call `build_quorum_manager_container` directly — it already takes
        // `&ValidatedKafkaSecurity` as a parameter, so a fixture at that level is enough.
        // Reuse the `kerberos()` fixture from `security.rs`'s existing test module (see
        // Task 2).
        let cluster = kraft_mode_cluster();
        let kerberos_security = crate::controller::build::security::tests::kerberos();

        let result = build_quorum_manager_container(&cluster.image, &kerberos_security, Vec::new())
            .expect("build_quorum_manager_container does not error for a kerberos security value");

        assert!(result.is_none());
    }

    #[test]
    fn broker_pods_never_get_a_quorum_manager_sidecar() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");
        let containers = sts
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers;

        assert!(
            !containers
                .iter()
                .any(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME)
        );
    }

    fn controller_kafka_container(
        cluster: &crate::controller::ValidatedCluster,
    ) -> stackable_operator::k8s_openapi::api::core::v1::Container {
        controller_containers(cluster)
            .into_iter()
            .find(|c| c.name == "kafka")
            .expect("the kafka container is built")
    }

    fn broker_kafka_container(
        cluster: &crate::controller::ValidatedCluster,
    ) -> stackable_operator::k8s_openapi::api::core::v1::Container {
        let resources = crate::controller::build::build(cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");
        sts.spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers
            .into_iter()
            .find(|c| c.name == "kafka")
            .expect("the kafka container is built")
    }

    #[test]
    fn broker_kafka_container_has_a_startup_probe() {
        let cluster = kraft_mode_cluster();
        let container = broker_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let startup_probe = container
            .startup_probe
            .expect("the broker kafka container must have a startupProbe");
        let exec = startup_probe
            .exec
            .expect("the startupProbe must be an exec check, not a bare tcpSocket check");
        let command = exec.command.expect("exec has a command");
        let script = command.last().expect("the exec command has a script arg");

        assert!(
            script.contains(&format!("/dev/tcp/localhost/{client_port}")),
            "expected a TCP reachability check against the broker's own client port, script was: {script}"
        );
        assert!(
            script.contains("kafka_server_kafkaserver_brokerstate 3"),
            "expected a check for the broker's JMX BrokerState metric being RUNNING (3), \
             script was: {script}"
        );
        assert_eq!(startup_probe.timeout_seconds, Some(5));
        assert_eq!(startup_probe.period_seconds, Some(5));
        assert_eq!(startup_probe.failure_threshold, Some(60));
    }

    /// The liveness probe must check both TCP reachability (a genuinely dead/hung process must
    /// still be restarted) and that the broker's JMX `BrokerState` metric reports `RUNNING`
    /// (state `3`) - see `probes::broker_running_probe`'s doc comment for why the same check
    /// backs both the startup and liveness probes.
    #[test]
    fn broker_kafka_container_liveness_probe_checks_tcp_and_running_state() {
        let cluster = kraft_mode_cluster();
        let container = broker_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let liveness_probe = container
            .liveness_probe
            .expect("the broker kafka container must have a livenessProbe");
        let exec = liveness_probe
            .exec
            .expect("the livenessProbe must be an exec check, not a bare tcpSocket check");
        let command = exec.command.expect("exec has a command");
        let script = command.last().expect("the exec command has a script arg");

        assert!(
            script.contains(&format!("/dev/tcp/localhost/{client_port}")),
            "expected a TCP reachability check against the broker's own client port, script was: {script}"
        );
        assert!(
            script.contains("kafka_server_kafkaserver_brokerstate 3"),
            "expected a check for the broker's JMX BrokerState metric being RUNNING (3), \
             script was: {script}"
        );

        assert_eq!(liveness_probe.timeout_seconds, Some(10));
        assert_eq!(liveness_probe.period_seconds, Some(30));
        assert_eq!(liveness_probe.failure_threshold, Some(20));
    }

    /// The `kcat`-based readiness probe runs directly on the `kafka` container - there is no
    /// separate `kcat-prober` sidecar (removed since `kcat` ships in the same product image the
    /// `kafka` container already uses, so a dedicated container was no longer needed).
    #[test]
    fn broker_kafka_container_readiness_probe_uses_kcat() {
        let cluster = kraft_mode_cluster();
        let container = broker_kafka_container(&cluster);

        let readiness_probe = container
            .readiness_probe
            .expect("the broker kafka container must have a readinessProbe");
        let exec = readiness_probe
            .exec
            .expect("the readinessProbe must be an exec check");
        let command = exec.command.expect("exec has a command");
        assert_eq!(command[0], "/stackable/kcat");
    }

    #[test]
    fn broker_pods_have_no_kcat_prober_sidecar() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");
        let sts = resources
            .stateful_sets
            .into_iter()
            .find(|sts| sts.metadata.name.as_deref() == Some("simple-kafka-broker-default"))
            .expect("the broker StatefulSet is built");
        let containers = sts
            .spec
            .expect("the StatefulSet has a spec")
            .template
            .spec
            .expect("the pod template has a spec")
            .containers;

        assert!(
            !containers.iter().any(|c| c.name == "kcat-prober"),
            "expected no separate kcat-prober container, got: {:?}",
            containers.iter().map(|c| &c.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn controller_kafka_container_has_a_startup_probe() {
        let cluster = kraft_mode_cluster();
        let container = controller_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let startup_probe = container
            .startup_probe
            .expect("the controller kafka container must have a startupProbe");
        let tcp_socket = startup_probe
            .tcp_socket
            .expect("the startupProbe must be a tcpSocket check");
        assert_eq!(tcp_socket.port, IntOrString::Int(client_port.into()));
        assert_eq!(startup_probe.timeout_seconds, Some(5));
        assert_eq!(startup_probe.period_seconds, Some(5));
        assert_eq!(startup_probe.failure_threshold, Some(60));
    }

    /// The liveness probe must check both TCP reachability (a genuinely dead/hung process must
    /// still be restarted, same as before) and local Raft state, failing specifically on
    /// `unattached` — see `controller_stuck_unattached_liveness_probe`'s doc comment for why
    /// only that state, not any non-healthy state, is treated as restart-worthy.
    #[test]
    fn controller_kafka_container_liveness_probe_checks_tcp_and_stuck_unattached_state() {
        let cluster = kraft_mode_cluster();
        let container = controller_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let liveness_probe = container
            .liveness_probe
            .expect("the controller kafka container must have a livenessProbe");
        let exec = liveness_probe
            .exec
            .expect("the livenessProbe must be an exec check, not a bare tcpSocket check");
        let command = exec.command.expect("exec has a command");
        let script = command.last().expect("the exec command has a script arg");

        assert!(
            script.contains(&format!("/dev/tcp/localhost/{client_port}")),
            "expected a TCP reachability check against the controller's own port, script was: {script}"
        );
        assert!(
            script.contains(r#"[ "$state" != "unattached" ]"#),
            "expected the check to fail specifically (and only) on the unattached state, \
             script was: {script}"
        );
        // Must not fail merely for being non-healthy in some *other* way (e.g. `candidate` or
        // `observer`) - only `unattached` is the specific, restart-fixable symptom.
        assert!(!script.contains("leader|follower"));

        assert_eq!(liveness_probe.timeout_seconds, Some(10));
        assert_eq!(liveness_probe.period_seconds, Some(30));
        assert_eq!(liveness_probe.failure_threshold, Some(20));
    }

    #[test]
    fn controller_kafka_container_readiness_probe_checks_raft_state() {
        let cluster = kraft_mode_cluster();
        let container = controller_kafka_container(&cluster);

        let readiness_probe = container.readiness_probe.expect("readiness probe is set");
        let exec = readiness_probe
            .exec
            .expect("readiness probe is an exec check");
        let command = exec.command.expect("exec has a command");
        assert_eq!(
            command,
            vec![
                "bash".to_string(),
                "-c".to_string(),
                "curl -s localhost:9606/metrics | grep -E 'kafka_server_raft_metrics_current_state\\{state=\"(leader|follower|voted)\",?\\}'".to_string(),
            ]
        );
        assert_eq!(readiness_probe.timeout_seconds, Some(10));
        assert_eq!(readiness_probe.period_seconds, Some(10));
        assert_eq!(readiness_probe.failure_threshold, Some(6));
    }
}

use std::{ops::Deref, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::FieldPathEnvVar, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder, volume::VolumeBuilder,
        },
    },
    commons::product_image_selection::ResolvedProductImage,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy},
            core::v1::{
                ConfigMapVolumeSource, ContainerPort, EnvVar, ExecAction, PodSpec, Probe,
                TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    product_logging,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::{
                container::{EnvVarName, EnvVarSet, new_container_builder},
                volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
            },
        },
        jvm_argument_overrides::JvmArgumentOverrides,
        product_logging::framework::{
            STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice, vector_container,
        },
        role_group_utils::ResourceNames,
        types::kubernetes::{ConfigMapKey, ContainerName, VolumeName},
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            command::{
                KAFKA_LOG4J_OPTS, broker_kafka_container_commands,
                controller_kafka_container_command, kafka_log_opts,
            },
            graceful_shutdown::add_graceful_shutdown_config,
            kerberos::{add_kerberos_pod_config, kerberos_env_vars},
            properties::product_logging::MAX_KAFKA_LOG_FILES_SIZE,
            recommended_labels_for_role_group_resources,
            recommended_labels_for_unversioned_role_group_resources, role_group_selector,
            security::{
                add_broker_volume_and_volume_mounts, add_controller_volume_and_volume_mounts,
                kcat_prober_container_commands,
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
stackable_operator::constant!(PRE_STOP_CONTROLLER_SLEEP_SECONDS: EnvVarName = "PRE_STOP_CONTROLLER_SLEEP_SECONDS");
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

const POD_MANAGEMENT_POLICY_PARALLEL: &str = "Parallel";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors {
        source: crate::controller::PodDescriptorsError,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments {
        source: crate::controller::build::jvm::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::controller::build::graceful_shutdown::Error,
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

    let mut cb_kcat_prober = new_container_builder(&container_name(BrokerContainer::KcatProber));
    let mut cb_kafka = new_container_builder(&container_name(BrokerContainer::Kafka));

    let mut pod_builder = PodBuilder::new();

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .deref()
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    add_broker_volume_and_volume_mounts(
        kafka_security,
        &mut pod_builder,
        &mut cb_kcat_prober,
        &mut cb_kafka,
        &requested_secret_lifetime,
    );

    let mut pvcs = merged_config.resources().storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    let bootstrap_listener_name =
        validated_cluster.bootstrap_listener_name(kafka_role, role_group_name);
    pvcs.push(listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(bootstrap_listener_name),
        &unversioned_recommended_labels,
        &LISTENER_BOOTSTRAP_VOLUME_NAME,
    ));

    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &mut pod_builder,
        );
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
            // we need controller pods
            validated_cluster
                .pod_descriptors(Some(&KafkaRole::Controller))
                .context(BuildPodDescriptorsSnafu)?,
            kafka_security,
            &resolved_product_image.product_version,
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
        .resources(merged_config.resources().clone().into());

    // Use kcat sidecar for probing container status rather than the official Kafka tools, since they incur a lot of
    // unacceptable perf overhead
    cb_kcat_prober
        .image_from_product_image(resolved_product_image)
        .command(vec!["sleep".to_string(), "infinity".to_string()])
        .add_env_vars(Vec::<EnvVar>::from(
            EnvVarSet::new()
                .with_field_path(&POD_NAME, &FieldPathEnvVar::Name)
                .merge(kerberos_env_vars(kafka_security)),
        ))
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        )
        .add_volume_mount(
            &*LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                // If the broker is able to get its fellow cluster members then it has at least completed basic registration at some point
                command: Some(kcat_prober_container_commands(kafka_security)),
            }),
            timeout_seconds: Some(5),
            period_seconds: Some(2),
            ..Probe::default()
        });

    add_log_config_volume(
        &mut pod_builder,
        &validated_rg.config.logging,
        &resource_names,
    );

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    if let Some(listener_class) = merged_config.listener_class() {
        pod_builder
            .add_listener_volume_by_listener_class(
                LISTENER_BROKER_VOLUME_NAME.as_ref(),
                listener_class.as_ref(),
                &recommended_labels,
            )
            .expect("The annotation keys are static, annotation values cannot be invalid, and the volume name is statically defined.");
    }

    if let Some(broker_id_config_map_name) = &validated_cluster
        .cluster_config
        .broker_id_pod_config_map_name
    {
        pod_builder
            .add_volume(
                VolumeBuilder::new(&*BROKER_ID_POD_MAP_DIR_NAME)
                    .with_config_map(broker_id_config_map_name)
                    .build(),
            )
            .expect("The volume names are statically defined and there should be no duplicates.");
        cb_kafka
            .add_volume_mount(&*BROKER_ID_POD_MAP_DIR_NAME, BROKER_ID_POD_MAP_DIR)
            .expect("The mount paths are statically defined and there should be no duplicates.");
    }

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .add_container(cb_kcat_prober.build())
        .affinity(&merged_config.affinity);

    add_common_pod_config(
        &mut pod_builder,
        &resource_names,
        validated_cluster
            .cluster_resource_names()
            .service_account_name()
            .as_ref(),
    );

    add_vector_container(
        &mut pod_builder,
        &container_name(BrokerContainer::Vector),
        &validated_rg.config.logging,
        resolved_product_image,
        &resource_names,
    );

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);

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

    let mut cb_kafka = new_container_builder(&container_name(ControllerContainer::Kafka));

    let mut pod_builder = PodBuilder::new();

    // Operator-set env vars first (common + controller-specific); the user's `envOverrides`
    // are merged on top last and win.
    let env: Vec<EnvVar> = common_operator_env_vars(validated_cluster, kafka_security)
        .with_field_path(&NAMESPACE, &FieldPathEnvVar::Namespace)
        .with_value(
            &ROLEGROUP_HEADLESS_SERVICE_NAME,
            resource_names.headless_service_name().to_string(),
        )
        .with_value(
            &CLUSTER_DOMAIN,
            validated_cluster.cluster_domain.to_string(),
        )
        .with_value(&PRE_STOP_CONTROLLER_SLEEP_SECONDS, "10")
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
            validated_cluster
                .pod_descriptors(Some(kafka_role))
                .context(BuildPodDescriptorsSnafu)?,
            &resolved_product_image.product_version,
        )]);

    cb_kafka
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(&*LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_CONFIG_DIR_NAME, STACKABLE_LOG_CONFIG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .add_volume_mount(&*STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        .resources(merged_config.resources().clone().into())
        // TODO: improve probes
        .liveness_probe(Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(kafka_security.client_port().into()),
                ..Default::default()
            }),
            timeout_seconds: Some(10),
            period_seconds: Some(10),
            failure_threshold: Some(6),
            ..Probe::default()
        })
        .readiness_probe(Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(kafka_security.client_port().into()),
                ..Default::default()
            }),
            timeout_seconds: Some(10),
            period_seconds: Some(10),
            failure_threshold: Some(6),
            ..Probe::default()
        });

    add_log_config_volume(
        &mut pod_builder,
        &validated_rg.config.logging,
        &resource_names,
    );

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
    );

    let kafka_container = cb_kafka.build();

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(kafka_container)
        .affinity(&merged_config.affinity);

    add_common_pod_config(
        &mut pod_builder,
        &resource_names,
        validated_cluster
            .cluster_resource_names()
            .service_account_name()
            .as_ref(),
    );

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
            pod_management_policy: Some(POD_MANAGEMENT_POLICY_PARALLEL.to_string()),
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

/// Adds the env vars that the broker and controller Kafka containers share: the JVM
/// arguments, log options, the `containerdebug` log directory and the node-id offset.
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
) {
    let config_map = match &logging.kafka_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    pod_builder
        .add_volume(
            VolumeBuilder::new(&*STACKABLE_LOG_CONFIG_DIR_NAME)
                .with_config_map(config_map)
                .build(),
        )
        .expect("The volume names are statically defined and there should be no duplicates.");
}

/// Adds the `config` volume, the `log` emptyDir, the service account and the pod security
/// context that the broker and controller pods share.
fn add_common_pod_config(
    pod_builder: &mut PodBuilder,
    resource_names: &ResourceNames,
    service_account_name: &str,
) {
    pod_builder
        .add_volume(Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .expect("The volume names are statically defined and there should be no duplicates.")
        .add_empty_dir_volume(
            &*STACKABLE_LOG_DIR_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .expect("The volume names are statically defined and there should be no duplicates.")
        .service_account_name(service_account_name)
        .security_context(
            PodSecurityContextBuilder::with_stackable_defaults()
                .fs_group(1000)
                .build(),
        );
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
        let _ = *PRE_STOP_CONTROLLER_SLEEP_SECONDS;
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

    /// Same guarantee for the controller role, whose env vars are assembled by a separate
    /// builder ([`build_controller_rolegroup_statefulset`]).
    #[test]
    fn controller_env_overrides_override_operator_set_env_vars() {
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
        let cluster = validated_cluster(&kafka);
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");
        let mut validated_rg =
            cluster.role_group_configs[&KafkaRole::Controller][&role_group_name].clone();
        validated_rg.env_overrides = validated_rg
            .env_overrides
            .with_value(&PRE_STOP_CONTROLLER_SLEEP_SECONDS, "42");

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

        let sleep_seconds: Vec<_> = env
            .iter()
            .filter(|env_var| env_var.name == "PRE_STOP_CONTROLLER_SLEEP_SECONDS")
            .collect();
        assert_eq!(
            sleep_seconds.len(),
            1,
            "the override must replace the operator-set value, not duplicate it"
        );
        assert_eq!(sleep_seconds[0].value.as_deref(), Some("42"));
    }
}

use std::{ops::Deref, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder,
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
                ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort, EnvVar, EnvVarSource,
                ExecAction, ObjectFieldSelector, PodSpec, Probe, ServiceAccount, TCPSocketAction,
                Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::ResourceExt,
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
        },
        jvm_argument_overrides::JvmArgumentOverrides,
        role_group_utils::ResourceNames,
        types::kubernetes::{ListenerName, PersistentVolumeClaimName},
    },
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            command::{
                broker_kafka_container_commands, controller_kafka_container_command,
                kafka_log_opts, kafka_log_opts_env_var,
            },
            graceful_shutdown::add_graceful_shutdown_config,
            kerberos::add_kerberos_pod_config,
            properties::logging::MAX_KAFKA_LOG_FILES_SIZE,
        },
        node_id_hasher::node_id_hash32_offset,
    },
    crd::{
        self, BROKER_ID_POD_MAP_DIR, KAFKA_HEAP_OPTS, LISTENER_BOOTSTRAP_VOLUME_NAME,
        LISTENER_BROKER_VOLUME_NAME, LOG_DIRS_VOLUME_NAME, METRICS_PORT, METRICS_PORT_NAME,
        MetadataManager, STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR,
        STACKABLE_LISTENER_BOOTSTRAP_DIR, STACKABLE_LISTENER_BROKER_DIR, STACKABLE_LOG_CONFIG_DIR,
        STACKABLE_LOG_DIR,
        role::{
            AnyConfig, KAFKA_NODE_ID_OFFSET, KafkaRole, broker::BrokerContainer,
            controller::ControllerContainer,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid metadata manager"))]
    InvalidMetadataManager { source: crate::crd::Error },

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig {
        source: crate::controller::build::kerberos::Error,
    },

    #[snafu(display("failed to add listener volume"))]
    AddListenerVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add Secret Volumes and VolumeMounts"))]
    AddVolumesAndVolumeMounts { source: crate::crd::security::Error },

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

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging {
        source: stackable_operator::product_logging::framework::LoggingError,
    },

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

    #[snafu(display("failed to retrieve rolegroup replicas"))]
    RoleGroupReplicas { source: crd::role::Error },

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,
}

/// The broker rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) from [`build_rolegroup_headless_service`](`crate::controller::build::resource::service::build_rolegroup_headless_service`).
pub fn build_broker_rolegroup_statefulset(
    kafka: &v1alpha1::KafkaCluster,
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
    validated_cluster: &ValidatedCluster,
    validated_rg: &ValidatedRoleGroupConfig,
    service_account: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config;
    let resource_names = validated_cluster.resource_names(kafka_role, role_group_name);
    let recommended_labels = validated_cluster.recommended_labels(kafka_role, role_group_name);
    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels =
        validated_cluster.unversioned_recommended_labels(kafka_role, role_group_name);

    let kcat_prober_container_name = BrokerContainer::KcatProber.to_string();
    let mut cb_kcat_prober =
        ContainerBuilder::new(&kcat_prober_container_name).context(InvalidContainerNameSnafu {
            name: kcat_prober_container_name.clone(),
        })?;

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
    kafka_security
        .add_broker_volume_and_volume_mounts(
            &mut pod_builder,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &requested_secret_lifetime,
        )
        .context(AddVolumesAndVolumeMountsSnafu)?;

    let mut pvcs = merged_config.resources().storage.build_pvcs();

    // bootstrap listener should be persistent,
    // main broker listener is an ephemeral PVC instead
    let bootstrap_listener_name = ListenerName::from_str(
        &validated_cluster.bootstrap_listener_name(kafka_role, role_group_name),
    )
    .expect("the bootstrap listener name is a valid Listener name");
    let bootstrap_pvc_name = PersistentVolumeClaimName::from_str(LISTENER_BOOTSTRAP_VOLUME_NAME)
        .expect("the bootstrap listener volume name is a valid PVC name");
    pvcs.push(listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(bootstrap_listener_name),
        &unversioned_recommended_labels,
        &bootstrap_pvc_name,
    ));

    if kafka_security.has_kerberos_enabled() {
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }

    let mut env = Vec::<EnvVar>::from(validated_rg.env_overrides.clone());

    if let Some(zookeeper_config_map_name) = &kafka.spec.cluster_config.zookeeper_config_map_name {
        env.push(EnvVar {
            name: "ZOOKEEPER".to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: zookeeper_config_map_name.to_string(),
                    key: "ZOOKEEPER".to_string(),
                    ..ConfigMapKeySelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        })
    };

    env.push(EnvVar {
        name: "POD_NAME".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "metadata.name".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });

    let metadata_manager = kafka
        .effective_metadata_manager()
        .context(InvalidMetadataManagerSnafu)?;

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
            metadata_manager == MetadataManager::KRaft,
            // we need controller pods
            validated_cluster
                .pod_descriptors(Some(&KafkaRole::Controller))
                .context(BuildPodDescriptorsSnafu)?,
            kafka_security,
            &resolved_product_image.product_version,
        )]);

    add_common_kafka_env(
        &mut cb_kafka,
        merged_config,
        &validated_rg.jvm_argument_overrides,
        resolved_product_image,
        kafka_role,
        role_group_name,
    )?;

    cb_kafka
        .add_env_var(
            "KAFKA_CLIENT_PORT".to_string(),
            kafka_security.client_port().to_string(),
        )
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(
            LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .resources(merged_config.resources().clone().into());

    // Use kcat sidecar for probing container status rather than the official Kafka tools, since they incur a lot of
    // unacceptable perf overhead
    cb_kcat_prober
        .image_from_product_image(resolved_product_image)
        .command(vec!["sleep".to_string(), "infinity".to_string()])
        .add_env_vars(vec![EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "metadata.name".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        )
        .add_volume_mount(
            LISTENER_BOOTSTRAP_VOLUME_NAME,
            STACKABLE_LISTENER_BOOTSTRAP_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LISTENER_BROKER_VOLUME_NAME, STACKABLE_LISTENER_BROKER_DIR)
        .context(AddVolumeMountSnafu)?
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                // If the broker is able to get its fellow cluster members then it has at least completed basic registration at some point
                command: Some(kafka_security.kcat_prober_container_commands()),
            }),
            timeout_seconds: Some(5),
            period_seconds: Some(2),
            ..Probe::default()
        });

    add_log_config_volume(&mut pod_builder, merged_config, &resource_names)?;

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    if let Some(listener_class) = merged_config.listener_class() {
        pod_builder
            .add_listener_volume_by_listener_class(
                LISTENER_BROKER_VOLUME_NAME,
                listener_class,
                &recommended_labels,
            )
            .context(AddListenerVolumeSnafu)?;
    }

    if let Some(broker_id_config_map_name) =
        &kafka.spec.cluster_config.broker_id_pod_config_map_name
    {
        pod_builder
            .add_volume(
                VolumeBuilder::new("broker-id-pod-map-dir")
                    .with_config_map(broker_id_config_map_name)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
        cb_kafka
            .add_volume_mount("broker-id-pod-map-dir", BROKER_ID_POD_MAP_DIR)
            .context(AddVolumeMountSnafu)?;
    }

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .add_container(cb_kcat_prober.build())
        .affinity(&merged_config.affinity);

    add_common_pod_config(&mut pod_builder, &resource_names, service_account)?;

    add_vector_container(
        &mut pod_builder,
        kafka,
        resolved_product_image,
        merged_config,
    )?;

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
            pod_management_policy: Some("Parallel".to_string()),
            replicas: kafka_role
                .replicas(kafka, role_group_name.as_ref())
                .context(RoleGroupReplicasSnafu)?
                .map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    validated_cluster
                        .role_group_selector(kafka_role, role_group_name)
                        .into(),
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
    kafka: &v1alpha1::KafkaCluster,
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
    validated_cluster: &ValidatedCluster,
    validated_rg: &ValidatedRoleGroupConfig,
    service_account: &ServiceAccount,
) -> Result<StatefulSet, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config;
    let resource_names = validated_cluster.resource_names(kafka_role, role_group_name);
    let recommended_labels = validated_cluster.recommended_labels(kafka_role, role_group_name);

    let kafka_container_name = ControllerContainer::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    let mut env = Vec::<EnvVar>::from(validated_rg.env_overrides.clone());

    env.push(EnvVar {
        name: "NAMESPACE".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "metadata.namespace".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "POD_NAME".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "metadata.name".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "ROLEGROUP_HEADLESS_SERVICE_NAME".to_string(),
        value: Some(resource_names.headless_service_name().to_string()),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "CLUSTER_DOMAIN".to_string(),
        value: Some(validated_cluster.cluster_domain.to_string()),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "KAFKA_CLIENT_PORT".to_string(),
        value: Some(kafka_security.client_port().to_string()),
        ..EnvVar::default()
    });

    // Controllers need the ZooKeeper connection string for migration
    if let Some(zookeeper_config_map_name) = &kafka.spec.cluster_config.zookeeper_config_map_name {
        env.push(EnvVar {
            name: "ZOOKEEPER".to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: zookeeper_config_map_name.to_string(),
                    key: "ZOOKEEPER".to_string(),
                    ..ConfigMapKeySelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        })
    };

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
        )])
        .add_env_var("PRE_STOP_CONTROLLER_SLEEP_SECONDS", "10");

    add_common_kafka_env(
        &mut cb_kafka,
        merged_config,
        &validated_rg.jvm_argument_overrides,
        resolved_product_image,
        kafka_role,
        role_group_name,
    )?;

    cb_kafka
        .add_env_vars(env)
        .add_container_ports(container_ports(kafka_security))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log-config", STACKABLE_LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount("log", STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
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

    add_log_config_volume(&mut pod_builder, merged_config, &resource_names)?;

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    // Add TLS related volumes and volume mounts
    let requested_secret_lifetime = merged_config
        .deref()
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    kafka_security
        .add_controller_volume_and_volume_mounts(
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

    add_common_pod_config(&mut pod_builder, &resource_names, service_account)?;

    add_vector_container(
        &mut pod_builder,
        kafka,
        resolved_product_image,
        merged_config,
    )?;

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
            pod_management_policy: Some("Parallel".to_string()),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..StatefulSetUpdateStrategy::default()
            }),
            replicas: kafka_role
                .replicas(kafka, role_group_name.as_ref())
                .context(RoleGroupReplicasSnafu)?
                .map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    validated_cluster
                        .role_group_selector(kafka_role, role_group_name)
                        .into(),
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
fn container_ports(kafka_security: &KafkaTlsSecurity) -> Vec<ContainerPort> {
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
fn add_common_kafka_env(
    cb_kafka: &mut ContainerBuilder,
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
    resolved_product_image: &ResolvedProductImage,
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
) -> Result<(), Error> {
    cb_kafka
        .add_env_var(
            "EXTRA_ARGS",
            crate::controller::build::jvm::construct_non_heap_jvm_args(
                merged_config,
                jvm_argument_overrides,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            KAFKA_HEAP_OPTS,
            crate::controller::build::jvm::construct_heap_jvm_args(
                merged_config,
                jvm_argument_overrides,
            )
            .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            kafka_log_opts_env_var(),
            kafka_log_opts(&resolved_product_image.product_version),
        )
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .add_env_var(
            KAFKA_NODE_ID_OFFSET,
            node_id_hash32_offset(kafka_role, role_group_name.as_ref()).to_string(),
        );
    Ok(())
}

/// Adds the `log-config` volume, sourced either from the user-supplied custom log config
/// `ConfigMap` or the rolegroup `ConfigMap`.
fn add_log_config_volume(
    pod_builder: &mut PodBuilder,
    merged_config: &AnyConfig,
    resource_names: &ResourceNames,
) -> Result<(), Error> {
    if let ContainerLogConfig {
        choice:
            Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                custom: ConfigMapLogConfig { config_map },
            })),
    } = &*merged_config.kafka_logging()
    {
        pod_builder
            .add_volume(
                VolumeBuilder::new("log-config")
                    .with_config_map(config_map)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    } else {
        pod_builder
            .add_volume(
                VolumeBuilder::new("log-config")
                    .with_config_map(resource_names.role_group_config_map().to_string())
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }
    Ok(())
}

/// Adds the `config` volume, the `log` emptyDir, the service account and the pod security
/// context that the broker and controller pods share.
fn add_common_pod_config(
    pod_builder: &mut PodBuilder,
    resource_names: &ResourceNames,
    service_account: &ServiceAccount,
) -> Result<(), Error> {
    pod_builder
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());
    Ok(())
}

/// Adds the Vector log-aggregation sidecar container, when Vector logging is enabled.
///
/// Errors if Vector logging is enabled but no Vector aggregator discovery `ConfigMap` is
/// configured on the cluster.
fn add_vector_container(
    pod_builder: &mut PodBuilder,
    kafka: &v1alpha1::KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
    merged_config: &AnyConfig,
) -> Result<(), Error> {
    // Add vector container after kafka container to keep the defaulting into kafka container
    if merged_config.vector_logging_enabled() {
        match &kafka.spec.cluster_config.vector_aggregator_config_map_name {
            Some(vector_aggregator_config_map_name) => {
                pod_builder.add_container(
                    product_logging::framework::vector_container(
                        resolved_product_image,
                        "config",
                        "log",
                        Some(&*merged_config.vector_logging()),
                        ResourceRequirementsBuilder::new()
                            .with_cpu_request("250m")
                            .with_cpu_limit("500m")
                            .with_memory_request("128Mi")
                            .with_memory_limit("128Mi")
                            .build(),
                        vector_aggregator_config_map_name,
                    )
                    .context(ConfigureLoggingSnafu)?,
                );
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
    }
    Ok(())
}

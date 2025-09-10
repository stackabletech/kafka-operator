use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
};

use product_config::types::PropertyNameKind;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference, VolumeBuilder},
        },
    },
    commons::product_image_selection::ResolvedProductImage,
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
    kvp::Labels,
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::RoleGroupRef,
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::{
    config::{
        command::{broker_kafka_container_commands, controller_kafka_container_command},
        node_id_hasher::node_id_hash32_offset,
    },
    crd::{
        self, APP_NAME, KAFKA_HEAP_OPTS, LISTENER_BOOTSTRAP_VOLUME_NAME,
        LISTENER_BROKER_VOLUME_NAME, LOG_DIRS_VOLUME_NAME, METRICS_PORT, METRICS_PORT_NAME,
        STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR, STACKABLE_LISTENER_BOOTSTRAP_DIR,
        STACKABLE_LISTENER_BROKER_DIR,
        listener::get_kafka_listener_config,
        role::{
            AnyConfig, KAFKA_NODE_ID_OFFSET, KafkaRole, broker::BrokerContainer,
            controller::ControllerContainer,
        },
        security::KafkaTlsSecurity,
        v1alpha1,
    },
    kafka_controller::KAFKA_CONTROLLER_NAME,
    kerberos::add_kerberos_pod_config,
    operations::graceful_shutdown::add_graceful_shutdown_config,
    product_logging::{
        MAX_KAFKA_LOG_FILES_SIZE, STACKABLE_LOG_CONFIG_DIR, STACKABLE_LOG_DIR, kafka_log_opts,
        kafka_log_opts_env_var,
    },
    utils::build_recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig { source: crate::kerberos::Error },

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

    #[snafu(display("failed to build bootstrap listener pvc"))]
    BuildBootstrapListenerPvc {
        source: stackable_operator::builder::pod::volume::ListenerOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build pod descriptors"))]
    BuildPodDescriptors { source: crate::crd::Error },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging {
        source: stackable_operator::product_logging::framework::LoggingError,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::crd::role::Error },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("invalid Container name [{name}]"))]
    InvalidContainerName {
        name: String,
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: crate::crd::listener::KafkaListenerError,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to merge pod overrides"))]
    MergePodOverrides { source: crd::role::Error },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to retrieve rolegroup replicas"))]
    RoleGroupReplicas { source: crd::role::Error },

    #[snafu(display("cluster does not define UID"))]
    ClusterUidMissing,

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,
}

/// The broker rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) from [`build_rolegroup_service`](`crate::resource::service::build_rolegroup_service`).
#[allow(clippy::too_many_arguments)]
pub fn build_broker_rolegroup_statefulset(
    kafka: &v1alpha1::KafkaCluster,
    kafka_role: &KafkaRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::KafkaCluster>,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    opa_connect_string: Option<&str>,
    kafka_security: &KafkaTlsSecurity,
    merged_config: &AnyConfig,
    service_account: &ServiceAccount,
    cluster_info: &KubernetesClusterInfo,
) -> Result<StatefulSet, Error> {
    let recommended_object_labels = build_recommended_labels(
        kafka,
        KAFKA_CONTROLLER_NAME,
        &resolved_product_image.app_version_label_value,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );
    let recommended_labels =
        Labels::recommended(recommended_object_labels.clone()).context(LabelBuildSnafu)?;
    // Used for PVC templates that cannot be modified once they are deployed
    let unversioned_recommended_labels = Labels::recommended(build_recommended_labels(
        kafka,
        KAFKA_CONTROLLER_NAME,
        // A version value is required, and we do want to use the "recommended" format for the other desired labels
        "none",
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    ))
    .context(LabelBuildSnafu)?;

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
    pvcs.push(
        ListenerOperatorVolumeSourceBuilder::new(
            &ListenerReference::ListenerName(kafka.bootstrap_service_name(rolegroup_ref)),
            &unversioned_recommended_labels,
        )
        .build_pvc(LISTENER_BOOTSTRAP_VOLUME_NAME)
        .context(BuildBootstrapListenerPvcSnafu)?,
    );

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

    let mut env = broker_config
        .get(&PropertyNameKind::Env)
        .into_iter()
        .flatten()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

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

    let kafka_listeners = get_kafka_listener_config(
        kafka,
        kafka_security,
        &rolegroup_ref.object_name(),
        cluster_info,
    )
    .context(InvalidKafkaListenersSnafu)?;

    let cluster_id = kafka.uid().context(ClusterUidMissingSnafu)?;

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
            kafka,
            cluster_id,
            // we need controller pods
            kafka
                .pod_descriptors(&KafkaRole::Controller, cluster_info)
                .context(BuildPodDescriptorsSnafu)?,
            &kafka_listeners,
            opa_connect_string,
            kafka_security,
            &resolved_product_image.product_version,
        )])
        .add_env_var(
            "EXTRA_ARGS",
            kafka_role
                .construct_non_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            KAFKA_HEAP_OPTS,
            kafka_role
                .construct_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            kafka_log_opts_env_var(&resolved_product_image.product_version),
            kafka_log_opts(&resolved_product_image.product_version),
        )
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .add_env_var(
            KAFKA_NODE_ID_OFFSET,
            node_id_hash32_offset(rolegroup_ref).to_string(),
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
                    .with_config_map(rolegroup_ref.object_name())
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels)
        .context(MetadataBuildSnafu)?
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
    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .add_container(cb_kcat_prober.build())
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        // bootstrap volume is a persistent volume template instead, to keep addresses persistent
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

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

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);

    pod_template.merge_from(
        kafka_role
            .role_pod_overrides(kafka)
            .context(MergePodOverridesSnafu)?,
    );
    pod_template.merge_from(
        kafka_role
            .role_group_pod_overrides(kafka, &rolegroup_ref.role_group)
            .context(MergePodOverridesSnafu)?,
    );

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label_value,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: kafka_role
                .replicas(kafka, &rolegroup_ref.role_group)
                .context(RoleGroupReplicasSnafu)?
                .map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        kafka,
                        APP_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            service_name: Some(rolegroup_ref.object_name()),
            template: pod_template,
            volume_claim_templates: Some(pvcs),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

/// The controller rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
#[allow(clippy::too_many_arguments)]
pub fn build_controller_rolegroup_statefulset(
    kafka: &v1alpha1::KafkaCluster,
    kafka_role: &KafkaRole,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::KafkaCluster>,
    controller_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    kafka_security: &KafkaTlsSecurity,
    merged_config: &AnyConfig,
    service_account: &ServiceAccount,
    cluster_info: &KubernetesClusterInfo,
) -> Result<StatefulSet, Error> {
    let recommended_object_labels = build_recommended_labels(
        kafka,
        KAFKA_CONTROLLER_NAME,
        &resolved_product_image.app_version_label_value,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    );

    let kafka_container_name = ControllerContainer::Kafka.to_string();
    let mut cb_kafka =
        ContainerBuilder::new(&kafka_container_name).context(InvalidContainerNameSnafu {
            name: kafka_container_name.clone(),
        })?;

    let mut pod_builder = PodBuilder::new();

    let mut env = controller_config
        .get(&PropertyNameKind::Env)
        .into_iter()
        .flatten()
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

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
        name: "ROLEGROUP_REF".to_string(),
        value: Some(rolegroup_ref.object_name()),
        ..EnvVar::default()
    });

    env.push(EnvVar {
        name: "CLUSTER_DOMAIN".to_string(),
        value: Some(cluster_info.cluster_domain.to_string()),
        ..EnvVar::default()
    });

    let kafka_listeners = get_kafka_listener_config(
        kafka,
        kafka_security,
        &rolegroup_ref.object_name(),
        cluster_info,
    )
    .context(InvalidKafkaListenersSnafu)?;

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
            kafka.uid().context(ClusterUidMissingSnafu)?,
            kafka
                .pod_descriptors(kafka_role, cluster_info)
                .context(BuildPodDescriptorsSnafu)?,
            &kafka_listeners,
            kafka_security,
            &resolved_product_image.product_version,
        )])
        .add_env_var(
            "EXTRA_ARGS",
            kafka_role
                .construct_non_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            KAFKA_HEAP_OPTS,
            kafka_role
                .construct_heap_jvm_args(merged_config, kafka, &rolegroup_ref.role_group)
                .context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var(
            kafka_log_opts_env_var(&resolved_product_image.product_version),
            kafka_log_opts(&resolved_product_image.product_version),
        )
        // Needed for the `containerdebug` process to log it's tracing information to.
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        .add_env_var(
            KAFKA_NODE_ID_OFFSET,
            node_id_hash32_offset(rolegroup_ref).to_string(),
        )
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
            timeout_seconds: Some(5),
            period_seconds: Some(5),
            ..Probe::default()
        })
        .readiness_probe(Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(kafka_security.client_port().into()),
                ..Default::default()
            }),
            timeout_seconds: Some(5),
            period_seconds: Some(5),
            ..Probe::default()
        });

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
                    .with_config_map(rolegroup_ref.object_name())
                    .build(),
            )
            .context(AddVolumeSnafu)?;
    }

    let metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(recommended_object_labels)
        .context(MetadataBuildSnafu)?
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

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_container(cb_kafka.build())
        .affinity(&merged_config.affinity)
        .add_volume(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        // bootstrap volume is a persistent volume template instead, to keep addresses persistent
        .add_empty_dir_volume(
            "log",
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_KAFKA_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .service_account_name(service_account.name_any())
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

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

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();
    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);

    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    // TODO: we need that?
    pod_template_spec.share_process_namespace = Some(true);

    pod_template.merge_from(
        kafka_role
            .role_pod_overrides(kafka)
            .context(MergePodOverridesSnafu)?,
    );
    pod_template.merge_from(
        kafka_role
            .role_group_pod_overrides(kafka, &rolegroup_ref.role_group)
            .context(MergePodOverridesSnafu)?,
    );

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                kafka,
                KAFKA_CONTROLLER_NAME,
                &resolved_product_image.app_version_label_value,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..StatefulSetUpdateStrategy::default()
            }),
            replicas: kafka_role
                .replicas(kafka, &rolegroup_ref.role_group)
                .context(RoleGroupReplicasSnafu)?
                .map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    Labels::role_group_selector(
                        kafka,
                        APP_NAME,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .context(LabelBuildSnafu)?
                    .into(),
                ),
                ..LabelSelector::default()
            },
            service_name: Some(rolegroup_ref.object_name()),
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

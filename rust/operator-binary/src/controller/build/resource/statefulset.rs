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
                ConfigMapVolumeSource, ContainerPort, EnvVar, EnvVarSource, ExecAction,
                LifecycleHandler, ObjectFieldSelector, PodSpec, Probe, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
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

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster, ValidatedRoleGroupConfig,
        build::{
            command::{
                broker_kafka_container_commands, controller_kafka_container_command,
                kafka_log_opts, kafka_log_opts_env_var, quorum_manager_container_command,
                quorum_manager_pre_stop_command,
            },
            graceful_shutdown::add_graceful_shutdown_config,
            kerberos::add_kerberos_pod_config,
            properties::{kraft_controllers, product_logging::MAX_KAFKA_LOG_FILES_SIZE},
            security::{
                STACKABLE_TLS_KAFKA_INTERNAL_DIR, STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
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

/// Name of both the env var and the ZooKeeper discovery ConfigMap key holding the
/// ZooKeeper connection string.
const ZOOKEEPER_ENV_VAR_NAME: &str = "ZOOKEEPER";

/// Parses a compile-time-known env var name; panics only on a programming error (a malformed
/// literal in this file).
fn env_var_name(name: &str) -> EnvVarName {
    EnvVarName::from_str(name).expect("a static env var name is valid")
}

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
        .with_field_path(&env_var_name("POD_NAME"), &FieldPathEnvVar::Name)
        .with_value(
            &env_var_name("KAFKA_CLIENT_PORT"),
            kafka_security.client_port().to_string(),
        );

    // Present in ZooKeeper mode only: brokers use it to connect, controllers for migration.
    if let Some(zookeeper_config_map_name) =
        &validated_cluster.cluster_config.zookeeper_config_map_name
    {
        env = env.with_config_map_key_ref(
            &env_var_name(ZOOKEEPER_ENV_VAR_NAME),
            zookeeper_config_map_name,
            &ConfigMapKey::from_str(ZOOKEEPER_ENV_VAR_NAME)
                .expect("a static config map key is valid"),
        );
    }

    env
}

/// Environment variables the operator sets that are common to *every* container in a
/// **controller** pod: today that's the `kafka` server process and, when present, the
/// `quorum-manager` sidecar.
///
/// The sidecar renders the very same `controller.properties` template (see
/// `properties/controller_properties.rs`) that the `kafka` container's own entrypoint does, to
/// build its own `add-controller`/`remove-controller` config — so it needs every
/// `${env:...}` placeholder that template references (`POD_NAME`, `KAFKA_CLIENT_PORT`,
/// `NAMESPACE`, `ROLEGROUP_HEADLESS_SERVICE_NAME`, `CLUSTER_DOMAIN`). Building this set once
/// and handing it to both containers means they can't silently drift apart over time (a real
/// bug found in review: the sidecar was originally given only `POD_NAME`/`NODE_ID_OFFSET`,
/// so its `controller.properties` render most likely produced a broken `listeners` value).
///
/// The caller merges the user's `envOverrides` on top (so a user override wins on a name
/// collision) and, for the `kafka` container only, adds container-specific env vars such as
/// `PRE_STOP_CONTROLLER_SLEEP_SECONDS`.
fn controller_pod_shared_env_vars(
    validated_cluster: &ValidatedCluster,
    kafka_security: &ValidatedKafkaSecurity,
    resource_names: &ResourceNames,
) -> EnvVarSet {
    common_operator_env_vars(validated_cluster, kafka_security)
        .with_field_path(&env_var_name("NAMESPACE"), &FieldPathEnvVar::Namespace)
        .with_value(
            &env_var_name("ROLEGROUP_HEADLESS_SERVICE_NAME"),
            resource_names.headless_service_name().to_string(),
        )
        .with_value(
            &env_var_name("CLUSTER_DOMAIN"),
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
    add_broker_volume_and_volume_mounts(
        kafka_security,
        &mut pod_builder,
        &mut cb_kcat_prober,
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
        add_kerberos_pod_config(
            kafka_security,
            kafka_role,
            &mut cb_kcat_prober,
            &mut cb_kafka,
            &mut pod_builder,
        )
        .context(AddKerberosConfigSnafu)?;
    }

    // Operator-set env vars first; the user's `envOverrides` are merged on top and win.
    let env: Vec<EnvVar> = common_operator_env_vars(validated_cluster, kafka_security)
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
        )]);

    let node_id_offset = node_id_hash32_offset(kafka_role, role_group_name.as_ref()).to_string();

    add_common_kafka_env(
        &mut cb_kafka,
        merged_config,
        &validated_rg
            .product_specific_common_config
            .jvm_argument_overrides,
        resolved_product_image,
        &node_id_offset,
    )?;

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
        .add_container(cb_kcat_prober.build())
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
    kafka_role: &KafkaRole,
    role_group_name: &RoleGroupName,
    validated_cluster: &ValidatedCluster,
    validated_rg: &ValidatedRoleGroupConfig,
) -> Result<StatefulSet, Error> {
    let kafka_security = &validated_cluster.cluster_config.kafka_security;
    let resolved_product_image = &validated_cluster.image;
    let merged_config = &validated_rg.config.config;
    let resource_names = validated_cluster.role_group_resource_names(kafka_role, role_group_name);
    let recommended_labels = validated_cluster.recommended_labels(kafka_role, role_group_name);

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
        .with_value(&env_var_name("PRE_STOP_CONTROLLER_SLEEP_SECONDS"), "10")
        .merge(validated_rg.env_overrides.clone())
        .into();

    let quorum_manager_env: Vec<EnvVar> = controller_shared_env
        .with_value(&env_var_name(KAFKA_NODE_ID_OFFSET), &node_id_offset)
        .merge(validated_rg.env_overrides.clone())
        .into();

    let controller_pod_descriptors = validated_cluster
        .pod_descriptors(Some(kafka_role))
        .context(BuildPodDescriptorsSnafu)?;
    // Comma-joined `host:port` list of all KRaft controller voters, consumed by the
    // `quorum-manager` sidecar container so it can talk to the quorum via
    // `kafka-metadata-quorum.sh`.
    let quorum_bootstrap_servers = kraft_controllers(&controller_pod_descriptors).join(",");

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

    add_common_kafka_env(
        &mut cb_kafka,
        merged_config,
        &validated_rg
            .product_specific_common_config
            .jvm_argument_overrides,
        resolved_product_image,
        &node_id_offset,
    )?;

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

    if let Some(quorum_manager_container) = build_quorum_manager_container(
        resolved_product_image,
        kafka_security,
        &quorum_bootstrap_servers,
        quorum_manager_env,
    )? {
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
///
/// `node_id_offset` is the pre-computed value of [`node_id_hash32_offset`] for this role
/// group, shared with the controller's `quorum-manager` sidecar (see
/// [`build_quorum_manager_container`]), which also needs `NODE_ID_OFFSET` in its `preStop`
/// script.
fn add_common_kafka_env(
    cb_kafka: &mut ContainerBuilder,
    merged_config: &AnyConfig,
    jvm_argument_overrides: &JvmArgumentOverrides,
    resolved_product_image: &ResolvedProductImage,
    node_id_offset: &str,
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
        .add_env_var(KAFKA_NODE_ID_OFFSET, node_id_offset);
    Ok(())
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
    quorum_bootstrap_servers: &str,
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
            quorum_manager_container_command(quorum_bootstrap_servers),
        ])
        .add_env_vars(env)
        // `kafka-metadata-quorum.sh` goes through `kafka-run-class.sh`, which defaults
        // `KAFKA_HEAP_OPTS` to `-Xmx256M` when unset. Set an explicit, modest heap so the
        // JVM's max heap plus its base/metaspace/SSL-buffer overhead stays comfortably
        // under the container's memory limit below.
        .add_env_var(KAFKA_HEAP_OPTS, "-Xmx128M")
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                // A JVM cold start plus an SSL handshake and an admin-client round-trip all
                // need to happen inside this sidecar's existing `timeout 15`/`25s preStop`
                // budgets (see `CLI_CALL_TIMEOUT_SECONDS` in `command.rs`).
                .with_cpu_limit("500m")
                .with_memory_request("256Mi")
                .with_memory_limit("512Mi")
                .build(),
        )
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        // `controller_admin_client_properties` (see `build/security.rs`) always points
        // its keystore/truststore at this directory, so the sidecar's admin-client calls
        // need it mounted here too, not just on the `kafka` container.
        .add_volume_mount(
            STACKABLE_TLS_KAFKA_INTERNAL_VOLUME_NAME,
            STACKABLE_TLS_KAFKA_INTERNAL_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .lifecycle_pre_stop(LifecycleHandler {
            exec: Some(ExecAction {
                command: Some(vec![
                    "/bin/bash".to_string(),
                    "-c".to_string(),
                    quorum_manager_pre_stop_command(quorum_bootstrap_servers),
                ]),
            }),
            ..LifecycleHandler::default()
        });

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
    use super::*;
    use crate::controller::test_support::{minimal_kafka, validated_cluster};

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

        let pre_stop_command = sidecar
            .lifecycle
            .as_ref()
            .and_then(|l| l.pre_stop.as_ref())
            .and_then(|h| h.exec.as_ref())
            .and_then(|e| e.command.as_ref())
            .expect("the sidecar has a preStop exec hook")
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
        // here would break the sidecar's main loop and its `preStop` hook silently (an unset
        // variable under `set -u` aborts the script).
        assert!(
            sidecar_env_names.contains(&KAFKA_NODE_ID_OFFSET),
            "quorum-manager sidecar is missing the {KAFKA_NODE_ID_OFFSET} env var, needed by \
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

        let result = build_quorum_manager_container(
            &cluster.image,
            &kerberos_security,
            "controller-0:9093",
            Vec::new(),
        )
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
}

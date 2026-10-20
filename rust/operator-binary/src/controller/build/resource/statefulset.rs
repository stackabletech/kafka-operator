use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{PodBuilder, security::PodSecurityContextBuilder},
    },
    commons::product_image_selection::ResolvedProductImage,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{ConfigMapVolumeSource, Volume},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    product_logging,
    v2::{
        builder::{meta::ownerreference_from_resource, pod::container::EnvVarSet},
        product_logging::framework::vector_container,
        role_group_utils::ResourceNames,
        types::kubernetes::{ContainerName, VolumeName},
    },
};

use crate::{
    controller::{
        build::{
            graceful_shutdown::add_graceful_shutdown_config,
            properties::product_logging::MAX_KAFKA_LOG_FILES_SIZE,
            recommended_labels_for_role_group_resources, role_group::RoleGroupBuilder,
            role_group_selector,
        },
        validate::ValidatedLogging,
    },
    crd::{STACKABLE_CONFIG_DIR_NAME, STACKABLE_LOG_DIR_NAME},
};

// The Vector container reads its `vector.yaml` from the `config` volume (the rolegroup
// ConfigMap) and tails product logs from the `log` volume.
stackable_operator::constant!(VECTOR_CONFIG_VOLUME_NAME: VolumeName = "config");
stackable_operator::constant!(VECTOR_LOG_VOLUME_NAME: VolumeName = "log");

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::controller::build::graceful_shutdown::Error,
    },
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// Starts from the containers and volumes the role's module built
/// ([`RoleGroupBuilder::role_containers`], [`RoleGroupBuilder::role_pod_volumes`]) and adds what
/// every role's pod has: the pod metadata, image pull secrets and affinity, the `config` and
/// `log` volumes, the service account and security context, the Vector sidecar and the graceful
/// shutdown period. Pod overrides are merged last.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding
/// [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) from [`build_rolegroup_headless_service`](`crate::controller::build::resource::service::build_rolegroup_headless_service`).
pub fn build_statefulset(builder: &RoleGroupBuilder) -> Result<StatefulSet, Error> {
    let validated_cluster = builder.cluster;
    let kafka_role = &builder.role;
    let role_group_name = &builder.role_group_name;
    let resolved_product_image = &validated_cluster.image;
    let resource_names = validated_cluster.role_group_resource_names(kafka_role, role_group_name);
    let recommended_labels =
        recommended_labels_for_role_group_resources(validated_cluster, kafka_role, role_group_name);

    let mut pod_builder = PodBuilder::new();
    pod_builder
        .add_volumes(builder.role_pod_volumes.clone())
        .context(AddVolumeSnafu)?;
    for container in &builder.role_containers {
        pod_builder.add_container(container.clone());
    }

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_labels.clone())
        .build();

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&builder.common.affinity);

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
        builder.vector_container_name,
        &builder.logging,
        resolved_product_image,
        &resource_names,
    );

    add_graceful_shutdown_config(builder.common.graceful_shutdown_timeout, &mut pod_builder)
        .context(GracefulShutdownSnafu)?;

    let mut pod_template = pod_builder.build_template();

    // Pod overrides were already merged (role <- role group) during validation.
    pod_template.merge_from(builder.pod_overrides.clone());

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
            pod_management_policy: Some(builder.pod_management_policy.to_string()),
            replicas: builder.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(
                    role_group_selector(validated_cluster, kafka_role, role_group_name).into(),
                ),
                ..LabelSelector::default()
            },
            service_name: Some(resource_names.headless_service_name().to_string()),
            template: pod_template,
            volume_claim_templates: Some(builder.volume_claim_templates.clone()),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
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
            &*STACKABLE_LOG_DIR_NAME,
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
    use crate::{
        controller::{
            build::role_group::controller::QUORUM_MANAGER_CONTAINER_NAME,
            test_support::{minimal_kafka, validated_cluster},
        },
        crd::role::KAFKA_NODE_ID_OFFSET,
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *VECTOR_CONFIG_VOLUME_NAME;
        let _ = *VECTOR_LOG_VOLUME_NAME;
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

    #[test]
    fn statefulsets_use_ordered_ready_pod_management_for_controllers_only() {
        let cluster = kraft_mode_cluster();
        let resources = crate::controller::build::build(&cluster).expect("build succeeds");

        for (name, expected_policy) in [
            ("simple-kafka-controller-default", "OrderedReady"),
            ("simple-kafka-broker-default", "Parallel"),
        ] {
            let sts = resources
                .stateful_sets
                .iter()
                .find(|sts| sts.metadata.name.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("the {name} StatefulSet is built"));
            assert_eq!(
                sts.spec
                    .as_ref()
                    .expect("the StatefulSet has a spec")
                    .pod_management_policy
                    .as_deref(),
                Some(expected_policy)
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
    fn the_sidecar_joins_the_quorum_and_the_kafka_container_leaves_it_on_stop() {
        let cluster = kraft_mode_cluster();
        let containers = controller_containers(&cluster);
        let sidecar = containers
            .iter()
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME.to_string())
            .expect("the quorum-manager sidecar is built");

        let command = sidecar
            .command
            .as_ref()
            .expect("the sidecar has a command")
            .join(" ");
        assert!(command.contains("add-controller"));
        assert!(sidecar.lifecycle.is_none());

        let pre_stop_command = controller_kafka_container(&cluster)
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
    /// The Java properties writer serializing `controller.properties`
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
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME.to_string())
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
        // `EXPORT_REPLICA_ID` bash logic under `set -u` (see `command.rs`), so losing it
        // would break the sidecar's `add-controller` main loop silently (an unset
        // variable under `set -u` aborts the script).
        let node_id_offset_name = KAFKA_NODE_ID_OFFSET.to_string();
        assert!(
            sidecar_env_names.contains(&node_id_offset_name.as_str()),
            "quorum-manager sidecar is missing the {node_id_offset_name} env var, needed by \
             its EXPORT_REPLICA_ID derivation under `set -u`; sidecar env vars: \
             {sidecar_env_names:?}"
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

    /// `spec.controllers.config.resources` and `spec.brokers.config.resources` must be applied
    /// verbatim to each role's `kafka` container: CPU request/limit come from `cpu.min`/`cpu.max`
    /// and memory request equals memory limit, both taken from `memory.limit` (there is no
    /// separate memory request field in the CRD).
    #[test]
    fn kafka_container_resources_match_configured_role_resources() {
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
                config:
                  resources:
                    memory:
                      limit: 2Gi
                    cpu:
                      min: 260m
                      max: 1024m
                    storage:
                      logDirs:
                        capacity: 2Gi
                roleGroups:
                  default:
                    replicas: 3
              brokers:
                config:
                  resources:
                    memory:
                      limit: 2Gi
                    cpu:
                      min: 300m
                      max: 500m
                    storage:
                      logDirs:
                        capacity: 2Gi
                roleGroups:
                  default:
                    replicas: 3
            "#,
        );
        let cluster = validated_cluster(&kafka);

        let controller_resources = controller_kafka_container(&cluster)
            .resources
            .expect("the controller kafka container has resources");
        assert_cpu_and_memory(&controller_resources, "260m", "1024m", "2Gi", "2Gi");

        let broker_resources = broker_kafka_container(&cluster)
            .resources
            .expect("the broker kafka container has resources");
        assert_cpu_and_memory(&broker_resources, "300m", "500m", "2Gi", "2Gi");
    }

    /// Asserts the container's CPU request/limit and memory request/limit against the given
    /// quantity strings (memory request and limit are always equal, see the CRD's
    /// `Resources`/`MemoryLimits` type).
    fn assert_cpu_and_memory(
        resources: &stackable_operator::k8s_openapi::api::core::v1::ResourceRequirements,
        expected_cpu_request: &str,
        expected_cpu_limit: &str,
        expected_memory_request: &str,
        expected_memory_limit: &str,
    ) {
        use stackable_operator::k8s_openapi::apimachinery::pkg::api::resource::Quantity;

        let requests = resources
            .requests
            .as_ref()
            .expect("the container has resource requests");
        let limits = resources
            .limits
            .as_ref()
            .expect("the container has resource limits");

        assert_eq!(
            requests.get("cpu"),
            Some(&Quantity(expected_cpu_request.to_string())),
            "cpu request"
        );
        assert_eq!(
            limits.get("cpu"),
            Some(&Quantity(expected_cpu_limit.to_string())),
            "cpu limit"
        );
        assert_eq!(
            requests.get("memory"),
            Some(&Quantity(expected_memory_request.to_string())),
            "memory request"
        );
        assert_eq!(
            limits.get("memory"),
            Some(&Quantity(expected_memory_limit.to_string())),
            "memory limit"
        );
    }

    /// Unlike the `kafka` container, the `quorum-manager` sidecar's resources are hardcoded in
    /// the controller's `build_quorum_manager_container` and not exposed through
    /// `config.resources` (which only applies to the `kafka` container, see
    /// [`kafka_container_resources_match_configured_role_resources`]). `podOverrides` is the only
    /// way to change them, applied via a strategic merge on `spec.containers` keyed by container
    /// `name` (see the `merge_from` call at the end of [`build_statefulset`]).
    #[test]
    fn quorum_manager_resources_can_be_changed_by_pod_overrides() {
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
                podOverrides:
                  spec:
                    containers:
                      - name: quorum-manager
                        resources:
                          requests:
                            cpu: 200m
                            memory: 256Mi
                          limits:
                            cpu: 750m
                            memory: 768Mi
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

        let containers = controller_containers(&cluster);
        let sidecar = containers
            .iter()
            .find(|c| c.name == QUORUM_MANAGER_CONTAINER_NAME.to_string())
            .expect("the quorum-manager sidecar is built");
        let resources = sidecar
            .resources
            .as_ref()
            .expect("the override gives the sidecar resources");

        // Request and limit are asserted independently (unlike
        // `kafka_container_resources_match_configured_role_resources`'s memory check) to show
        // that `podOverrides` sets the raw Kubernetes fields directly, without the
        // request-equals-limit constraint the CRD's `config.resources` memory field imposes.
        assert_cpu_and_memory(resources, "200m", "750m", "256Mi", "768Mi");
    }

    /// The startup and liveness probes share the same check - TCP reachability (a genuinely
    /// dead/hung process must still be restarted) plus the broker's JMX `BrokerState` metric
    /// reporting `RUNNING` (state `3`); see `probes::broker_running_probe`'s doc comment.
    #[test]
    fn broker_kafka_container_probes_check_tcp_and_running_state() {
        let cluster = kraft_mode_cluster();
        let container = broker_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let startup_probe = container
            .startup_probe
            .clone()
            .expect("the broker kafka container must have a startupProbe");
        assert_eq!(startup_probe.timeout_seconds, Some(5));
        assert_eq!(startup_probe.period_seconds, Some(5));
        assert_eq!(startup_probe.failure_threshold, Some(60));

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

    /// The startup probe is a plain TCP check, while the liveness probe additionally inspects
    /// the local Raft state and fails specifically on `unattached`
    #[test]
    fn controller_kafka_container_probes_check_tcp_and_raft_state() {
        let cluster = kraft_mode_cluster();
        let container = controller_kafka_container(&cluster);
        let client_port = cluster.cluster_config.kafka_security.client_port();

        let startup_probe = container
            .startup_probe
            .clone()
            .expect("the controller kafka container must have a startupProbe");
        let tcp_socket = startup_probe
            .tcp_socket
            .expect("the startupProbe must be a tcpSocket check");
        assert_eq!(
            tcp_socket.port,
            IntOrString::Int(client_port.clone().into())
        );
        assert_eq!(startup_probe.timeout_seconds, Some(5));
        assert_eq!(startup_probe.period_seconds, Some(5));
        assert_eq!(startup_probe.failure_threshold, Some(60));

        let liveness_probe = container
            .liveness_probe
            .clone()
            .expect("the controller kafka container must have a livenessProbe");
        let exec = liveness_probe
            .exec
            .expect("the livenessProbe must be an exec check, not a bare tcpSocket check");
        let command = exec.command.expect("exec has a command");
        let script = command.last().expect("the exec command has a script arg");

        assert!(
            script.contains(&format!("/dev/tcp/$POD_IP/{client_port}")),
            "expected the TCP check to dial this pod's own IP (from the downward API), not a \
             DNS name that could be affected by CoreDNS being unavailable, script was: {script}"
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

        let readiness_probe = container
            .readiness_probe
            .expect("the controller kafka container must have a readinessProbe");
        let exec = readiness_probe
            .exec
            .expect("the readinessProbe must be an exec check");
        assert_eq!(
            exec.command.expect("exec has a command"),
            vec![
                "bash".to_string(),
                "-c".to_string(),
                "curl -s localhost:9606/metrics | grep -E 'kafka_server_raft_metrics_current_state\\{state=\"(leader|follower|voted)\",?\\}'".to_string(),
            ]
        );
    }
}

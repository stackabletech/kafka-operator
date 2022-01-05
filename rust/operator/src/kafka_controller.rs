//! Ensures that `Pod`s are configured and running for each [`KafkaCluster`]

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{
    KafkaCluster, KafkaRole, APP_NAME, APP_PORT, METRICS_PORT, SERVER_PROPERTIES_FILE,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, EmptyDirVolumeSource,
                EnvVar, EnvVarSource, ExecAction, ObjectFieldSelector, PersistentVolumeClaim,
                PersistentVolumeClaimSpec, Probe, ResourceRequirements, Service, ServicePort,
                ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        api::ObjectMeta,
        runtime::controller::{Context, ReconcilerAction},
    },
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};

use crate::{
    discovery::{self, build_discovery_configmaps},
    pod_svc_controller,
};

const FIELD_MANAGER_SCOPE: &str = "kafkacluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no broker role"))]
    NoBrokerRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig {
        source: discovery::Error,
    },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    RoleGroupNotFound,
}
type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_kafka(kafka: KafkaCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;

    let validated_config = validate_all_roles_and_groups_config(
        &kafka.spec.version,
        &transform_all_roles_to_config(
            &kafka,
            [(
                KafkaRole::Broker.to_string(),
                (
                    vec![
                        PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()),
                        PropertyNameKind::Env,
                    ],
                    kafka.spec.brokers.clone().context(NoBrokerRole)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfig)?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfig)?;
    let role_broker_config = validated_config
        .get(&KafkaRole::Broker.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let broker_role_service = build_broker_role_service(&kafka)?;
    let broker_role_service = client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &broker_role_service,
            &broker_role_service,
        )
        .await
        .context(ApplyRoleService)?;
    for (rolegroup_name, rolegroup_config) in role_broker_config.iter() {
        let rolegroup = kafka.broker_rolegroup_ref(rolegroup_name);

        let rg_service = build_broker_rolegroup_service(&rolegroup, &kafka)?;
        let rg_configmap = build_broker_rolegroup_config_map(&rolegroup, &kafka, rolegroup_config)?;
        let rg_statefulset =
            build_broker_rolegroup_statefulset(&rolegroup, &kafka, rolegroup_config)?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
            .await
            .with_context(|| ApplyRoleGroupService {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
            .await
            .with_context(|| ApplyRoleGroupConfig {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
            .await
            .with_context(|| ApplyRoleGroupStatefulSet {
                rolegroup: rolegroup.clone(),
            })?;
    }

    for discovery_cm in build_discovery_configmaps(client, &kafka, &kafka, &broker_role_service)
        .await
        .context(BuildDiscoveryConfig)?
    {
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfig)?;
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The broker-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_broker_role_service(kafka: &KafkaCluster) -> Result<Service> {
    let role_name = KafkaRole::Broker.to_string();
    let role_svc_name = kafka
        .broker_role_service_name()
        .context(GlobalServiceNameNotFound)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&role_svc_name)
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(kafka, APP_NAME, kafka_version(kafka)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("kafka".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(kafka, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_broker_rolegroup_config_map(
    rolegroup: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap> {
    let server_cfg = broker_config
        .get(&PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()))
        .cloned()
        .unwrap_or_default();
    let server_cfg = server_cfg
        .into_iter()
        .map(|(k, v)| (k, Some(v)))
        .collect::<Vec<_>>();
    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(kafka)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(kafka, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRef)?
                .with_recommended_labels(
                    kafka,
                    APP_NAME,
                    kafka_version(kafka)?,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(
            "server.properties",
            to_java_properties_string(server_cfg.iter().map(|(k, v)| (k, v))).with_context(
                || SerializeZooCfg {
                    rolegroup: rolegroup.clone(),
                },
            )?,
        )
        .build()
        .with_context(|| BuildRoleGroupConfig {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_broker_rolegroup_service(
    rolegroup: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                kafka_version(kafka)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![
                ServicePort {
                    name: Some("kafka".to_string()),
                    port: APP_PORT.into(),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: 9505,
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(role_group_selector_labels(
                kafka,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_broker_rolegroup_statefulset(
    rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let role = kafka.spec.brokers.as_ref().context(NoBrokerRole)?;
    let rolegroup = role
        .role_groups
        .get(&rolegroup_ref.role_group)
        .context(RoleGroupNotFound)?;
    let kafka_version = kafka_version(kafka)?;
    let image = format!(
        "docker.stackable.tech/stackable/kafka:{}-stackable0",
        kafka.spec.version
    );
    let container_get_svc = ContainerBuilder::new("get-svc")
        .image("bitnami/kubectl:1.21.1")
        .command(vec!["bash".to_string()])
        .args(vec![
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
            [
                "kubectl get service \"$POD_NAME\" -o jsonpath='{.spec.ports[0].nodePort}'",
                "tee /stackable/tmp/nodeport",
            ]
            .join(" | "),
        ])
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
        .add_volume_mount("tmp", "/stackable/tmp")
        .build();
    let mut env = broker_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();
    env.push(EnvVar {
        name: "ZOOKEEPER".to_string(),
        value_from: Some(EnvVarSource {
            config_map_key_ref: Some(ConfigMapKeySelector {
                name: Some(kafka.spec.zookeeper_config_map_name.clone()),
                key: "ZOOKEEPER".to_string(),
                ..ConfigMapKeySelector::default()
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });
    let opa_url_env_var = if let Some(opa) = &kafka.spec.opa {
        let env_var = "OPA";
        env.push(EnvVar {
            name: env_var.to_string(),
            value_from: Some(EnvVarSource {
                config_map_key_ref: Some(ConfigMapKeySelector {
                    name: Some(opa.config_map_name.clone()),
                    key: "OPA".to_string(),
                    ..ConfigMapKeySelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        });
        Some(env_var)
    } else {
        None
    };
    env.push(EnvVar {
        name: "NODE".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                api_version: Some("v1".to_string()),
                field_path: "status.hostIP".to_string(),
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    });
    let jvm_args = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/broker.yaml", METRICS_PORT);
    let zookeeper_override = "--override \"zookeeper.connect=$ZOOKEEPER\"";
    let advertised_listeners_override =
        "--override \"advertised.listeners=PLAINTEXT://$NODE:$(cat /stackable/tmp/nodeport)\"";
    let opa_url_override = &opa_url_env_var.map_or(String::new(), |opa| {
        format!("--override \"opa.authorizer.url=${}\"", opa)
    });
    let container_kafka = ContainerBuilder::new("kafka")
        .image(image)
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            [
                "bin/kafka-server-start.sh",
                &format!("/stackable/config/{}", SERVER_PROPERTIES_FILE),
                zookeeper_override,
                advertised_listeners_override,
                opa_url_override,
            ]
            .join(" "),
        ])
        .add_env_vars(env)
        .add_env_var("EXTRA_ARGS", jvm_args)
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                // If the broker is able to get its cluster ID then it has at least completed basic registration at some point
                command: Some(vec![
                    "bin/kafka-cluster.sh".to_string(),
                    "cluster-id".to_string(),
                    "--bootstrap-server".to_string(),
                    format!("localhost:{}", APP_PORT),
                ]),
            }),
            timeout_seconds: Some(3),
            period_seconds: Some(1),
            ..Probe::default()
        })
        .add_container_port("kafka", APP_PORT.into())
        .add_container_port("metrics", METRICS_PORT.into())
        .add_volume_mount("data", "/stackable/data")
        .add_volume_mount("config", "/stackable/config")
        .add_volume_mount("tmp", "/stackable/tmp")
        .build();
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                kafka_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if kafka.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.replicas.map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    kafka,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        kafka,
                        APP_NAME,
                        kafka_version,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                    .with_label(pod_svc_controller::LABEL_ENABLE, "true")
                })
                .add_init_container(container_get_svc)
                .add_container(container_kafka)
                .add_volume(Volume {
                    name: "config".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                })
                .add_volume(Volume {
                    name: "tmp".to_string(),
                    empty_dir: Some(EmptyDirVolumeSource::default()),
                    ..Volume::default()
                })
                .build_template(),
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..ObjectMeta::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                            map
                        }),
                        ..ResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn kafka_version(kafka: &KafkaCluster) -> Result<&str> {
    Ok(&kafka.spec.version)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

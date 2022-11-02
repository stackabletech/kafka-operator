//! Ensures that `Pod`s are configured and running for each [`KafkaCluster`]

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::{
    listener::get_kafka_listener_config, KafkaCluster, KafkaConfig, KafkaRole, TlsSecretClass,
    APP_NAME, CLIENT_PORT, CLIENT_PORT_NAME, DOCKER_IMAGE_BASE_NAME, KAFKA_HEAP_OPTS,
    LOG_DIRS_VOLUME_NAME, METRICS_PORT, METRICS_PORT_NAME, SECURE_CLIENT_PORT,
    SECURE_CLIENT_PORT_NAME, SERVER_PROPERTIES_FILE, STACKABLE_CONFIG_DIR, STACKABLE_DATA_DIR,
    STACKABLE_TLS_CLIENT_AUTH_DIR, STACKABLE_TLS_CLIENT_DIR, STACKABLE_TLS_INTERNAL_DIR,
    STACKABLE_TMP_DIR, TLS_DEFAULT_SECRET_CLASS,
};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        SecretOperatorVolumeSourceBuilder, SecurityContextBuilder, VolumeBuilder,
    },
    cluster_resources::ClusterResources,
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        opa::OpaApiVersion,
        product_image_selection::ResolvedProductImage,
        tls::TlsAuthenticationProvider,
    },
    config::fragment::ValidationError,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort,
                EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction, ObjectFieldSelector,
                PodSpec, Probe, ResourceRequirements, Service, ServiceAccount, ServicePort,
                ServiceSpec, Volume,
            },
            rbac::v1::{RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::{
        api::DynamicObject,
        runtime::{controller::Action, reflector::ObjectRef},
        Resource,
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        types::PropertyNameKind, writer::to_java_properties_string, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::command::{get_svc_container_cmd_args, kcat_container_cmd_args};
use crate::{
    command,
    discovery::{self, build_discovery_configmaps},
    pod_svc_controller,
    utils::{self, ObjectRefExt},
    ControllerConfig,
};

const RESOURCE_SCOPE: &str = "kafka-operator_kafkacluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub controller_config: ControllerConfig,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no name"))]
    ObjectHasNoName,
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no broker role"))]
    NoBrokerRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to apply role Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyRoleServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleRoleBinding {
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
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to find rolegroup {}", rolegroup))]
    RoleGroupNotFound {
        rolegroup: RoleGroupRef<KafkaCluster>,
    },
    #[snafu(display("failed to validate configuration of rolegroup {rolegroup}"))]
    RoleGroupValidation {
        rolegroup: RoleGroupRef<KafkaCluster>,
        source: ValidationError,
    },
    #[snafu(display("invalid ServiceAccount"))]
    InvalidServiceAccount {
        source: utils::NamespaceMismatchError,
    },
    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("invalid java heap config: {source}"))]
    InvalidJavaHeapConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve {}", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
        "failed to use authentication mechanism {} - supported methods: {:?}",
        method,
        supported
    ))]
    AuthenticationMethodNotSupported {
        authentication_class: ObjectRef<AuthenticationClass>,
        supported: Vec<String>,
        method: String,
    },
    #[snafu(display("invalid kafka listeners"))]
    InvalidKafkaListeners {
        source: stackable_kafka_crd::listener::KafkaListenerError,
    },
    #[snafu(display("invalid container name [{name}]"))]
    InvalidContainerName {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            Error::ObjectHasNoNamespace => None,
            Error::ObjectHasNoVersion => None,
            Error::NoBrokerRole => None,
            Error::GlobalServiceNameNotFound => None,
            Error::ApplyRoleService { .. } => None,
            Error::ApplyRoleServiceAccount { .. } => None,
            Error::ApplyRoleRoleBinding { .. } => None,
            Error::ApplyRoleGroupService { .. } => None,
            Error::BuildRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupConfig { .. } => None,
            Error::ApplyRoleGroupStatefulSet { .. } => None,
            Error::GenerateProductConfig { .. } => None,
            Error::InvalidProductConfig { .. } => None,
            Error::SerializeZooCfg { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::BuildDiscoveryConfig { .. } => None,
            Error::ApplyDiscoveryConfig { .. } => None,
            Error::RoleGroupNotFound { .. } => None,
            Error::RoleGroupValidation { .. } => None,
            Error::InvalidServiceAccount { .. } => None,
            Error::InvalidOpaConfig { .. } => None,
            Error::InvalidJavaHeapConfig { .. } => None,
            Error::AuthenticationClassRetrieval {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::AuthenticationMethodNotSupported {
                authentication_class,
                ..
            } => Some(authentication_class.clone().erase()),
            Error::InvalidKafkaListeners { .. } => None,
            Error::InvalidContainerName { .. } => None,
            Error::DeleteOrphans { .. } => None,
        }
    }
}

pub async fn reconcile_kafka(kafka: Arc<KafkaCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let client = &ctx.client;

    let resolved_product_image = kafka.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let mut cluster_resources =
        ClusterResources::new(APP_NAME, RESOURCE_SCOPE, &kafka.object_ref(&())).unwrap();

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            &*kafka,
            [(
                KafkaRole::Broker.to_string(),
                (
                    vec![
                        PropertyNameKind::File(SERVER_PROPERTIES_FILE.to_string()),
                        PropertyNameKind::Env,
                    ],
                    kafka.spec.brokers.clone().context(NoBrokerRoleSnafu)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;
    let role_broker_config = validated_config
        .get(&KafkaRole::Broker.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let client_authentication_class = if let Some(auth_class) = kafka.client_authentication_class()
    {
        Some(
            AuthenticationClass::resolve(client, auth_class)
                .await
                .context(AuthenticationClassRetrievalSnafu {
                    authentication_class: ObjectRef::<AuthenticationClass>::new(auth_class),
                })?,
        )
    } else {
        None
    };

    // Assemble the OPA connection string from the discovery and the given path if provided
    // Will be passed as --override parameter in the cli in the state ful set
    let opa_connect = if let Some(opa_spec) = &kafka.spec.opa {
        Some(
            opa_spec
                .full_document_url_from_config_map(
                    client,
                    &*kafka,
                    Some("allow"),
                    OpaApiVersion::V1,
                )
                .await
                .context(InvalidOpaConfigSnafu)?,
        )
    } else {
        None
    };

    let broker_role_service = build_broker_role_service(&kafka, &resolved_product_image)?;
    let (broker_role_serviceaccount, broker_role_rolebinding) =
        build_broker_role_serviceaccount(&kafka, &ctx.controller_config, &resolved_product_image)?;
    let broker_role_serviceaccount_ref = ObjectRef::from_obj(&broker_role_serviceaccount);
    let broker_role_service = cluster_resources
        .add(client, &broker_role_service)
        .await
        .context(ApplyRoleServiceSnafu)?;
    cluster_resources
        .add(client, &broker_role_serviceaccount)
        .await
        .context(ApplyRoleServiceAccountSnafu)?;
    cluster_resources
        .add(client, &broker_role_rolebinding)
        .await
        .context(ApplyRoleRoleBindingSnafu)?;

    for (rolegroup_name, rolegroup_config) in role_broker_config.iter() {
        let rolegroup_ref = kafka.broker_rolegroup_ref(rolegroup_name);
        let (role, rolegroup) =
            kafka
                .rolegroup(&rolegroup_ref)
                .with_context(|| RoleGroupNotFoundSnafu {
                    rolegroup: rolegroup_ref.clone(),
                })?;
        let rolegroup_typed_config = rolegroup
            .validate_config::<KafkaConfig>(role, &KafkaCluster::broker_default_config())
            .with_context(|_| RoleGroupValidationSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;

        let rg_service =
            build_broker_rolegroup_service(&rolegroup_ref, &kafka, &resolved_product_image)?;
        let rg_configmap = build_broker_rolegroup_config_map(
            &rolegroup_ref,
            &kafka,
            rolegroup_config,
            &resolved_product_image,
        )?;
        let rg_statefulset = build_broker_rolegroup_statefulset(
            &rolegroup_ref,
            &kafka,
            rolegroup_config,
            &broker_role_serviceaccount_ref,
            opa_connect.as_deref(),
            client_authentication_class.as_ref(),
            &rolegroup_typed_config,
            &resolved_product_image,
        )?;
        cluster_resources
            .add(client, &rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;
        cluster_resources
            .add(client, &rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;
        cluster_resources
            .add(client, &rg_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup_ref.clone(),
            })?;
    }

    for discovery_cm in build_discovery_configmaps(
        client,
        &*kafka,
        &kafka,
        &broker_role_service,
        &resolved_product_image,
        RESOURCE_SCOPE,
    )
    .await
    .context(BuildDiscoveryConfigSnafu)?
    {
        cluster_resources
            .add(client, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    Ok(Action::await_change())
}

/// The broker-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_broker_role_service(
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let role_name = KafkaRole::Broker.to_string();
    let role_svc_name = kafka
        .broker_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&role_svc_name)
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
                &role_name,
                "global",
            )
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports(kafka)),
            selector: Some(role_selector_labels(kafka, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn build_broker_role_serviceaccount(
    kafka: &KafkaCluster,
    controller_config: &ControllerConfig,
    resolved_product_image: &ResolvedProductImage,
) -> Result<(ServiceAccount, RoleBinding)> {
    let role_name = KafkaRole::Broker.to_string();
    let sa_name = format!("{}-{}", kafka.metadata.name.as_ref().unwrap(), role_name);
    let sa = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&sa_name)
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
                &role_name,
                "global",
            )
            .build(),
        ..ServiceAccount::default()
    };
    let binding_name = &sa_name;
    let binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(binding_name)
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
                &role_name,
                "global",
            )
            .build(),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(), // k8s_openapi 1.24 has made ClusterRole::GROUP crate private
            kind: "ClusterRole".to_string(),
            name: controller_config.broker_clusterrole.clone(),
        },
        subjects: Some(vec![Subject {
            api_group: Some("".to_string()),
            kind: "ServiceAccount".to_string(),
            name: sa_name,
            namespace: sa.metadata.namespace.clone(),
        }]),
    };
    Ok((sa, binding))
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_broker_rolegroup_config_map(
    rolegroup: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    resolved_product_image: &ResolvedProductImage,
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
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(
                    kafka,
                    APP_NAME,
                    &resolved_product_image.product_version,
                    RESOURCE_SCOPE,
                    &rolegroup.role,
                    "global",
                )
                .build(),
        )
        .add_data(
            SERVER_PROPERTIES_FILE,
            to_java_properties_string(server_cfg.iter().map(|(k, v)| (k, v))).with_context(
                |_| SerializeZooCfgSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?,
        )
        .add_data(
            "log4j.properties",
            kafka.spec.log4j.as_ref().unwrap_or(&"".to_string()),
        )
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_broker_rolegroup_service(
    rolegroup: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports(kafka)),
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
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_broker_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_broker_rolegroup_statefulset(
    rolegroup_ref: &RoleGroupRef<KafkaCluster>,
    kafka: &KafkaCluster,
    broker_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    serviceaccount: &ObjectRef<ServiceAccount>,
    opa_connect_string: Option<&str>,
    client_authentication_class: Option<&AuthenticationClass>,
    rolegroup_typed_config: &KafkaConfig,
    resolved_product_image: &ResolvedProductImage,
) -> Result<StatefulSet> {
    let mut cb_kafka =
        ContainerBuilder::new(APP_NAME).context(InvalidContainerNameSnafu { name: APP_NAME })?;
    let mut cb_prepare = ContainerBuilder::new("prepare").context(InvalidContainerNameSnafu {
        name: "prepare".to_string(),
    })?;
    let mut cb_kcat_prober =
        ContainerBuilder::new("kcat-prober").context(InvalidContainerNameSnafu {
            name: "kcat-prober".to_string(),
        })?;
    let mut pod_builder = PodBuilder::new();

    let role = kafka.spec.brokers.as_ref().context(NoBrokerRoleSnafu)?;
    let rolegroup = role
        .role_groups
        .get(&rolegroup_ref.role_group)
        .with_context(|| RoleGroupNotFoundSnafu {
            rolegroup: rolegroup_ref.clone(),
        })?;

    let get_svc_args = get_svc_container_cmd_args(kafka);

    // add client authentication volumes if required
    if let Some(auth_class) = client_authentication_class {
        match &auth_class.spec.provider {
            AuthenticationClassProvider::Tls(TlsAuthenticationProvider {
                client_cert_secret_class: Some(secret_class),
            }) => {
                cb_prepare.add_volume_mount(
                    "client-tls-authentication-certificate",
                    STACKABLE_TLS_CLIENT_AUTH_DIR,
                );
                cb_kafka.add_volume_mount(
                    "client-tls-authentication-certificate",
                    STACKABLE_TLS_CLIENT_AUTH_DIR,
                );
                cb_kcat_prober.add_volume_mount(
                    "client-tls-authentication-certificate",
                    STACKABLE_TLS_CLIENT_AUTH_DIR,
                );
                pod_builder.add_volume(create_tls_volume(
                    "client-tls-authentication-certificate",
                    Some(&TlsSecretClass {
                        secret_class: secret_class.clone(),
                    }),
                ));
            }
            _ => {
                return Err(Error::AuthenticationMethodNotSupported {
                    authentication_class: ObjectRef::from_obj(auth_class),
                    supported: vec!["tls".to_string()],
                    method: auth_class.spec.provider.to_string(),
                })
            }
        }
    } else if let Some(tls) = kafka.client_tls_secret_class() {
        cb_prepare.add_volume_mount("client-tls-certificate", STACKABLE_TLS_CLIENT_DIR);
        cb_kafka.add_volume_mount("client-tls-certificate", STACKABLE_TLS_CLIENT_DIR);
        cb_kcat_prober.add_volume_mount("client-tls-certificate", STACKABLE_TLS_CLIENT_DIR);
        pod_builder.add_volume(create_tls_volume("client-tls-certificate", Some(tls)));
    }

    if let Some(tls_internal) = kafka.internal_tls_secret_class() {
        cb_prepare.add_volume_mount("internal-tls-certificate", STACKABLE_TLS_INTERNAL_DIR);
        cb_kafka.add_volume_mount("internal-tls-certificate", STACKABLE_TLS_INTERNAL_DIR);
        pod_builder.add_volume(create_tls_volume(
            "internal-tls-certificate",
            Some(tls_internal),
        ));
    }

    let container_get_svc = ContainerBuilder::new("get-svc")
        .context(InvalidContainerNameSnafu {
            name: "get-svc".to_string(),
        })?
        .image("docker.stackable.tech/stackable/tools:0.2.0-stackable0.3.0")
        .command(vec!["bash".to_string()])
        .args(vec![
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
            get_svc_args,
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
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .build();

    cb_prepare
        .image("docker.stackable.tech/stackable/tools:0.2.0-stackable0.3.0")
        .command(vec![
            "/bin/bash".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![command::prepare_container_cmd_args(kafka)])
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .security_context(SecurityContextBuilder::run_as_root());

    let resources = rolegroup_typed_config.resources.clone();
    let pvcs = resources.storage.build_pvcs();
    let container_resources: ResourceRequirements = resources.into();

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

    if let Some(heap_limits) = kafka
        .heap_limits(&container_resources)
        .context(InvalidJavaHeapConfigSnafu)?
    {
        env.push(EnvVar {
            name: KAFKA_HEAP_OPTS.to_string(),
            value: Some(heap_limits),
            ..EnvVar::default()
        });
    }

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

    // add env var for log4j if set
    if kafka.spec.log4j.is_some() {
        env.push(EnvVar {
            name: "KAFKA_LOG4J_OPTS".to_string(),
            value: Some(
                "-Dlog4j.configuration=file:/stackable/config/log4j.properties".to_string(),
            ),
            ..EnvVar::default()
        });
    }

    let jvm_args = format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/broker.yaml", METRICS_PORT);
    let zookeeper_override = "--override \"zookeeper.connect=$ZOOKEEPER\"";

    let kafka_listeners = get_kafka_listener_config(kafka, &rolegroup_ref.object_name())
        .context(InvalidKafkaListenersSnafu)?;
    let listeners_override = format!("--override \"listeners={}\"", kafka_listeners.listeners());
    let advertised_listeners_override = format!(
        "--override \"advertised.listeners={}\"",
        kafka_listeners.advertised_listeners()
    );
    let listener_security_protocol_map_override = format!(
        "--override \"listener.security.protocol.map={}\"",
        kafka_listeners.listener_security_protocol_map()
    );
    let opa_url_override = opa_connect_string.map_or("".to_string(), |opa| {
        format!("--override \"opa.authorizer.url={}\"", opa)
    });

    cb_kafka
        .image_from_product_image(resolved_product_image)
        .args(vec![
            "sh".to_string(),
            "-c".to_string(),
            [
                "bin/kafka-server-start.sh",
                &format!("/stackable/config/{}", SERVER_PROPERTIES_FILE),
                zookeeper_override,
                &listeners_override,
                &advertised_listeners_override,
                &listener_security_protocol_map_override,
                &opa_url_override,
            ]
            .join(" "),
        ])
        .add_env_vars(env)
        .add_env_var("EXTRA_ARGS", jvm_args)
        .add_container_ports(container_ports(kafka))
        .add_volume_mount(LOG_DIRS_VOLUME_NAME, STACKABLE_DATA_DIR)
        .add_volume_mount("config", STACKABLE_CONFIG_DIR)
        .add_volume_mount("tmp", STACKABLE_TMP_DIR)
        .resources(container_resources);

    // Use kcat sidecar for probing container status rather than the official Kafka tools, since they incur a lot of
    // unacceptable perf overhead
    let mut container_kcat_prober = cb_kcat_prober
        .image("edenhill/kcat:1.7.0")
        .command(vec!["sh".to_string()])
        // Only allow the global load balancing service to send traffic to pods that are members of the quorum
        // This also acts as a hint to the StatefulSet controller to wait for each pod to enter quorum before taking down the next
        .readiness_probe(Probe {
            exec: Some(ExecAction {
                // If the broker is able to get its fellow cluster members then it has at least completed basic registration at some point
                command: Some(kcat_container_cmd_args(kafka)),
            }),
            timeout_seconds: Some(5),
            period_seconds: Some(2),
            ..Probe::default()
        })
        .build();
    container_kcat_prober.stdin = Some(true);
    let mut pod_template = pod_builder
        .metadata_builder(|m| {
            m.with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .with_label(pod_svc_controller::LABEL_ENABLE, "true")
        })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_init_container(cb_prepare.build())
        .add_init_container(container_get_svc)
        .add_container(cb_kafka.build())
        .add_container(container_kcat_prober)
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
        .build_template();
    let pod_template_spec = pod_template.spec.get_or_insert_with(PodSpec::default);
    // Don't run kcat pod as PID 1, to ensure that default signal handlers apply
    pod_template_spec.share_process_namespace = Some(true);
    pod_template_spec.service_account_name = Some(
        serviceaccount
            .name_in_ns_of(kafka)
            .context(InvalidServiceAccountSnafu)?
            .to_string(),
    );
    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(kafka)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(kafka, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                kafka,
                APP_NAME,
                &resolved_product_image.product_version,
                RESOURCE_SCOPE,
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
            template: pod_template,
            volume_claim_templates: Some(pvcs),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn error_policy(_obj: Arc<KafkaCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn service_ports(kafka: &KafkaCluster) -> Vec<ServicePort> {
    let mut ports = vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }];

    if kafka.client_tls_secret_class().is_some() || kafka.client_authentication_class().is_some() {
        ports.push(ServicePort {
            name: Some(SECURE_CLIENT_PORT_NAME.to_string()),
            port: SECURE_CLIENT_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        });
    } else {
        ports.push(ServicePort {
            name: Some(CLIENT_PORT_NAME.to_string()),
            port: CLIENT_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        });
    }

    ports
}

/// We only expose client HTTP / HTTPS and Metrics ports.
fn container_ports(kafka: &KafkaCluster) -> Vec<ContainerPort> {
    let mut ports = vec![ContainerPort {
        name: Some(METRICS_PORT_NAME.to_string()),
        container_port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ContainerPort::default()
    }];

    if kafka.client_tls_secret_class().is_some() || kafka.client_authentication_class().is_some() {
        ports.push(ContainerPort {
            name: Some(SECURE_CLIENT_PORT_NAME.to_string()),
            container_port: SECURE_CLIENT_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        });
    } else {
        ports.push(ContainerPort {
            name: Some(CLIENT_PORT_NAME.to_string()),
            container_port: CLIENT_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ContainerPort::default()
        });
    }

    ports
}

fn create_tls_volume(volume_name: &str, tls_secret_class: Option<&TlsSecretClass>) -> Volume {
    let secret_class_name = tls_secret_class
        .map(|t| t.secret_class.as_ref())
        .unwrap_or(TLS_DEFAULT_SECRET_CLASS);

    VolumeBuilder::new(volume_name)
        .ephemeral(
            SecretOperatorVolumeSourceBuilder::new(secret_class_name)
                .with_pod_scope()
                .with_node_scope()
                .build(),
        )
        .build()
}

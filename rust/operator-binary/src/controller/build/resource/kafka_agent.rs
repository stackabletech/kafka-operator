//! Builds the per-cluster **kafka-agent** (spike): a Deployment running the standalone **agent
//! image** (its own binary now, spike R2.2), plus the ServiceAccount and a **RoleBinding** it needs.
//!
//! RBAC mirrors the product workload: the operator *binds* the pre-defined `kafka-agent-clusterrole`
//! (shipped in the Helm chart) to the agent's ServiceAccount via a namespaced RoleBinding — so the
//! grant is confined to the cluster's namespace, and the operator needs no privilege-escalation (it
//! never mints a Role). Only produced when the cluster has `platformAccess` configured and an agent
//! image is known.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            volume::{
                SecretFormat, SecretOperatorVolumeSourceBuilder,
                SecretOperatorVolumeSourceBuilderError, VolumeBuilder,
            },
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    kvp::{Label, Labels},
    k8s_openapi::{
        api::{
            apps::v1::{Deployment, DeploymentSpec},
            coordination::v1::{Lease, LeaseSpec},
            core::v1::{
                ConfigMapVolumeSource, EnvVar, EnvVarSource, ObjectFieldSelector,
                PodSecurityContext, SecretVolumeSource, ServiceAccount, Volume,
            },
            rbac::v1::{RoleBinding, RoleRef, Subject},
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    shared::time::Duration,
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    controller::{
        RoleName, ValidatedCluster,
        build::{
            recommended_labels_for_role_group_resources, recommended_labels_for_role_resources,
            role_group_selector,
        },
    },
    agent_lease::LEASE_DURATION_SECONDS,
    crd::{APP_NAME, KAFKA_OPERATOR_NAME, platform_access::v1alpha1::KafkaPlatformAccessCredential},
};

type Result<T, E = Error> = std::result::Result<T, E>;

/// A distinct `app.kubernetes.io/managed-by` controller for the agent's resources.
///
/// The KafkaCluster controller applies its own resources through `ClusterResources`, whose
/// orphan-cleanup deletes any Deployment/ServiceAccount/RoleBinding carrying
/// `managed-by = <the KafkaCluster manager>` that it did not add to its set. The agent's resources are
/// applied **separately** (plain server-side apply, owner-ref'd for GC), so they must NOT share that
/// `managed-by` value — otherwise they get orphan-deleted and recreated on every reconcile, and the
/// agent pod never stabilises. Giving them this distinct controller name takes them out of that scope.
const AGENT_MANAGED_BY_CONTROLLER: &str = "kafkaagent";

/// Overrides the `managed-by` label so the KafkaCluster orphan-cleanup does not claim the resource
/// (see [`AGENT_MANAGED_BY_CONTROLLER`]). The pod selector uses `role_group_selector`, which does not
/// include `managed-by`, so this does not affect selection.
fn agent_managed(mut labels: Labels) -> Labels {
    labels.insert(
        Label::managed_by(KAFKA_OPERATOR_NAME, AGENT_MANAGED_BY_CONTROLLER)
            .expect("static agent managed-by is a valid label"),
    );
    labels
}

/// Where the agent's client credential (`tls.crt` / `tls.key` / `ca.crt`) is mounted.
const PLATFORM_ACCESS_CERT_DIR: &str = "/stackable/platform_access_tls";
const PLATFORM_ACCESS_VOLUME_NAME: &str = "platform-access-tls";

/// Where the Kafka **server** CA (`ca.crt`) is mounted (cross-CA mTLS).
const SERVER_CA_DIR: &str = "/stackable/platform_access_server_ca";
const SERVER_CA_VOLUME_NAME: &str = "platform-access-server-ca";

/// Where the cluster's discovery `ConfigMap` (its `KAFKA` bootstrap key) is mounted. The agent reads
/// bootstrap servers from here each reconcile — a ConfigMap volume is synced in place, so address
/// changes are picked up without restarting the agent.
const DISCOVERY_DIR: &str = "/stackable/discovery";
const DISCOVERY_VOLUME_NAME: &str = "discovery";

/// The agent credential's requested lifetime (kept above secret-operator's pod-restart buffer).
const AGENT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build the platform-access credential volume"))]
    BuildCredentialVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add a volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add a volume mount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("invalid agent container name"))]
    ContainerName {
        source: builder::pod::container::Error,
    },
}

/// The Kubernetes resources for one cluster's kafka-agent (namespace-scoped RBAC via a RoleBinding to
/// the pre-defined agent ClusterRole).
pub struct KafkaAgentResources {
    pub deployment: Deployment,
    pub service_account: ServiceAccount,
    pub role_binding: RoleBinding,
    /// The agent's liveness Lease. The **operator** creates it (owner-ref'd to the KafkaCluster, so it
    /// is garbage-collected with the cluster — a namespaced, agent-created Lease with no owner ref would
    /// otherwise linger when only the cluster is deleted). The **agent** owns only its heartbeat fields
    /// (`spec.holderIdentity`/`renewTime`) via a distinct field manager, so these applies don't clobber.
    pub lease: Lease,
}

/// Builds the kafka-agent for `cluster`, or `None` when it has no `platformAccess` (no agent) or no
/// `agent_image` was provided (nothing to run).
pub fn build_kafka_agent(
    cluster: &ValidatedCluster,
    platform_access: &crate::crd::platform_access::v1alpha1::KafkaPlatformAccess,
    agent_image: Option<&str>,
) -> Result<Option<KafkaAgentResources>> {
    let Some(agent_image) = agent_image else {
        tracing::warn!(
            cluster = %cluster.name,
            "platformAccess is set but no agent image is known; skipping the kafka-agent Deployment"
        );
        return Ok(None);
    };

    let name = format!("{}-kafka-agent", cluster.name);
    let namespace = cluster.namespace.to_string();
    let agent_role: RoleName = "kafka-agent".parse().expect("valid role name");
    // Single fixed role group for the agent. Used for both the pod-template labels and the Deployment
    // selector, so the selector stays a subset of the template labels (k8s rejects otherwise).
    let role_group = "default".parse().expect("valid role group name");

    // The credential source drives both the mounted volume and the SecretClass passed to the drain
    // Job (so its ephemeral credential is minted from the same class).
    let credential_secret_class = match &platform_access.credential {
        KafkaPlatformAccessCredential::SecretClass(secret_class) => Some(secret_class.to_string()),
        KafkaPlatformAccessCredential::Secret(_) => None,
    };

    // --- Pod ---
    let credential_volume = build_credential_volume(&platform_access.credential)?;
    let server_ca_volume =
        build_server_ca_volume(platform_access.trust_anchor_secret_class.as_ref())?;

    let mut container = ContainerBuilder::new("kafka-agent").context(ContainerNameSnafu)?;
    container
        .image(agent_image)
        .image_pull_policy("IfNotPresent")
        // Modest, explicit resources — a lean Rust reconciler. Also silences the PodBuilder's
        // per-reconcile "missing limits / limit-to-request ratio" warnings (cpu ratio ≤5, memory ==1).
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("500m")
                .with_memory_request("256Mi")
                .with_memory_limit("256Mi")
                .build(),
        )
        // The agent binary IS the image entrypoint (no subcommand); pass only its flags.
        .args(build_agent_args(
            cluster,
            &namespace,
            credential_secret_class.as_deref(),
        ))
        .add_env_var(
            "KUBERNETES_CLUSTER_DOMAIN",
            cluster.cluster_domain.to_string(),
        )
        .add_env_vars(vec![EnvVar {
            name: "KUBERNETES_NODE_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    api_version: Some("v1".to_string()),
                    field_path: "spec.nodeName".to_string(),
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }])
        .add_volume_mount(PLATFORM_ACCESS_VOLUME_NAME, PLATFORM_ACCESS_CERT_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(SERVER_CA_VOLUME_NAME, SERVER_CA_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(DISCOVERY_VOLUME_NAME, DISCOVERY_DIR)
        .context(AddVolumeMountSnafu)?;
    let container = container.build();

    let mut pod_metadata = ObjectMetaBuilder::new()
        // Role-GROUP labels (they include `app.kubernetes.io/role-group`) so the pod template is a
        // superset of the Deployment `selector` below, which uses `role_group_selector`.
        .with_labels(agent_managed(recommended_labels_for_role_group_resources(
            cluster,
            &agent_role,
            &role_group,
        )))
        .build();
    pod_metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            "internal.stackable.tech/image".to_string(),
            agent_image.to_string(),
        );

    let mut pod_builder = PodBuilder::new();
    pod_builder
        .metadata(pod_metadata)
        .add_container(container)
        .add_volume(credential_volume)
        .context(AddVolumeSnafu)?
        .add_volume(server_ca_volume)
        .context(AddVolumeSnafu)?
        // The cluster's discovery ConfigMap (named after the cluster). `optional` so the agent pod can
        // start before the operator has created it; the agent then requeues until `KAFKA` is populated.
        .add_volume(Volume {
            name: DISCOVERY_VOLUME_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: cluster.name.to_string(),
                optional: Some(true),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .service_account_name(name.clone())
        .security_context(PodSecurityContext {
            fs_group: Some(1000),
            ..PodSecurityContext::default()
        });
    let pod_template = pod_builder.build_template();

    // --- Deployment ---
    let deployment = Deployment {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(agent_managed(recommended_labels_for_role_resources(
                cluster,
                &agent_role,
            )))
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
            .build(),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(
                    role_group_selector(cluster, &agent_role, &role_group).into(),
                ),
                ..LabelSelector::default()
            },
            template: pod_template,
            ..DeploymentSpec::default()
        }),
        status: None,
    };

    // --- ServiceAccount + RoleBinding (to the pre-defined agent ClusterRole) ---
    let labels = agent_managed(recommended_labels_for_role_resources(cluster, &agent_role));

    let service_account = ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(labels.clone())
            .build(),
        ..ServiceAccount::default()
    };

    // Bind the pre-defined `{APP_NAME}-agent-clusterrole` (shipped in the Helm chart) to the agent's
    // ServiceAccount via a namespaced RoleBinding — access is confined to this namespace, and the
    // operator needs no privilege-escalation (it binds a ClusterRole rather than minting a Role).
    let role_binding = RoleBinding {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(labels)
            .build(),
        role_ref: RoleRef {
            api_group: Some("rbac.authorization.k8s.io".to_string()),
            kind: "ClusterRole".to_string(),
            name: format!("{APP_NAME}-agent-clusterrole"),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: name.clone(),
            namespace: Some(namespace),
            ..Subject::default()
        }]),
    };

    // --- Liveness Lease ---
    // Owner-ref'd to the KafkaCluster so it GCs with the cluster. Created here WITHOUT `renewTime`/
    // `holderIdentity`: the agent sets those on its heartbeat (distinct field manager). Until the agent
    // first renews, an absent `renewTime` reads as "not alive" — correct for an agent that isn't up yet.
    // The Lease name equals the agent's (`{cluster}-kafka-agent`), matching `agent_lease::agent_lease_name`.
    let lease = Lease {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(name.clone())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(agent_managed(recommended_labels_for_role_resources(
                cluster,
                &agent_role,
            )))
            .build(),
        spec: Some(LeaseSpec {
            lease_duration_seconds: Some(LEASE_DURATION_SECONDS),
            ..LeaseSpec::default()
        }),
    };

    Ok(Some(KafkaAgentResources {
        deployment,
        service_account,
        role_binding,
        lease,
    }))
}

/// The CLI arguments for the agent container. The agent binary is the image entrypoint, so these
/// are its bare flags (no subcommand).
fn build_agent_args(
    cluster: &ValidatedCluster,
    namespace: &str,
    credential_secret_class: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        format!("--kafka-cluster-name={}", cluster.name),
        format!("--namespace={namespace}"),
        format!("--discovery-config-dir={DISCOVERY_DIR}"),
        format!("--product-image={}", cluster.image.image),
        format!("--platform-access-cert-dir={PLATFORM_ACCESS_CERT_DIR}"),
        format!("--platform-access-server-ca-dir={SERVER_CA_DIR}"),
    ];
    if let Some(secret_class) = credential_secret_class {
        args.push(format!("--credential-secret-class={secret_class}"));
    }
    args
}

/// Builds the agent's client-credential volume (`tls.crt` / `tls.key` under [`PLATFORM_ACCESS_CERT_DIR`]).
fn build_credential_volume(credential: &KafkaPlatformAccessCredential) -> Result<Volume> {
    let volume = match credential {
        KafkaPlatformAccessCredential::SecretClass(secret_class) => {
            VolumeBuilder::new(PLATFORM_ACCESS_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(
                        secret_class.as_ref(),
                        SecretClassVolumeProvisionParts::PublicPrivate,
                    )
                    .with_pod_scope()
                    .with_format(SecretFormat::TlsPem)
                    .with_auto_tls_cert_lifetime(AGENT_SECRET_LIFETIME)
                    .build()
                    .context(BuildCredentialVolumeSnafu)?,
                )
                .build()
        }
        KafkaPlatformAccessCredential::Secret(secret_name) => Volume {
            name: PLATFORM_ACCESS_VOLUME_NAME.to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(secret_name.clone()),
                optional: Some(true),
                ..SecretVolumeSource::default()
            }),
            ..Volume::default()
        },
    };
    Ok(volume)
}

/// Mounts only the CA (`ca.crt`) of the Kafka server trust anchor, so the agent can verify the
/// server it connects to.
fn build_server_ca_volume(secret_class_name: &str) -> Result<Volume> {
    let volume = VolumeBuilder::new(SERVER_CA_VOLUME_NAME)
        .ephemeral(
            SecretOperatorVolumeSourceBuilder::new(
                secret_class_name,
                SecretClassVolumeProvisionParts::Public,
            )
            .with_format(SecretFormat::TlsPem)
            .with_auto_tls_cert_lifetime(AGENT_SECRET_LIFETIME)
            .build()
            .context(BuildCredentialVolumeSnafu)?,
        )
        .build();
    Ok(volume)
}

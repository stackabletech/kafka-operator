//! The operator-side Scaler controller + `KafkaScalingHooks` (spike, Part C).
//!
//! Runs in the OPERATOR (`Command::Run`), alongside the KafkaCluster controller. It drives the
//! operator-rs [`Scaler`](stackable_operator::crd::scaler) state machine for each broker role group.
//! The hooks **never drain in-process** — the operator holds no Kafka credentials. Instead, on a
//! scale-**down** the `pre_scale` hook creates an `AgentRequest` (the out-of-process transport) and
//! gates on its `status.phase`, which the credentialed agent is the sole writer of. This is the
//! coordination the spike exists to prove: operator = topology plane, agent = protocol plane.

use std::{collections::BTreeMap, sync::Arc};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::OwnerReferenceBuilder,
    client::Client,
    crd::{
        action::{
            ActionType, AgentRequestPhase, ClusterRef,
            v1alpha1::{AgentRequest, AgentRequestSpec},
        },
        scaler::{
            FailedInState, HookOutcome, ScalingContext, ScalingHooks, build_scaler,
            initialize_scaler_status, reconcile_scaler, v1alpha1::Scaler,
        },
    },
    k8s_openapi::api::apps::v1::StatefulSet,
    kube::{
        api::ListParams,
        core::{DeserializeGuard, DynamicObject, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    controller::node_id_hasher::node_id_hash32_offset,
    crd::{APP_NAME, FIELD_MANAGER, role::KafkaRole, v1alpha1::KafkaCluster},
};

/// The product string written into `AgentRequest.spec.product` — the agent filters on it.
const PRODUCT_KAFKA: &str = "kafka";
/// The broker role the drain applies to.
const BROKER_ROLE: &str = "broker";
/// How long a finished AgentRequest is retained for inspection. A TTL-GC controller honouring this is
/// a SPIKE-TODO — for now finished requests simply persist (unique per-generation names keep them from
/// colliding, so accumulation is harmless and lets you inspect past drains).
const AGENT_REQUEST_TTL_SECONDS: u32 = 86400;

pub struct Ctx {
    pub client: Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Scaler object is invalid"))]
    InvalidScaler {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("Scaler is missing metadata (name/namespace)"))]
    ObjectMissingMetadata,

    #[snafu(display("failed to read the broker StatefulSet {name:?} for scaler stability"))]
    ReadStatefulSet {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to run the scaler state machine"))]
    ReconcileScaler {
        source: stackable_operator::crd::scaler::ReconcilerError,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors from [`ensure_broker_scalers`], surfaced by the KafkaCluster reconcile.
#[derive(Snafu, Debug)]
pub enum EnsureScalerError {
    #[snafu(display("the KafkaCluster is missing metadata (name/namespace/uid)"))]
    MissingMetadata,

    #[snafu(display("failed to build the owner reference for the Scaler"))]
    OwnerReference {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build the Scaler for broker role group {role_group:?}"))]
    BuildScaler {
        source: stackable_operator::crd::scaler::BuildScalerError,
        role_group: String,
    },

    #[snafu(display("failed to create the Scaler {name:?}"))]
    CreateScaler {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to initialize the Scaler {name:?} status"))]
    InitScalerStatus {
        source: stackable_operator::crd::scaler::InitializeStatusError,
        name: String,
    },

    #[snafu(display("failed to list Scalers in namespace {namespace:?}"))]
    ListScalers {
        source: stackable_operator::client::Error,
        namespace: String,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        None
    }
}

pub async fn reconcile_scaler_object(
    scaler: Arc<DeserializeGuard<Scaler>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting Scaler reconcile");
    let scaler = scaler
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidScalerSnafu)?;

    let name = scaler.metadata.name.clone().context(ObjectMissingMetadataSnafu)?;
    let namespace = scaler
        .metadata
        .namespace
        .clone()
        .context(ObjectMissingMetadataSnafu)?;
    let client = &ctx.client;

    // The scaler is named `{cluster}-{role}-{role_group}-scaler` by `build_scaler`. Derive the
    // pieces back out so the hooks can name the AgentRequest and read the broker STS.
    let ScalerNameParts {
        cluster_name,
        role_group_name,
    } = parse_scaler_name(&name);

    // `statefulset_stable`: the broker STS has converged to its target replica count. Only relevant
    // during the `Scaling` stage; a missing STS is treated as not-yet-stable.
    let statefulset_stable = broker_statefulset_stable(client, &namespace, &cluster_name, &role_group_name)
        .await?;

    // Pod label selector for HPA counting / status.selector.
    let selector = format!(
        "app.kubernetes.io/name=kafka,app.kubernetes.io/instance={cluster_name},app.kubernetes.io/component={BROKER_ROLE}"
    );

    let hooks = KafkaScalingHooks {
        client: client.clone(),
        scaler_name: name.clone(),
        // `metadata.generation` bumps on each scale (spec change) but is stable across the reconciles
        // of one PreScaling episode — the ideal key for a unique-per-operation AgentRequest name.
        scaler_generation: scaler.metadata.generation.unwrap_or(0),
        namespace: namespace.clone(),
        cluster_name: cluster_name.clone(),
        role_group_name: role_group_name.clone(),
    };

    let result = reconcile_scaler(
        scaler,
        &hooks,
        client,
        statefulset_stable,
        &selector,
        &role_group_name,
    )
    .await
    .context(ReconcileScalerSnafu)?;

    // SPIKE-TODO: propagate `result.scaling_condition` into the KafkaCluster status conditions. The
    // Scaler is owned by the KafkaCluster, so the KafkaCluster controller (woken by the Scaler watch)
    // would surface it; here we just log it so the state-machine progress is observable.
    tracing::info!(?result.scaling_condition, "Scaler reconcile step complete");

    Ok(result.action)
}

/// The broker STS is stable when its `status.ready_replicas == spec.replicas`.
async fn broker_statefulset_stable(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    role_group_name: &str,
) -> Result<bool> {
    let sts_name = format!("{cluster_name}-{BROKER_ROLE}-{role_group_name}");
    let sts = client
        .get_opt::<StatefulSet>(&sts_name, namespace)
        .await
        .context(ReadStatefulSetSnafu {
            name: sts_name.clone(),
        })?;
    let Some(sts) = sts else {
        // No STS yet ⇒ not stable.
        return Ok(false);
    };
    let spec_replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
    let ready_replicas = sts
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    Ok(ready_replicas == spec_replicas)
}

struct ScalerNameParts {
    cluster_name: String,
    role_group_name: String,
}

/// Parses a `{cluster}-{role}-{role_group}-scaler` name back into its parts.
///
/// `build_scaler` composes the name as `{cluster_name}-{role}-{role_group}-scaler`. For the broker
/// role this is `{cluster}-broker-{role_group}-scaler`. We split on the fixed `-broker-` infix and
/// the `-scaler` suffix, which is robust to hyphens in the cluster or role-group name.
fn parse_scaler_name(name: &str) -> ScalerNameParts {
    let without_suffix = name.strip_suffix("-scaler").unwrap_or(name);
    let infix = format!("-{BROKER_ROLE}-");
    if let Some(idx) = without_suffix.find(&infix) {
        let cluster_name = without_suffix[..idx].to_string();
        let role_group_name = without_suffix[idx + infix.len()..].to_string();
        ScalerNameParts {
            cluster_name,
            role_group_name,
        }
    } else {
        // Fallback: unknown shape; treat the whole thing as the cluster name and default the group.
        ScalerNameParts {
            cluster_name: without_suffix.to_string(),
            role_group_name: "default".to_string(),
        }
    }
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<Scaler>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidScaler { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(10)),
    }
}

/// Ensures one [`Scaler`] exists per broker role group of `kafka`, creating and
/// initializing it once (guarded so the initial create does not scale the STS to zero), and returns
/// the current Scalers keyed by role-group name so the STS build can consult them via
/// `resolve_replicas` (spike Part C).
///
/// `broker_role_groups` maps each broker role-group name to its configured replica count (from the
/// spec). The Scaler's initial status is seeded with that count.
pub async fn ensure_broker_scalers(
    client: &Client,
    kafka: &KafkaCluster,
    broker_role_groups: &BTreeMap<String, i32>,
) -> Result<BTreeMap<String, Scaler>, EnsureScalerError> {
    let name = kafka.metadata.name.clone().context(MissingMetadataSnafu)?;
    let namespace = kafka
        .metadata
        .namespace
        .clone()
        .context(MissingMetadataSnafu)?;

    let owner_ref = OwnerReferenceBuilder::new()
        .initialize_from_resource(kafka)
        .controller(true)
        .block_owner_deletion(true)
        .build()
        .context(OwnerReferenceSnafu)?;

    for (role_group_name, configured_replicas) in broker_role_groups {
        let scaler_name = format!("{name}-{BROKER_ROLE}-{role_group_name}-scaler");
        // Guard: only create-and-initialize the Scaler once. If it already exists, leave its status
        // (owned by the state machine) untouched — re-initializing would clobber an in-progress
        // scaling operation and could momentarily scale the STS to zero.
        let existing: Option<Scaler> = client
            .get_opt(&scaler_name, namespace.as_str())
            .await
            .ok()
            .flatten();
        if existing.is_some() {
            continue;
        }

        // `build_scaler`/`initialize_scaler_status` take the replica count as `u16` (0.116.0); the
        // spec boundary is `i32`. Clamp into range — a negative or oversized replica count is
        // nonsensical for a role group.
        let initial_replicas = (*configured_replicas).clamp(0, u16::MAX as i32) as u16;

        let scaler = build_scaler(
            &name,
            APP_NAME,
            &namespace,
            BROKER_ROLE,
            role_group_name,
            initial_replicas,
            &owner_ref,
            FIELD_MANAGER,
        )
        .context(BuildScalerSnafu {
            role_group: role_group_name.clone(),
        })?;

        client
            .apply_patch(FIELD_MANAGER, &scaler, &scaler)
            .await
            .context(CreateScalerSnafu {
                name: scaler_name.clone(),
            })?;

        let selector = format!(
            "app.kubernetes.io/name=kafka,app.kubernetes.io/instance={name},app.kubernetes.io/component={BROKER_ROLE}"
        );
        initialize_scaler_status(client, &scaler, initial_replicas, &selector)
            .await
            .context(InitScalerStatusSnafu {
                name: scaler_name.clone(),
            })?;
    }

    // Re-read the current Scalers for this cluster's broker role groups.
    let mut scalers = BTreeMap::new();
    let all: Vec<Scaler> = client
        .list(namespace.as_str(), &ListParams::default())
        .await
        .context(ListScalersSnafu {
            namespace: namespace.clone(),
        })?;
    for scaler in all {
        let Some(scaler_name) = scaler.metadata.name.as_deref() else {
            continue;
        };
        let parts = parse_scaler_name(scaler_name);
        if parts.cluster_name == name && broker_role_groups.contains_key(&parts.role_group_name) {
            scalers.insert(parts.role_group_name, scaler);
        }
    }
    Ok(scalers)
}

// ---------------------------------------------------------------------------------------------
// KafkaScalingHooks — the coordination logic. Never drains in-process.
// ---------------------------------------------------------------------------------------------

/// Hooks for the broker Scaler. Carries the scaler identity (which [`ScalingContext`] does not
/// expose — spike finding #2) so the AgentRequest can be named deterministically and owner-ref'd.
struct KafkaScalingHooks {
    client: Client,
    scaler_name: String,
    /// The Scaler's `metadata.generation` — makes the AgentRequest name unique per scale-down.
    scaler_generation: i64,
    namespace: String,
    cluster_name: String,
    role_group_name: String,
}

#[derive(Snafu, Debug)]
pub enum HookError {
    #[snafu(display("failed to create/read the AgentRequest {name:?}"))]
    AgentRequest {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display(
        "the AgentRequest {name:?} reported phase {phase}; the scaling step cannot proceed"
    ))]
    AgentActionFailed { name: String, phase: String },
}

impl KafkaScalingHooks {
    /// AgentRequest name for the drain (`PreScaling`) step of this scaler's role group.
    ///
    /// Unique per scale-down operation via the Scaler's generation: a fixed name would collide with a
    /// retained, already-`Done` request from a previous scale-down (finished requests are kept for
    /// inspection now), and the hook would read that stale `Done` and skip the drain entirely. The
    /// generation is stable across one PreScaling episode, so retries within it reuse the same object.
    fn scaledown_request_name(&self) -> String {
        format!("{}-scaledown-gen{}", self.scaler_name, self.scaler_generation)
    }

    /// AgentRequest name for the unregister (`PostScaling`) step — same per-generation uniqueness as
    /// [`Self::scaledown_request_name`], but a distinct object so the two steps' statuses don't clash.
    fn unregister_request_name(&self) -> String {
        format!("{}-unregister-gen{}", self.scaler_name, self.scaler_generation)
    }
}

impl ScalingHooks for KafkaScalingHooks {
    type Error = HookError;

    async fn pre_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, HookError> {
        // Scale-up needs no drain.
        if !ctx.is_scale_down() {
            return Ok(HookOutcome::Done);
        }
        // Gate `PreScaling` on the drain (partition reassignment off the removed brokers).
        let broker_ids = self.removed_broker_ids(ctx);
        self.ensure_and_gate(
            &self.scaledown_request_name(),
            ActionType::ScaleDown,
            &broker_ids,
        )
        .await
    }

    async fn post_scale(&self, ctx: &ScalingContext<'_>) -> Result<HookOutcome, HookError> {
        // Only a scale-down leaves brokers to unregister; a scale-up's PostScaling is a no-op.
        if !ctx.is_scale_down() {
            return Ok(HookOutcome::Done);
        }
        // The drained brokers' pods are gone now (the STS shrank during `Scaling`), so their
        // registrations linger in the cluster metadata as stale/fenced entries. The operator holds no
        // Kafka credentials, so it gates `PostScaling` on a *second* AgentRequest — mirroring the
        // drain — that the credentialed agent executes via `UnregisterBroker`. Reconcile-safe:
        // unregister is idempotent (an already-gone broker is success).
        let broker_ids = self.removed_broker_ids(ctx);
        self.ensure_and_gate(
            &self.unregister_request_name(),
            ActionType::Unregister,
            &broker_ids,
        )
        .await
    }

    async fn on_failure(
        &self,
        _ctx: &ScalingContext<'_>,
        failed_in: &FailedInState,
    ) -> Result<(), HookError> {
        // Best-effort: leave the AgentRequest in place for inspection.
        tracing::warn!(
            scaler = self.scaler_name,
            ?failed_in,
            "Scaler entered Failed; the AgentRequest is retained for inspection"
        );
        Ok(())
    }
}

impl KafkaScalingHooks {
    /// Maps the removed pod ordinals to Kafka node ids as a comma-separated `context.brokerIds`.
    ///
    /// The mapping is `node_id_hash32_offset(role, role_group) + pod_ordinal` (see `node_id_hasher`),
    /// e.g. ordinal 2 → 1243966390 — NOT the raw ordinal 2. Passing the raw ordinal made the agent's
    /// `reassign_off` match no partition, so the drain silently no-op'd and the broker was removed
    /// with its replicas still on it; this is the fix for that bug. The unregister step reuses the
    /// same ids so it removes exactly the brokers that were drained.
    fn removed_broker_ids(&self, ctx: &ScalingContext<'_>) -> String {
        let node_id_offset = node_id_hash32_offset(&KafkaRole::Broker, &self.role_group_name);
        ctx.removed_ordinals()
            .map(|ordinal| ((node_id_offset + ordinal as u32) as i32).to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Idempotently creates (server-side apply) the AgentRequest for a gated step and reads back the
    /// agent-written `status.phase`, mapping it to a [`HookOutcome`]. Shared by `pre_scale` (drain)
    /// and `post_scale` (unregister): both create a request and gate the state machine on its phase.
    async fn ensure_and_gate(
        &self,
        name: &str,
        action_type: ActionType,
        broker_ids: &str,
    ) -> Result<HookOutcome, HookError> {
        let request = self.build_agent_request(name, action_type, broker_ids);
        self.client
            .apply_patch(FIELD_MANAGER, &request, &request)
            .await
            .context(AgentRequestSnafu {
                name: name.to_string(),
            })?;

        // Re-fetch and read the agent-written status.phase.
        let current: Option<AgentRequest> = self
            .client
            .get_opt(name, self.namespace.as_str())
            .await
            .context(AgentRequestSnafu {
                name: name.to_string(),
            })?;

        let phase = current
            .as_ref()
            .and_then(|r| r.status.as_ref())
            .map(|s| s.phase.clone())
            .unwrap_or(AgentRequestPhase::Pending);

        match phase {
            // Retain the finished AgentRequest for inspection (unique per-generation name means the
            // next scaling op won't collide with it; honouring `ttlSecondsAfterFinished` via a TTL-GC
            // controller is a SPIKE-TODO). The Scaler has already read Done and advances.
            AgentRequestPhase::Done => Ok(HookOutcome::Done),
            // Retain the failed request for inspection; surface the failure to the state machine.
            AgentRequestPhase::Failed | AgentRequestPhase::Rejected => AgentActionFailedSnafu {
                name: name.to_string(),
                phase: phase.to_string(),
            }
            .fail(),
            AgentRequestPhase::Pending | AgentRequestPhase::InProgress => Ok(HookOutcome::InProgress),
        }
    }

    fn build_agent_request(
        &self,
        name: &str,
        action_type: ActionType,
        broker_ids: &str,
    ) -> AgentRequest {
        let mut context = BTreeMap::new();
        context.insert("brokerIds".to_string(), broker_ids.to_string());

        let mut request = AgentRequest::new(
            name,
            AgentRequestSpec {
                cluster_ref: ClusterRef {
                    name: self.cluster_name.clone(),
                },
                product: PRODUCT_KAFKA.to_string(),
                role: BROKER_ROLE.to_string(),
                action_type,
                context,
                ttl_seconds_after_finished: Some(AGENT_REQUEST_TTL_SECONDS),
            },
        );
        request.metadata.namespace = Some(self.namespace.clone());
        // SPIKE-TODO: owner-ref the AgentRequest to the Scaler so it is GC'd with it. Skipped here to
        // avoid re-fetching the Scaler's uid inside the hook; the TTL + explicit delete-on-Done cover
        // cleanup for the spike.
        let _ = self.role_group_name;
        request
    }
}

# Spike findings: Kafka agent + broker draining via the operator-rs Scaler

Throwaway spike validating the "agent per product cluster" + Scaler-based scale-down design
(decisions#94, issues#865, scaling epic issues#872) against Kafka. Goal: surface problems/limitations in
the operator ⇄ agent ⇄ Scaler coordination — **not** production code.

Three branches, all compiling green **in the plain sandbox** (fully pure-Rust after R2.5 — rustls/ring, no
C toolchain, no JVM):
- `operator-rs` → `spike/kafka-agent-scaler` — the ported 0.116-faithful `Scaler` + the generic `AgentRequest` CRD.
- `kafka-operator` → `spike/topic-agent` — split into `operator-binary` (operator, no Kafka client) + `agent-binary` (the pure-Rust agent).
- `commons-operator` → `spike/agentrequest-scaler-crds` — installs the two platform CRDs.

Status: **compiles + unit-tests green everywhere** (operator-rs: 693 tests + 50 doctests). Live behaviour
not yet run — see the live-run boundary at the end. Build note: `cargo` works directly in the sandbox now;
commons-operator pins a toolchain (1.97.1) that isn't installed here, so build it with `RUSTUP_TOOLCHAIN=stable`.

---

## Findings

### 1. The Scaler needs a companion generic action CRD — confirmed, and it's a platform CRD
The operator-rs `Scaler` alone can't coordinate an out-of-process hook. We added a standalone, namespaced
`AgentRequest` CRD **beside** the Scaler in operator-rs. The operator's hook writes `spec`; the agent is the
**sole writer of `status`** (a `/status` subresource → the two never clobber). The envelope is generic
(`context: BTreeMap<String,String>`, no Kafka-isms) so it serves kafka/hdfs/nifi/trino; only the agent's
executor is product-specific. **Both `AgentRequest` and `Scaler` are installed by commons-operator** (they're
operator-rs platform CRDs, not product-specific) — kafka-operator only *uses* the types. This introduces a
**deploy-ordering dependency**: commons-operator must be up (CRDs Established) before kafka-operator starts,
because the kafka Scaler controller watches `Scaler` and waits on `crd_established`.

### 2. `ScalingContext` doesn't expose the Scaler object/name — confirmed, minor but real
The hook receives `{client, namespace, role_group_name, current/desired_replicas, direction}` — not the
`Scaler` it belongs to. So `KafkaScalingHooks` carries the scaler name itself, and we can't owner-ref the
`AgentRequest` to the Scaler from inside the hook (no uid). **Fix upstream:** give `ScalingContext` a handle
to (or the name/uid of) its Scaler.

### 3. No timeout in `PreScaling` — confirmed; a stuck/absent agent hangs scale-down forever
`reconcile_scaler` has no deadline: `pre_scale` can return `InProgress` indefinitely. A drain that never
completes pins the state machine with no auto-fail. Mitigation: Scalers are only created when
`platformAccess` (⇒ an agent) is set, so no-agent clusters never gate. But once gated there's no timeout, and
it ties to the Lease/`AgentUnavailable` heartbeat (the operator *sees* the agent is down but the Scaler won't
act). **Fix upstream:** a hook-side deadline → `Err` → `Failed`, and/or let the Scaler consult agent liveness.

### 4. Adapt to upstream (full 0.116), don't regress it — the Scaler port is a type-rewrite, not a copy
We keep 0.116.0's **shipped** scaler types as canonical (`Scaler` kind, `u16`, complex-enum `ScalerState`,
`FailedInState`, top-level `last_transition_time`) and rewrote the `feat/autoscale` reconciler/hooks/builders
onto them. (An earlier shortcut — overwriting shipped `mod.rs` with feat/autoscale's older 0.106-era types —
was backed out.) The rewrite is bounded and passes the ported reconciler unit tests, incl. the
`previous_replicas`-in-variant direction logic. Bonus: `feat/autoscale` has a real bug (a stray `~` in
`cluster_resource_impl.rs`) that only surfaces under the `webhook` feature — worth flagging upstream.
**Implication:** landing the real Scaler on a current base is low-effort *if you adapt to the shipped types*.

### 5. Must-gate ordering holds — structurally validated
`PreScaling` (drain) before `Scaling` (STS decrement) before `PostScaling` (unregister) is exactly right for
Kafka: reassignment needs the broker alive as the data *source*, so drain-before-delete is correct and the
state machine expresses it naturally. Runtime confirmation pending the live run.

### 6. `AgentRequest` lifecycle — implemented; owner-ref + TTL-GC are the gaps
Per-generation name (`{scaler}-{scaledown,unregister}-gen<N>` — two request kinds since finding #14),
created idempotently by the hook, `status` driven solely by the agent. **Retained** on every terminal phase
(`Done`/`Failed`/`Rejected`) for inspection — the per-generation name means a later scaling op never collides
with a retained one (the R4 fix: a fixed name let the hook read a stale `Done` and skip the drain).
`ttlSecondsAfterFinished` is carried but nothing honours it yet, and owner-ref to the Scaler is skipped
(finding #2) — a TTL-GC controller / owner-ref GC is the remaining SPIKE-TODO.

### 7. The drain is native (no JVM Job) — crash-safe via re-derivation
The agent performs the reassignment itself over the Kafka wire protocol: `Metadata` → compute a plan (drop
draining brokers from each affected partition's replica set, keep RF) → `AlterPartitionReassignments`(45) →
poll `ListPartitionReassignments`(46). Crash-safe **without a stored plan**: each reconcile re-reads Metadata
+ the in-progress list and re-derives `remaining`. No product-image Job, no bash, no `--generate` over-move.
**Headline risk (partly proven live):** the "remaining" completion heuristic (a partition counts as
still-moving if a draining id is in its `removing_replicas`/current `replicas`) drives the `Done` transition —
a wrong read either hangs `PreScaling` or reports done before data has moved. The first live run surfaced a
related re-derivation correctness bug (finding #15) — the in-flight list now feeds `build_plan` too, not just
`remaining`. The `Done`-timing itself still needs a fully green live run to confirm.

### 8. Fully pure-Rust Kafka access via `kafka-protocol` — rdkafka/librdkafka/JVM all dropped
Both topics (`Metadata`/`CreateTopics`/`DeleteTopics`/`CreatePartitions`/`IncrementalAlterConfigs`) and the
drain reassignment go over **one `tokio` + `rustls` (mTLS) `kafka-protocol` connection**. This replaced
rdkafka (topics) and the ephemeral JVM `kafka-reassign-partitions.sh` Job (drain) from R1. Consequences:
- **No librdkafka / `openssl-sys` / `pkg-config` / Nix `rdkafka-sys` override / cmake** — the whole workspace
  builds in the plain sandbox (rustls `ring` provider). This dissolved the entire R1 build ordeal.
- **No JVM anywhere** in the agent path; the agent image is a lean pure-Rust binary. (`kafka-reassign-partitions.sh`
  et al. are just JVM CLIs over the same Admin protocol — the only reason rdkafka couldn't drain is librdkafka
  doesn't expose `AlterPartitionReassignments`; we implement the request directly.)

### 9. The cost of owning a slice of a Kafka client (the central trade this spike evaluates)
Speaking the protocol ourselves is feasible and clean, but we now maintain client internals the JVM/librdkafka
hide. Concrete, honest costs surfaced building it:
- **Framing / per-type header-version is the highest-risk code** — e.g. `ApiVersions` uses a flexible (v2)
  *request* header at api v3 but a v0 *response* header always; getting a header version wrong desyncs the
  whole TCP stream. Handled by deriving `header_version` per message type, never hard-coding.
- **Controller routing is now implemented** (R3, finding #11): the client reads `controller_id` from the
  `Metadata` it already fetches and re-points its connection at the controller broker. This is load-bearing
  precisely because **not every Kafka is KRaft** — ZooKeeper-mode brokers answer `NOT_CONTROLLER` instead of
  forwarding. Unproven against a live ZK-mode cluster.
- **mTLS only** — SASL/SCRAM/Kerberos each need their own pre-request handshake, absent here. A real gap vs. the JVM tools.
- **Plan is RF-correct but not rack-aware / balanced**; survivors picked in id order. And it silently drops a
  partition when survivors < RF instead of raising a distinct "cannot drain" condition (SPIKE-TODO).
- **Connection reuse** (R3): one connection behind a lock, reused across the ops within a reconcile (was: a
  fresh TCP+TLS handshake per trait call). Sequential (no pipelining) — right for an agent's op-rate.
- Net: materially less battle-tested than `kafka-reassign-partitions.sh`. For a per-cluster agent doing
  occasional admin ops this is an acceptable, self-owned trade; the framing layer is where the risk concentrates.

### 10. The agent is its own binary + OCI image, sharing code via the operator lib
`operator-binary` is now `lib`+`bin`; `agent-binary` depends on it for the shared `crd` types/constants and
adds the pure-Rust Kafka client. The operator binary/image contains **no Kafka client at all** (it only creates
`AgentRequest`s and reconciles the `Scaler`). Matches decision.md Coordination Option A. (Original motivation —
keep librdkafka out of the operator image — became moot once R2.5 removed librdkafka everywhere; the split now
stands on lifecycle/separation grounds.) The zk-agent pattern (two reconcilers + condition + Lease heartbeat +
namespaced Role/RoleBinding) ported near-directly; the operator-rs Scaler+`AgentRequest` integration is small.

### 11. The client slice, extracted into a standalone `tokio-kafka` crate (R3)
The R2.5 in-agent `kafka_client` module is now a sibling crate `~/stackable/tokio-kafka`, modelled on
`tokio-zookeeper`. Building it out gave the clearest read on "the actual complexity" the spike set out to
measure:
- **How much of a real client this is: less than it looks — because most of what makes `tokio-zookeeper`
  big does not apply.** Its 314-line background packetizer (`Pin`/poll `Future`, reply/watcher maps, reconnect
  with `session_id`/`zxid`/`passwd`) exists to serve **watches** (server-push interleaved with responses) and
  **session resumption**. Kafka admin has **neither**. So a *faithful* port would be over-engineered; the
  honest analog is **one connection reused behind a `tokio::sync::Mutex`** (ops are sequential — fine at an
  agent's rate). We mirror tokio-zookeeper's *directory layout* (`proto/` codec, `types/`, a `recipes/`-style
  `reassignment` module, builder + client) but drop the packetizer, `watch.rs`, and hand-rolled
  `request`/`response` enums.
- **`kafka-protocol` erases the single biggest cost.** The generated message types + codec are exactly the
  `proto/request.rs` + `proto/response.rs` a from-scratch ZK-style client hand-rolls with `byteorder`. With
  Claude Code, authoring the *rest* (framing, version negotiation, mTLS, routing, the drain planner) was hours,
  not days — the honest cost is not typing but **long-tail protocol correctness** (per-type header versions,
  error taxonomy, SASL/Kerberos we don't do), which only a live cluster exercises.
- **Controller routing is the one Kafka-specific addition with no ZK analog** (`cluster.rs`) and it's
  load-bearing because **not every Kafka is KRaft** (see finding #9): ZK-mode returns `NOT_CONTROLLER`. Reusing
  the `Metadata` the admin ops already fetch makes it near-free. The crate therefore works against both modes.
- **Bootstrap now comes from the mounted discovery `ConfigMap`** (`KAFKA` key), not a hard-coded DNS name (that
  earlier SPIKE-TODO is resolved). Mounted as a **volume** so the kubelet syncs it in place — the agent
  re-reads it each reconcile and tolerates a transient-empty value (→ requeue) without a restart. **Caveat:
  under Kerberos the discovery CM advertises the *bootstrap (Kerberos) port*, which an mTLS agent can't use** —
  so CM-as-bootstrap assumes the spike's TLS-client-auth setup (ties to the SASL/Kerberos gap).
- Net: a genuinely reusable ~700-line admin client, testable in isolation (the reassignment planner has unit
  tests; the wire path needs a broker). The extraction is low-churn; the residual risk is unchanged from #9.

### 12. Two live-validated drain bugs → a real client's error-handling layer (R4)
The first live drain exercised the gate but exposed two bugs, both now fixed:
- **Broker-id ↔ pod-ordinal mismatch (silent under-replication).** The pre-scale hook passed the pod *ordinal*
  (`2`) as the broker id, but KRaft node ids are hashed (`node_id_hash32_offset(role, rg) + ordinal`, e.g.
  `1243966390`). `reassign_off` matched no partition → `total_moving=0` → instant `Done` → the broker was
  removed **with its replicas still on it** (4 partitions left under-replicated against a dead node). Fixed by
  mapping ordinals through the operator's own `node_id_hasher` in the hook. The plan then computed the correct
  `total=4`. **Lesson: the operator (topology plane) and the agent (protocol plane) must agree on broker
  identity; the ordinal is the operator's, the node id is Kafka's.**
- **A transient error aborted the whole drain.** A single `AlterPartitionReassignments` → `RequestTimedOut`
  (off a flapping controller) was written as `AgentRequest phase: Failed` → Scaler `failedIn: PreScaling` →
  drain aborted. The *topic* path hit the same timeout and self-healed on requeue — proving the asymmetry: the
  agent didn't classify errors. **The gate itself worked correctly** (it refused to decrement on a failed
  drain), which is a positive validation.
  - Fix = a proper client resilience layer in `tokio-kafka`: `Error::is_retryable()` (reusing
    `kafka_protocol::ResponseError::is_retriable()`) + `Error::is_connection_fatal()`; **`backon` exponential
    backoff** wrapping each idempotent op (Layer 1, in-client, seconds); **reconnect-on-poison** (drop + re-dial
    + re-route on a connection-fatal error — one mechanism covering broken sockets, reactive `NOT_CONTROLLER`
    re-routing, and timeout retry); and a **client-side request timeout** so a wedged peer can't block forever.
    Agent side (Layer 2): a retryable drain error now → `InProgress` + requeue (not terminal `Failed`); the
    reconcile loop is the outer retry. Two composed layers = the standard operator pattern.
- **Circuit breaker considered, rejected.** No hot path here (reconcile-driven, occasional admin ops); the
  requeue loop already provides backpressure, and no breaker crate is in-tree. Backoff + classification is the
  right-sized, idiomatic Rust answer; a breaker would only earn its keep on a high-throughput produce/consume
  path. Documented, not built.
- Minor live fixes folded in: the agent 403'd publishing events (`events.k8s.io` missing from its ClusterRole —
  granted); the agent container had no resource requests/limits (per-reconcile PodBuilder warnings — set
  `100m/500m` cpu, `256Mi/256Mi` memory).

### 13. Two spike-TODOs promoted to fixes + a thorough kuttl suite (R5)
The full-run drain proved out (finding #12), so the `topic-agent-spike` kuttl test was hardened from a
happy-path smoke test into a real lifecycle + failure suite (topic create/update/**delete** all verified
*inside Kafka* via `kafka-topics.sh`; produce 100k msgs → drain → assert **no data lost**; the drain's
Scaler/AgentRequest transitions asserted mid-flight and at rest; a 2nd drain for the unique-name/retain path).
Writing the failure cases forced two SPIKE-TODOs to become real fixes:
- **Below-RF drain was a silent-Done under-replication bug → fixed.** `reassign_off` on a drain that can't keep
  RF (`plan_replacement` → `None` for all touched partitions) skipped submit, saw nothing ongoing, and reported
  `remaining: 0` → `Done` → the broker was removed under-replicated. `build_plan` now reports a `blocked` count
  and `reassign_off` returns a **non-retryable** `CannotDrainBelowReplicationFactor` → agent `Failed` → Scaler
  `Failed`, gate holds, STS not decremented. (Was the `plan_replacement` "silently under-plans" TODO in #7/#9.)
- **`AgentUnavailable` is now surfaced operator-side.** The agent's liveness Lease was renewed but nothing
  watched it (the `is_agent_alive` helper was dead code *in the agent crate*). Moved the shared lease bits
  (`agent_lease_name`, `LEASE_DURATION_SECONDS`, `is_agent_alive`) into the operator crate (single source of
  truth; the agent depends on it), and the KafkaCluster controller now polls the Lease each reconcile (requeue
  ~20s, since a Lease has no "expired" event) and writes a dedicated **`status.agent.{available,message}`** field.
  A dedicated field, *not* a `ClusterCondition`: the condition-merge machinery aggregates on `Available`
  semantics, so an agent-down `Degraded=True` would be overridden by the healthy STS `Degraded=False`. (Was
  finding #3's Lease/`AgentUnavailable` gap.)
- Deploy note: the new `status.agent` field needs the KafkaCluster CRD regenerated (`make regenerate-charts`),
  else it's pruned and the agent-down assertion can't pass.

### 17. The operator maintains the agent, so "agent down" can't be simulated by scaling it to zero
Live-observed while validating the `AgentUnavailable` surface (finding #13): scaling the agent Deployment
to `replicas=0` does **not** make `status.agent.available` flip to `false`. The operator owns the agent
Deployment and re-applies `replicas=1` on its (~20s) reconcile, and the fresh pod renews the liveness Lease
well inside the 30s staleness window — so the agent is resurrected before it ever reads as stale. This is
*correct* operator behaviour (keep the agent running), and it means a genuine `AgentUnavailable` in
production comes from the agent being **wedged/crash-looping** (Deployment at 1 but not renewing), not from
a scale-to-zero.
- **Test fix:** step 90 now *deletes the Lease* (and scales to 0 to stop the old pod renewing). `is_agent_alive`
  treats an absent Lease as not-alive, and the operator — which watches the Lease — writes `available=false`
  almost immediately on the delete event. The signal is transient (the resurrected pod recreates the Lease in
  ~20-30s), so the assert has no pre-sleep and must poll from the start; kuttl passes on the first match.
- Worth noting the asymmetry this exposes: the liveness signal is a *Lease*, but the thing that keeps the
  agent alive is the *operator*. A cleaner production design might have the operator distinguish "agent
  Deployment unavailable" (it can see that directly — replicas/readiness) from "agent process wedged" (the
  Lease), rather than relying only on the Lease.
- **Lease ownership fixed (operator creates, agent renews).** The agent used to *create* its own Lease via SSA
  with no owner reference, so it was an orphan — not GC'd when the KafkaCluster was deleted (it only vanished
  on *namespace* deletion). Now the **operator creates** the Lease as part of the agent's resources
  (owner-ref'd to the KafkaCluster → GCs with it), and the **agent renews** only `spec.holderIdentity`/
  `renewTime` via a **distinct field manager** so the two server-side applies never clobber each other. This
  is the same operator=lifecycle / agent=heartbeat split as everything else, and it means deleting the cluster
  now cleans up the Lease. (Operator ClusterRole gains `create`/`patch` on `leases`.)

### 16. A namespaced agent can't run its own finalizers during namespace teardown → operator safety net
Live-observed: old test namespaces wedged in `Terminating` forever, each blocked by a single leftover
`KafkaTopic` whose `kafka.stackable.tech/kafkatopic` finalizer was never cleared. Root cause is structural, not
a bug in the cleanup logic (which already handles a gone cluster — it skips `DeleteTopics` and lets the
finalizer complete): the **agent is namespaced**, so namespace GC deletes the agent Deployment *before* (or
racing with) the finalized `KafkaTopic`, leaving no controller alive to run the finalizer. General lesson: a
namespaced controller must not be the sole owner of a finalizer it needs to clear during namespace deletion.
- **Design split (the fix):** the finalizer still earns its keep — deleting a `KafkaTopic` from a *live*
  cluster should reclaim the topic (the D in the agent's topic CRUD; kuttl step 85). So the **agent** keeps
  owning credentialed delete on a live cluster, and the **cluster-scoped operator** (which survives namespace
  teardown) adds a safety net: a `KafkaTopic` controller that releases the finalizer *only* when the topic is
  being deleted AND its `KafkaCluster` is absent/terminating — because then deleting the topic inside Kafka is
  meaningless (it dies with the cluster). While the cluster is alive the operator only requeues (never strips
  the finalizer — that would orphan the topic if the agent were merely down, not gone), which also closes the
  teardown race where the topic's `deletionTimestamp` lands a beat before the cluster's.
- This is the same operator=lifecycle / agent=protocol division the drain already uses: the operator owns the
  k8s-lifecycle safety net (no Kafka creds needed for a no-op release), the agent owns the credentialed op.
- New: `operator-binary/src/topic_finalizer_controller.rs` + operator ClusterRole `kafkatopics`(+`/finalizers`)
  get/list/watch/patch. Needs the operator image rebuilt + chart re-applied.

### 15. The crash-safe re-derivation had a subtle correctness hole (live-validated) → fixed
The first live run of the hardened suite failed the drain — but for a *good* reason it took a live cluster to
surface. The drain submitted the reassignment fine (reconcile 1 → `InProgress`, 6 partitions moving), then the
**next reconcile marked it `Failed` with `CannotDrainBelowReplicationFactor`** on a 3→2 / RF2 drain where two
survivors is plenty. Root cause: while a reassignment is in flight, `Metadata` reports each partition's
`replica_nodes` as the **union of original + target** replicas (the target survivor already listed, the
draining broker not yet removed), e.g. `[0,2]` shows as `[0,2,1]`. The re-derivation (`build_plan`, run every
reconcile by design — finding #7) recomputed `rf = replicas.len()` from that inflated set → RF looks like 3 →
"can't keep RF with 2 survivors" → the below-RF safety check (finding #13) fired a **false positive** and
aborted the drain.
- **The lesson is the spike's thesis, sharpened:** owning a client slice means owning the protocol's *transient*
  state semantics, not just the request/response types. "Re-derive from the cluster each reconcile" is the right
  crash-safe design, but a single mid-operation Metadata snapshot is not a clean source of truth — an in-flight
  reassignment's replica set is deliberately ambiguous. This is exactly the long-tail correctness cost finding
  #9 predicted, now with a concrete instance.
- **Fix:** `build_plan` now takes the set of partitions already reassigning (from `ListPartitionReassignments`,
  which the drain already polls) and **skips** them — not re-planned, not counted as blocked, still counted in
  `total_moving`. Only *not-yet-started* partitions are planned or safety-checked, so the RF inference always
  runs on a clean (non-inflated) replica set. Two unit tests pin it: one reproduces the inflated-set false-block,
  the other asserts the in-flight skip. The below-RF safety check (finding #13) still fires correctly for a
  genuine 2→1 drain (nothing in flight on the first reconcile → the clean set is legitimately unsatisfiable).
- Also validated *around* the bug: the ordinal→node-id mapping is correct (`total=6` real partitions found on
  the hashed id `1243966390`), and the safety gate did its job — the Scaler held in `PreScaling`/`Failed` and
  the broker STS was **not** decremented, so nothing was left under-replicated.

### 14. `post_scale` broker unregister — implemented natively, the last stubbed step of the procedure
`post_scale` was a no-op stub, so a drained broker's registration lingered in the (KRaft) cluster metadata as a
fenced entry after its pod was gone — the drain's final cleanup step was missing. Now closed end-to-end, and
it made the **operator=topology / agent=protocol** split concrete a second time: the operator holds no Kafka
credentials, so `post_scale` cannot unregister inline. It gates `PostScaling` on a **second AgentRequest**
(`{scaler}-unregister-gen<N>`, new `ActionType::Unregister`) exactly as `pre_scale` gates the drain, and the
credentialed agent executes it via a new native `unregister_brokers` op in `tokio-kafka`
(`UnregisterBroker`(64), one request per id). Idempotent by construction — an already-gone broker replies
`BrokerIdNotRegistered`(102), treated as success — so it's reconcile-/restart-safe.
- **Why a second request and not the drain's**: the drain AgentRequest gates `PreScaling` and reaches `Done`
  *before* the STS shrinks; unregister must run *after* the pod is gone. Folding it into the drain would
  deadlock (unregister needs the pod gone → needs `Done` → the STS decrement → which needs `Done`). The
  `PreScaling`(drain) → `Scaling`(decrement) → `PostScaling`(unregister) ordering is the natural home for it.
- **Transient-vs-terminal reused**: the agent classifies unregister errors with the same `Error::is_retryable`
  layer as the drain — a still-expiring broker session (unregister runs right after the pod is deleted) is
  transient → requeue + gate holds; only a genuinely terminal error → `Failed`. So the previously-silent gap is
  now either completed or surfaced, never ignored.
- **CRD note**: `ActionType::Unregister` is a new variant on the generic `AgentRequest` CRD (installed by
  commons-operator), so its CRD must be regenerated alongside the `status.agent` one before the live run.
- This was the one remaining SPIKE-TODO with a real *correctness* claim (vs. observability/GC housekeeping).

---

## Live-run boundary (what only a cluster resolves)
Everything compiles and is structurally faithful; these need a live KRaft Kafka (`make run-dev` + kuttl):
- **tokio-kafka client** (R3): `ApiVersions` v3→v0 fallback; **controller routing** (re-dial the `controller_id`
  broker — only actually exercised on a ZK-mode cluster); the `ListPartitionReassignments` completion semantics
  that drive `Done` (finding #7); `DeleteTopics` dual-field (`topic_names` vs `topics`) version behaviour;
  broker-id ↔ pod-ordinal mapping; **reading bootstrap from the mounted discovery ConfigMap**, incl. the
  transient-empty → requeue path (brokers start at 0 in the kuttl test, so the CM is empty until scale-up).
- **Scaler hooks (operator side)**: `post_scale` broker unregister is now implemented natively (finding #14),
  but it's a new **live frontier** — it runs after the STS shrinks (still-expiring broker session), and the
  native `UnregisterBroker`(64) round-trip is unproven against a real controller; expect to iterate on the
  first run like the drain. Still open: propagate `scaling_condition` into KafkaCluster status (logged only);
  AgentRequest owner-ref + honouring `ttlSecondsAfterFinished`.
- **Agent RBAC**: the operator minting the agent's namespaced Role trips k8s privilege-escalation — rework to
  bind a pre-defined `clusterrole-agent.yaml` (like `clusterrole-product`) rather than create a Role.
- **Images/deploy**: a second `agent` OCI image (Dockerfile/Nix/Tilt/Makefile) is not yet wired — only the
  workspace compiles; `AGENT_IMAGE` is threaded through the operator but the image itself isn't built. And the
  operator-rs path-patch must be consumable by the image build (build context / personal fork).

## Upstream asks this spike motivates
1. **operator-rs Scaler**: `ScalingContext` should expose its `Scaler` (name/uid) — finding #2; a `PreScaling`
   deadline / agent-liveness check so a stuck out-of-process hook auto-fails — finding #3; bless the
   "hook delegates to an out-of-process agent via `AgentRequest`" pattern as first-class (progress channel).
2. **feat/autoscale**: fix the `cluster_resource_impl.rs` `~` bug; consider rebasing it onto a current base.
3. **Ecosystem**: `AlterPartitionReassignments`/`ListPartitionReassignments` are missing from librdkafka — either
   contribute them (benefits all C/rdkafka users) or standardise a small pure-Rust admin client. Finding #7/#9.
   The R3 `tokio-kafka` crate (finding #11) is a concrete candidate for the latter — a reusable, mode-agnostic
   admin client we own end-to-end, if SDP wants to grow it beyond this spike's admin subset.

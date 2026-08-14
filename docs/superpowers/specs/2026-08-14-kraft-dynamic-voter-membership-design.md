# KRaft dynamic voter membership (scale-up / scale-down)

Status: approved for planning
Date: 2026-08-14
Branch this was designed on: `main` @ `5211842`

## Problem

Apache Kafka's KRaft dynamic quorum (KIP-853) requires an explicit
follow-up step to change the voter set of an already-formed quorum:
`kafka-metadata-quorum.sh add-controller` to admit a new controller,
`remove-controller` to retire one. The Stackable Kafka operator
currently only performs the one-time `--initial-controllers` step at
`kafka-storage.sh format` time. Any controller pod added after initial
cluster formation registers itself and starts up, but never leaves the
Raft `observer` state — it can never become `leader`/`follower`/`voted`,
so it never becomes healthy, and there is no supported way to remove a
controller from the voter set either. This is documented today as a
flat "do not scale controller replicas on a running cluster" limitation
in `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`.

Goal: let `spec.controllers.roleGroups.<name>.replicas` be scaled up
and down on a running cluster, with the voter set kept in sync
automatically.

## Scope

- In scope: admitting new controllers to the voter set on scale-up,
  removing controllers from the voter set on scale-down, for live
  replica-count changes on an already-formed cluster.
- Out of scope: whole-cluster graceful-deletion draining (a
  finalizer-based mechanism is a separate concern from live replica
  changes and is not addressed here). ZooKeeper-to-KRaft migration is
  unaffected. Kerberos support for KRaft is unaffected.
- This design was developed independently of, and does not reuse code
  from, any prior unmerged work on a KRaft "quorum health gate" or
  graceful-teardown finalizer — that work was discarded (local
  branches deleted) before this design was started.

## Non-goals / explicitly rejected approaches

- **Operator-side kube-exec.** An earlier direction had the operator
  itself exec into pods (`pods/exec`) to run
  `kafka-metadata-quorum.sh`, gated by new reconcile phases (a
  pre-`build` gate clamping the effective replica count on scale-down,
  a post-`apply` phase admitting new voters on scale-up). This was
  rejected in favor of the sidecar approach below: it needed new RBAC,
  a new "live cluster" client capability the operator has never had,
  and two new reconcile phases, none of which are needed once the pods
  manage their own membership.
- **`controller.quorum.auto.join.enable`.** Delegates scale-up
  self-promotion entirely to Kafka with zero new operator capability,
  but doesn't address scale-down at all (still needs an active
  `remove-controller` step), and gives up visibility into *why*
  admission might be stuck. Not chosen because scale-down still needs
  the same sidecar mechanism anyway, so this would only save the
  add-controller half of the problem while adding a version dependency
  to check.

## Design

### Architecture

The operator gains **no new awareness of live quorum state**. The
existing reconcile pipeline (`dereference → validate → build → apply →
update_status`) is untouched. All quorum membership management is
delegated to the controller pods themselves, via:

1. A new sidecar container, controller-role-only, reusing the `kafka`
   product image (so `kafka-metadata-quorum.sh` and the TLS trust
   material already mounted for the `kafka` container are available
   without new volumes).
2. A `preStop` lifecycle hook on that sidecar.
3. `podManagementPolicy: OrderedReady` on the controller StatefulSet
   (currently `Parallel`).

Both the sidecar and the `preStop` hook are only added for Kafka
versions that support KIP-853 dynamic quorum tooling — mirrors the
existing per-version special-casing already present around
`--initial-controllers` for 3.7.x. Older versions get no sidecar at
all and keep today's documented "unsupported" behavior.

### Components

**Add-loop script** (the sidecar's main process, runs for the pod's
whole lifetime):

- Polls the local JMX Prometheus metrics endpoint (the same
  `kafka_server_raft_metrics_current_state` series the existing
  readiness probe already reads) on a short interval.
- While state is `observer`, runs
  `kafka-metadata-quorum.sh add-controller` against
  `controller.quorum.bootstrap.servers` (this must be invoked locally
  on the joining node — it reads local KRaft directory state
  automatically, which also sidesteps the fake placeholder directory-id
  used by `KafkaPodDescriptor::as_voter()` at format time; that
  placeholder was flagged during design exploration as a hazard for
  any tooling that validates directory ids, but `add-controller` does
  not consume it).
- Treats "already a voter" responses as success and keeps polling at
  the same interval indefinitely (cheap, idempotent, self-healing —
  no persisted state, no operator involvement).

This also resolves what looked like a circular dependency during
design: the existing readiness probe can only pass once raft state
leaves `observer`, so gating admission on pod-readiness would be
circular. The sidecar's loop is independent of the pod's own readiness
state, so there is no cycle.

**Remove script** (the sidecar's `preStop` hook, runs once at
termination):

1. Runs `kafka-metadata-quorum.sh describe --replication` to get the
   current voter list.
2. Checks that removing itself would still leave a majority of the
   *pre-removal* voter count. This check is done explicitly by the
   script — the design does not assume `remove-controller` refuses an
   unsafe removal on Kafka's side.
3. If safe, calls `remove-controller` for itself.
4. The whole hook is bounded by a timeout comfortably inside
   `terminationGracePeriodSeconds`, and always exits `0` — a stuck or
   failed check must never block pod termination indefinitely.

### Data flow

**Scale-up:** an ordinary declarative replica increase on the
controller StatefulSet (no change from today) creates a new pod. Its
`kafka` container boots exactly as today (format + start). Its sidecar
independently loops until it observes itself admitted. The existing
readiness probe starts passing once raft state leaves `observer`. If
multiple controllers are added at once, each pod's sidecar self-admits
independently; Kafka's leader serializes the actual `AddVoter`
application, so no operator-side coordination is required.

**Scale-down:** an ordinary declarative replica decrease (no change
from today). `OrderedReady` means Kubernetes terminates exactly the
highest-ordinal pod, runs its `preStop` hook (self-removal via the
script above), and waits for full termination before considering the
next pod — this is what gives one-at-a-time, majority-checked draining
for a decrease of any size, entirely via a StatefulSet setting. No
Rust-side "gate the effective replica count" logic is needed.

### Error handling

- Transient `add-controller` / `describe` failures (e.g. a leader
  election in flight) are simply retried by the loop on its normal
  interval. There is no alerting path today: the sidecar has no
  Kubernetes API access by design (that's the point — no new RBAC),
  so failures are visible only via `kubectl logs` on the sidecar
  container.
- **Known observability gap:** the sidecar's stdout will *not* be
  picked up by the existing vector log-aggregation pipeline, which
  only tails structured `*.log4j.xml` / `*.log4j2.xml` files written
  by the JVM's own logging config (confirmed by direct inspection of
  the deployed `vector.yaml` ConfigMaps during an unrelated
  investigation). This is a real, known limitation of this design, not
  something papered over — a future iteration could have the sidecar
  write structured lines to a file under the shared log directory to
  get picked up, but that is not included in this design's initial
  scope.
- `preStop` removal timing out or failing (e.g. no reachable leader
  within the grace period): the pod still terminates on schedule. A
  stale voter entry can be left behind in the quorum in that case.
  This is a genuine, stated limitation — recovery in that scenario is
  manual (the same "no supported automated path" caveat that already
  exists in the current docs for quorum-reconfiguration edge cases).

### Testing

- The existing `tests/templates/kuttl/operations-kraft/60-scale-controller-up.yaml.j2`
  / `70-scale-controller-down.yaml.j2` kuttl tests (active on `main`,
  not disabled) should pass once this is implemented. Strengthen their
  `*-assert.yaml.j2` counterparts beyond "StatefulSet reports N/N
  ready" to also verify voter count matches replica count post-scale
  (e.g. via `describe --replication` run from the test's `python-0`
  pod), so the test catches a silently-stuck-in-`observer` regression,
  not just a stuck-not-ready one.
- Unit tests in `rust/operator-binary/src/controller/build/resource/statefulset.rs`,
  mirroring the existing probe tests added in `ec59dab`:
  - the sidecar container is present only on the controller role, and
    only for Kafka versions that support dynamic quorum tooling;
  - the sidecar's `preStop` command matches the expected removal
    script invocation;
  - the controller StatefulSet's `podManagementPolicy` is
    `OrderedReady`.

## Open questions for implementation planning

- Exact minimum Kafka version for the version gate (needs verification
  against Kafka's own KIP-853 tooling maturity, not assumed here).
- Exact script implementation (shell, embedded via ConfigMap vs. an
  inline `bash -c` command similar to the existing probe commands in
  `statefulset.rs`) and its `--command-config` security settings
  (matching whatever TLS/SASL configuration the `kafka` container
  already uses for its internal listener).
- Whether to close the sidecar-log observability gap noted above as
  part of this work or as explicit follow-up.

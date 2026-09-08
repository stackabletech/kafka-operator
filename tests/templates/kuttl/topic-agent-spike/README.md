# `topic-agent-spike` — integration test

> **Spike / throwaway.** Proof artifact for the "agent per product cluster" + Scaler-based scale-down
> design (decision.md / decisions#94, epics #865/#872). Exercises the full topic lifecycle and the gated
> broker drain end-to-end against a live Kafka, including failure paths. Not intended to ship.

## What it proves

| Area | Proven | Steps |
|---|---|---|
| Deploy | `platformAccess` ⇒ operator deploys the per-cluster **kafka-agent** (Deployment + liveness Lease) + a **Scaler** per broker role group | 20 |
| Scale-up | Scaler drives the broker STS up (no-drain path; **no** AgentRequest created) | 30 |
| Topic create | agent provisions the `KafkaTopic` natively via `tokio-kafka` (mTLS wire protocol) — verified **in Kafka** (partitions/RF/config) | 40 |
| Topic update | growing partitions + config change hits the "topic exists" branch (`CreatePartitions`/`IncrementalAlterConfigs`) — verified in Kafka | 45–47 |
| Drain | gated scale-down: `preScaling` → AgentRequest → real `AlterPartitionReassignments` → STS shrinks only after `Done`, then `postScaling` → a 2nd (`unregister`) AgentRequest → `UnregisterBroker` removes the drained brokers' stale registrations; **verified mid-flight and at rest**, plus no data loss | 50–70 |
| Repeat drain | a 2nd drain gets its own `gen<N>` AgentRequest and doesn't collide with the retained first (R4 unique-name + retain) | 75–77 |
| Below-RF | draining below the replication factor **fails safe** — Scaler `Failed`, STS **not** decremented (the fix for the silent-Done under-replication bug) | 80 |
| Delete | deleting the CR runs the finalizer → topic gone from Kafka | 85 |
| Agent down | killing the agent → operator surfaces `AgentUnavailable` on the KafkaCluster status | 90 |

## Flow

```
10  AuthenticationClass + SecretClass (autoTLS)
20  KafkaCluster: controllers=1, brokers=0, platformAccess     assert: controllers 1/1 · Scaler · agent Deploy · Lease
30  scale scaler --replicas=3   (scale-up)                     assert: broker STS 3 · Scaler 3 · NO AgentRequest
40  create KafkaTopic (6 partitions, RF2, retention.ms)        assert: Available=True · in-Kafka describe: 6/RF2/retention
45  produce 100k messages (real data to move)
47  update KafkaTopic (partitions 6→9, retention change)       assert: in-Kafka describe: 9 partitions · new retention
50  scale scaler --replicas=2   (the gated DRAIN)              assert(mid): Scaler preScaling · AgentRequest InProgress total>0
60    (postScaling: unregister the drained broker)             assert(end): Scaler idle/2 · drain+unregister AgentRequests Done · STS 2/2
70                                                             assert(kafka): no under-replicated · no ongoing reassign · offsets==100k
75  scale scaler --replicas=3   (bring a broker back)          assert: STS 3 · Scaler 3
77  scale scaler --replicas=2   (2nd drain)                    assert: 2 distinct scaledown AgentRequests · both Done · Scaler idle/2
80  scale scaler --replicas=1   (below-RF, must fail)          assert: Scaler Failed · AgentRequest Failed · STS STILL 2
85  delete KafkaTopic                                          assert: CR gone · topic gone from Kafka
90  scale deploy/...-kafka-agent --replicas=0  (kill agent)    assert: KafkaCluster status.agent.available == false
```

## How the flow steps are verified (and the one flaky spot)

- The `preScaling`/`InProgress` states are **sustained for the whole drain** (not instantaneous), and step
  45 produces ~50 MB so the reassignment copies real data — so a plain assert (step 50) reliably observes
  them. **Step 50 is the single timing-sensitive assert**: if a drain ever finishes before kuttl's first
  poll, it misses `preScaling`. Mitigations: the produced data widens the window; the durable
  `progress.total` at step 60 proves the same thing. If it flaps, bump `--num-records`/`--record-size` in
  `45-produce`, or relax 50-assert to only the durable evidence.
- **Prefer resource-stanza assertions over `commands:`.** kuttl polls a resource stanza (subset match)
  *silently*, but re-echoes a `commands:` assert's whole `script:` on *every* retry until it passes — the
  source of log spam. So the drain/unregister/Scaler checks are written as stanzas wherever possible. This
  works because the spike is **deterministic**: the `AgentRequest` names are predictable (`{scaler}-
  {scaledown,unregister}-gen<N>`, where `N` is the Scaler's `metadata.generation` — it bumps once per
  `kubectl scale`, so 50=gen3, 77=gen5, 80=gen6), so they can be asserted by *name* rather than a jsonpath
  list-filter. The Scaler's complex-enum `status.state` is asserted as a nested-map stanza
  (`state: {idle: {}}` / `{preScaling: {}}` / `{failed: {}}`).
- `commands:` remain only where a stanza genuinely can't express the check: **Kafka CLI** via `kubectl exec`
  (40/47/70/85), a **`> 0`** comparison (50's `progress.total`, the node-id regression guard), and an
  **absence/count** (30's "no AgentRequest"). For those, the explanatory comments live as YAML comments
  *above* the `script:` (not inside it, where they'd be echoed each retry).
- **Pacing:** each slow assert is preceded by a realistic `sleep` — in the action step, or a dedicated
  `NN-wait` step for pure asserts like `60` — so kuttl starts polling near when the assert will be true
  instead of spinning through the whole settle window.
- The **post-scale unregister** (`postScaling`) is verified by its own `unregister` AgentRequest reaching
  `Done` (step 60) — the agent only writes `Done` after `UnregisterBroker` succeeded, so this is the same
  proof standard as the drain. A *Kafka-metadata*-level check (the drained id no longer registered) would
  need `kafka-metadata-shell` reading the `__cluster_metadata` snapshot — no stock CLI lists fenced-but-
  registered brokers — so it's out of scope for this CLI-based test. Note also that unregister is the drain's
  **live frontier**: it runs after the STS shrinks, so a still-expiring broker session can make the first
  attempt transient-fail — the agent requeues (it does *not* fail the request), and the `postScaling` gate
  simply holds until it succeeds.
- Kafka CLI runs via `kubectl exec` into `test-kafka-broker-default-0` using the operator-provisioned
  `/stackable/config/client.properties`; bootstrap is the **FQDN**
  `test-kafka-broker-default-bootstrap.$NAMESPACE.svc.cluster.local:9093` (the short name / `localhost` fail
  TLS hostname verification — the cert SANs are FQDNs).

## Running

Cannot run in the plain sandbox — needs kind + the running operators.

```bash
make regenerate-charts        # REQUIRED: the AgentUnavailable feature added status.agent to the KafkaCluster
                              # CRD; without regenerating, the field is pruned and step 90 never passes
make run-dev                  # Tilt: kafka-operator + agent image + commons-operator (Scaler/AgentRequest CRDs)
                              #       + secret-operator + listener-operator
./scripts/run-tests --skip-release --test topic-agent-spike
```

**Notes:**
- **Deploy order:** commons-operator up (its `Scaler` + `AgentRequest` CRDs Established) *before* kafka-operator.
- **Resources:** this test runs 1 controller + up to 3 broker JVMs + produces data on one kind node — size the
  node's CPU generously or the controllers thrash on liveness probes (env-only; see spike-findings). Controllers
  are deliberately `replicas: 1` for this reason.
- **CLI flags** (`kafka-get-offsets.sh`, `--under-replicated-partitions`, perf-test props) may need per-version
  tweaks on the first live run — these are authored against Kafka 4.2 and not yet run green in CI.

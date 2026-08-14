Tests Kraft cluster operations:

- Cluster stop/pause/restart
- Scale brokers up/down
- Scale controllers up/down

Notes

- Kafka 3.7 controllers do not scale at all.
  The scaling test steps are disabled for this version.
- Scaling controllers from 3 -> 1 doesn't work.
  Both brokers and controllers try to communicate with old controllers.
  This is why, the last step scales from 5 -> 3 controllers.
  This at least, leaves the cluster in a working state.
  This was not re-tested after the `quorum-manager` sidecar was added (see
  `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`); this suite only ever
  exercises 3 -> 5 -> 3, so this caveat is left in place until scaling down to 1 is
  actually covered by a test.

Tests Kraft cluster operations:

- Cluster stop/pause/restart
- Scale brokers up/down
- Scale controllers up/down

Notes

- Kafka 3.7.x is not supported for KRaft mode at all (see
  `docs/modules/kafka/pages/usage-guide/kraft-controller.adoc`), so this suite is not run
  against it.
- Scaling controllers down to a single replica is not verified under the sidecar-based
  mechanism described in `kraft-controller.adoc`; this suite only ever exercises 3 -> 5 -> 3,
  so the last step scales back to 3 (not 1), leaving the cluster in a state this suite has
  actually tested.

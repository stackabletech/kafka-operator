# Kraft + Kerberos test

Proves that a KRaft-mode Kafka cluster (`spec.controllers` present, no ZooKeeper) can be
secured with Kerberos authentication (`spec.clusterConfig.authentication` referencing a
Kerberos `AuthenticationClass`) end to end: controllers form a quorum, brokers join, and a
client can authenticate via GSSAPI to produce/consume a message.

This bundles the KRaft cluster setup from `smoke-kraft` with the KDC deployment,
`SecretClass`/`AuthenticationClass` and produce/consume job from `kerberos`.

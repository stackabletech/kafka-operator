
### Setup

k create --save-config -f docs/modules/kafka/examples/mirror_maker/01-setup-source.yaml
k create --save-config -f docs/modules/kafka/examples/mirror_maker/02-setup-target.yaml

k cp -n mm-migration -c kafka target-broker-default-0:/stackable/tls-kafka-server/keystore.p12 docs/modules/kafka/examples/mirror_maker/keystore.p12
k cp -n mm-migration -c kafka target-broker-default-0:/stackable/tls-kafka-server/truststore.p12 docs/modules/kafka/examples/mirror_maker/truststore.p12

k cp -n mm-migration -c kafka docs/modules/kafka/examples/mirror_maker/truststore.p12 source-broker-default-0:/stackable/truststore.p12
k cp -n mm-migration -c kafka docs/modules/kafka/examples/mirror_maker/keystore.p12 source-broker-default-0:/stackable/keystore.p12

k cp -n mm-migration -c kafka docs/modules/kafka/examples/mirror_maker/mm.properties source-broker-default-0:/stackable/mm.properties

### Create a topic and publish some data

/stackable/kafka/bin/kafka-topics.sh --create --topic test --partitions 1 --bootstrap-server source-broker-default-bootstrap.mm-migration.svc.cluster.local:9093 --command-config /stackable/config/client.properties

/stackable/kafka/bin/kafka-producer-perf-test.sh --producer-props bootstrap.servers=source-broker-default-bootstrap.mm-migration.svc.cluster.local:9093 --payload-monotonic --throughput 1 --num-records 100 --producer.config /stackable/config/client.properties --topic test

/stackable/kafka/bin/kafka-console-consumer.sh --bootstrap-server source-broker-default-bootstrap.mm-migration.svc.cluster.local:9093 --consumer.config /stackable/config/client.properties --topic test --offset earliest --partition 0 --timeout-ms 10000

### Run MirrorMaker

EXTRA_ARGS="" /stackable/kafka/bin/connect-mirror-maker.sh /stackable/mm.properties

### Verify the topic is mirrored

/stackable/kafka/bin/kafka-topics.sh --list --bootstrap-server target-broker-default-bootstrap.mm-migration.svc.cluster.local:9093 --command-config /stackable/config/client.properties

/stackable/kafka/bin/kafka-console-consumer.sh --bootstrap-server target-broker-default-bootstrap.mm-migration.svc.cluster.local:9093 --consumer.config /stackable/config/client.properties --topic source.test --offset earliest --partition 0 --timeout-ms 10000

### Cleanup

k delete -n mm-migration kafkaclusters source
k delete -n mm-migration kafkaclusters target
k delete -n mm-migration zookeeperznodes source-znode
k delete -n mm-migration zookeeperclusters zookeeper
k delete -n mm-migration secretclasses source-internal-tls
k delete -n mm-migration secretclasses source-client-auth-secret
k delete -n mm-migration secretclasses target-internal-tls
k delete -n mm-migration secretclasses target-client-auth-secret
k delete -n mm-migration authenticationclasses target-client-auth
k delete -n mm-migration authenticationclasses source-client-auth
k delete -n mm-migration persistentvolumeclaims --all
k delete ns mm-migration

#!/usr/bin/env bash
set -euo pipefail

# DO NOT EDIT THE SCRIPT
# Instead, update the j2 template, and regenerate it for dev with `make render-docs`.

# The getting started guide script
# It uses tagged regions which are included in the documentation
# https://docs.asciidoctor.org/asciidoc/latest/directives/include-tagged-regions/
#
# There are two variants to go through the guide - using stackablectl or helm
# The script takes either 'stackablectl' or 'helm' as an argument
#
# The script can be run as a test as well, to make sure that the tutorial works
# It includes some assertions throughout, and at the end especially.

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 25.11.0
helm install --wait secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 25.11.0
helm install --wait listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 25.11.0
helm install --wait zookeeper-operator oci://oci.stackable.tech/sdp-charts/zookeeper-operator --version 25.11.0
helm install --wait kafka-operator oci://oci.stackable.tech/sdp-charts/kafka-operator --version 25.11.0
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=25.11.0 \
  secret=25.11.0 \
  listener=25.11.0 \
  zookeeper=25.11.0 \
  kafka=25.11.0
# end::stackablectl-install-operators[]
;;
*)
echo "Need to provide 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Installing ZooKeeper from zookeeper.yaml"
# tag::install-zookeeper[]
kubectl apply -f zookeeper.yaml
# end::install-zookeeper[]

echo "Installing ZNode from kafka-znode.yaml"
# tag::install-znode[]
kubectl apply -f kafka-znode.yaml
# end::install-znode[]

sleep 15

echo "Awaiting ZooKeeper rollout finish"
# tag::watch-zookeeper-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-zk-server-default
# end::watch-zookeeper-rollout[]

echo "Install KafkaCluster from kafka.yaml"
# tag::install-kafka[]
kubectl apply --server-side -f kafka.yaml
# end::install-kafka[]

sleep 15

echo "Awaiting Kafka rollout finish"
# tag::watch-kafka-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-kafka-broker-default
# end::watch-kafka-rollout[]

echo "Starting port-forwarding of port 9092"
# shellcheck disable=2069 # we want all output to be blackholed
# tag::port-forwarding[]
kubectl port-forward svc/simple-kafka-broker-default-bootstrap 9092 2>&1 >/dev/null &
# end::port-forwarding[]
PORT_FORWARD_PID=$!
# shellcheck disable=2064 # we want the PID evaluated now, not at the time the trap is
trap "kill $PORT_FORWARD_PID" EXIT

sleep 15

echo "Creating test topic test-data-topic"
# tag::create-topic[]
kubectl exec -n default simple-kafka-broker-default-0 -c kafka -t -- /stackable/kafka/bin/kafka-topics.sh \
--create \
--topic test-data-topic \
--partitions 1 \
--bootstrap-server localhost:9092
# end::create-topic[]

echo "Publish test data"
# tag::write-data[]
kubectl exec -n default simple-kafka-broker-default-0 -c kafka -t -- /stackable/kafka/bin/kafka-producer-perf-test.sh \
--producer-props bootstrap.servers=localhost:9092 \
--topic test-data-topic \
--payload-monotonic \
--throughput 1 \
--num-records 5
# end::write-data[]

echo "Consume test data"
# tag::read-data[]
kubectl exec -n default simple-kafka-broker-default-0 -c kafka -t -- /stackable/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic test-data-topic \
--offset earliest \
--partition 0 \
--timeout-ms 1000
# end::read-data[]

echo "Success!"

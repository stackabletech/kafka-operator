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
echo "Adding 'stackable-stable' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-stable https://repo.stackable.tech/repository/helm-stable/
# end::helm-add-repo[]
echo "Updating Helm repositories"
helm repo update
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-stable/commons-operator --version 24.11.1
helm install --wait secret-operator stackable-stable/secret-operator --version 24.11.1
helm install --wait listener-operator stackable-stable/listener-operator --version 24.11.1
helm install --wait zookeeper-operator stackable-stable/zookeeper-operator --version 24.11.1
helm install --wait kafka-operator stackable-stable/kafka-operator --version 24.11.1
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=24.11.1 \
  secret=24.11.1 \
  listener=24.11.1 \
  zookeeper=24.11.1 \
  kafka=24.11.1
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

echo "Creating test data"
# tag::kcat-create-data[]
echo "some test data" > data
# end::kcat-create-data[]

echo "Writing test data"
# tag::kcat-write-data[]
kcat -b localhost:9092 -t test-data-topic -P data
# end::kcat-write-data[]

echo "Reading test data"
# tag::kcat-read-data[]
kcat -b localhost:9092 -t test-data-topic -C -e > read-data.out
# end::kcat-read-data[]

echo "Check contents"
# tag::kcat-check-data[]
grep "some test data" read-data.out
# end::kcat-check-data[]

echo "Cleanup"
# tag::kcat-cleanup-data[]
rm data
rm read-data.out
# end::kcat-cleanup-data[]

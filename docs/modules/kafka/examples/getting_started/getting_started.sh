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

echo "Waiting for node(s) to be ready..."
kubectl wait node --all --for=condition=Ready --timeout=120s

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 0.0.0-dev
helm install --wait secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 0.0.0-dev
helm install --wait listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 0.0.0-dev
helm install --wait kafka-operator oci://oci.stackable.tech/sdp-charts/kafka-operator --version 0.0.0-dev
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.0.0-dev \
  secret=0.0.0-dev \
  listener=0.0.0-dev \
  kafka=0.0.0-dev
# end::stackablectl-install-operators[]
;;
*)
echo "Need to provide 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Install KafkaCluster from kafka.yaml"
# tag::install-kafka[]
kubectl apply --server-side -f kafka.yaml
# end::install-kafka[]

sleep 15

echo "Awaiting Kafka rollout finish"
# tag::watch-kafka-rollout[]
kubectl rollout status --watch --timeout=5m statefulset/simple-kafka-controller-default
kubectl rollout status --watch --timeout=5m statefulset/simple-kafka-broker-default
# end::watch-kafka-rollout[]

echo "Awaiting the Kafka agent"
# tag::watch-agent-rollout[]
kubectl rollout status --watch --timeout=5m deployment/simple-kafka-agent
# end::watch-agent-rollout[]

echo "Creating test topic test-data-topic from kafka-topic.yaml"
# tag::create-topic[]
kubectl apply -f kafka-topic.yaml
# end::create-topic[]

echo "Awaiting the KafkaTopic"
# tag::wait-topic[]
kubectl wait --for=condition=Ready --timeout=5m kafkatopics.kafka.stackable.tech/test-data-topic
# end::wait-topic[]

echo "Publish test data"
# shellcheck disable=SC2016 # the script is expanded in the broker Pod, not here
# Capture the output of the producer and only show it if sending failed.
produced="$(
{
# tag::write-data[]
kubectl exec -n default simple-kafka-broker-default-0 -c kafka -- bash -c '
  bootstrap="$(cat /stackable/listener-broker/default-address/address):$(cat /stackable/listener-broker/default-address/ports/kafka-tls)"
  /stackable/kafka/bin/kafka-producer-perf-test.sh \
    --producer-props bootstrap.servers="$bootstrap" \
    --producer.config /stackable/config/client.properties \
    --topic test-data-topic \
    --payload-monotonic \
    --throughput 1 \
    --num-records 5
'
# end::write-data[]
} 2>&1
)"

if ! grep -q '^5 records sent,' <<< "$produced"; then
  echo "ERROR: expected 5 records to be sent, producer output:" >&2
  echo "$produced" >&2
  exit 1
fi

echo "Consume test data"
# shellcheck disable=SC2016 # the script is expanded in the broker Pod, not here
# The consumer exits with 0 even if it times out without reading anything,
# so capture its output and verify the summary line explicitly below.
consumed="$(
{
# tag::read-data[]
kubectl exec -n default simple-kafka-broker-default-0 -c kafka -- bash -c '
  bootstrap="$(cat /stackable/listener-broker/default-address/address):$(cat /stackable/listener-broker/default-address/ports/kafka-tls)"
  /stackable/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$bootstrap" \
    --consumer.config /stackable/config/client.properties \
    --topic test-data-topic \
    --offset earliest \
    --partition 0 \
    --max-messages 5 \
    --timeout-ms 30000
'
# end::read-data[]
} 2>&1
)"

if ! grep -qx 'Processed a total of 5 messages' <<< "$consumed"; then
  echo "ERROR: expected 5 consumed messages, consumer output:" >&2
  echo "$consumed" >&2
  exit 1
fi

echo "Success!"

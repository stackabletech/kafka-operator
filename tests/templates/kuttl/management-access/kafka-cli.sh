#!/usr/bin/env bash
# Usage: kafka-cli.sh <tool> <arguments>, e.g. kafka-cli.sh kafka-topics.sh --describe --topic orders
# Runs a Kafka CLI tool in the broker Pod against the brokers.
set -euo pipefail

# The script is expanded in the broker Pod, not here.
# shellcheck disable=SC2016
kubectl exec --namespace "$NAMESPACE" test-kafka-broker-default-0 --container kafka -- bash -c '
  bootstrap="$(cat /stackable/listener-broker/default-address/address):$(cat /stackable/listener-broker/default-address/ports/kafka-tls)"
  tool=$1
  shift
  "/stackable/kafka/bin/$tool" --bootstrap-server "$bootstrap" --command-config /stackable/config/client.properties "$@"
' kafka-cli "$@"

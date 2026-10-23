#!/usr/bin/env bash
# Usage: topic-condition.sh <KafkaTopic> <condition type> <status> <reason>
# Fails unless the condition of the KafkaTopic has the given status and reason.
set -euo pipefail

actual=$(kubectl get kafkatopic --namespace "$NAMESPACE" "$1" \
  --output "jsonpath={.status.conditions[?(@.type==\"$2\")].status}/{.status.conditions[?(@.type==\"$2\")].reason}")
if [ "$actual" != "$3/$4" ]; then
  echo "$1: expected the condition $2 to be $3/$4, but it is \"$actual\""
  exit 1
fi

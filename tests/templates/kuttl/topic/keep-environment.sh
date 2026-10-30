#!/usr/bin/env bash
# Creates the KafkaCluster and a KafkaTopic again after the deletion steps, if kuttl was started
# with --skip-delete, so that the kept namespace has a running cluster to experiment with. kuttl
# does not expose the flag to test steps, hence looking for it in the argv of the kuttl ancestor.
set -euo pipefail

pid=$PPID
while [ "${pid:-0}" -gt 1 ]; do
  args=$(ps -o args= -p "$pid" 2>/dev/null || true)
  case "$args" in
    *kuttl*--skip-delete*) break ;;
  esac
  pid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)
done
if [ "${pid:-0}" -le 1 ]; then
  echo "kuttl deletes the namespace, nothing to keep"
  exit 0
fi

# The cluster id is the name of the KafkaCluster, so the topics in the kept PersistentVolumeClaims
# are back once the cluster runs.
kubectl apply --namespace "$NAMESPACE" --filename 10-install-kafka.yaml
kubectl apply --namespace "$NAMESPACE" --filename 98_topic.yaml

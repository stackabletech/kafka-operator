#!/usr/bin/env bash
# Usage: test_heap.sh

# 1Gi * 0.8 -> 819
EXPECTED_HEAP=-"Xmx819m"

# Check if ZK_SERVER_HEAP is set to the correct calculated value
if [[ $KAFKA_HEAP_OPTS == "$EXPECTED_HEAP" ]]
then
  echo "[SUCCESS] KAFKA_HEAP_OPTS set to $EXPECTED_HEAP"
else
  echo "[ERROR] KAFKA_HEAP_OPTS not set or set with wrong value: $ZK_SERVER_HEAP"
  exit 1
fi

echo "[SUCCESS] All heap settings tests successful!"

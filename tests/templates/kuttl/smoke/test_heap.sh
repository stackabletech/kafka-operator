#!/usr/bin/env bash
# Usage: test_heap.sh

# 2Gi * 0.8 -> 1638
EXPECTED_HEAP="-Xmx1638m -Xms1638m"

# Check if KAFKA_HEAP_OPTS is set to the correct calculated value
if [[ $KAFKA_HEAP_OPTS == "$EXPECTED_HEAP" ]]
then
  echo "[SUCCESS] KAFKA_HEAP_OPTS set to $EXPECTED_HEAP"
else
  echo "[ERROR] KAFKA_HEAP_OPTS not set or set with wrong value: $KAFKA_HEAP_OPTS"
  exit 1
fi

echo "[SUCCESS] All heap settings tests successful!"

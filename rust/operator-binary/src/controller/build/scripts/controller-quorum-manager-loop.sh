#!/usr/bin/env bash
# The `quorum-manager` sidecar's main loop: while this controller's local Raft state is
# `observer`, admit it into the KRaft voter set.
#
# If the controller is `leader`, `follower` or `unattched` it does nothing.
#
# It has a special case handling when the second controller is added to the voter list.
# In that case, it polls the metrics endpoint `STABILITY_REQUIRES_POLLS` times before
# adding the controller.
# This is a precaution mechanism to ensure that a quorum with two voters stays healthy.
# A quorum with two voters is problematic in Kraft because none of them should fail.
# KRaft redundancy really only starts at a quorum of 3.
#
# Inputs:
#   REPLICA_ID                  this pod's KRaft `node.id`
#   BOOTSTRAP_SERVERS           the `controller.quorum.bootstrap.servers` to talk to
#   QUORUM_CLI                  path to `kafka-metadata-quorum.sh`
#   ADMIN_CLIENT_CONFIG         `--command-config` used for read-only `describe` calls
#   ADD_CONTROLLER_CONFIG       `--command-config` used for `add-controller` (see
#                               `ADD_CONTROLLER_PROPERTIES_PATH`: it must also carry this
#                               controller's own `node.id`/listener config)
#   METRICS_URL                 this controller's own Prometheus metrics endpoint
#   CLI_TIMEOUT_SECONDS         wall-clock bound for a single CLI call
#   CLI_KILL_AFTER_SECONDS      grace period before `timeout` escalates to `SIGKILL`
#   POLL_INTERVAL_SECONDS       pause between two iterations
#   STABILITY_REQUIRED_POLLS    consecutive healthy polls required before the *first* voter
#                               is joined by a second one (see below)
#   VOTER_STALE_FETCH_SECONDS   how long an existing voter may go without fetching before the
#                               quorum counts as degraded
#
# This loop runs forever and never exits non-zero on its own: a deferred admission is always
# retried on the next poll.

set -uo pipefail

: "${REPLICA_ID:?must be set by the operator-generated preamble}"
: "${BOOTSTRAP_SERVERS:?must be set by the operator-generated preamble}"
: "${QUORUM_CLI:?must be set by the operator-generated preamble}"
: "${ADMIN_CLIENT_CONFIG:?must be set by the operator-generated preamble}"
: "${ADD_CONTROLLER_CONFIG:?must be set by the operator-generated preamble}"
: "${METRICS_URL:?must be set by the operator-generated preamble}"
: "${CLI_TIMEOUT_SECONDS:?must be set by the operator-generated preamble}"
: "${CLI_KILL_AFTER_SECONDS:?must be set by the operator-generated preamble}"
: "${POLL_INTERVAL_SECONDS:?must be set by the operator-generated preamble}"
: "${STABILITY_REQUIRED_POLLS:?must be set by the operator-generated preamble}"
: "${VOTER_STALE_FETCH_SECONDS:?must be set by the operator-generated preamble}"

ADD_CONTROLLER_PID=""

handle_term_signal() {
  [ -n "$ADD_CONTROLLER_PID" ] && kill -TERM "$ADD_CONTROLLER_PID" 2>/dev/null
  exit 0
}

trap 'handle_term_signal' TERM

# This controller's own Raft state, as reported by its metrics endpoint. Empty when the
# endpoint could not be scraped (the Kafka process is still starting, or already gone).
local_raft_state() {
  curl -s --max-time 5 --connect-timeout 2 "$METRICS_URL" \
    | grep -oE 'kafka_server_raft_metrics_current_state\{state="[a-z]+",?\}' \
    | grep -oE '"[a-z]+"' \
    | tr -d '"'
}

# The current voter rows of `describe --replication`, one per line, header dropped.
#
# Observers carry a replica state of their own and are deliberately excluded: this pod is
# itself an observer while it waits to be admitted.
current_voters() {
  timeout --kill-after="$CLI_KILL_AFTER_SECONDS" "$CLI_TIMEOUT_SECONDS" "$QUORUM_CLI" \
    --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_CLIENT_CONFIG" \
    describe --replication 2>/dev/null \
    | tail -n +2 \
    | awk '$NF == "Leader" || $NF == "Follower"'
}

# Node ids of voters that have not fetched recently enough to be considered alive.
#
# `LastFetchTimestamp` (column 5) is epoch milliseconds as observed by the *leader*, compared
# here against this pod's own clock. Nodes in a Kubernetes cluster are expected to be roughly
# time-synchronised, and VOTER_STALE_FETCH_SECONDS is generous enough to absorb the usual
# skew; this is a liveness heuristic, not a correctness mechanism.
stale_voters() {
  local voters=$1 now_ms
  now_ms=$(date +%s%3N)

  echo "$voters" | awk -v now="$now_ms" -v max_age="$((VOTER_STALE_FETCH_SECONDS * 1000))" \
    '(now - $5) > max_age { print $1 }'
}

echo "Starting KRaft voter admission loop against bootstrap servers: $BOOTSTRAP_SERVERS"

# Consecutive polls that found this controller up and reporting `observer`. Reset by anything
# that interrupts that run, so a flapping controller never accumulates a stable streak.
stable_polls=0

while true; do
  state=$(local_raft_state)

  if [ -z "$state" ]; then
    echo "Could not determine local Raft state (metrics scrape returned nothing), will retry"
    stable_polls=0
  elif [ "$state" != "observer" ]; then
    echo "Local Raft state is '$state', nothing to do"
    stable_polls=0
  else
    stable_polls=$((stable_polls + 1))

    voters=$(current_voters)
    voter_count=$(echo "$voters" | grep -c .)

    if [ "$voter_count" -eq 0 ]; then
      echo "Local Raft state is observer, but the quorum could not be described (unreachable, or its output was unrecognized); deferring add-controller"
    elif [ -n "$(stale_voters "$voters")" ]; then
      echo "Local Raft state is observer, but the existing quorum is degraded (voter(s) $(stale_voters "$voters" | tr '\n' ' ')have not fetched within ${VOTER_STALE_FETCH_SECONDS}s); deferring add-controller rather than perturbing it"
    elif [ "$voter_count" -eq 1 ] && [ "$stable_polls" -lt "$STABILITY_REQUIRED_POLLS" ]; then
      # Joining the single existing voter makes both nodes load-bearing, so this controller
      # has to prove it stays up first. Waiting costs nothing: the quorum stays at one voter.
      echo "Local Raft state is observer and the quorum has a single voter; proving stability before joining it ($stable_polls/$STABILITY_REQUIRED_POLLS consecutive healthy polls)"
    else
      echo "Local Raft state is observer, attempting add-controller..."
      timeout --kill-after="$CLI_KILL_AFTER_SECONDS" "$CLI_TIMEOUT_SECONDS" "$QUORUM_CLI" \
        --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config "$ADD_CONTROLLER_CONFIG" \
        add-controller &
      ADD_CONTROLLER_PID=$!
      wait "$ADD_CONTROLLER_PID" \
        || echo "add-controller attempt failed (this is expected if it already succeeded or a leader election is in progress), will retry"
      ADD_CONTROLLER_PID=""
    fi
  fi

  sleep "$POLL_INTERVAL_SECONDS" &
  wait $!
done

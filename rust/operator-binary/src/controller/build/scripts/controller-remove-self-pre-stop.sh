#!/usr/bin/env bash
# The `preStop` hook of a KRaft controller pod's `kafka` container: remove this pod from the
# KRaft voter set before it terminates.
#
# The last pod (the one with the lowest `node.id`) is never removed as that would effectively
# lead to all cluster state being lost.
#
# Inputs:
#   REPLICA_ID                this pod's KRaft `node.id`
#   BOOTSTRAP_SERVERS         the `controller.quorum.bootstrap.servers` to talk to
#   QUORUM_CLI                path to `kafka-metadata-quorum.sh`
#   ADMIN_CLIENT_CONFIG       `--command-config` file passed to that CLI
#   CLI_TIMEOUT_SECONDS       wall-clock bound for a single CLI call
#   CLI_KILL_AFTER_SECONDS    grace period before `timeout` escalates to `SIGKILL`
#   REMOVAL_DEADLINE_SECONDS  total budget for retrying the removal
#   RETRY_INTERVAL_SECONDS    pause between two attempts
#
# Always exits 0 (a missing input aside, which is an operator bug): a failed or stuck removal
# must never be the reason a pod fails to terminate.

set -uo pipefail

: "${REPLICA_ID:?must be set by the operator-generated preStop preamble}"
: "${BOOTSTRAP_SERVERS:?must be set by the operator-generated preStop preamble}"
: "${QUORUM_CLI:?must be set by the operator-generated preStop preamble}"
: "${ADMIN_CLIENT_CONFIG:?must be set by the operator-generated preStop preamble}"
: "${CLI_TIMEOUT_SECONDS:?must be set by the operator-generated preStop preamble}"
: "${CLI_KILL_AFTER_SECONDS:?must be set by the operator-generated preStop preamble}"
: "${REMOVAL_DEADLINE_SECONDS:?must be set by the operator-generated preStop preamble}"
: "${RETRY_INTERVAL_SECONDS:?must be set by the operator-generated preStop preamble}"

quorum_cli() {
  timeout --kill-after="$CLI_KILL_AFTER_SECONDS" "$CLI_TIMEOUT_SECONDS" "$QUORUM_CLI" \
    --bootstrap-controller "$BOOTSTRAP_SERVERS" --command-config "$ADMIN_CLIENT_CONFIG" "$@"
}

# One removal attempt.
#
# Returns 0 when there is nothing left to do - either this pod was removed, or it never was
# (or no longer is) a voter, or removing it would leave zero voters. Returns 1 when the
# attempt was inconclusive and is worth retrying while the deadline holds.
attempt_removal() {
  local describe voters total_voters directory_id

  describe=$(quorum_cli describe --replication 2>/dev/null)
  if [ -z "$describe" ]; then
    echo "Could not describe the quorum (unreachable or the call timed out), will retry"
    return 1
  fi

  # Skip the table header, then keep the rows of actual voters. Observers carry a replica
  # state of their own and must not be counted here.
  voters=$(echo "$describe" | tail -n +2 | awk '$NF == "Leader" || $NF == "Follower"')
  total_voters=$(echo "$voters" | grep -c .)
  if [ "$total_voters" -eq 0 ]; then
    echo "Could not identify any voters in the describe output (unrecognized format), skipping removal for safety and retrying..."
    return 1
  fi

  # Fewer than two voters means this pod is the last one, so removing it would leave zero.
  # This can never become safe later during this pod's own termination - nothing else will
  # add a voter on its behalf - so give up instead of retrying until the deadline.
  if [ "$total_voters" -lt 2 ]; then
    echo "Removing self would leave zero voters, skipping (this can't become safe later during my own termination -- nothing else will add a voter for me)"
    return 0
  fi

  directory_id=$(echo "$voters" | awk -v id="$REPLICA_ID" '$1 == id { print $2 }')
  if [ -z "$directory_id" ]; then
    echo "Could not find own node $REPLICA_ID among current voters (already removed?), nothing to do"
    return 0
  fi

  echo "Removing self (node $REPLICA_ID, directory $directory_id) from the voter set..."
  if ! quorum_cli remove-controller \
    --controller-id "$REPLICA_ID" --controller-directory-id "$directory_id"; then
    echo "remove-controller attempt failed, will retry if time remains"
    return 1
  fi

  return 0
}

DEADLINE=$((SECONDS + REMOVAL_DEADLINE_SECONDS))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
  if attempt_removal; then
    exit 0
  fi
  sleep "$RETRY_INTERVAL_SECONDS"
done

# Loud on purpose (`ERROR:`, so it is greppable and alertable in the container logs)
echo "ERROR: could not remove self (node $REPLICA_ID) from the voter set before terminating (every attempt within ${REMOVAL_DEADLINE_SECONDS}s failed or the quorum was unreachable throughout); the on-disk voter set may now list this pod even though it is gone -- if nothing else corrects this, a later restart may get stuck and require manual recovery, see kraft-controller.adoc"
exit 0

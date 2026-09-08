First kafka node in STS creates a 1-node quorum and becomes leader.

quorum-manager sidecar: bash loop that tries to change state from `observer` to `voter` using `kafka-metadata-quorum.sh add-controller`. It always has access to the current state via the main container's metrics endpoint

main container has a SIGTERM hook (bash) that removes Pod from `voters`. Has a check that prevents removing the last voter

STS has `OrderedReady` pod management policy which only restarts a single Pod at the same time.\

Note:
The bash scripts have highly nested if clauses and are hard to read.

If we can't remove the controller after the deadline we give up. Is that preferred to blocking?

If the SIGTERM capture doesn't work or SIGKILL occurs we don't remove the voter.


Agent-based approach:
kafka-agent has read-only access to Kafka STS & Pods
kafka-agent can always query metrics endpoints of controller to get their current state.
Scale up: Watch STS, replica change -> Can upgrade observers to voters.
Scale down: Watch STS, replica change -> Remove node from voters. No deadline/SIGKILL problems
Agent broken = dynamic quorum management is not working.

Broker-draining:

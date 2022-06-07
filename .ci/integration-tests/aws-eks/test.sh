#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/kafka-operator.git
(cd kafka-operator/ && ./scripts/run_tests.sh --parallel 1)
exit_code=$?
./operator-logs.sh kafka > /target/kafka-operator.log
exit $exit_code

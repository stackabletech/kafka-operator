# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Reconciliation errors are now reported as Kubernetes events ([#346]).
- Support for Kafka 3.1.0 ([#347]).
- Use cli parameter `watch-namespace` to specify one(!) namespace to watch

### Changed

- `operator-rs` `0.10.0` → `0.12.0` ([#346]).
- `--kafka-broker-clusterrole` is now only accepted for the `run` subcommand ([#349]).

[#346]: https://github.com/stackabletech/kafka-operator/pull/346
[#347]: https://github.com/stackabletech/kafka-operator/pull/347
[#349]: https://github.com/stackabletech/kafka-operator/pull/349

## [0.5.0] - 2022-02-14

### Changed
- Complete rework ([#256]).

[#256]: https://github.com/stackabletech/kafka-operator/pull/256

## [0.4.0] - 2021-12-06


- `operator-rs` `0.3.0` → `0.4.0` ([#214]).
- `stackable-opa-crd` `0.4.1` → `0.5.0` ([#214]).
- `stackable-zookeeper-crd` `0.4.1` → `0.5.0` ([#214]).
- Adapted pod image and container command to docker image ([#214]).
- Adapted documentation to represent new workflow with docker images ([#214]).

[#214]: https://github.com/stackabletech/kafka-operator/pull/214

## [0.3.0] - 2021-10-27

### Added
- Added versioning code from operator-rs for up and downgrades ([#167]).
- Added `ProductVersion` to status ([#167]).
- Added `Condition` to status ([#167]).
- Use sticky scheduler ([#181])
- Added support for Start, Stop Restart commands ([#194]).

### Changed
- `operator-rs` `0.2.2` → `0.3.0` ([#207]).
- `stackable-zookeeper-crd`: `0.2.0` → `0.4.1` ([#207]).
- `stackable-opa-crd`: `0.2.0` → `0.4.1` ([#207]).
- `kube-rs`: `0.58` → `0.60` ([#167]).
- `k8s-openapi` `0.12` → `0.13` and features: `v1_21` → `v1_22` ([#167]).
- `stackable-zookeeper-crd::util` to `stackable-zookeeper-crd::discovery` ([#194]).
- Moved CRD availability check to operator-binary ([#194]).

### Removed

- `kube-runtime` dependency ([#167]).

[#207]: https://github.com/stackabletech/kafka-operator/pull/207
[#167]: https://github.com/stackabletech/kafka-operator/pull/167
[#181]: https://github.com/stackabletech/kafka-operator/pull/181
[#194]: https://github.com/stackabletech/kafka-operator/pull/194

## [0.2.1] - 2021-09-14

- Fixed Dockerfile to use the correct binary ([#167])

[#167]: https://github.com/stackabletech/kafka-operator/pull/167

## [0.2.0] - 2021.09.10

### Changed
- **Breaking:** Repository structure was changed and the -server crate renamed to -binary. As part of this change the -server suffix was removed from both the package name for os packages and the name of the executable ([#157]).

[#157]: https://github.com/stackabletech/kafka-operator/pull/157

## [0.1.0] - 2021.09.07

### Added

- Initial release

# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- Added versioning code from operator-rs for up and downgrades ([#167]).
- Added `ProductVersion` to status ([#167]).
- Added `Condition` to status ([#167]).

### Changed

- `kube-rs`: `0.58` → `0.60` ([#167]).
- `k8s-openapi` `0.12` → `0.13` and features: `v1_21` → `v1_22` ([#167]).
- `operator-rs` `0.2.1` → `0.2.2` ([#167]).
- `stackable-zookeeper-crd`: `0.2.0` → `0.4.0` ([#167]).
- `stackable-opa-crd`: `0.2.0` → `0.4.0` ([#167]).

### Removed

- `kube-runtime` dependency ([#167]).

[#167]: https://github.com/stackabletech/kafka-operator/pull/167

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

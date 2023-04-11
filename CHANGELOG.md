# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Enabled logging and log aggregation ([#547]).
- Deploy default and support custom affinities ([#557]).
- Openshift compatibility ([#572]).
- Extend cluster resources for status and cluster operation (paused, stopped) ([#574]).
- Cluster status conditions ([#575]).

### Changed

- `operator-rs` `0.30.1` -> `0.40.0` ([#545], [#572], [#574], [#577]).
- Bumped stackable versions to "23.4.0-rc1" ([#545]).
- Bumped kafka stackable versions to "23.4.0-rc2" ([#547]).
- Use operator-rs `build_rbac_resources` method ([#572]).
- Updated landing page and restructured usage guide ([#573]).

### Fixed

- Avoid empty log events dated to 1970-01-01 and improve the precision of the
  log event timestamps ([#577]).

[#545]: https://github.com/stackabletech/kafka-operator/pull/545
[#547]: https://github.com/stackabletech/kafka-operator/pull/547
[#557]: https://github.com/stackabletech/kafka-operator/pull/557
[#572]: https://github.com/stackabletech/kafka-operator/pull/572
[#573]: https://github.com/stackabletech/kafka-operator/pull/573
[#574]: https://github.com/stackabletech/kafka-operator/pull/574
[#575]: https://github.com/stackabletech/kafka-operator/pull/575
[#577]: https://github.com/stackabletech/kafka-operator/pull/577

## [23.1.0] - 2023-01-23

### Changed

- Fixed the RoleGroup `selector`. It was not used before. ([#530])
- Updated stackable image versions ([#513]).
- operator-rs: 0.26.0 -> 0.30.1 ([#519], [#530]).
- Don't run init container as root and avoid chmod and chowning ([#524]).
- [BREAKING] Use Product image selection instead of version. `spec.version` has been replaced by `spec.image` ([#482]).
- [BREAKING]: Removed tools image for init and get-svc container and replaced with Kafka product image. This means the latest stackable version has to be used in the product image selection ([#527])
- [BREAKING] Consolidated top-level configuration. Split up TLS encryption and authentication. Moved all top-level fields except `spec.image` below `spec.clusterConfig` ([#532]).

[#530]: https://github.com/stackabletech/kafka-operator/pull/530
[#482]: https://github.com/stackabletech/kafka-operator/pull/482
[#513]: https://github.com/stackabletech/kafka-operator/pull/513
[#519]: https://github.com/stackabletech/kafka-operator/pull/519
[#524]: https://github.com/stackabletech/kafka-operator/pull/524
[#527]: https://github.com/stackabletech/kafka-operator/pull/527
[#532]: https://github.com/stackabletech/kafka-operator/pull/532

## [0.8.0] - 2022-11-07

### Added

- Added default resource requests (memory and cpu) for Kafka pods ([#485]).
- Support for Kafka 3.3.1 ([#492]).
- Orphaned resources are deleted ([#495]).

### Changed

- Role and rolegroup configurations are merged correctly ([#499]).
- operator-rs: 0.22.0 -> 0.26.0 ([#495], [#499]).

[#485]: https://github.com/stackabletech/kafka-operator/pull/485
[#492]: https://github.com/stackabletech/kafka-operator/pull/492
[#495]: https://github.com/stackabletech/kafka-operator/pull/495
[#499]: https://github.com/stackabletech/kafka-operator/pull/499

### Changed

- Change port names from `http`/`https` to `kafka`/`kafka-tls` ([#472]).

[#472]: https://github.com/stackabletech/kafka-operator/pull/472

## [0.7.0] - 2022-09-06

### Added

- BREAKING: TLS encryption and authentication support for internal and client communications. This is breaking for clients because the cluster is secured per default, which results in a client port change ([#442]).

### Changed

- operator-rs: 0.21.1 -> 0.22.0 ([#430]).
- Include chart name when installing with a custom release name ([#429], [#431]).
- Kafka init container now uses Stackable tools rather than Bitnami kubectl ([#434]).

[#429]: https://github.com/stackabletech/kafka-operator/pull/429
[#430]: https://github.com/stackabletech/kafka-operator/pull/430
[#431]: https://github.com/stackabletech/kafka-operator/pull/431
[#434]: https://github.com/stackabletech/kafka-operator/pull/434
[#442]: https://github.com/stackabletech/kafka-operator/pull/442

## [0.6.0] - 2022-06-30

### Added

- Reconciliation errors are now reported as Kubernetes events ([#346]).
- Support for Kafka 3.1.0 ([#347]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#351]).
- Optional CRD field `log4j` to adapt the `log4j.properties` ([#364]).
- PVCs for data storage, cpu and memory limits are now configurable ([#405]).
- Moved tests from integration tests repo to operator repo ([#409]).

### Changed

- `operator-rs` `0.10.0` → `0.21.1` ([#346], [#351], [#385], [#405]).
- `--kafka-broker-clusterrole` is now only accepted for the `run` subcommand ([#349]).
- BREAKING: Adapted the `opa` field in the crd to `opaConfigMapName` and fixed `authorizer.class.name` to `org.openpolicyagent.kafka.OpaAuthorizer` and `opa.authorizer.metrics.enabled` to `true`. Other settings can be changed via `configOverrides` ([#364]).
- BREAKING: `opaConfigMapName` in CRD adapted to `opa` using the `OpaConfig` from operator-rs ([#385]).
- BREAKING: Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so version: 3.1.0 becomes (for example) version: 3.1.0-stackable0 ([#409])

[#346]: https://github.com/stackabletech/kafka-operator/pull/346
[#347]: https://github.com/stackabletech/kafka-operator/pull/347
[#349]: https://github.com/stackabletech/kafka-operator/pull/349
[#351]: https://github.com/stackabletech/kafka-operator/pull/351
[#364]: https://github.com/stackabletech/kafka-operator/pull/364
[#385]: https://github.com/stackabletech/kafka-operator/pull/385
[#405]: https://github.com/stackabletech/kafka-operator/pull/405
[#409]: https://github.com/stackabletech/kafka-operator/pull/409

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

# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [24.11.1] - 2025-01-10

## [24.11.1-rc2] - 2024-12-12

## [24.11.1-rc1] - 2024-12-06

### Fixed

- BREAKING: Use distinct ServiceAccounts for the Stacklets, so that multiple Stacklets can be
  deployed in one namespace. Existing Stacklets will use the newly created ServiceAccounts after
  restart ([#793]).

[#793]: https://github.com/stackabletech/kafka-operator/pull/793

## [24.11.0] - 2024-11-18

### Added

- Support version `3.8.0` ([#753]).
- Add support for Kerberos authentication ([#762]).
- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#771]).

### Changed

- Reduce CRD size from `479KB` to `53KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#750]):
  - `podOverrides`
  - `affinity`
- Migrate to exposing Kafka using Listener Operator ([#443]).
  - BREAKING: The existing services will be migrated to the new format. Clients will need to re-read settings from the discovery configmap.
  - BREAKING: Kafka is now only accessible from within the Kubernetes cluster by default. Set listener classes manually to expose it to the outside world (again).
  - BREAKING: To complete an upgrade to this kafka-operator, all existing Kafka StatefulSets must be deleted manually. This will cause some downtime.

### Fixed

- Include the global Kafka bootstrap service (not the rolegroup-specific) DNS record as SAN entry in the generated
  certificates used by Kafka. This allows you to access Kafka brokers secured using TLS via the global bootstrap
  service ([#741]).
- An invalid `KafkaCluster` doesn't cause the operator to stop functioning ([#773]).

### Removed

- Remove versions `3.4.1`, `3.6.1`, `3.6.2` ([#753]).

[#443]: https://github.com/stackabletech/kafka-operator/pull/443
[#741]: https://github.com/stackabletech/kafka-operator/pull/741
[#750]: https://github.com/stackabletech/kafka-operator/pull/750
[#753]: https://github.com/stackabletech/kafka-operator/pull/753
[#762]: https://github.com/stackabletech/kafka-operator/pull/762
[#771]: https://github.com/stackabletech/kafka-operator/pull/771
[#773]: https://github.com/stackabletech/kafka-operator/pull/773

## [24.7.0] - 2024-07-24

### Added

- Support for versions `3.6.2`, `3.7.1` ([#723]).

### Changed

- Bump `stackable-operator` from `0.64.0` to `0.70.0` ([#725]).
- Bump `product-config` from `0.6.0` to `0.7.0` ([#725]).
- Bump other dependencies ([#728]).

### Removed

- Support for version `3.5.2` ([#723]).
- BREAKING: Remove field/arg `controller_config` from `kafka_controller::Ctx`
  struct and `create_controller` function ([#726]).

[#723]: https://github.com/stackabletech/kafka-operator/pull/723
[#725]: https://github.com/stackabletech/kafka-operator/pull/725
[#726]: https://github.com/stackabletech/kafka-operator/pull/726
[#728]: https://github.com/stackabletech/kafka-operator/pull/728

## [24.3.0] - 2024-03-20

### Added

- Various documentation of the CRD ([#645]).
- Helm: support labels in values.yaml ([#657]).
- Support new versions `3.5.2`, `3.6.1` ([#664]).

### Removed

- Support for versions `2.8.2`, `3.4.0`, `3.5.1` ([#664]).

### Fixed

- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#715]).

[#645]: https://github.com/stackabletech/kafka-operator/pull/645
[#657]: https://github.com/stackabletech/kafka-operator/pull/657
[#664]: https://github.com/stackabletech/kafka-operator/pull/664
[#715]: https://github.com/stackabletech/kafka-operator/pull/715

## [23.11.0] - 2023-11-24

### Added

- Default stackableVersion to operator version. It is recommended to remove `spec.image.stackableVersion` from your custom resources ([#611], [#613]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#616]).
- Support PodDisruptionBudgets ([#625]).
- Support new versions 2.8.2, 3.4.1, 3.5.1 ([#627]).
- Document internal clusterId check ([#631]).
- Support graceful shutdown ([#635]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#612], [#627]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#611], [#621], [#625], [#627]).
- [BREAKING]: Let secret-operator handle certificate conversion. Doing so we were able to remove the `prepare` init container
  with the effect, that you can't configure the log level for this container anymore.
  You need to remove the field `spec.brokers.config.logging.container.prepare` in case you have specified it ([#621]).
- Combine the operator lib and binary crates ([#638]).

### Removed

- Removed support for versions 2.7.1, 3.1.0, 3.2.0, 3.3.1 ([#627]).

[#611]: https://github.com/stackabletech/kafka-operator/pull/611
[#612]: https://github.com/stackabletech/kafka-operator/pull/612
[#613]: https://github.com/stackabletech/kafka-operator/pull/613
[#616]: https://github.com/stackabletech/kafka-operator/pull/616
[#621]: https://github.com/stackabletech/kafka-operator/pull/621
[#625]: https://github.com/stackabletech/kafka-operator/pull/625
[#627]: https://github.com/stackabletech/kafka-operator/pull/627
[#631]: https://github.com/stackabletech/kafka-operator/pull/631
[#635]: https://github.com/stackabletech/kafka-operator/pull/635
[#638]: https://github.com/stackabletech/kafka-operator/pull/638

## [23.7.0] - 2023-07-14

### Added

- Generate OLM bundle for Release 23.4.0 ([#585]).
- Fixed upgrade test on Openshift ([#585]).
- Missing CRD defaults for `status.conditions` field ([#588]).
- Support Kafka 3.4.0 ([#591]).
- Add support for resource quotas ([#595])
- Support podOverrides ([#602])

### Fixed

- Increase the size limit of the log volume ([#604])

### Changed

- `operator-rs` `0.40.2` -> `0.44.0` ([#583], [#604]).
- Use 0.0.0-dev product images for testing ([#584])
- Use testing-tools 0.2.0 ([#584])
- Added kuttl test suites ([#599])

[#583]: https://github.com/stackabletech/kafka-operator/pull/583
[#584]: https://github.com/stackabletech/kafka-operator/pull/584
[#585]: https://github.com/stackabletech/kafka-operator/pull/585
[#588]: https://github.com/stackabletech/kafka-operator/pull/588
[#591]: https://github.com/stackabletech/kafka-operator/pull/591
[#595]: https://github.com/stackabletech/kafka-operator/pull/595
[#599]: https://github.com/stackabletech/kafka-operator/pull/599
[#602]: https://github.com/stackabletech/kafka-operator/pull/602
[#604]: https://github.com/stackabletech/kafka-operator/pull/604

## [23.4.0] - 2023-04-17

### Added

- Enabled logging and log aggregation ([#547]).
- Deploy default and support custom affinities ([#557]).
- Openshift compatibility ([#572]).
- Extend cluster resources for status and cluster operation (paused, stopped) ([#574]).
- Cluster status conditions ([#575]).

### Changed

- `operator-rs` `0.30.1` -> `0.40.2` ([#545], [#572], [#574], [#577]).
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

- Change port names from `http`/`https` to `kafka`/`kafka-tls` ([#472]).
- Role and rolegroup configurations are merged correctly ([#499]).
- operator-rs: 0.22.0 -> 0.26.0 ([#495], [#499]).

[#472]: https://github.com/stackabletech/kafka-operator/pull/472
[#485]: https://github.com/stackabletech/kafka-operator/pull/485
[#492]: https://github.com/stackabletech/kafka-operator/pull/492
[#495]: https://github.com/stackabletech/kafka-operator/pull/495
[#499]: https://github.com/stackabletech/kafka-operator/pull/499

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
[#181]: https://github.com/stackabletech/kafka-operator/pull/181
[#194]: https://github.com/stackabletech/kafka-operator/pull/194

## [0.2.1] - 2021-09-14

- Fixed Dockerfile to use the correct binary ([#167]).

[#167]: https://github.com/stackabletech/kafka-operator/pull/167

## [0.2.0] - 2021.09.10

### Changed

- **Breaking:** Repository structure was changed and the -server crate renamed to -binary. As part of this change the -server suffix was removed from both the package name for os packages and the name of the executable ([#157]).

[#157]: https://github.com/stackabletech/kafka-operator/pull/157

## [0.1.0] - 2021.09.07

### Added

- Initial release

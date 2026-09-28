use stackable_operator::{
    commons::{
        affinity::{StackableAffinityFragment, affinity_between_role_pods},
        opa::OpaConfig,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::APP_NAME;

pub fn get_affinity(
    cluster_name: &str,
    role: &str,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    // Only brokers use the OPA authorizer. The discovery ConfigMap has the same name as the
    // OpaCluster, and Kafka resolves it in its own namespace.
    let pod_affinity = opa_config
        .filter(|_| role == "broker")
        .map(|opa_config| PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods("opa", &opa_config.config_map_name, "server", 50),
            ]),
            required_during_scheduling_ignored_during_execution: None,
        });

    StackableAffinityFragment {
        pod_affinity,
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods(APP_NAME, cluster_name, role, 70),
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rstest::rstest;
    use stackable_operator::{
        commons::affinity::StackableAffinity,
        k8s_openapi::{
            api::core::v1::{
                PodAffinity, PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use crate::{
        controller::test_support::{minimal_kafka, validated_cluster},
        crd::KafkaRole,
    };

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_affinity_defaults(#[case] with_opa: bool) {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.9.2
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;

        let input = if with_opa {
            input.replace(
                "            zookeeperConfigMapName: xyz",
                "            zookeeperConfigMapName: xyz\n            authorization:\n              opa:\n                configMapName: test-opa",
            )
        } else {
            input.to_string()
        };
        let kafka = minimal_kafka(&input);
        let validated = validated_cluster(&kafka);
        let merged_config = validated
            .role_group_configs
            .get(&KafkaRole::Broker)
            .and_then(|groups| groups.get(&"default".parse().unwrap()))
            .map(|rg| &rg.config.config)
            .expect("role group should exist");

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: with_opa.then_some(PodAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_expressions: None,
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "opa".to_string()),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "test-opa".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            "server".to_string(),
                                        ),
                                    ])),
                                }),
                                topology_key: "kubernetes.io/hostname".to_string(),
                                ..PodAffinityTerm::default()
                            },
                            weight: 50,
                        },
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: Some(PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_expressions: None,
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "kafka".to_string(),),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "simple-kafka".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            "broker".to_string(),
                                        )
                                    ]))
                                }),
                                topology_key: "kubernetes.io/hostname".to_string(),
                                ..PodAffinityTerm::default()
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: None,
                node_selector: None,
            }
        );
    }

    #[test]
    fn controller_has_no_opa_affinity() {
        let opa_config = serde_yaml::from_str("configMapName: test-opa").unwrap();
        assert_eq!(
            super::get_affinity("simple-kafka", "controller", Some(&opa_config)).pod_affinity,
            None
        );
    }
}

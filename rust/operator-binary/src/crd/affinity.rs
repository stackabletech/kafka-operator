use stackable_operator::{
    commons::affinity::{StackableAffinityFragment, affinity_between_role_pods},
    k8s_openapi::api::core::v1::PodAntiAffinity,
};

use crate::crd::APP_NAME;

pub fn get_affinity(cluster_name: &str, role: &str) -> StackableAffinityFragment {
    StackableAffinityFragment {
        pod_affinity: None,
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
            api::core::v1::{PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm},
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use crate::{
        controller::test_support::{minimal_kafka, validated_cluster},
        crd::KafkaRole,
    };

    #[rstest]
    #[case(KafkaRole::Broker)]
    fn test_affinity_defaults(#[case] role: KafkaRole) {
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

        let kafka = minimal_kafka(input);
        let validated = validated_cluster(&kafka);
        let merged_config = validated
            .role_group_configs
            .get(&role)
            .and_then(|groups| groups.get(&"default".parse().unwrap()))
            .map(|rg| &rg.config.config)
            .expect("role group should exist");

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: None,
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
}

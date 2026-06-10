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
        kube::ResourceExt,
    };

    use crate::{
        crd::{
            KafkaRole,
            role::{AnyConfig, broker::BrokerConfig},
            v1alpha1,
        },
        framework::role_utils::with_validated_config,
    };

    #[rstest]
    #[case(KafkaRole::Broker)]
    fn test_affinity_defaults(#[case] role: KafkaRole) {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
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

        let kafka: v1alpha1::KafkaCluster =
            serde_yaml::from_str(input).expect("illegal test input");
        let broker_role = kafka.spec.brokers.clone().unwrap();
        let role_group = broker_role.role_groups.get("default").unwrap();
        let default_config = BrokerConfig::default_config(&kafka.name_any(), &role.to_string());
        let validated = with_validated_config(role_group, &broker_role, &default_config).unwrap();
        let merged_config = AnyConfig::Broker(validated.config);

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

mod discovery;
mod kafka_controller;
mod pod_svc_controller;
mod product_logging;
mod utils;

use crate::kafka_controller::KAFKA_CONTROLLER_NAME;
use crate::pod_svc_controller::POD_SERVICE_CONTROLLER_NAME;

use futures::StreamExt;
use stackable_kafka_crd::{KafkaCluster, OPERATOR_NAME};
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Pod, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{api::ListParams, runtime::Controller},
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    product_config::ProductConfigManager,
};
use std::sync::Arc;

pub struct ControllerConfig {
    pub broker_clusterrole: String,
}

pub async fn create_controller(
    client: Client,
    controller_config: ControllerConfig,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let kafka_controller = Controller::new(
        namespace.get_api::<KafkaCluster>(&client),
        ListParams::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        ListParams::default(),
    )
    .owns(namespace.get_api::<Service>(&client), ListParams::default())
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        ListParams::default(),
    )
    .owns(
        namespace.get_api::<ServiceAccount>(&client),
        ListParams::default(),
    )
    .owns(
        namespace.get_api::<RoleBinding>(&client),
        ListParams::default(),
    )
    .shutdown_on_signal()
    .run(
        kafka_controller::reconcile_kafka,
        kafka_controller::error_policy,
        Arc::new(kafka_controller::Ctx {
            client: client.clone(),
            controller_config,
            product_config,
        }),
    )
    .map(|res| {
        report_controller_reconciled(
            &client,
            &format!("{KAFKA_CONTROLLER_NAME}.{OPERATOR_NAME}"),
            &res,
        );
    });

    let pod_svc_controller = Controller::new(
        namespace.get_api::<Pod>(&client),
        ListParams::default().labels(&format!("{}=true", pod_svc_controller::LABEL_ENABLE)),
    )
    .owns(namespace.get_api::<Pod>(&client), ListParams::default())
    .shutdown_on_signal()
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Arc::new(pod_svc_controller::Ctx {
            client: client.clone(),
        }),
    )
    .map(|res| {
        report_controller_reconciled(
            &client,
            &format!("{POD_SERVICE_CONTROLLER_NAME}.{OPERATOR_NAME}"),
            &res,
        );
    });

    futures::stream::select(kafka_controller, pod_svc_controller)
        .collect::<()>()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use stackable_kafka_crd::KafkaRole;
    use stackable_operator::{
        commons::affinities::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    #[test]
    fn test_affinity_defaults() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc2"
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let merged_config = kafka.merged_config(&KafkaRole::Broker, "default").unwrap();

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
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
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
    fn test_affinity_legacy_node_selector() {
        let input = r#"
        apiVersion: kafka.stackable.tech/v1alpha1
        kind: KafkaCluster
        metadata:
          name: simple-kafka
        spec:
          image:
            productVersion: 3.3.1
            stackableVersion: "23.4.0-rc2"
          clusterConfig:
            zookeeperConfigMapName: xyz
          brokers:
            roleGroups:
              default:
                replicas: 1
                selector:
                  matchLabels:
                    disktype: ssd
                  matchExpressions:
                    - key: topology.kubernetes.io/zone
                      operator: In
                      values:
                        - antarctica-east1
                        - antarctica-west1
        "#;
        let kafka: KafkaCluster = serde_yaml::from_str(input).expect("illegal test input");
        let merged_config = kafka.merged_config(&KafkaRole::Broker, "default").unwrap();

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
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: Some(NodeAffinity {
                    preferred_during_scheduling_ignored_during_execution: None,
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: "topology.kubernetes.io/zone".to_string(),
                                operator: "In".to_string(),
                                values: Some(vec![
                                    "antarctica-east1".to_string(),
                                    "antarctica-west1".to_string()
                                ]),
                            }]),
                            match_fields: None,
                        }]
                    }),
                }),
                node_selector: Some(StackableNodeSelector {
                    node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())])
                }),
            }
        );
    }
}

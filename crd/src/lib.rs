use kube::CustomResource;
use serde::{Deserialize, Serialize};
use stackable_operator::CRD;

#[derive(Clone, CustomResource, Debug, Deserialize, Serialize)]
#[kube(
    group = "kafka.stackable.de",
    version = "v1",
    kind = "KafkaCluster",
    shortname = "kafka",
    namespaced
)]
#[kube(status = "KafkaClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    pub version: KafkaVersion,
    pub brokers: Vec<KafkaBroker>,
    pub zoo_keeper_reference: String
}

impl CRD for KafkaCluster {
    const RESOURCE_NAME: &'static str = "kafkaclusters.kafka.stackable.de";
    const CRD_DEFINITION: &'static str = r#"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: kafkaclusters.kafka.stackable.de
spec:
  group: kafka.stackable.de
  names:
    kind: KafkaCluster
    plural: kafkaclusters
    singular: kafkacluster
    shortNames:
      - kafka
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              properties:
                version:
                  type: string
                  enum: [ 2.6.0 ]
                brokers:
                  type: array
                  items:
                    type: object
                    properties:
                      node_name:
                        type: string
                zooKeeperReference:
                  type: string
              required: [ "version", "brokers", "zooKeeperReference" ]
            status:
               type: object
               properties:
                 is_bad:
                   type: boolean
      subresources:
         status: {}
    "#;
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum KafkaVersion {
    #[serde(rename = "2.6.0")]
    v2_6_0,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaBroker {
    pub node_name: String
}


#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct KafkaClusterStatus {
    is_bad: bool,
}

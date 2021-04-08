use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube_derive::CustomResource;
use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use stackable_operator::Crd;
use std::collections::HashMap;

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "kafka.stackable.tech",
    version = "v1",
    kind = "KafkaCluster",
    shortname = "kafka",
    namespaced
)]
#[kube(status = "KafkaClusterStatus")]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    pub version: KafkaVersion,
    pub brokers: NodeGroup<KafkaConfig>,
    pub zoo_keeper_reference: NamespaceName,
}

impl Crd for KafkaCluster {
    const RESOURCE_NAME: &'static str = "kafkaclusters.kafka.stackable.tech";
    const CRD_DEFINITION: &'static str = include_str!("../kafkaclusters.crd.yaml");
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub enum KafkaVersion {
    #[serde(rename = "2.6.0")]
    v2_6_0,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct KafkaClusterStatus {}

/// This is the address to a namespaced resource.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
pub struct NamespaceName {
    pub namespace: String,
    pub name: String,
}

impl NamespaceName {
    pub fn new(namespace: String, name: String) -> Self {
        NamespaceName { namespace, name }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGroup<T> {
    pub selectors: HashMap<String, SelectorAndConfig<T>>,
    pub config: Option<T>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectorAndConfig<T> {
    pub instances: u8,
    pub instances_per_node: u8,
    pub config: Option<T>,
    #[schemars(schema_with = "schema")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaConfig {}

pub fn schema(_: &mut SchemaGenerator) -> Schema {
    from_value(json!({
      "description": "A label selector is a label query over a set of resources. The result of matchLabels and matchExpressions are ANDed. An empty label selector matches all objects. A null label selector matches no objects.",
      "properties": {
        "matchExpressions": {
          "description": "matchExpressions is a list of label selector requirements. The requirements are ANDed.",
          "items": {
            "description": "A label selector requirement is a selector that contains values, a key, and an operator that relates the key and values.",
            "properties": {
              "key": {
                "description": "key is the label key that the selector applies to.",
                "type": "string",
                "x-kubernetes-patch-merge-key": "key",
                "x-kubernetes-patch-strategy": "merge"
              },
              "operator": {
                "description": "operator represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists and DoesNotExist.",
                "type": "string"
              },
              "values": {
                "description": "values is an array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. This array is replaced during a strategic merge patch.",
                "items": {
                  "type": "string"
                },
                "type": "array"
              }
            },
            "required": [
              "key",
              "operator"
            ],
            "type": "object"
          },
          "type": "array"
        },
        "matchLabels": {
          "additionalProperties": {
            "type": "string"
          },
          "description": "matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels map is equivalent to an element of matchExpressions, whose key field is \"key\", the operator is \"In\", and the values array contains only \"value\". The requirements are ANDed.",
          "type": "object"
        }
      },
      "type": "object"
    })).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KafkaClusterSpec, KafkaVersion, SelectorAndConfig};
    use schemars::gen::SchemaGenerator;
    use stackable_operator::conditions::schema;
    use std::error::Error;
    use std::str::FromStr;

    #[test]
    fn print_crd() {
        let schema = KafkaCluster::crd();
        let string_schema = serde_yaml::to_string(&schema).unwrap();
        println!("KafkaCluster CRD:\n{}\n", string_schema);
    }

    #[test]
    fn print_schema() {
        let schema = schema(&mut SchemaGenerator::default());

        let string_schema = serde_yaml::to_string(&schema).unwrap();
        println!("LabelSelector Schema:\n{}\n", string_schema);
    }
}

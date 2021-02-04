#![feature(backtrace)]
mod error;

use crate::error::Error;

use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, ConfigMapVolumeSource, Container, Pod, PodAffinityTerm, PodAntiAffinity,
    PodSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::ListParams;
use kube::Api;
use serde_json::json;
use stackable_kafka_crd::{KafkaBroker, KafkaCluster, KafkaClusterSpec};
use stackable_operator::client::Client;
use stackable_operator::controller::{Controller, ControllerStrategy, ReconciliationState};
use stackable_operator::metadata;
use stackable_operator::reconcile::{
    ReconcileFunctionAction, ReconcileResult, ReconciliationContext,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;

const FINALIZER_NAME: &str = "kafka.stackable.de/cleanup";

type KafkaReconcileResult = ReconcileResult<error::Error>;

struct KafkaState {
    context: ReconciliationContext<KafkaCluster>,
    kafka_spec: KafkaClusterSpec,
}

impl KafkaState {
    async fn read_existing_pod_information(&mut self) -> KafkaReconcileResult {
        let name = self.context.name();

        let mut labels = BTreeMap::new();
        labels.insert("kafka-name".to_string(), name.clone());

        /*
        let zk: Option<KafkaCluster> = match zk_api.get(&self.kafka_spec.zoo_keeper_reference).await
        {
            Ok(zk) => Some(zk),
            Err(err) => {
                // TODO: return None for kube:error:ErrorResponse with reason "NotFound" and return with error otherwise?
                None
            }
        };
         */

        let mut options = HashMap::new();
        options.insert(
            "zookeeper.connect".to_string(),
            "localhost:2181".to_string(),
        );

        let mut handlebars = Handlebars::new();
        handlebars
            .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
            .expect("template should work");

        for broker in &self.kafka_spec.brokers {
            let pod_name = format!("kafka-{}-{}", name, broker.node_name); // TODO: These names need to contain a random component, also check the maximum length of names
            let cm_name = format!("kafka-{}", pod_name);

            // First we need to create/update the necessary pods...
            let pod = build_pod(
                &self.context.resource,
                &broker,
                &labels,
                &pod_name,
                &cm_name,
            )?;
            self.context.client.create(&pod).await?;

            // ...then we create the ConfigMap (one per pod):
            let config = handlebars
                .render("conf", &json!({ "options": options }))
                .unwrap();

            let mut data = BTreeMap::new();
            data.insert("server.properties".to_string(), config);

            let tmp_name = cm_name.clone() + "-config"; // TODO: Create these names once and pass them around so we are consistent
            let cm =
                stackable_operator::create_config_map(&self.context.resource, &tmp_name, data)?;
            self.context.client.create(&cm).await?;
        }

        Ok(ReconcileFunctionAction::Continue)
    }
}

impl ReconciliationState for KafkaState {
    type Error = error::Error;

    fn reconcile(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileFunctionAction, Self::Error>> + Send + '_>>
    {
        Box::pin(async move { self.read_existing_pod_information().await })
    }
}

struct KafkaStrategy {}

impl KafkaStrategy {
    pub fn new() -> KafkaStrategy {
        KafkaStrategy {}
    }
}

impl ControllerStrategy for KafkaStrategy {
    type Item = KafkaCluster;
    type State = KafkaState;

    fn finalizer_name(&self) -> String {
        FINALIZER_NAME.to_string()
    }

    fn init_reconcile_state(&self, context: ReconciliationContext<Self::Item>) -> Self::State {
        KafkaState {
            kafka_spec: context.resource.spec.clone(),
            context,
        }
    }
}

pub async fn create_controller(client: Client) {
    let kafka_api: Api<KafkaCluster> = client.get_all_api();
    let pods_api: Api<Pod> = client.get_all_api();
    let config_maps_api: Api<ConfigMap> = client.get_all_api();

    let controller = Controller::new(kafka_api)
        .owns(pods_api, ListParams::default())
        .owns(config_maps_api, ListParams::default());

    let strategy = KafkaStrategy::new();

    controller.run(client, strategy).await;
}

fn build_pod(
    resource: &KafkaCluster,
    broker: &KafkaBroker,
    labels: &BTreeMap<String, String>,
    pod_name: &str,
    cm_name: &str,
) -> Result<Pod, Error> {
    let pod = Pod {
        // Metadata
        metadata: metadata::build_metadata(pod_name.to_string(), Some(labels.clone()), resource)?,
        // Spec
        spec: Some(PodSpec {
            node_name: Some(broker.node_name.clone()),
            tolerations: Some(stackable_operator::create_tolerations()),
            containers: vec![Container {
                image: Some(format!(
                    "stackable/kafka:{}",
                    serde_json::json!(resource.spec.version).as_str().unwrap()
                )),
                name: "kafka".to_string(),
                command: Some(vec![
                    "kafka_2.12-2.6.0/bin/kafka-server-start.sh".to_string(),
                    "{{ configroot }}/config/server.properties".to_string(),
                ]),
                volume_mounts: Some(vec![VolumeMount {
                    mount_path: "config".to_string(),
                    name: "config-volume".to_string(),
                    ..VolumeMount::default()
                }]),
                ..Container::default()
            }],
            volumes: Some(vec![Volume {
                name: "config-volume".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                    name: Some(format!("{}-config", cm_name)),
                    ..ConfigMapVolumeSource::default()
                }),
                ..Volume::default()
            }]),
            affinity: Some(Affinity {
                pod_anti_affinity: Some(PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![
                        PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_labels: Some(labels.clone()),
                                ..LabelSelector::default()
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                    ]),
                    ..PodAntiAffinity::default()
                }),
                ..Affinity::default()
            }),
            ..PodSpec::default()
        }),
        ..Pod::default()
    };
    Ok(pod)
}

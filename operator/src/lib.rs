mod error;

use stackable_kafka_crd::{KafkaCluster, KafkaClusterSpec, KafkaBroker};
use crate::error::Error;

use futures::StreamExt;
use handlebars::Handlebars;
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, ConfigMapVolumeSource, Container, Pod, PodAffinityTerm, PodAntiAffinity,
    PodSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use kube::api::{ListParams, Meta, ObjectMeta};
use kube::{Api, Client};
use kube_runtime::controller::{Context, ReconcilerAction};
use kube_runtime::Controller;
use serde_json::json;
use stackable_operator::{
    create_config_map, create_tolerations, decide_controller_action, finalizer,
    object_to_owner_reference, patch_resource, ContextData, ControllerAction,
};
use stackable_zookeeper_crd::ZooKeeperCluster;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tracing::{error, info};

const FINALIZER_NAME: &str = "kafka.stackable.de/cleanup";
const FIELD_MANAGER: &str = "kafka.stackable.de";


pub async fn create_controller(client: Client) {
    let api: Api<KafkaCluster> = Api::all(client.clone());
    let pods_api: Api<Pod> = Api::all(client.clone());
    let context = ContextData::new_context(client);

    Controller::new(api, ListParams::default())
        .owns(pods_api, ListParams::default())
        .run(reconcile, error_policy, context)
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("Reconciled {:?}", o),
                Err(e) => info!("Reconcile failed: {}", e),
            };
        })
        .await
}

/// This method contains the logic of reconciling an object (the desired state) we received with the actual state.
///
/// We distinguish between three different types:
/// * Create
/// * Update
/// * Delete
async fn reconcile(
    kafka_cluster: KafkaCluster,
    context: Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    match decide_controller_action(&kafka_cluster, FINALIZER_NAME) {
        Some(ControllerAction::Create) => {
            create_deployment(&kafka_cluster, &context).await?;
        }
        Some(ControllerAction::Update) => {
            update_deployment(&kafka_cluster, &context).await?;
        }
        Some(ControllerAction::Delete) => {
            delete_deployment(&kafka_cluster, &context).await?;
        }
        None => {
            //TODO: debug!
        }
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// This method is being called by the Controller whenever there's an error during reconcilation.
/// We just log the error and requeue the event.
fn error_policy(error: &Error, _context: Context<ContextData>) -> ReconcilerAction {
    error!("Reconciliation error:\n{}", error);
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(10)),
    }
}

async fn create_deployment(
    kafka_cluster: &KafkaCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    finalizer::add_finalizer(
        context.get_ref().client.clone(),
        kafka_cluster,
        FINALIZER_NAME,
    )
    .await?;

    update_deployment(kafka_cluster, context).await
}

async fn update_deployment(
    kafka_cluster: &KafkaCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    let kafka_spec: KafkaClusterSpec = kafka_cluster.spec.clone();
    let client = context.get_ref().client.clone();

    let name = Meta::name(kafka_cluster);
    let ns = Meta::namespace(kafka_cluster).expect("KafkaCluster is namespaced");
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns);

    let mut labels = BTreeMap::new();
    labels.insert("kafka-name".to_string(), name.clone());

    let zk_api: Api<ZooKeeperCluster> = Api::namespaced(client.clone(), "TODO");
    let zk: Option<ZooKeeperCluster> = match zk_api.get(&kafka_spec.zoo_keeper_reference).await {
        Ok(zk) => Some(zk),
        Err(err) => {
            // TODO: return None for kube:error:ErrorResponse with reason "NotFound" and return with error otherwise?
            None
        }
    };

    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &ns);

    // Writing simple
    //zookeeper.connect=localhost:2181
    let mut options = HashMap::new();
    options.insert(
        "zookeeper.connect".to_string(),
        "localhost:2181".to_string(),
    );

    let mut handlebars = Handlebars::new();
    handlebars
        .register_template_string("conf", "{{#each options}}{{@key}}={{this}}\n{{/each}}")
        .expect("template should work");

    for broker in kafka_spec.brokers {
        let pod_name = format!("kafka-{}-{}", name, broker.node_name); // TODO: These names need to contain a random component, also check the maximum length of names
        let cm_name = format!("kafka-{}", pod_name);

        // First we need to create/update the necessary pods...
        let pod = build_pod(&kafka_cluster, &broker, &labels, &pod_name, &cm_name)?;
        patch_resource(&pods_api, &pod_name, &pod, FIELD_MANAGER).await?;

        // ...then we create the ConfigMap (one per pod):
        let config = handlebars
            .render("conf", &json!({ "options": options }))
            .unwrap();

        let mut data = BTreeMap::new();
        data.insert("server.properties".to_string(), config);

        let tmp_name = cm_name.clone() + "-config"; // TODO: Create these names once and pass them around so we are consistent
        let cm = create_config_map(kafka_cluster, &tmp_name, data)?;
        patch_resource(&cm_api, &tmp_name, &cm, FIELD_MANAGER).await?;
    }

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}

async fn delete_deployment(
    kafka_cluster: &KafkaCluster,
    context: &Context<ContextData>,
) -> Result<ReconcilerAction, Error> {
    finalizer::remove_finalizer(context.get_ref().client.clone(), kafka_cluster, FINALIZER_NAME).await?;

    Ok(ReconcilerAction {
        requeue_after: Option::None,
    })
}

fn build_pod(
    resource: &KafkaCluster,
    broker: &KafkaBroker,
    labels: &BTreeMap<String, String>,
    pod_name: &String,
    cm_name: &String,
) -> Result<Pod, Error> {
    let pod = Pod {
        // Metadata
        metadata: ObjectMeta {
            name: Some(pod_name.clone()),
            owner_references: Some(vec![OwnerReference {
                controller: Some(true),
                ..object_to_owner_reference::<KafkaCluster>(resource.metadata.clone())?
            }]),
            labels: Some(labels.clone()),
            ..ObjectMeta::default()
        },

        // Spec
        spec: Some(PodSpec {
            node_name: Some(broker.node_name.clone()),
            tolerations: Some(create_tolerations()),
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

mod discovery;
mod kafka_controller;
mod pod_svc_controller;
mod utils;

use futures::StreamExt;
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::client::Client;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Pod, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::Context;
use stackable_operator::kube::runtime::Controller;
use stackable_operator::product_config::ProductConfigManager;
use tracing::info_span;
use tracing_futures::Instrument;
use utils::erase_controller_result_type;

pub struct ControllerConfig {
    pub broker_clusterrole: String,
}

pub async fn create_controller(
    client: Client,
    controller_config: ControllerConfig,
    product_config: ProductConfigManager,
) {
    let kafka_controller =
        Controller::new(client.get_all_api::<KafkaCluster>(), ListParams::default())
            .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
            .owns(client.get_all_api::<Service>(), ListParams::default())
            .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
            .shutdown_on_signal()
            .run(
                kafka_controller::reconcile_kafka,
                kafka_controller::error_policy,
                Context::new(kafka_controller::Ctx {
                    client: client.clone(),
                    controller_config,
                    product_config,
                }),
            )
            .instrument(info_span!("kafka_controller"));

    let pod_svc_controller = Controller::new(
        client.get_all_api::<Pod>(),
        ListParams::default().labels(&format!("{}=true", pod_svc_controller::LABEL_ENABLE)),
    )
    .owns(client.get_all_api::<Pod>(), ListParams::default())
    .shutdown_on_signal()
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Context::new(pod_svc_controller::Ctx { client }),
    )
    .instrument(info_span!("pod_svc_controller"));

    futures::stream::select(
        kafka_controller.map(erase_controller_result_type),
        pod_svc_controller.map(erase_controller_result_type),
    )
    .for_each(|res| async {
        match res {
            Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
            Err(err) => {
                tracing::error!(error = &*err, "Failed to reconcile object",)
            }
        }
    })
    .await;
}

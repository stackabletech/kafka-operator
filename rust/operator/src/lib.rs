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
use utils::erase_controller_result_type;

pub async fn create_controller(client: Client, product_config: ProductConfigManager) {
    let kafka_controller =
        Controller::new(client.get_all_api::<KafkaCluster>(), ListParams::default())
            .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
            .owns(client.get_all_api::<Service>(), ListParams::default())
            .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
            .run(
                kafka_controller::reconcile_kafka,
                kafka_controller::error_policy,
                Context::new(kafka_controller::Ctx {
                    client: client.clone(),
                    product_config,
                }),
            );

    let pod_svc_controller = Controller::new(
        client.get_all_api::<Pod>(),
        ListParams::default().labels(&format!("{}=true", pod_svc_controller::LABEL_ENABLE)),
    )
    .owns(client.get_all_api::<Pod>(), ListParams::default())
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Context::new(pod_svc_controller::Ctx { client }),
    );

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

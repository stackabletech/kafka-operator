mod kafka_controller;

use futures::StreamExt;
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::client::Client;
use stackable_operator::error::OperatorResult;
use stackable_operator::k8s_openapi::api::apps::v1::StatefulSet;
use stackable_operator::k8s_openapi::api::core::v1::{ConfigMap, Service};
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::runtime::controller::Context;
use stackable_operator::kube::runtime::Controller;
use stackable_operator::product_config::ProductConfigManager;

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
) -> OperatorResult<()> {
    let controller = Controller::new(client.get_all_api::<KafkaCluster>(), ListParams::default())
        .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
        .owns(client.get_all_api::<Service>(), ListParams::default())
        .owns(client.get_all_api::<ConfigMap>(), ListParams::default());

    controller
        .run(
            kafka_controller::reconcile_kafka,
            kafka_controller::error_policy,
            Context::new(kafka_controller::Ctx {
                client,
                product_config,
            }),
        )
        .for_each(|res| async {
            match res {
                Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                Err(err) => {
                    tracing::error!(
                        error = &err as &dyn std::error::Error,
                        "Failed to reconcile object",
                    )
                }
            }
        })
        .await;

    Ok(())
}

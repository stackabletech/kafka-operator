mod discovery;
mod kafka_controller;
mod product_logging;
mod utils;

use crate::kafka_controller::KAFKA_CONTROLLER_NAME;

use futures::StreamExt;
use stackable_kafka_crd::{KafkaCluster, OPERATOR_NAME};
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::runtime::{watcher, Controller},
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    product_config::ProductConfigManager,
};
use std::sync::Arc;

mod built_info {
    pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
}

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
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Service>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ServiceAccount>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<RoleBinding>(&client),
        watcher::Config::default(),
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

    kafka_controller.collect::<()>().await;
}

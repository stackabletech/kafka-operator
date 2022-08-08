mod command;
mod discovery;
mod kafka_controller;
mod pod_svc_controller;
mod utils;

use std::sync::Arc;

use futures::StreamExt;
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::namespace::WatchNamespace;
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Pod, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{api::ListParams, runtime::Controller},
    logging::controller::report_controller_reconciled,
    product_config::ProductConfigManager,
};

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
        report_controller_reconciled(&client, "kafkacluster.kafka.stackable.tech", &res);
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
        report_controller_reconciled(&client, "pod-service.kafka.stackable.tech", &res);
    });

    futures::stream::select(kafka_controller, pod_svc_controller)
        .collect::<()>()
        .await;
}

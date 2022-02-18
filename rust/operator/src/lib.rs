mod discovery;
mod kafka_controller;
mod pod_svc_controller;
mod utils;

use futures::StreamExt;
use stackable_kafka_crd::KafkaCluster;
use stackable_operator::{
    client::Client,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Pod, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::{api::ListParams, runtime::controller::Context, runtime::Controller},
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
) {
    let kafka_controller =
        Controller::new(client.get_all_api::<KafkaCluster>(), ListParams::default())
            .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
            .owns(client.get_all_api::<Service>(), ListParams::default())
            .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
            .owns(
                client.get_all_api::<ServiceAccount>(),
                ListParams::default(),
            )
            .owns(client.get_all_api::<RoleBinding>(), ListParams::default())
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
            .map(|res| {
                report_controller_reconciled(&client, "kafkacluster.kafka.stackable.tech", &res);
            });

    let pod_svc_controller = Controller::new(
        client.get_all_api::<Pod>(),
        ListParams::default().labels(&format!("{}=true", pod_svc_controller::LABEL_ENABLE)),
    )
    .owns(client.get_all_api::<Pod>(), ListParams::default())
    .shutdown_on_signal()
    .run(
        pod_svc_controller::reconcile_pod,
        pod_svc_controller::error_policy,
        Context::new(pod_svc_controller::Ctx {
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

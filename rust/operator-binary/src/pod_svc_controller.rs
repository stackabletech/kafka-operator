use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::APP_NAME;
use stackable_operator::{
    k8s_openapi::{
        api::core::v1::{Container, Pod, Service, ServicePort, ServiceSpec},
        apimachinery::pkg::apis::meta::v1::OwnerReference,
    },
    kube::{core::ObjectMeta, runtime::controller::Action},
    logging::controller::ReconcilerError,
    time::Duration,
};
use std::sync::Arc;
use strum::{EnumDiscriminants, IntoStaticStr};

pub const POD_SERVICE_CONTROLLER_NAME: &str = "pod-service";
pub const LABEL_ENABLE: &str = "kafka.stackable.tech/pod-service";

const LABEL_STS_POD_NAME: &str = "statefulset.kubernetes.io/pod-name";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no name"))]
    ObjectHasNoName,
    #[snafu(display("object has no UID"))]
    ObjectHasNoUid,
    #[snafu(display("failed to apply Service for Pod"))]
    ApplyServiceFailed {
        source: stackable_operator::client::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_pod(pod: Arc<Pod>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");
    let name = pod.metadata.name.clone().context(ObjectHasNoNameSnafu)?;
    let mut ports: Vec<ServicePort> = vec![];

    if let Some(spec) = &pod.spec {
        for container in &spec
            .containers
            .iter()
            .filter(|container| container.name == APP_NAME)
            .collect::<Vec<&Container>>()
        {
            if let Some(container_ports) = &container.ports {
                for port in container_ports {
                    ports.push(ServicePort {
                        name: port.name.clone(),
                        port: port.container_port,
                        ..ServicePort::default()
                    });
                }
            }
        }
    }

    let svc = Service {
        metadata: ObjectMeta {
            namespace: pod.metadata.namespace.clone(),
            name: pod.metadata.name.clone(),
            owner_references: Some(vec![OwnerReference {
                api_version: "v1".to_string(),
                kind: "Pod".to_string(),
                name: name.clone(),
                uid: pod.metadata.uid.clone().context(ObjectHasNoUidSnafu)?,
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("NodePort".to_string()),
            external_traffic_policy: Some("Local".to_string()),
            ports: Some(ports),
            selector: Some([(LABEL_STS_POD_NAME.to_string(), name)].into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    ctx.client
        .apply_patch(POD_SERVICE_CONTROLLER_NAME, &svc, &svc)
        .await
        .context(ApplyServiceFailedSnafu)?;
    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<Pod>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}

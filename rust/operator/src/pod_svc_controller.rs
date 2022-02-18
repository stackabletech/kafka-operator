use snafu::{OptionExt, ResultExt, Snafu};
use stackable_kafka_crd::APP_PORT;
use stackable_operator::{
    k8s_openapi::{
        api::core::v1::{Pod, Service, ServicePort, ServiceSpec},
        apimachinery::pkg::apis::meta::v1::OwnerReference,
    },
    kube::{
        core::ObjectMeta,
        runtime::controller::{Context, ReconcilerAction},
    },
    logging::controller::ReconcilerError,
};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

pub const LABEL_ENABLE: &str = "kafka.stackable.tech/pod-service";
const LABEL_STS_POD_NAME: &str = "statefulset.kubernetes.io/pod-name";

const FIELD_MANAGER_SCOPE: &str = "pod-service";

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
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_pod(pod: Arc<Pod>, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let name = pod.metadata.name.clone().context(ObjectHasNoNameSnafu)?;
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
            ports: Some(vec![ServicePort {
                name: Some("kafka".to_string()),
                port: APP_PORT.into(),
                ..ServicePort::default()
            }]),
            selector: Some([(LABEL_STS_POD_NAME.to_string(), name)].into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    ctx.get_ref()
        .client
        .apply_patch(FIELD_MANAGER_SCOPE, &svc, &svc)
        .await
        .context(ApplyServiceFailedSnafu)?;
    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

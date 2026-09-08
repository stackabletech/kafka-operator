//! Wires up and runs the kafka-agent's two controllers, both scoped to the agent's single namespace.
//!
//! Mirrors the zk-agent's `znode_controller::run`: the agent must NOT call `signal::crd_established`
//! (needs cluster-scoped CRD list/watch, which the namespaced agent RBAC does not grant) and must
//! NOT create a conversion webhook. It simply runs the controllers plus the liveness lease renewal
//! (started separately in `main.rs`).

use std::{future::Future, sync::Arc};

use futures::StreamExt;
use stackable_operator::{
    client::Client,
    crd::action::v1alpha1 as action_v1alpha1,
    kube::{
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
};

use stackable_kafka_operator::crd::topic::v1alpha1 as topic_v1alpha1;

use crate::{KAFKA_AGENT_CONTROLLER_NAME, agentrequest_controller, topic_controller};

/// Parameters resolved once at agent startup, shared by the two controllers.
pub struct AgentConfig {
    pub cluster_name: String,
    pub namespace: String,
    /// Directory the cluster's discovery `ConfigMap` is mounted at; the controllers read the `KAFKA`
    /// bootstrap key from it each reconcile (spike R3).
    pub discovery_config_dir: std::path::PathBuf,
    pub platform_access_cert_dir: Option<std::path::PathBuf>,
    pub platform_access_server_ca_dir: Option<std::path::PathBuf>,
}

/// Builds both agent controllers and returns a future that runs them to completion.
///
/// Two independent shutdown futures are taken (the framework's `SignalWatcher::handle` yields a
/// fresh, non-`Clone` future per call), one per controller.
pub fn run(
    client: Client,
    config: AgentConfig,
    topic_shutdown: impl Future<Output = ()> + Send + Sync + 'static,
    request_shutdown: impl Future<Output = ()> + Send + Sync + 'static,
) -> impl Future<Output = ()> {
    let watch_namespace = WatchNamespace::One(config.namespace.clone());

    // --- KafkaTopic controller ---
    let topic_ctx = Arc::new(topic_controller::Ctx {
        client: client.clone(),
        cluster_name: config.cluster_name.clone(),
        discovery_config_dir: config.discovery_config_dir.clone(),
        platform_access_cert_dir: config.platform_access_cert_dir.clone(),
        platform_access_server_ca_dir: config.platform_access_server_ca_dir.clone(),
    });
    let topic_recorder = Arc::new(Recorder::new(
        client.as_kube_client(),
        Reporter {
            controller: format!("{KAFKA_AGENT_CONTROLLER_NAME}.topic"),
            instance: None,
        },
    ));
    let topic_controller = Controller::new(
        watch_namespace.get_api::<DeserializeGuard<topic_v1alpha1::KafkaTopic>>(&client),
        watcher::Config::default(),
    )
    .graceful_shutdown_on(topic_shutdown)
    .run(
        topic_controller::reconcile_topic,
        topic_controller::error_policy,
        topic_ctx,
    )
    .for_each_concurrent(16, move |result| {
        let recorder = topic_recorder.clone();
        async move {
            report_controller_reconciled(
                &recorder,
                &format!("{KAFKA_AGENT_CONTROLLER_NAME}.topic"),
                &result,
            )
            .await;
        }
    });

    // --- AgentRequest controller (drain executor) ---
    let request_ctx = Arc::new(agentrequest_controller::Ctx {
        client: client.clone(),
        cluster_name: config.cluster_name.clone(),
        discovery_config_dir: config.discovery_config_dir.clone(),
        platform_access_cert_dir: config.platform_access_cert_dir.clone(),
        platform_access_server_ca_dir: config.platform_access_server_ca_dir.clone(),
    });
    let request_recorder = Arc::new(Recorder::new(
        client.as_kube_client(),
        Reporter {
            controller: format!("{KAFKA_AGENT_CONTROLLER_NAME}.agentrequest"),
            instance: None,
        },
    ));
    let request_controller = Controller::new(
        watch_namespace.get_api::<DeserializeGuard<action_v1alpha1::AgentRequest>>(&client),
        watcher::Config::default(),
    )
    .graceful_shutdown_on(request_shutdown)
    .run(
        agentrequest_controller::reconcile_agent_request,
        agentrequest_controller::error_policy,
        request_ctx,
    )
    .for_each_concurrent(16, move |result| {
        let recorder = request_recorder.clone();
        async move {
            report_controller_reconciled(
                &recorder,
                &format!("{KAFKA_AGENT_CONTROLLER_NAME}.agentrequest"),
                &result,
            )
            .await;
        }
    });

    async move {
        futures::join!(topic_controller, request_controller);
    }
}

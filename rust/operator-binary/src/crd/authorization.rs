use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::opa::{OpaApiVersion, OpaConfig},
    k8s_openapi::api::core::v1::ConfigMap,
    schemars::{self, JsonSchema},
};

use crate::crd::v1alpha1;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to fetch OPA ConfigMap {configmap_name}"))]
    FetchOpaConfigMap {
        source: stackable_operator::client::Error,
        configmap_name: String,
        namespace: String,
    },

    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,
}

#[derive(Clone, Deserialize, Debug, Default, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAuthorization {
    // no doc - docs in the OpaConfig struct.
    pub opa: Option<OpaConfig>,
}

pub struct KafkaAuthorizationConfig {
    pub opa_connect: String,
    pub secret_class: Option<String>,
}

impl KafkaAuthorization {
    pub async fn get_opa_config(
        self,
        client: &Client,
        kafka: &v1alpha1::KafkaCluster,
    ) -> Result<Option<KafkaAuthorizationConfig>, Error> {
        let auth_config = if let Some(opa) = self.opa {
            let namespace = kafka
                .metadata
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?;
            // Resolve the secret class from the ConfigMap
            let secret_class = client
                .get::<ConfigMap>(&opa.config_map_name, namespace)
                .await
                .with_context(|_| FetchOpaConfigMapSnafu {
                    configmap_name: &opa.config_map_name,
                    namespace,
                })?
                .data
                .and_then(|mut data| data.remove("OPA_SECRET_CLASS"));
            let opa_connect = opa
                .full_document_url_from_config_map(client, kafka, Some("allow"), OpaApiVersion::V1)
                .await
                .context(InvalidOpaConfigSnafu)?;
            Some(KafkaAuthorizationConfig {
                opa_connect,
                secret_class,
            })
        } else {
            None
        };

        Ok(auth_config)
    }
}

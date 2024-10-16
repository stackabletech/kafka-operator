use crate::ObjectRef;

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::commons::authentication::AuthenticationClassProvider;
use stackable_operator::{
    client::Client,
    commons::authentication::AuthenticationClass,
    schemars::{self, JsonSchema},
};

pub const SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS: [&str; 2] = ["TLS", "Kerberos"];

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to retrieve AuthenticationClass [{}]", authentication_class))]
    AuthenticationClassRetrieval {
        source: stackable_operator::client::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },

    #[snafu(display("only one authentication class at a time is currently supported. Possible Authentication class providers are {SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS:?}"))]
    MultipleAuthenticationClassesProvided,

    #[snafu(display(
        "failed to use authentication provider [{provider}] for authentication class [{authentication_class}] - supported providers: {SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS:?}",
    ))]
    AuthenticationProviderNotSupported {
        authentication_class: ObjectRef<AuthenticationClass>,
        provider: String,
    },
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAuthentication {
    /// The AuthenticationClass <https://docs.stackable.tech/home/nightly/concepts/authenticationclass.html> to use.
    ///
    /// ## TLS provider
    ///
    /// Only affects client connections. This setting controls:
    /// - If clients need to authenticate themselves against the broker via TLS
    /// - Which ca.crt to use when validating the provided client certs
    ///
    /// This will override the server TLS settings (if set) in `spec.clusterConfig.tls.serverSecretClass`.
    ///
    /// ## Kerberos provider
    ///
    /// This affects client connections and also requires TLS for encryption.
    /// This setting is used to reference an `AuthenticationClass` and in turn, a `SecretClass` that is
    /// used to create keytabs.
    pub authentication_class: String,
}

#[derive(Clone, Debug)]
/// Helper struct that contains resolved AuthenticationClasses to reduce network API calls.
pub struct ResolvedAuthenticationClasses {
    resolved_authentication_classes: Vec<AuthenticationClass>,
}

impl ResolvedAuthenticationClasses {
    pub fn new(resolved_authentication_classes: Vec<AuthenticationClass>) -> Self {
        Self {
            resolved_authentication_classes,
        }
    }

    /// Resolve provided AuthenticationClasses via API calls and validate the contents.
    /// Currently errors out if:
    /// - AuthenticationClass could not be resolved
    /// - Validation failed
    pub async fn from_references(
        client: &Client,
        auth_classes: &Vec<KafkaAuthentication>,
    ) -> Result<ResolvedAuthenticationClasses, Error> {
        let mut resolved_authentication_classes: Vec<AuthenticationClass> = vec![];

        for auth_class in auth_classes {
            resolved_authentication_classes.push(
                AuthenticationClass::resolve(client, &auth_class.authentication_class)
                    .await
                    .context(AuthenticationClassRetrievalSnafu {
                        authentication_class: ObjectRef::<AuthenticationClass>::new(
                            &auth_class.authentication_class,
                        ),
                    })?,
            );
        }

        ResolvedAuthenticationClasses::new(resolved_authentication_classes).validate()
    }

    /// Return the (first) TLS `AuthenticationClass` if available
    pub fn get_tls_authentication_class(&self) -> Option<&AuthenticationClass> {
        self.resolved_authentication_classes
            .iter()
            .find(|auth| matches!(auth.spec.provider, AuthenticationClassProvider::Tls(_)))
    }

    /// Return the (first) Kerberos `AuthenticationClass` if available
    pub fn get_kerberos_authentication_class(&self) -> Option<&AuthenticationClass> {
        self.resolved_authentication_classes
            .iter()
            .find(|auth| matches!(auth.spec.provider, AuthenticationClassProvider::Kerberos(_)))
    }

    /// Validates the resolved AuthenticationClasses.
    /// Currently errors out if:
    /// - More than one AuthenticationClass was provided
    /// - AuthenticationClass provider was not supported
    pub fn validate(&self) -> Result<Self, Error> {
        if self.resolved_authentication_classes.len() > 1 {
            return Err(Error::MultipleAuthenticationClassesProvided);
        }

        for auth_class in &self.resolved_authentication_classes {
            match &auth_class.spec.provider {
                // explicitly list each branch so new elements do not get overlooked
                AuthenticationClassProvider::Tls(_) | AuthenticationClassProvider::Kerberos(_) => {}
                AuthenticationClassProvider::Static(_)
                | AuthenticationClassProvider::Ldap(_)
                | AuthenticationClassProvider::Oidc(_) => {
                    return Err(Error::AuthenticationProviderNotSupported {
                        authentication_class: ObjectRef::from_obj(auth_class),
                        provider: auth_class.spec.provider.to_string(),
                    })
                }
            }
        }

        Ok(self.clone())
    }
}

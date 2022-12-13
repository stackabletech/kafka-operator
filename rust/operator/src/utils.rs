use snafu::Snafu;
use stackable_kafka_crd::{KafkaCluster, APP_NAME, OPERATOR_NAME};
use stackable_operator::{
    kube::{core::DynamicObject, runtime::reflector::ObjectRef, Resource},
    labels::ObjectLabels,
};

#[derive(Debug, Snafu)]
#[snafu(display(
    "cannot build a local reference to {} from {}, because it is in another namespace",
    obj,
    peer
))]
pub struct NamespaceMismatchError {
    obj: ObjectRef<DynamicObject>,
    peer: ObjectRef<DynamicObject>,
}
pub trait ObjectRefExt {
    fn name_in_ns_of<'a, KPeer: Resource<DynamicType = ()>>(
        &'a self,
        peer_obj: &KPeer,
    ) -> Result<&'a str, NamespaceMismatchError>;
}

impl<K: Resource<DynamicType = ()>> ObjectRefExt for ObjectRef<K> {
    fn name_in_ns_of<KPeer: Resource<DynamicType = ()>>(
        &self,
        peer_obj: &KPeer,
    ) -> Result<&str, NamespaceMismatchError> {
        if self.namespace == peer_obj.meta().namespace {
            Ok(&self.name)
        } else {
            NamespaceMismatchSnafu {
                obj: self.clone().erase(),
                peer: ObjectRef::from_obj(peer_obj).erase(),
            }
            .fail()
        }
    }
}

/// Build recommended values for labels
pub fn build_recommended_labels<'a>(
    owner: &'a KafkaCluster,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, KafkaCluster> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}

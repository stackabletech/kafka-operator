use snafu::Snafu;
use stackable_operator::kube::{core::DynamicObject, runtime::reflector::ObjectRef, Resource};

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

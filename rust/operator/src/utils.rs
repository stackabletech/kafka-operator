use snafu::Snafu;
use stackable_operator::kube::{
    core::DynamicObject,
    runtime::{controller::ReconcilerAction, reflector::ObjectRef},
    Resource,
};

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
pub fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> Result<(ObjectRef<DynamicObject>, ReconcilerAction), Box<dyn std::error::Error>> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
}

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

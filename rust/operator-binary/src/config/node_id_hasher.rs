use stackable_operator::role_utils::RoleGroupRef;

use crate::crd::v1alpha1::KafkaCluster;

pub fn node_id_hash32_offset(rolegroup_ref: &RoleGroupRef<KafkaCluster>) -> u32 {
    let hash = fnv_hash32(&format!(
        "{role}-{rolegroup}",
        role = rolegroup_ref.role,
        rolegroup = rolegroup_ref.role_group
    ));
    let range = hash & 0x0000FFFF;
    // Kafka uses signed integer
    range * 0x00007FFF
}

/// Simple FNV-1a hash impl
fn fnv_hash32(input: &str) -> u32 {
    const FNV_OFFSET: u32 = 0x811c9dc5;
    const FNV_PRIME: u32 = 0x01000193;

    let mut hash = FNV_OFFSET;
    for byte in input.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

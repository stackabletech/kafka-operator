use crate::crd::role::KafkaRole;

/// The Kafka node.id needs to be unique across the Kafka cluster.
/// This function generates an integer that is stable for a given role group
/// regardless if broker or controllers.
/// This integer is then added to the pod index to compute the final node.id
/// The node.id is only set and used in Kraft mode.
/// Warning: this is not safe from collisions.
pub fn node_id_hash32_offset(role: &KafkaRole, role_group: &str) -> u32 {
    let hash = fnv_hash32(&format!("{role}-{role_group}"));
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

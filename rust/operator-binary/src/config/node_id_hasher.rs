pub fn node_id_hash32_offset(rolegroup: &str) -> u32 {
    let hash = fnv_hash32(rolegroup);
    let range = hash & 0x0000FFFF;
    let offset = range * 0x0000FFFF;
    offset
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

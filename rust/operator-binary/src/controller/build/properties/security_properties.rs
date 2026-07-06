//! Builder for `security.properties` (the JVM security properties file).
//!
//! `networkaddress.cache.ttl` and `networkaddress.cache.negative.ttl` are `required`
//!  properties with recommended values, so they are always emitted.

use std::collections::BTreeMap;

const NETWORKADDRESS_CACHE_TTL: &str = "networkaddress.cache.ttl";
const NETWORKADDRESS_CACHE_NEGATIVE_TTL: &str = "networkaddress.cache.negative.ttl";

const DEFAULT_NETWORKADDRESS_CACHE_TTL: &str = "30";
const DEFAULT_NETWORKADDRESS_CACHE_NEGATIVE_TTL: &str = "0";

/// Build the `security.properties` key/value pairs.
///
/// `overrides` are the resolved user overrides for `security.properties`
/// (highest precedence).
pub fn build(overrides: BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut props: BTreeMap<String, String> = BTreeMap::new();

    // 1. Defaults (recommended values for the required properties).
    props.insert(
        NETWORKADDRESS_CACHE_TTL.to_string(),
        DEFAULT_NETWORKADDRESS_CACHE_TTL.to_string(),
    );
    props.insert(
        NETWORKADDRESS_CACHE_NEGATIVE_TTL.to_string(),
        DEFAULT_NETWORKADDRESS_CACHE_NEGATIVE_TTL.to_string(),
    );

    // 2. User overrides (highest precedence).
    for (k, v) in overrides {
        props.insert(k, v);
    }

    props
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_present_without_overrides() {
        let props = build(BTreeMap::new());
        assert_eq!(
            props.get("networkaddress.cache.ttl"),
            Some(&"30".to_string())
        );
        assert_eq!(
            props.get("networkaddress.cache.negative.ttl"),
            Some(&"0".to_string())
        );
    }

    #[test]
    fn user_override_wins() {
        let overrides = [("networkaddress.cache.ttl".to_string(), "60".to_string())]
            .into_iter()
            .collect();
        let props = build(overrides);
        assert_eq!(
            props.get("networkaddress.cache.ttl"),
            Some(&"60".to_string())
        );
    }

    #[test]
    fn extra_user_override_key_is_added() {
        let overrides = [("custom.security.prop".to_string(), "x".to_string())]
            .into_iter()
            .collect();
        let props = build(overrides);
        assert_eq!(props.get("custom.security.prop"), Some(&"x".to_string()));
    }
}

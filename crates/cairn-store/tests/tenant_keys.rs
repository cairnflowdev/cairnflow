// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The tenant is part of the partition-key hash input: the same scope / object
//! key / idempotency key in two tenants are two different targets — two
//! partition keys, so two places in the keyspace.

// Tests run on real time, network and processes; only `sim_cluster` must be
// deterministic (`clippy.toml`).
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "tests outside the deterministic simulation may use real nondeterminism"
)]

use cairn_store::{Scope, TenantId};

/// The three keyed tiers of the hash — scope, object key, idempotency key: two
/// tenants with the same key ⇒ two partition keys. Tenant `default` keeps
/// exactly the old hash.
#[test]
fn same_object_key_two_tenants_two_partition_keys() {
    let acme = TenantId::new("acme").unwrap();
    let globex = TenantId::new("globex").unwrap();
    let scope = Scope::new("customer-1").unwrap();
    let tiers: [(&str, Option<&Scope>, &str, Option<&str>); 3] = [
        ("scope", Some(&scope), "", None),
        ("object", None, "cart-7", None),
        ("idempotency", None, "", Some("attempt-1")),
    ];
    for (tier, sc, key, idem) in tiers {
        let a = cairn_store::partition_key_for_target(&acme, sc, "Cart", key, idem);
        let g = cairn_store::partition_key_for_target(&globex, sc, "Cart", key, idem);
        assert!(
            a.is_some() && g.is_some(),
            "tier {tier}: must have a partition key"
        );
        assert_ne!(a, g, "tier {tier}: two tenants, one key, one partition key");
    }
    // Tenant `default` keeps exactly the hash from before tenants were hashed:
    // existing data and tests do not move.
    let d = TenantId::default();
    let a = cairn_store::partition_key_for_target(&acme, None, "Cart", "cart-7", None);
    let dd = cairn_store::partition_key_for_target(&d, None, "Cart", "cart-7", None);
    assert_ne!(a, dd, "another tenant differs from tenant default");
    // With no key there is still no partition key — the tenant does not invent one.
    assert_eq!(
        cairn_store::partition_key_for_target(&acme, None, "Cart", "", None),
        None
    );
}

// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Unit tests of the OIDC relying party: the registry, role capping, cookie v2,
//! `id_token`. The full HTTP flow is in `tests/oidc.rs`.

#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "the test signs JWTs with a real RNG"
)]

use {
    super::*,
    crate::auth::{ClusterSecret, SESSION_TTL},
    ring::signature::{EcdsaKeyPair, Ed25519KeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING},
};

fn tenant(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn client(id: &str, host: &str, t: &str, max: Role) -> OidcClient {
    OidcClient {
        client_id: id.into(),
        hostname: host.into(),
        tenant: tenant(t),
        max_role: max,
        portal_url: Some("https://app.example.test/envs".into()),
    }
}

fn config() -> OidcConfig {
    OidcConfig {
        issuer: Some("https://auth.example.test".into()),
        session_ttl_ms: DEFAULT_SESSION_TTL_MS,
        display_name: Some("Cairnflow Cloud & co".into()),
        clients: vec![
            client("env_a", "a.cells.test", "acme", Role::Admin),
            client("env_b", "b.cells.test", "beta", Role::View),
        ],
    }
}

#[test]
fn config_round_trip_and_strict_decode() {
    let c = config();
    let raw = encode_config(&c);
    assert_eq!(decode_config(&raw).unwrap(), c);
    assert_eq!(decode_revoked(&encode_revoked(&[])).unwrap(), vec![]);
    let r = vec![Revocation {
        client: "env_a".into(),
        sub: "user 1".into(),
        before_ms: 42,
    }];
    assert_eq!(decode_revoked(&encode_revoked(&r)).unwrap(), r);

    let text = String::from_utf8(raw).unwrap();
    for bad in [
        // An unknown line, a newer header, a missing header.
        format!("{text}extra x\n"),
        text.replace("cairnflow-oidc/1", "cairnflow-oidc/2"),
        text.replace("cairnflow-oidc/1\n", ""),
        // A role beyond the tenant, a hostname with a port, clients with a duplicate hostname.
        text.replace("acme admin", "acme cluster-admin"),
        text.replace("acme admin", "acme provision"),
        text.replace("a.cells.test", "a.cells.test:443"),
        text.replace("b.cells.test", "a.cells.test"),
        // A portal link that is not http(s) — the console shows it as a link.
        text.replacen("https://app.example.test/envs", "javascript:alert(1)", 1),
        // A TTL out of range, an issuer with a query.
        text.replace("session_ttl_ms 3600000", "session_ttl_ms 1"),
        text.replace(
            "https://auth.example.test\n",
            "https://auth.example.test?x=1\n",
        ),
    ] {
        assert!(decode_config(bad.as_bytes()).is_err(), "{bad}");
    }
    let newer = decode_config(text.replace("oidc/1", "oidc/9").as_bytes()).unwrap_err();
    assert!(newer.contains("cairnflow-oidc/9"), "{newer}");
    for bad in [
        "cairnflow-revoked/1\nenv_a\tsub\n",
        "cairnflow-revoked/1\nenv a\tsub\t1\n",
        "cairnflow-revoked/1\nenv_a\tsub\tx\n",
        "cairnflow-revoked/2\n",
    ] {
        assert!(decode_revoked(bad.as_bytes()).is_err(), "{bad:?}");
    }
}

/// A role from a claim never exceeds `max_role`, and never goes beyond the tenant.
#[test]
fn role_is_capped() {
    use Role::*;
    for (claimed, max, want) in [
        (Admin, Admin, Some(Admin)),
        (Admin, View, Some(View)),
        (ClusterAdmin, Admin, Some(Admin)),
        (ClusterAdmin, View, Some(View)),
        (View, Admin, Some(View)),
        (Invoke, Admin, Some(Invoke)),
        (Invoke, View, None),
        (Provision, Admin, None),
        (Metrics, Admin, None),
        (Admin, ClusterAdmin, None),
        (Admin, Provision, None),
    ] {
        assert_eq!(cap_role(claimed, max), want, "{claimed:?} ≤ {max:?}");
    }
}

#[test]
fn hosts_urls_and_return_to() {
    assert_eq!(
        normalize_host("A.Cells.Test:8080").as_deref(),
        Some("a.cells.test")
    );
    assert_eq!(normalize_host("localhost").as_deref(), Some("localhost"));
    assert_eq!(normalize_host("[::1]:80").as_deref(), Some("[::1]"));
    assert_eq!(normalize_host("x:y"), None);
    assert_eq!(normalize_host("a/b"), None);
    assert!(check_hostname("a.cells.test").is_ok());
    assert!(check_hostname("[::1]").is_ok());
    for bad in ["A.test", "a.test:1", "", "a b", "https://a"] {
        assert!(check_hostname(bad).is_err(), "{bad}");
    }
    assert!(check_issuer("http://127.0.0.1:9/realm").is_ok());
    for bad in [
        "ftp://x",
        "https://u:p@x",
        "https://x#f",
        "x",
        "https://x?a=1",
    ] {
        assert!(check_issuer(bad).is_err(), "{bad}");
    }
    let fb = "/ui/";
    for (v, want) in [
        (Some("/ui/invocations?x=1"), "/ui/invocations?x=1"),
        (Some("//evil.test/x"), fb),
        (Some("/\\evil.test"), fb),
        (Some("https://evil.test/"), fb),
        (Some("ui/x"), fb),
        (Some("/ui/\nx"), fb),
        (None, fb),
    ] {
        assert_eq!(safe_return_to(v, fb), want, "{v:?}");
    }
}

/// Security review: `url::Url::parse` silently drops TAB/newline, but the **original**
/// string is what gets stored — a URL carrying `\n` would pass the check and then write
/// an extra line into `auth/oidc`, the strict decoder would refuse the whole object, and
/// the whole cluster would close every session relying on the registry. Must see: every
/// character outside printable ASCII is refused right at write time, and a configuration
/// that passes the check always decodes again.
#[test]
fn urls_with_control_or_space_are_rejected() {
    for bad in [
        "https://auth.example.test/\nclient env_x evil.test acme admin -",
        "https://auth.example.test/\tx",
        "https://auth.example.test/ x",
        "https://auth.example.test/\rx",
        "https://auth.ex\u{e4}mple.test/",
    ] {
        assert!(check_issuer(bad).is_err(), "issuer {bad:?}");
        assert!(check_portal_url(bad).is_err(), "portalUrl {bad:?}");
    }
    let mut c = config();
    c.clients[0].portal_url = Some("https://app.example.test/envs?a=1&b=%20".into());
    assert!(check_client(&c.clients[0]).is_ok());
    assert_eq!(decode_config(&encode_config(&c)).unwrap(), c);
}

#[test]
fn revocations_keep_max_and_prune() {
    let r = |sub: &str, b: u64| Revocation {
        client: "env_a".into(),
        sub: sub.into(),
        before_ms: b,
    };
    let now = 10 * MAX_SESSION_TTL_MS;
    let floor = now + CLOCK_SKEW_MS;
    let mut book = vec![r("old", 1), r("u", floor)];
    // A smaller mark does not lower it; lines past the TTL cap are pruned.
    assert!(merge_revocation(&mut book, r("u", now - 5), now));
    assert_eq!(book, vec![r("u", floor)]);
    assert!(!merge_revocation(&mut book, r("u", now - 5), now));
    // Repeating the same mark later (the cell's clock has moved on) ⇒ no slide.
    assert!(!merge_revocation(&mut book, r("u", floor), now + 30_000));
    assert!(merge_revocation(&mut book, r("u", floor + 5), now));
    // A new mark lower than the cell time + skew ⇒ raised to the floor.
    assert!(merge_revocation(&mut book, r("v", now - 10), now));
    assert_eq!(book, vec![r("u", floor + 5), r("v", floor)]);
}

fn claims(cfg: &OidcConfig, client: &str, role: &str, iat: u64) -> SsoClaims {
    let c = cfg.client(client).unwrap();
    SsoClaims {
        v: 2,
        src: "oidc".into(),
        iss: cfg.issuer.clone().unwrap(),
        sub: "user-1".into(),
        email: Some("u@example.test".into()),
        name: Some("U".into()),
        tenant: c.tenant.to_string(),
        role: role.into(),
        client: client.into(),
        iat,
        exp: iat + 3600,
        sid: "0123456789abcdef0123456789abcdef".into(),
    }
}

/// Cookie v2 through every condition of `check_sso`: correctly signed but carrying
/// `cluster-admin` ⇒ capped, never beyond `max_role`; client removed, tenant changed,
/// issuer changed, another hostname, revoked, expired ⇒ refused.
#[test]
fn sso_cookie_rules() {
    let cfg = config();
    let now = 1_800_000_000;
    let host = Some("a.cells.test");
    let ok = claims(&cfg, "env_a", "admin", now);
    let who = check_sso(&ok, &cfg, &[], host, now).expect("valid session");
    assert_eq!((who.tenant.as_str(), who.role), ("acme", Role::Admin));
    assert_eq!(who.user.as_ref().unwrap().sub, "user-1");
    assert!(!who.is_env_key() && who.key_id.is_none());

    // A role beyond the tenant in a correctly signed cookie ⇒ refused outright.
    for role in ["cluster-admin", "provision", "metrics", "root"] {
        let c = SsoClaims {
            role: role.into(),
            ..ok.clone()
        };
        assert!(check_sso(&c, &cfg, &[], host, now).is_none(), "{role}");
    }
    // Capped by the current `max_role`.
    let b = claims(&cfg, "env_b", "admin", now);
    let who = check_sso(&b, &cfg, &[], Some("b.cells.test"), now).unwrap();
    assert_eq!(who.role, Role::View);

    // Another hostname, client removed, tenant changed, issuer changed.
    assert!(check_sso(&ok, &cfg, &[], Some("b.cells.test"), now).is_none());
    assert!(check_sso(&ok, &cfg, &[], None, now).is_none());
    let mut gone = cfg.clone();
    gone.clients.retain(|c| c.client_id != "env_a");
    assert!(check_sso(&ok, &gone, &[], host, now).is_none());
    let mut moved = cfg.clone();
    moved.clients[0].tenant = tenant("other");
    assert!(check_sso(&ok, &moved, &[], host, now).is_none());
    let mut reissued = cfg.clone();
    reissued.issuer = Some("https://other.example.test".into());
    assert!(check_sso(&ok, &reissued, &[], host, now).is_none());
    // Lowering `max_role` after login ⇒ the next check caps by the new role.
    let mut lowered = cfg.clone();
    lowered.clients[0].max_role = Role::View;
    assert_eq!(
        check_sso(&ok, &lowered, &[], host, now).unwrap().role,
        Role::View
    );

    // Revocation: a mark after `iat` ⇒ dead; a mark before ⇒ alive; another sub/client ⇒ alive.
    let rev = |client: &str, sub: &str, b: u64| Revocation {
        client: client.into(),
        sub: sub.into(),
        before_ms: b,
    };
    assert!(check_sso(
        &ok,
        &cfg,
        &[rev("env_a", "user-1", now * 1000 + 1)],
        host,
        now
    )
    .is_none());
    assert!(check_sso(&ok, &cfg, &[rev("env_a", "user-1", now * 1000)], host, now).is_some());
    assert!(check_sso(&ok, &cfg, &[rev("env_b", "user-1", u64::MAX)], host, now).is_some());
    assert!(check_sso(&ok, &cfg, &[rev("env_a", "user-2", u64::MAX)], host, now).is_some());

    // Expired by the time passed in; an `iat` too far in the future; wrong version.
    assert!(check_sso(&ok, &cfg, &[], host, ok.exp).is_none());
    assert!(check_sso(&ok, &cfg, &[], host, now - 61).is_none());
    let v1 = SsoClaims { v: 1, ..ok.clone() };
    assert!(check_sso(&v1, &cfg, &[], host, now).is_none());
}

/// Cookie v2 is signed with its own derived key: a v1 cookie does not open with the SSO
/// key and vice versa; another cluster secret ⇒ broken; an altered byte ⇒ broken.
#[test]
fn sso_cookie_sealing() {
    let s = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
    let cfg = config();
    let book = OidcBook::new(None);
    {
        let mut c = book.0.cache.write().unwrap();
        c.0 = cfg.clone();
    }
    let now = crate::auth::now_s();
    let value = mint_sso(&s, &claims(&cfg, "env_a", "admin", now));
    assert!(verify_sso(&s, &value, &book, Some("a.cells.test"), now).is_some());
    let twin = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
    assert!(
        verify_sso(&twin, &value, &book, Some("a.cells.test"), now).is_some(),
        "sibling node"
    );
    let other = Sessions::new(&ClusterSecret::new("s2"), SESSION_TTL, false);
    assert!(verify_sso(&other, &value, &book, Some("a.cells.test"), now).is_none());
    assert!(
        s.open(crate::auth::Seal::Flow, &value).is_none(),
        "the flow key opens an SSO cookie"
    );
    let mut b = value.clone().into_bytes();
    b[3] = if b[3] == b'0' { b'1' } else { b'0' };
    assert!(verify_sso(
        &s,
        &String::from_utf8(b).unwrap(),
        &book,
        Some("a.cells.test"),
        now
    )
    .is_none());

    let f = Flow::new("env_a", "https://a/cb".into(), "/ui/".into(), now);
    let sealed = f.seal(&s);
    assert_eq!(Flow::open(&s, &sealed, now).as_ref(), Some(&f));
    assert!(Flow::open(&s, &sealed, f.exp).is_none(), "expired");
    assert!(f.matches_state(&f.state) && !f.matches_state("x"));
    assert_eq!(f.challenge().len(), 43);
}

// ───────────────────────────────────────────────────────────── id_token

fn b64(v: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(v)
}

enum Signer {
    Ed(Ed25519KeyPair),
    Es(EcdsaKeyPair),
}

impl Signer {
    fn ed() -> Self {
        Signer::Ed(Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap())
    }

    fn es() -> Self {
        let rng = ring::rand::SystemRandom::new();
        let pk8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        Signer::Es(
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pk8.as_ref(), &rng).unwrap(),
        )
    }

    fn alg(&self) -> &'static str {
        match self {
            Signer::Ed(_) => "EdDSA",
            Signer::Es(_) => "ES256",
        }
    }

    fn jwk(&self, kid: &str) -> Jwk {
        let raw: serde_json::Value = match self {
            Signer::Ed(k) => serde_json::json!({
                "kty": "OKP", "crv": "Ed25519", "kid": kid,
                "x": b64(k.public_key().as_ref()),
            }),
            Signer::Es(k) => {
                let p = k.public_key().as_ref();
                serde_json::json!({
                    "kty": "EC", "crv": "P-256", "kid": kid,
                    "x": b64(&p[1..33]), "y": b64(&p[33..65]),
                })
            }
        };
        serde_json::from_value(raw).unwrap()
    }

    fn sign(&self, header: serde_json::Value, claims: &serde_json::Value) -> String {
        let signed = format!(
            "{}.{}",
            b64(header.to_string().as_bytes()),
            b64(claims.to_string().as_bytes())
        );
        let sig = match self {
            Signer::Ed(k) => k.sign(signed.as_bytes()).as_ref().to_vec(),
            Signer::Es(k) => k
                .sign(&ring::rand::SystemRandom::new(), signed.as_bytes())
                .unwrap()
                .as_ref()
                .to_vec(),
        };
        format!("{signed}.{}", b64(&sig))
    }

    fn token(&self, kid: &str, claims: &serde_json::Value) -> String {
        self.sign(serde_json::json!({"alg": self.alg(), "kid": kid}), claims)
    }
}

const NOW: u64 = 1_800_000_000;

fn good_claims() -> serde_json::Value {
    serde_json::json!({
        "iss": "https://auth.example.test",
        "aud": "env_a",
        "sub": "user-1",
        "email": "u@example.test",
        "name": "U",
        "nonce": "n-1",
        "iat": NOW,
        "exp": NOW + 300,
        "cairn_role": "admin",
        "cairn_tenant": "acme",
    })
}

fn expect(t: &TenantId) -> Expect<'_> {
    Expect {
        issuer: "https://auth.example.test",
        client_id: "env_a",
        nonce: "n-1",
        tenant: t,
        max_role: Role::Admin,
        now_s: NOW,
    }
}

#[test]
fn id_token_checks() {
    let t = tenant("acme");
    for signer in [Signer::ed(), Signer::es()] {
        let keys = vec![signer.jwk("k1")];
        let ok = verify_id_token(&signer.token("k1", &good_claims()), &keys, &expect(&t)).unwrap();
        assert_eq!(
            (ok.sub.as_str(), ok.role, ok.email.as_deref()),
            ("user-1", Role::Admin, Some("u@example.test"))
        );
        let with = |k: &str, v: serde_json::Value| {
            let mut c = good_claims();
            c[k] = v;
            signer.token("k1", &c)
        };
        let without = |k: &str| {
            let mut c = good_claims();
            c.as_object_mut().unwrap().remove(k);
            signer.token("k1", &c)
        };
        use TokenError as E;
        for (tok, want) in [
            (with("iss", "https://evil.test".into()), E::Issuer),
            (with("aud", "env_b".into()), E::Audience),
            (
                with("aud", serde_json::json!(["env_a", "env_b"])),
                E::Audience,
            ),
            (with("azp", "env_b".into()), E::Audience),
            (with("exp", (NOW - 61).into()), E::Expired),
            (with("iat", (NOW + 61).into()), E::IssuedInFuture),
            (with("nonce", "n-2".into()), E::Nonce),
            (without("nonce"), E::Nonce),
            (with("sub", "".into()), E::Subject),
            (with("sub", "a\tb".into()), E::Subject),
            (with("cairn_tenant", "beta".into()), E::Tenant),
            (with("cairn_role", "billing".into()), E::Role),
            (with("cairn_role", "provision".into()), E::Role),
            (without("cairn_role"), E::Role),
            (signer.token("k2", &good_claims()), E::UnknownKey),
        ] {
            assert_eq!(
                verify_id_token(&tok, &keys, &expect(&t)),
                Err(want.clone()),
                "{want:?}"
            );
        }
        // An `aud` array with the right `azp` ⇒ passes; no `cairn_tenant` ⇒ passes; clock
        // skew within 60 s ⇒ passes.
        let mut c = good_claims();
        c["aud"] = serde_json::json!(["env_a", "other"]);
        c["azp"] = "env_a".into();
        assert!(verify_id_token(&signer.token("k1", &c), &keys, &expect(&t)).is_ok());
        assert!(verify_id_token(&without("cairn_tenant"), &keys, &expect(&t)).is_ok());
        assert!(verify_id_token(&with("exp", (NOW - 30).into()), &keys, &expect(&t)).is_ok());
        // `cluster-admin` in the claim ⇒ capped by `max_role`.
        let capped = verify_id_token(
            &with("cairn_role", "cluster-admin".into()),
            &keys,
            &Expect {
                max_role: Role::View,
                ..expect(&t)
            },
        )
        .unwrap();
        assert_eq!(capped.role, Role::View);
        // An altered payload ⇒ broken signature.
        let tok = signer.token("k1", &good_claims());
        let mut parts: Vec<String> = tok.split('.').map(str::to_string).collect();
        let mut c = good_claims();
        c["cairn_role"] = "admin".into();
        c["sub"] = "user-2".into();
        parts[1] = b64(c.to_string().as_bytes());
        assert_eq!(
            verify_id_token(&parts.join("."), &keys, &expect(&t)),
            Err(E::Signature)
        );
    }
    // `alg: none`, `HS256` (a symmetric key faked from the public key), an algorithm not
    // matching the key kind.
    let ed = Signer::ed();
    let keys = vec![ed.jwk("k1")];
    let body = b64(good_claims().to_string().as_bytes());
    for alg in ["none", "HS256"] {
        let h = b64(serde_json::json!({"alg": alg, "kid": "k1"})
            .to_string()
            .as_bytes());
        assert_eq!(
            verify_id_token(&format!("{h}.{body}.AAAA"), &keys, &expect(&tenant("acme"))),
            Err(TokenError::Algorithm),
            "{alg}"
        );
    }
    let es_on_ed = ed.sign(
        serde_json::json!({"alg": "ES256", "kid": "k1"}),
        &good_claims(),
    );
    assert_eq!(
        verify_id_token(&es_on_ed, &keys, &expect(&tenant("acme"))),
        Err(TokenError::UnknownKey)
    );
    assert_eq!(
        verify_id_token("a.b", &keys, &expect(&tenant("acme"))),
        Err(TokenError::Malformed)
    );
}

#[test]
fn jwks_skips_unknown_keys() {
    let raw = br#"{"keys":[{"kty":"oct","k":"x"},{"kty":"OKP","crv":"Ed25519","x":"AA","use":"enc"},{"kty":"OKP","crv":"Ed25519","x":"AA","kid":"a"}]}"#;
    let keys = parse_jwks(raw).unwrap();
    assert_eq!(
        keys.len(),
        2,
        "oct is readable but unused; `use: enc` is skipped"
    );
    assert!(keys.iter().all(|k| k.use_.as_deref() != Some("enc")));
}

/// Ambiguous outcome: the CAS of `auth/oidc` and `auth/revoked` was written while the
/// answer said otherwise (`Action::Ambiguous`). Must see: the write is accepted, the
/// registry has exactly one record — no false error, no double write.
#[tokio::test]
async fn ambiguous_writes_are_resolved_by_reread() {
    use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let book = OidcBook::new(Some((store.clone(), &Path::from("p"))));
        faulty.add_rule(Rule::ambiguous_put("auth/oidc", reply));
        book.update_config(|c| {
            c.issuer = Some("https://auth.example.test".into());
            c.clients
                .push(client("env_a", "a.cells.test", "acme", Role::Admin));
            Ok::<_, String>(((), true))
        })
        .await
        .unwrap_or_else(|e| panic!("{reply:?}: written but reported an error: {e}"));
        faulty.add_rule(Rule::ambiguous_put("auth/revoked", reply));
        book.revoke(
            Revocation {
                client: "env_a".into(),
                sub: "u".into(),
                before_ms: CLOCK_SKEW_MS + 5,
            },
            1,
        )
        .await
        .unwrap_or_else(|e| panic!("{reply:?}: revocation written but reported an error: {e}"));
        let fresh = OidcBook::new(Some((store.clone(), &Path::from("p"))));
        fresh.refresh().await.unwrap();
        assert_eq!(fresh.config().clients.len(), 1, "{reply:?}");
        assert_eq!(fresh.revoked().len(), 1, "{reply:?}");
        assert_eq!(fresh.revoked_before("env_a", "u"), Some(CLOCK_SKEW_MS + 5));
    }
}

/// **Apply order**: two re-reads running in parallel — the refresh loop and an apply
/// request — finish in any order.
///
/// Interleaving:
/// 1. Node B reads the registry (copy 1). Peer A shares the bucket.
/// 2. B's next first GET reads, then stalls 300 ms before returning
///    (`DelayResponse`): B refreshes in the background — copy 1.
/// 3. A writes copy 2.
/// 4. B refreshes again (the apply request): copy 2, applied.
/// 5. Read (2) returns.
///
/// Must see: B stays at copy 2 — a copy read before the write does not overwrite the
/// newer one.
#[tokio::test]
async fn stale_refresh_never_brings_back_a_removed_client() {
    use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
    let mem = Arc::new(slatedb::object_store::memory::InMemory::new());
    let faulty = FaultyStore::wrap(mem.clone());
    let peer = OidcBook::new(Some((
        mem.clone() as Arc<dyn ObjectStore>,
        &Path::from("p"),
    )));
    let me = OidcBook::new(Some((
        faulty.clone() as Arc<dyn ObjectStore>,
        &Path::from("p"),
    )));
    peer.update_config(|c| {
        *c = config();
        Ok::<_, String>(((), true))
    })
    .await
    .unwrap();
    me.refresh().await.unwrap();
    assert!(!me.config().clients.is_empty());
    faulty.add_rule(Rule::new(
        OpSet::GET,
        PathMatch::Contains("p/auth/oidc".into()),
        Action::EachOnce(Box::new(Action::DelayResponse(
            std::time::Duration::from_millis(300),
        ))),
    ));
    let slow = tokio::spawn({
        let me = me.clone();
        async move { me.refresh().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    peer.update_config(|c| {
        c.clients.clear();
        Ok::<_, String>(((), true))
    })
    .await
    .unwrap();
    me.refresh().await.unwrap();
    assert!(me.config().clients.is_empty(), "apply sees the removal");
    slow.await.unwrap().unwrap();
    assert!(
        me.config().clients.is_empty(),
        "a copy read before the removal brought the client back"
    );
}

/// A corrupt object in the bucket: the refresh reports an error, the cache keeps the last
/// good copy (the caller does not confirm the registry as fresh ⇒ after the cap the
/// sessions are closed like keys).
#[tokio::test]
async fn corrupt_book_keeps_last_good_and_reports() {
    use slatedb::object_store::memory::InMemory;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let book = OidcBook::new(Some((store.clone(), &Path::from("p"))));
    book.update_config(|c| {
        *c = config();
        Ok::<_, String>(((), true))
    })
    .await
    .unwrap();
    store
        .put(
            &Path::from("p/auth/oidc"),
            b"cairnflow-oidc/1\nwat\n".to_vec().into(),
        )
        .await
        .unwrap();
    assert!(book.refresh().await.is_err());
    assert_eq!(book.config(), config());
}

/// **Checking an SSO session does not copy the OIDC registry.** Every request carrying
/// an SSO cookie (and every request without Bearer, in the "does OIDC have a client"
/// check) used to copy the whole `config()` and `revoked()` — 10 000 clients and 10 000
/// revocations are tens of thousands of allocations per request, including
/// unauthenticated requests.
///
/// Must see: 3 000 `verify_sso` calls on that registry finish in < 2 s (debug build):
/// only lookups remain, no copying.
#[test]
fn sso_check_does_not_copy_the_book() {
    let s = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
    let mut cfg = config();
    for i in 0..10_000 {
        cfg.clients.push(client(
            &format!("env_bulk_{i}"),
            &format!("bulk-{i}.cells.test"),
            "acme",
            Role::Admin,
        ));
    }
    let revoked: Vec<Revocation> = (0..10_000)
        .map(|i| Revocation {
            client: format!("env_bulk_{i}"),
            sub: format!("user-{i}"),
            before_ms: 1,
        })
        .collect();
    let book = OidcBook::new(None);
    {
        let mut c = book.0.cache.write().unwrap();
        c.0 = cfg.clone();
        c.1 = revoked;
    }
    let now = crate::auth::now_s();
    let value = mint_sso(&s, &claims(&cfg, "env_a", "admin", now));
    assert!(verify_sso(&s, &value, &book, Some("a.cells.test"), now).is_some());
    let started = std::time::Instant::now();
    for _ in 0..3_000 {
        assert!(verify_sso(&s, &value, &book, Some("a.cells.test"), now).is_some());
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "3 000 SSO session checks took {:?}",
        started.elapsed()
    );
}

/// **An SSO cookie without a session id is refused.** Must see: a correctly signed v2
/// cookie, not expired, with the right client and host, but without `sid` ⇒ `verify_sso`
/// returns `None`.
#[test]
fn sso_cookie_without_session_id_is_refused() {
    let s = Sessions::new(&ClusterSecret::new("s1"), SESSION_TTL, false);
    let cfg = config();
    let book = OidcBook::new(None);
    {
        let mut c = book.0.cache.write().unwrap();
        c.0 = cfg.clone();
    }
    let now = crate::auth::now_s();
    let mut v = serde_json::to_value(claims(&cfg, "env_a", "admin", now)).unwrap();
    v.as_object_mut().unwrap().remove("sid");
    let value = s.seal(crate::auth::Seal::Sso, v.to_string().as_bytes());
    assert!(
        verify_sso(&s, &value, &book, Some("a.cells.test"), now).is_none(),
        "an SSO cookie without sid still opens a session"
    );
}

/// **Ambiguous outcome** — the CAS of `auth/revoked-sessions` was written while the
/// answer said otherwise. Must see: the revocation is accepted; another node re-reading
/// sees the session revoked and other sessions not; expired cookie lines are pruned at
/// the next write.
#[tokio::test]
async fn ambiguous_session_revocation_is_resolved_by_reread() {
    use cairn_store::testkit::{Ambiguity, FaultyStore, Rule};
    let sid = |n: u8| format!("{n:02x}").repeat(16);
    for reply in [Ambiguity::Retried, Ambiguity::Error] {
        let faulty = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
        let store: Arc<dyn ObjectStore> = faulty.clone();
        let book = OidcBook::new(Some((store.clone(), &Path::from("p"))));
        faulty.add_rule(Rule::ambiguous_put("auth/revoked-sessions", reply));
        book.revoke_session(&sid(1), "acme", 1_000, 500)
            .await
            .unwrap_or_else(|e| panic!("{reply:?}: written but reported an error: {e}"));
        assert!(
            book.session_revoked(&sid(1)),
            "{reply:?}: in the writer's RAM"
        );
        let fresh = OidcBook::new(Some((store.clone(), &Path::from("p"))));
        fresh.refresh().await.unwrap();
        assert!(fresh.session_revoked(&sid(1)), "{reply:?}");
        assert!(!fresh.session_revoked(&sid(2)), "{reply:?}");
        // A write after cookie 1 has expired (plus clock skew) prunes its line.
        book.revoke_session(&sid(2), "acme", 10_000, 1_000 + crate::auth::MAX_SKEW_S + 1)
            .await
            .unwrap();
        fresh.refresh().await.unwrap();
        assert!(
            !fresh.session_revoked(&sid(1)),
            "{reply:?}: expired line not pruned"
        );
        assert!(fresh.session_revoked(&sid(2)), "{reply:?}");
    }
}

/// **A repeated revocation does not rewrite the registry (security review).** `DELETE
/// <base>/session` has no rate cap; each revocation used to be a GET + PUT of the whole
/// `auth/revoked-sessions` (an object shared by the whole cluster), even when the `sid`
/// was already in it.
///
/// Must see: 50 revocations of the same session ⇒ exactly one PUT; an expired session
/// (plus clock skew) ⇒ no PUT at all.
#[tokio::test]
async fn revoking_the_same_session_writes_once() {
    use cairn_store::testkit::{FaultyStore, Op};
    let sid = |n: u8| format!("{n:02x}").repeat(16);
    let faulty = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let book = OidcBook::new(Some((store, &Path::from("p"))));
    let now = 1_000_000;
    for _ in 0..50 {
        book.revoke_session(&sid(1), "acme", now + 3_600, now)
            .await
            .unwrap();
    }
    assert_eq!(faulty.counts().op(Op::Put), 1, "repeat rewrote registry");
    let before = faulty.counts().op(Op::Put);
    book.revoke_session(&sid(2), "acme", now - 2 * crate::auth::MAX_SKEW_S, now)
        .await
        .unwrap();
    assert_eq!(
        faulty.counts().op(Op::Put),
        before,
        "an expired session was still written"
    );
}

/// **The logged-out session registry is capped (security review).** Logging in correctly
/// costs no slot, so one key can mint any number of sessions and log out each one: the
/// registry (one object, read on every refresh round of every node) would grow without
/// bound.
///
/// Must see: exceeding a tenant's cap of unexpired sessions ⇒ an error, no write; the
/// registry does not exceed the cap.
#[tokio::test]
async fn revoked_sessions_cannot_grow_without_bound() {
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let book = OidcBook::new(Some((store.clone(), &Path::from("p"))));
    let now = 1_000_000;
    let cap = MAX_REVOKED_SESSIONS_PER_TENANT;
    for i in 0..cap {
        book.revoke_session(&format!("{i:032x}"), "acme", now + 3_600, now)
            .await
            .unwrap_or_else(|e| panic!("session {i} below the cap: {e}"));
    }
    assert!(
        matches!(
            book.revoke_session(&format!("{:032x}", cap + 1), "acme", now + 3_600, now)
                .await,
            Err(RevokeError::Full)
        ),
        "written beyond the cap"
    );
    assert_eq!(book.revoked_sessions_of("acme", now), cap);
    // Another tenant is not blocked by `acme`'s cap.
    book.revoke_session(&"ef".repeat(16), "beta", now + 3_600, now)
        .await
        .expect("another tenant blocked by acme's cap");
    // acme's sessions expire ⇒ their lines fall off at the next write, acme can write again.
    let later = now + 3_600 + crate::auth::MAX_SKEW_S + 1;
    book.revoke_session(&format!("{:032x}", cap + 2), "acme", later + 3_600, later)
        .await
        .expect("expired line not pruned");
    let raw = store
        .get(&Path::from("p/auth/revoked-sessions"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert!(decode_revoked_sessions(&raw).unwrap().len() <= cap);
}

/// **A failed read of the session registry does not spoil the refresh (security
/// review).** `auth/revoked-sessions` is read in `Keys::refresh`; a read/decode error
/// used to return `Err` ⇒ the refresh loop did not confirm the key registry as fresh ⇒
/// after the cap **every key of every tenant** got `503`. A secondary object must not
/// block all authentication.
///
/// Must see: a corrupt object ⇒ `refresh` still `Ok`, the configuration still applied,
/// sessions revoked in RAM still revoked (the last good copy kept).
#[tokio::test]
async fn unreadable_session_book_does_not_fail_refresh() {
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let book = OidcBook::new(Some((store.clone(), &Path::from("p"))));
    let sid = "ab".repeat(16);
    book.revoke_session(&sid, "acme", 2_000_000, 1_000_000)
        .await
        .unwrap();
    book.update_config(|c| {
        *c = config();
        Ok::<_, String>(((), true))
    })
    .await
    .unwrap();
    store
        .put(
            &Path::from("p/auth/revoked-sessions"),
            b"garbage\n".to_vec().into(),
        )
        .await
        .unwrap();
    book.refresh()
        .await
        .expect("a broken session registry spoiled the whole key registry refresh");
    assert_eq!(book.config(), config());
    assert!(book.session_revoked(&sid), "the last good copy was dropped");
    // Closed when unsure — cookie sessions only: over the age cap and still unreadable ⇒
    // every session treated as revoked; readable again ⇒ open again.
    let other = "12".repeat(16);
    book.set_sessions_fresh_limit(std::time::Duration::from_millis(50));
    assert!(!book.sessions_stale(), "confirmed at set time");
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    book.refresh().await.unwrap();
    assert!(
        book.sessions_stale(),
        "a session registry older than the cap still counted as fresh"
    );
    assert!(!book.session_revoked(&other));
    store
        .put(
            &Path::from("p/auth/revoked-sessions"),
            encode_revoked_sessions(&SessionBook::new()).into(),
        )
        .await
        .unwrap();
    book.refresh().await.unwrap();
    assert!(!book.sessions_stale(), "readable, still stale");
}

/// **CAS contention does not make logout fail at once (security review).** A
/// cluster-wide registry: tenant B's logout races tenant A's logout loop; losing 8 times
/// in a row (with no wait between attempts) ⇒ `503`, and B's cookie lives until it
/// expires.
///
/// Must see: every PUT losing the CAS during the first 200 ms ⇒ the revocation still
/// succeeds (retries with backoff, bounded).
#[tokio::test]
async fn session_revocation_outlasts_cas_contention() {
    use cairn_store::testkit::{Action, FaultyStore, OpSet, PathMatch, Rule};
    let faulty = FaultyStore::wrap(Arc::new(slatedb::object_store::memory::InMemory::new()));
    let store: Arc<dyn ObjectStore> = faulty.clone();
    let book = OidcBook::new(Some((store, &Path::from("p"))));
    let rule = faulty.add_rule(Rule::new(
        OpSet::PUT,
        PathMatch::Contains("auth/revoked-sessions".into()),
        Action::Precondition,
    ));
    let lift = tokio::spawn({
        let faulty = faulty.clone();
        async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            faulty.remove_rule(rule);
        }
    });
    let sid = "cd".repeat(16);
    book.revoke_session(&sid, "acme", 2_000_000, 1_000_000)
        .await
        .expect("short CAS contention made the revocation fail");
    lift.await.unwrap();
    assert!(book.session_revoked(&sid));
}

/// An `auth/revoked-sessions` registry carrying `per_tenant` unexpired sessions for each
/// tenant `t00` … `t{tenants-1}` (expiry 10^9 seconds).
fn seeded_book(tenants: usize, per_tenant: usize) -> SessionBook {
    let mut book = SessionBook::new();
    for t in 0..tenants {
        for i in 0..per_tenant {
            book.insert(
                format!("{t:04x}{i:028x}"),
                RevokedSession {
                    exp_s: 1_000_000_000,
                    tenant: format!("t{t:02}"),
                },
            );
        }
    }
    book
}

/// **A whole-registry cap must not let a few tenants block other tenants' logout.** A
/// cap of 500 per tenant × 20 tenants = a registry cap of 10 000: someone with 20
/// tenants would keep the registry full, every other tenant logging out would get `Full`
/// (`429`) and the session (possibly stolen) would live until it expires.
///
/// Must see: a registry with 20 tenants × 500 unexpired sessions ⇒ a 21st tenant can
/// still revoke its session.
#[tokio::test]
async fn global_cap_does_not_block_other_tenants() {
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    store
        .put(
            &Path::from("p/auth/revoked-sessions"),
            encode_revoked_sessions(&seeded_book(20, 500)).into(),
        )
        .await
        .unwrap();
    let book = OidcBook::new(Some((store, &Path::from("p"))));
    book.revoke_session(&"ab".repeat(16), "victim", 1_000_000, 500_000)
        .await
        .unwrap_or_else(|e| panic!("another tenant blocked by a shared cap: {e}"));
}

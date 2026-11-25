// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Request identity towards customer endpoints.
//!
//! Every discovery and invoke request carries:
//!
//! - `x-cairn-signature-scheme: v1`;
//! - `x-cairn-jwt-v1: <EdDSA JWT>` — header `{alg: EdDSA, typ: JWT, kid}`,
//!   claims `aud` = the request path, `sub` = tenant, `iat`, `nbf` = `iat − 60`,
//!   `exp` = `iat + 60` (seconds), `jti` = `<invocation id hex>:<attempt>`
//!   (matching `Start.id` and `Start.retry_count_since_last_stored_entry` of
//!   the request itself; discovery: `discover`), `host` = the host of the
//!   request URL (exactly the `Host` header string), and on `/invoke` `bsh` =
//!   base64url(sha256 of the body **before** compression; BIDI: the body that
//!   opens the turn — `Start` + the replayed part). `host`/`bsh` bind the JWT
//!   to the destination and body of the request itself: a captured JWT cannot
//!   carry a forged body. Binding to the **destination** is only certain when
//!   the SDK sets `identityHosts` (the registered public hosts): without it the
//!   SDK compares `host` with the request's own `Host` header — text chosen by
//!   the sender — so the JWT can be replayed at another endpoint with the same
//!   path and the same tenant that does not route by host (a request without a
//!   host is refused). `jti` keeps its old layout (older SDKs compare it
//!   verbatim); the turn (`known_entries`) is already inside the `Start` that
//!   `bsh` covers.
//! - `nonce` = base64url of [`NONCE_LEN`] fresh random bytes, drawn for every
//!   request ([`fresh_nonce`]). `jti`, `iat` (whole seconds) and `bsh` alone do
//!   not make two requests differ: resuming a paused `BIDI_STREAM` invocation
//!   twice within one second, with no journal growth in between, opens two
//!   streams with the same `Start` — and Ed25519 is deterministic, so without
//!   `nonce` both carry the same JWT, byte for byte. Every verifying SDK lets
//!   one JWT open one stream (a ledger keyed by the signature), so the second
//!   stream was refused. The SDKs ignore claims they do not know: `nonce` needs
//!   no SDK change, it only makes every signature distinct.
//!
//! `kid` = `publickeyv1_<base58 of the public key>` (the same format as
//! Restate's service protocol, for compatibility).
//!
//! **Per-tenant keys, derived, never stored.** The private key of `(tenant,
//! generation)` is the Ed25519 seed = `HKDF-SHA256(empty salt, ikm = master,
//! info = "cairn-identity/v1" ‖ len(tenant) u32be ‖ tenant ‖ generation u64be)`.
//! The tenant length is in `info` so that `("a1", 2)` and `("a", 12)` never
//! produce the same `info`. The cell's master (`CAIRNFLOW_IDENTITY_MASTER`,
//! ≥ 32 bytes) lives outside the bucket: backups and `cairn-ctl fork` carry no
//! private keys, and a restore never silently generates new keys. No master ⇒
//! no signing (self-hosting as before request identity existed).
//!
//! **Deterministic.** HKDF and Ed25519 (`ring`) use no randomness: an Ed25519
//! signature is a function of the key and the message. The claim times come
//! from the caller (the engine's injectable clock, `cairn_store::entropy::now_ms`
//! at ingress) — the JWT is signed by hand here, not through a library that
//! reads `SystemTime::now` itself. The `nonce` also comes from the caller
//! ([`Bound::nonce`]); [`fresh_nonce`] draws it from `cairn_store::entropy`,
//! which the deterministic simulation seeds.
//!
//! **Generation** (`tenants`: `identity=<signing>`, `identity-keys=<published>`;
//! absent = 1 and {1}) follows four rotation stages, each one a
//! `cairn-ctl identity …` command recorded in the audit log: publish `g+1`,
//! the customer adds the `kid`, activate (sign with `g+1`), retire the old key.

use {
    base64::Engine as _,
    cairn_store::TenantId,
    std::{
        collections::BTreeMap,
        sync::{Arc, RwLock},
    },
};

/// Header declaring the signature scheme.
pub const SCHEME_HEADER: &str = "x-cairn-signature-scheme";
/// The only value of [`SCHEME_HEADER`] today.
pub const SCHEME_V1: &str = "v1";
/// Header mang JWT.
pub const JWT_HEADER: &str = "x-cairn-jwt-v1";
/// Environment variable carrying the cell's master.
pub const MASTER_ENV: &str = "CAIRNFLOW_IDENTITY_MASTER";
/// Minimum master length (bytes).
pub const MIN_MASTER_LEN: usize = 32;
/// `nbf`/`exp` margin around `iat`, seconds.
pub const SKEW_S: u64 = 60;
/// Prefix of `kid`.
pub const KID_PREFIX: &str = "publickeyv1_";
/// The `jti` of discovery: there is no invocation to bind to.
pub const DISCOVER_JTI: &str = "discover";
/// Bytes of the `nonce` claim (before base64url): 128 bits, so two requests
/// never draw the same one — not on one node, not across nodes.
pub const NONCE_LEN: usize = 16;

const INFO: &[u8] = b"cairn-identity/v1";

/// The cell's master. `Debug` does not print the bytes.
#[derive(Clone)]
pub struct Master(Arc<[u8]>);

impl std::fmt::Debug for Master {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Master(<{} byte>)", self.0.len())
    }
}

impl Master {
    /// `raw` is the environment variable's bytes. Shorter than
    /// [`MIN_MASTER_LEN`] ⇒ error: the node refuses to start instead of signing
    /// with a weak secret.
    pub fn new(raw: &[u8]) -> Result<Self, String> {
        if raw.len() < MIN_MASTER_LEN {
            return Err(format!(
                "{MASTER_ENV} must be at least {MIN_MASTER_LEN} bytes long (got {})",
                raw.len()
            ));
        }
        Ok(Self(Arc::from(raw)))
    }
}

/// A tenant's key generations: the one signing and the set published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Generations {
    pub active: u64,
    /// Always contains `active`.
    pub published: std::collections::BTreeSet<u64>,
}

impl Default for Generations {
    fn default() -> Self {
        Self {
            active: 1,
            published: [1].into(),
        }
    }
}

/// One key-rotation stage of a tenant. Between `Publish` and `Activate`, the
/// customer adds the new `kid` to `serve({identityKeys})`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityStep {
    /// Publish `g+1` (both keys at `GET /identity`), still signing with `g`.
    Publish,
    /// Sign with the highest published generation.
    Activate,
    /// Remove every generation other than the one signing.
    Retire,
}

impl IdentityStep {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "publish" => Some(Self::Publish),
            "activate" => Some(Self::Activate),
            "retire" => Some(Self::Retire),
            _ => None,
        }
    }
}

impl Generations {
    /// The generation after `step`, or the reason for refusing. Never "sign
    /// with the newest key" at once: activate only what was published before,
    /// publish only one waiting key at a time, retire only when keys other
    /// than the signing one remain.
    pub fn after(&self, step: IdentityStep) -> Result<Generations, String> {
        let newest = self.published.last().copied().unwrap_or(self.active);
        let mut next = self.clone();
        match step {
            IdentityStep::Publish => {
                if newest > self.active {
                    return Err(format!(
                        "generation {newest} is published but not activated — `activate` or retire it first"
                    ));
                }
                next.published.insert(self.active + 1);
            }
            IdentityStep::Activate => {
                if newest <= self.active {
                    return Err(format!(
                        "no generation newer than {} is waiting — `publish` first",
                        self.active
                    ));
                }
                next.active = newest;
            }
            IdentityStep::Retire => {
                if self.published.len() <= 1 {
                    return Err("only the signing key is left — nothing to retire".into());
                }
                if newest > self.active {
                    return Err(format!(
                        "generation {newest} is waiting for activation — `activate` before retiring the old key"
                    ));
                }
                next.published = [self.active].into();
            }
        }
        Ok(next)
    }

    /// `active=<g> published=<g1,g2>` — for the audit record.
    pub fn describe(&self) -> String {
        let keys: Vec<String> = self.published.iter().map(u64::to_string).collect();
        format!("active={} published={}", self.active, keys.join(","))
    }
}

fn seed(master: &Master, tenant: &str, generation: u64) -> [u8; 32] {
    let mut info = Vec::with_capacity(INFO.len() + 4 + tenant.len() + 8);
    info.extend_from_slice(INFO);
    info.extend_from_slice(&(tenant.len() as u32).to_be_bytes());
    info.extend_from_slice(tenant.as_bytes());
    info.extend_from_slice(&generation.to_be_bytes());
    let prk = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, &[]).extract(&master.0);
    let info = [info.as_slice()];
    let okm = prk
        .expand(&info, ring::hkdf::HKDF_SHA256)
        .expect("32 bytes is within HKDF-SHA256's limit");
    let mut out = [0u8; 32];
    okm.fill(&mut out).expect("length matches HKDF_SHA256");
    out
}

/// The signing key of `(tenant, generation)`.
fn keypair(master: &Master, tenant: &str, generation: u64) -> ring::signature::Ed25519KeyPair {
    ring::signature::Ed25519KeyPair::from_seed_unchecked(&seed(master, tenant, generation))
        .expect("a 32-byte seed is always a valid Ed25519 key")
}

/// `publickeyv1_<base58>` of public key `pk`.
pub fn key_id(pk: &[u8]) -> String {
    format!("{KID_PREFIX}{}", base58(pk))
}

/// Base58 (the Bitcoin alphabet), as Restate's service protocol uses for `kid`.
pub fn base58(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let zeros = bytes.iter().take_while(|&&b| b == 0).count();
    // Base58 digits, least significant first.
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len() * 138 / 100 + 1);
    for &b in &bytes[zeros..] {
        let mut carry = u32::from(b);
        for d in digits.iter_mut() {
            carry += u32::from(*d) << 8;
            *d = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + digits.len());
    out.extend(std::iter::repeat_n('1', zeros));
    out.extend(digits.iter().rev().map(|&d| ALPHABET[d as usize] as char));
    out
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// One published public key of a tenant.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PublishedKey {
    pub generation: u64,
    pub kid: String,
    /// This generation is signing.
    pub active: bool,
}

/// Derived keys by `(tenant, generation)`.
type KeyCache = BTreeMap<(String, u64), Arc<ring::signature::Ed25519KeyPair>>;

/// The node's signer: the master (if any) and the per-tenant generations. `Clone` is cheap.
#[derive(Clone, Debug, Default)]
pub struct Identity {
    master: Option<Master>,
    /// Generations per tenant (the latest read of the `tenants` registry);
    /// absent = [`Generations::default`].
    gens: Arc<RwLock<BTreeMap<String, Generations>>>,
    /// Derived keys, by `(tenant, generation)`.
    keys: Arc<RwLock<KeyCache>>,
}

impl Identity {
    /// No master ⇒ no signing.
    pub fn new(master: Option<Master>) -> Self {
        Self {
            master,
            ..Default::default()
        }
    }

    pub fn signs(&self) -> bool {
        self.master.is_some()
    }

    /// Replaces the generation table (the `tenants` registry was just re-read).
    pub fn set_generations(&self, gens: BTreeMap<String, Generations>) {
        if let Ok(mut g) = self.gens.write() {
            *g = gens;
        }
    }

    pub fn generations(&self, tenant: &str) -> Generations {
        self.gens
            .read()
            .ok()
            .and_then(|g| g.get(tenant).cloned())
            .unwrap_or_default()
    }

    fn key(&self, tenant: &str, generation: u64) -> Option<Arc<ring::signature::Ed25519KeyPair>> {
        let master = self.master.as_ref()?;
        let k = (tenant.to_string(), generation);
        if let Some(kp) = self.keys.read().ok().and_then(|m| m.get(&k).cloned()) {
            return Some(kp);
        }
        let kp = Arc::new(keypair(master, tenant, generation));
        if let Ok(mut m) = self.keys.write() {
            m.insert(k, Arc::clone(&kp));
        }
        Some(kp)
    }

    /// The published public keys of `tenant` (`GET /identity`). Empty when the
    /// node does not sign.
    pub fn published(&self, tenant: &TenantId) -> Vec<PublishedKey> {
        let g = self.generations(tenant.as_str());
        g.published
            .iter()
            .filter_map(|&gen| {
                use ring::signature::KeyPair as _;
                let kp = self.key(tenant.as_str(), gen)?;
                Some(PublishedKey {
                    generation: gen,
                    kid: key_id(kp.public_key().as_ref()),
                    active: gen == g.active,
                })
            })
            .collect()
    }

    /// The JWT for a request to `aud` (path) on behalf of `tenant`, signed with
    /// the active generation, bound to `bound` (host, body). `None` when the
    /// node does not sign.
    pub fn sign(
        &self,
        tenant: &str,
        aud: &str,
        jti: &str,
        now_ms: u64,
        bound: Bound<'_>,
    ) -> Option<String> {
        let gen = self.generations(tenant).active;
        let kp = self.key(tenant, gen)?;
        Some(jwt(&kp, tenant, aud, jti, now_ms, bound))
    }

    /// The two identity headers, ready to attach to a request. Empty when not signing.
    pub fn headers(
        &self,
        tenant: &str,
        aud: &str,
        jti: &str,
        now_ms: u64,
        bound: Bound<'_>,
    ) -> Vec<(&'static str, String)> {
        match self.sign(tenant, aud, jti, now_ms, bound) {
            Some(t) => vec![(SCHEME_HEADER, SCHEME_V1.into()), (JWT_HEADER, t)],
            None => Vec::new(),
        }
    }
}

/// The part of the request the JWT is bound to: the URL's host (claim `host`),
/// the body before compression (claim `bsh`), and this one request (claim
/// `nonce`, see the module doc). Absent ⇒ no such claim. Every request the
/// runtime sends carries a nonce ([`crate::invoker::Signing::apply`]); only
/// tests sign without one.
#[derive(Clone, Copy, Debug, Default)]
pub struct Bound<'a> {
    pub host: Option<&'a str>,
    pub body: Option<&'a [u8]>,
    pub nonce: Option<[u8; NONCE_LEN]>,
}

/// A fresh `nonce` for one request, from `cairn_store::entropy` (seeded in the
/// deterministic simulation, the operating system's source otherwise).
pub fn fresh_nonce() -> [u8; NONCE_LEN] {
    let mut n = [0u8; NONCE_LEN];
    cairn_store::entropy::fill(&mut n);
    n
}

/// The `jti` of a turn: `<id hex>:<attempt>`.
pub fn invocation_jti(id: &cairn_store::InvocationId, retry_count: u32) -> String {
    format!("{}:{retry_count}", cairn_store::id_hex(id))
}

fn jwt(
    kp: &ring::signature::Ed25519KeyPair,
    tenant: &str,
    aud: &str,
    jti: &str,
    now_ms: u64,
    bound: Bound<'_>,
) -> String {
    use ring::signature::KeyPair as _;
    let iat = now_ms / 1000;
    let header = serde_json::json!({
        "alg": "EdDSA",
        "typ": "JWT",
        "kid": key_id(kp.public_key().as_ref()),
    });
    let mut claims = serde_json::json!({
        "aud": aud,
        "sub": tenant,
        "iat": iat,
        "nbf": iat.saturating_sub(SKEW_S),
        "exp": iat + SKEW_S,
        "jti": jti,
    });
    if let Some(h) = bound.host {
        claims["host"] = h.into();
    }
    if let Some(body) = bound.body {
        claims["bsh"] = b64(ring::digest::digest(&ring::digest::SHA256, body).as_ref()).into();
    }
    if let Some(nonce) = bound.nonce {
        claims["nonce"] = b64(&nonce).into();
    }
    let signing_input = format!(
        "{}.{}",
        b64(header.to_string().as_bytes()),
        b64(claims.to_string().as_bytes())
    );
    let sig = kp.sign(signing_input.as_bytes());
    format!("{signing_input}.{}", b64(sig.as_ref()))
}

/// Verifies `token` with public key `kid` (for tests and `cairn-ctl`): returns
/// the claims when the signature is valid. Does not check times — that is the
/// receiver's job.
pub fn verify(token: &str, kid: &str) -> Result<serde_json::Value, String> {
    let pk = kid
        .strip_prefix(KID_PREFIX)
        .and_then(unbase58)
        .ok_or_else(|| format!("bad kid: {kid}"))?;
    let (input, sig) = token.rsplit_once('.').ok_or("JWT has no signature")?;
    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig)
        .map_err(|e| format!("signature is not base64url: {e}"))?;
    ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &pk)
        .verify(input.as_bytes(), &sig)
        .map_err(|_| "invalid signature".to_string())?;
    let (header, claims) = input.split_once('.').ok_or("JWT is missing a part")?;
    let header: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(header)
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    if header["kid"] != kid || header["alg"] != "EdDSA" {
        return Err(format!("JWT header does not match: {header}"));
    }
    serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(claims)
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())
}

fn unbase58(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let zeros = s.bytes().take_while(|&c| c == b'1').count();
    let mut bytes: Vec<u8> = Vec::new(); // least significant first
    for c in s.bytes().skip(zeros) {
        let mut carry = ALPHABET.iter().position(|&a| a == c)? as u32;
        for b in bytes.iter_mut() {
            carry += u32::from(*b) * 58;
            *b = (carry & 0xff) as u8;
            carry >>= 8;
        }
        while carry > 0 {
            bytes.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let mut out = vec![0u8; zeros];
    out.extend(bytes.iter().rev());
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn master() -> Master {
        Master::new(b"cf_test_golden_vector_master_key").unwrap()
    }

    #[test]
    fn short_master_is_refused() {
        assert!(Master::new(&[1u8; 31]).is_err());
        assert!(Master::new(b"").is_err());
    }

    /// Base58 vectors from the `bs58` test suite.
    #[test]
    fn base58_matches_bitcoin_alphabet() {
        assert_eq!(base58(b""), "");
        assert_eq!(base58(&[0]), "1");
        assert_eq!(base58(&[0, 0, 1]), "112");
        assert_eq!(base58(b"hello world"), "StV1DL6CwTryKyV");
        assert_eq!(base58(&[0xff; 4]), "7YXq9G");
        for v in [&b"hello world"[..], &[0, 0, 7, 255][..], &[9u8; 32][..]] {
            assert_eq!(unbase58(&base58(v)).as_deref(), Some(v));
        }
    }

    /// The key is a function of `(master, tenant, generation)`: deterministic,
    /// and different in every component — including two `(tenant,
    /// generation)` pairs whose raw concatenation is the same.
    #[test]
    fn keys_are_deterministic_and_separated() {
        use ring::signature::KeyPair as _;
        let m = master();
        let pk = |t: &str, g: u64| keypair(&m, t, g).public_key().as_ref().to_vec();
        assert_eq!(pk("acme", 1), pk("acme", 1));
        assert_ne!(pk("acme", 1), pk("acme", 2));
        assert_ne!(pk("acme", 1), pk("beta", 1));
        assert_ne!(pk("a1", 2), pk("a", 12));
        let other = Master::new(&[43u8; 32]).unwrap();
        assert_ne!(
            keypair(&other, "acme", 1).public_key().as_ref(),
            pk("acme", 1).as_slice()
        );
    }

    #[test]
    fn jwt_verifies_and_carries_claims() {
        let id = Identity::new(Some(master()));
        let acme = TenantId::new("acme").unwrap();
        let kid = id.published(&acme)[0].kid.clone();
        assert!(kid.starts_with(KID_PREFIX));
        let t = id
            .sign(
                "acme",
                "/invoke/S/h",
                "ab:0",
                1_700_000_000_123,
                Bound::default(),
            )
            .unwrap();
        let c = verify(&t, &kid).unwrap();
        assert!(c.get("host").is_none() && c.get("bsh").is_none(), "{c}");
        assert_eq!(c["aud"], "/invoke/S/h");
        assert_eq!(c["sub"], "acme");
        assert_eq!(c["jti"], "ab:0");
        assert_eq!(c["iat"], 1_700_000_000u64);
        assert_eq!(c["nbf"], 1_700_000_000u64 - SKEW_S);
        assert_eq!(c["exp"], 1_700_000_000u64 + SKEW_S);
        // Same input ⇒ same bytes (Ed25519 is deterministic).
        assert_eq!(
            Some(t.clone()),
            id.sign(
                "acme",
                "/invoke/S/h",
                "ab:0",
                1_700_000_000_123,
                Bound::default()
            )
        );
        // Host and body: claim `host` verbatim, `bsh` = base64url(sha256).
        let b = id
            .sign(
                "acme",
                "/invoke/S/h",
                "ab:0",
                1_700_000_000_123,
                Bound {
                    host: Some("sdk.example:8080"),
                    body: Some(b"abc"),
                    nonce: None,
                },
            )
            .unwrap();
        let c = verify(&b, &kid).unwrap();
        assert_eq!(c["host"], "sdk.example:8080");
        assert_eq!(c["bsh"], "ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0");
        assert!(c.get("nonce").is_none(), "{c}");
        // Nonce: claim `nonce` = base64url of the bytes; another nonce ⇒ another
        // JWT for the same request at the same second.
        let with = |nonce| {
            id.sign(
                "acme",
                "/invoke/S/h",
                "ab:0",
                1_700_000_000_123,
                Bound {
                    nonce: Some(nonce),
                    ..Bound::default()
                },
            )
            .unwrap()
        };
        let n1 = with([1; NONCE_LEN]);
        assert_eq!(
            verify(&n1, &kid).unwrap()["nonce"],
            "AQEBAQEBAQEBAQEBAQEBAQ"
        );
        assert_ne!(n1, with([2; NONCE_LEN]));
        assert_ne!(fresh_nonce(), fresh_nonce());
        // Another tenant's key does not verify.
        let beta = id.published(&TenantId::new("beta").unwrap())[0].kid.clone();
        assert!(verify(&t, &beta).is_err());
        // No master ⇒ no signing, nothing published.
        assert!(Identity::default()
            .sign("acme", "/", "x", 0, Bound::default())
            .is_none());
        assert!(Identity::default().published(&acme).is_empty());
    }

    /// Golden vector — `sdk/typescript/test/identity.test.ts`,
    /// `sdk/go/internal/identity/identity_test.go` and
    /// `sdk/python/tests/test_endpoint.py` verify exactly this string. Changing
    /// the derivation, the JWT layout or base58 is a wire change: every copy
    /// changes in the same commit.
    #[test]
    fn golden_vector_is_stable() {
        let id = Identity::new(Some(master()));
        let kid = id.published(&TenantId::new("acme").unwrap())[0].kid.clone();
        assert_eq!(kid, GOLDEN_KID);
        let t = id
            .sign(
                "acme",
                "/invoke/S/h",
                "ab:0",
                1_700_000_000_123,
                Bound {
                    host: Some(GOLDEN_HOST),
                    body: Some(GOLDEN_BODY),
                    nonce: Some(*GOLDEN_NONCE),
                },
            )
            .unwrap();
        assert_eq!(t, GOLDEN_JWT);
    }

    const GOLDEN_HOST: &str = "sdk.example:8080";
    const GOLDEN_BODY: &[u8] = b"golden-body";
    /// Claim `nonce` = `Z29sZGVuLW5vbmNlLTE2Yg` (base64url of these 16 bytes).
    const GOLDEN_NONCE: &[u8; NONCE_LEN] = b"golden-nonce-16b";
    const GOLDEN_KID: &str = "publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr";
    const GOLDEN_JWT: &str = "eyJhbGciOiJFZERTQSIsImtpZCI6InB1YmxpY2tleXYxXzNDUFladXBoSEFxRk5zaXg5eTkxeGhXOEw2Q245c1R1TDJ0ZlRWRGhaSmRyIiwidHlwIjoiSldUIn0.eyJhdWQiOiIvaW52b2tlL1MvaCIsImJzaCI6IlpUUDBpVUVFWDhCT1otdzdycVh0RzFwZHVMSDJWdXFWUGtpQ1o0OXR1ZzAiLCJleHAiOjE3MDAwMDAwNjAsImhvc3QiOiJzZGsuZXhhbXBsZTo4MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJqdGkiOiJhYjowIiwibmJmIjoxNjk5OTk5OTQwLCJub25jZSI6IloyOXNaR1Z1TFc1dmJtTmxMVEUyWWciLCJzdWIiOiJhY21lIn0.ye5aaOY8bgpblEegb7oSCNtv_Ue0Lx56LK40CuSZC9XaXOb9IwBKHxiLhzf2y3UQkNyjf2rHDErF4DBul47yAg";

    /// The rules of each stage: publish one waiting key at a time, activate
    /// only when a key is waiting, retire only once activated and an old key
    /// remains.
    #[test]
    fn rotation_steps_refuse_out_of_order() {
        let g1 = Generations::default();
        assert!(g1.after(IdentityStep::Activate).is_err());
        assert!(g1.after(IdentityStep::Retire).is_err());
        let p = g1.after(IdentityStep::Publish).unwrap();
        assert_eq!((p.active, p.published.clone()), (1, [1, 2].into()));
        assert!(p.after(IdentityStep::Publish).is_err());
        assert!(p.after(IdentityStep::Retire).is_err());
        let a = p.after(IdentityStep::Activate).unwrap();
        assert_eq!((a.active, a.published.clone()), (2, [1, 2].into()));
        let r = a.after(IdentityStep::Retire).unwrap();
        assert_eq!((r.active, r.published.clone()), (2, [2].into()));
        assert_eq!(r.describe(), "active=2 published=2");
        // The next round continues from 2.
        assert_eq!(
            r.after(IdentityStep::Publish).unwrap().published,
            [2, 3].into()
        );
    }

    /// The four rotation stages at the generation level: publish two keys, sign
    /// with the active one.
    #[test]
    fn rotation_publishes_two_and_signs_with_active() {
        let id = Identity::new(Some(master()));
        let acme = TenantId::new("acme").unwrap();
        let g1 = id.published(&acme)[0].kid.clone();
        id.set_generations(
            [(
                "acme".to_string(),
                Generations {
                    active: 1,
                    published: [1, 2].into(),
                },
            )]
            .into(),
        );
        let both = id.published(&acme);
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].kid, g1);
        assert!(both[0].active && !both[1].active);
        let t = id
            .sign("acme", "/discover", DISCOVER_JTI, 0, Bound::default())
            .unwrap();
        assert!(verify(&t, &g1).is_ok());
        id.set_generations(
            [(
                "acme".to_string(),
                Generations {
                    active: 2,
                    published: [1, 2].into(),
                },
            )]
            .into(),
        );
        let t = id
            .sign("acme", "/discover", DISCOVER_JTI, 0, Bound::default())
            .unwrap();
        assert!(verify(&t, &both[1].kid).is_ok());
        assert!(verify(&t, &g1).is_err());
    }
}

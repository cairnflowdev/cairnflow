// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Segment read URLs for SDK wire v6.
//!
//! The node neither GETs nor holds large values for v6 SDKs: the `Value` goes down exactly
//! as in the journal (refs + tail), and each ref comes with a short-lived URL so the SDK
//! fetches the bytes itself:
//!
//! - **`direct`**: a store GET signed in advance (S3 v4 presign, `object_store::signer`),
//!   exactly one key, valid for `ttl`. Signing is a local computation on the client's
//!   credentials.
//! - **`proxy`**: `<base>/values/v1/<token>` — an HMAC token of the node
//!   ([`Token`]), openable by any node of the cluster (the key is derived from the cluster
//!   secret). The route is in `ingress/values.rs`.
//! - **`inline`**: no URL; the shell assembles the value as for older SDKs, v6 included.
//!
//! **Security** (reviewed with `/security-review`): URLs are only built for refs that
//! passed `Logical::of` of a message about to be sent down to the tenant's **own**
//! deployment, with the owner derived from the invocation's target — there is no path that
//! signs a key chosen by the SDK. A URL is a read capability: never log it (not even at `debug`).

use {
    base64::Engine as _,
    cairn_protocol::pb,
    cairn_store::{
        value::{Owner, Segment},
        PartitionKey,
    },
    slatedb::object_store::signer::Signer,
    std::{sync::Arc, time::Duration},
};

/// `--value-read-path`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ReadPath {
    /// `direct` if the store is S3 and `--value-public-endpoint` is set; `proxy` if
    /// `--value-public-url` is set; otherwise `inline`.
    #[default]
    Auto,
    Direct,
    Proxy,
    Inline,
}

impl std::str::FromStr for ReadPath {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "auto" => Ok(Self::Auto),
            "direct" => Ok(Self::Direct),
            "proxy" => Ok(Self::Proxy),
            "inline" => Ok(Self::Inline),
            _ => Err(format!(
                "unknown value read path {s:?} (expected auto, direct, proxy or inline)"
            )),
        }
    }
}

/// Default lifetime of a URL (`--value-presign-ttl-s`).
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);
/// Lifetime cap: a URL is a read capability; the cap stops an operator from setting it to days
/// by mistake.
pub const MAX_TTL: Duration = Duration::from_secs(3600);

/// The node's `--value-*` flags.
#[derive(Clone, Debug, Default)]
pub struct ValueConfig {
    pub read_path: ReadPath,
    /// `--value-presign-ttl-s`; `None` = [`DEFAULT_TTL`].
    pub ttl: Option<Duration>,
    /// `--value-public-endpoint`: the store endpoint the customer's app sees.
    pub public_endpoint: Option<String>,
    /// `--value-public-url`: the base address of the node's ingress that the customer's app sees.
    pub public_url: Option<String>,
    /// SaaS profile: `auto` picks `direct` on an S3 store even without
    /// `--value-public-endpoint` (the store's endpoint is already public).
    pub prefer_direct: bool,
}

impl ValueUrls {
    /// The node's URL issuer per `cfg`: `auto` ⇒ `direct` if the store is S3 and has a
    /// public endpoint; `proxy` if there is a public URL; otherwise `inline`. An explicit
    /// choice that cannot be honoured ⇒ a start-up error.
    /// `ingress_url`: the bound ingress address (`http://ip:port`) — the default for
    /// `proxy` when the IP is specific.
    pub fn build(
        cfg: &ValueConfig,
        main: &cairn_store::StoreLocation,
        secret: &crate::auth::ClusterSecret,
        ingress_url: &str,
    ) -> Result<Self, String> {
        let ttl = cfg.ttl.unwrap_or(DEFAULT_TTL);
        if ttl.is_zero() || ttl > MAX_TTL {
            return Err(format!(
                "--value-presign-ttl-s must be between 1 and {}",
                MAX_TTL.as_secs()
            ));
        }
        let values =
            || cairn_store::value::ValueStore::new(Arc::clone(main.store()), main.prefix());
        let direct = |endpoint: Option<&str>| -> Result<Self, String> {
            match main.presigner(endpoint) {
                Some(Ok(signer)) => Ok(Self::direct(signer, values(), ttl)),
                Some(Err(e)) => Err(format!("--value-read-path direct: {e}")),
                None => Err(format!(
                    "--value-read-path direct needs an s3:// main store, not {}",
                    main.uri()
                )),
            }
        };
        let specific = specific_url(ingress_url);
        let proxy = |url: Option<&str>| -> Result<Self, String> {
            let base = url.map(str::to_string).or_else(|| specific.clone());
            let Some(base) = base else {
                return Err(
                    "--value-read-path proxy needs --value-public-url when the node binds an unspecified address"
                        .into(),
                );
            };
            check_base(&base)?;
            Ok(Self::proxy(Some(base), ProxyKey::from_cluster(secret), ttl))
        };
        match cfg.read_path {
            ReadPath::Inline => Ok(Self::inline()),
            ReadPath::Direct => direct(cfg.public_endpoint.as_deref()),
            ReadPath::Proxy => proxy(cfg.public_url.as_deref()),
            ReadPath::Auto => {
                let s3 = main.presigner(None).is_some();
                match (&cfg.public_endpoint, &cfg.public_url) {
                    (Some(e), _) if s3 => direct(Some(e)),
                    (None, _) if s3 && cfg.prefer_direct => direct(None),
                    (_, Some(u)) => proxy(Some(u)),
                    _ => Ok(Self::inline()),
                }
            }
        }
    }
}

/// `ingress_url` if its IP is specific (not `0.0.0.0`/`[::]`).
fn specific_url(ingress_url: &str) -> Option<String> {
    let u = url::Url::parse(ingress_url).ok()?;
    let unspecified = match u.host()? {
        url::Host::Ipv4(ip) => ip.is_unspecified(),
        url::Host::Ipv6(ip) => ip.is_unspecified(),
        url::Host::Domain(_) => false,
    };
    (!unspecified).then(|| ingress_url.trim_end_matches('/').to_string())
}

/// `base` is an `http(s)://` URL without query or fragment.
fn check_base(base: &str) -> Result<(), String> {
    match url::Url::parse(base) {
        Ok(u)
            if matches!(u.scheme(), "http" | "https")
                && u.host().is_some()
                && u.query().is_none()
                && u.fragment().is_none() =>
        {
            Ok(())
        }
        _ => Err(format!(
            "--value-public-url {base:?} must be an http(s) URL without query or fragment"
        )),
    }
}

/// HKDF label of the `proxy` token key.
const PROXY_KEY_INFO: &[u8] = b"cairnflow/value-proxy/1";
/// Token layout version.
const TOKEN_V1: u8 = 1;
/// Route prefix of `proxy` (after `base`).
pub const PROXY_ROUTE: &str = "/values/v1/";

/// The node's URL issuer.
pub struct ValueUrls {
    kind: Kind,
    ttl: Duration,
}

enum Kind {
    Inline,
    Direct {
        signer: Arc<dyn Signer>,
        /// Only used to build object paths (`values.path`), no I/O.
        values: cairn_store::value::ValueStore,
    },
    Proxy {
        /// The base address the customer's app can reach (`http://host:port`, no trailing
        /// `/`). Set late when taken from the bound address ([`ValueUrls::bound`]).
        base: std::sync::OnceLock<String>,
        key: ProxyKey,
    },
}

impl std::fmt::Debug for ValueUrls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self.kind {
            Kind::Inline => "ValueUrls(inline)",
            Kind::Direct { .. } => "ValueUrls(direct)",
            Kind::Proxy { .. } => "ValueUrls(proxy)",
        })
    }
}

impl Default for ValueUrls {
    fn default() -> Self {
        Self::inline()
    }
}

impl ValueUrls {
    pub fn inline() -> Self {
        Self {
            kind: Kind::Inline,
            ttl: DEFAULT_TTL,
        }
    }

    pub fn direct(
        signer: Arc<dyn Signer>,
        values: cairn_store::value::ValueStore,
        ttl: Duration,
    ) -> Self {
        Self {
            kind: Kind::Direct { signer, values },
            ttl,
        }
    }

    /// `base` absent ⇒ set later with [`Self::bound`].
    pub fn proxy(base: Option<String>, key: ProxyKey, ttl: Duration) -> Self {
        let cell = std::sync::OnceLock::new();
        if let Some(b) = base {
            let _ = cell.set(b.trim_end_matches('/').to_string());
        }
        Self {
            kind: Kind::Proxy { base: cell, key },
            ttl,
        }
    }

    /// Set `proxy`'s `base` from the bound ingress address, if not set yet.
    pub fn bound(&self, addr: std::net::SocketAddr) {
        if let Kind::Proxy { base, .. } = &self.kind {
            let _ = base.set(format!("http://{addr}"));
        }
    }

    /// Whether refs are sent (instead of assembling) to v6 SDKs.
    pub fn sends_refs(&self) -> bool {
        match &self.kind {
            Kind::Inline => false,
            Kind::Direct { .. } => true,
            Kind::Proxy { base, .. } => base.get().is_some(),
        }
    }

    pub fn name(&self) -> &'static str {
        match self.kind {
            Kind::Inline => "inline",
            Kind::Direct { .. } => "direct",
            Kind::Proxy { .. } => "proxy",
        }
    }

    /// The URL of each segment, in the same order. `now_ms`: the engine's clock (token
    /// expiry). Never log the result.
    pub async fn urls(&self, segs: &[Segment], now_ms: u64) -> Result<Vec<pb::SegmentUrl>, String> {
        let mut out = Vec::with_capacity(segs.len());
        for s in segs {
            let url = match &self.kind {
                Kind::Inline => return Err("value read path is inline".into()),
                Kind::Direct { signer, values } => {
                    let path = values.path(s);
                    signer
                        .signed_url(reqwest::Method::GET, &path, self.ttl)
                        .await
                        .map_err(|e| format!("cannot presign a segment URL: {e}"))?
                        .to_string()
                }
                Kind::Proxy { base, key } => {
                    let Some(base) = base.get() else {
                        return Err("value proxy has no public URL yet".into());
                    };
                    let exp = now_ms.saturating_add(self.ttl.as_millis() as u64);
                    format!("{base}{PROXY_ROUTE}{}", Token::seal(key, s, exp))
                }
            };
            out.push(pb::SegmentUrl {
                sha256: s.sha.to_vec().into(),
                gen: s.gen,
                url,
            });
        }
        Ok(out)
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// The URLs of every ref in `msgs` (each `(sha, gen)` once, in order of appearance),
    /// segments under the owner `(tenant, pk, owner)` — the owner of the invocation
    /// **itself**. Empty ⇒ no refs. A corrupt `Value` layout ⇒ error.
    pub async fn for_messages(
        &self,
        tenant: &str,
        pk: PartitionKey,
        owner: Owner,
        msgs: &[cairn_protocol::Message],
        now_ms: u64,
    ) -> Result<Vec<pb::SegmentUrl>, String> {
        let mut seen = std::collections::BTreeSet::new();
        let mut segs = Vec::new();
        for m in msgs {
            for r in cairn_protocol::value::refs_in(m).map_err(|e| e.to_string())? {
                if seen.insert((r.sha256, r.gen)) {
                    segs.push(Segment {
                        tenant: tenant.to_string(),
                        pk,
                        owner,
                        sha: r.sha256,
                        gen: r.gen,
                    });
                }
            }
        }
        if segs.is_empty() {
            return Ok(Vec::new());
        }
        self.urls(&segs, now_ms).await
    }
}

/// HMAC key of the `proxy` token: HKDF(cluster secret, [`PROXY_KEY_INFO`]).
#[derive(Clone)]
pub struct ProxyKey(ring::hmac::Key);

impl ProxyKey {
    pub fn from_cluster(secret: &crate::auth::ClusterSecret) -> Self {
        Self::from_material(&secret.derive(PROXY_KEY_INFO))
    }

    pub fn from_material(k: &[u8; 32]) -> Self {
        Self(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, k))
    }
}

/// `proxy` token: `v1 ‖ u16 len(tenant) ‖ tenant ‖ pk u64 ‖ owner[32] ‖ sha[32]
/// ‖ gen u64 ‖ exp_ms u64 ‖ mac[32]`, unpadded base64url. `mac` covers every byte
/// before it — tenant, range, owner, segment and expiry.
pub struct Token;

/// Every token error has one body (a prober must not learn which part is wrong).
#[derive(Debug, PartialEq, Eq)]
pub struct BadToken;

impl Token {
    pub fn seal(key: &ProxyKey, s: &Segment, exp_ms: u64) -> String {
        let mut b = Vec::with_capacity(1 + 2 + s.tenant.len() + 8 + 32 + 32 + 8 + 8 + 32);
        b.push(TOKEN_V1);
        b.extend_from_slice(&(s.tenant.len() as u16).to_be_bytes());
        b.extend_from_slice(s.tenant.as_bytes());
        b.extend_from_slice(&s.pk.0.to_be_bytes());
        b.extend_from_slice(&s.owner.0);
        b.extend_from_slice(&s.sha);
        b.extend_from_slice(&s.gen.to_be_bytes());
        b.extend_from_slice(&exp_ms.to_be_bytes());
        let mac = ring::hmac::sign(&key.0, &b);
        b.extend_from_slice(mac.as_ref());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    }

    /// Open a token: the mac is right (constant-time comparison, `ring::hmac::verify`) and
    /// not expired at `now_ms`.
    pub fn open(key: &ProxyKey, token: &str, now_ms: u64) -> Result<Segment, BadToken> {
        // The longest token: tenant ≤ u16 — bounded before decoding.
        if token.len() > 1 << 12 {
            return Err(BadToken);
        }
        let b = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| BadToken)?;
        let (body, mac) = b
            .split_at_checked(b.len().checked_sub(32).ok_or(BadToken)?)
            .ok_or(BadToken)?;
        ring::hmac::verify(&key.0, body, mac).map_err(|_| BadToken)?;
        let mut r = body;
        let mut take = |n: usize| -> Result<&[u8], BadToken> {
            let (h, t) = r.split_at_checked(n).ok_or(BadToken)?;
            r = t;
            Ok(h)
        };
        if take(1)? != [TOKEN_V1] {
            return Err(BadToken);
        }
        let tlen = u16::from_be_bytes(take(2)?.try_into().map_err(|_| BadToken)?) as usize;
        let tenant = std::str::from_utf8(take(tlen)?)
            .map_err(|_| BadToken)?
            .to_string();
        let pk = u64::from_be_bytes(take(8)?.try_into().map_err(|_| BadToken)?);
        let owner: [u8; 32] = take(32)?.try_into().map_err(|_| BadToken)?;
        let sha: [u8; 32] = take(32)?.try_into().map_err(|_| BadToken)?;
        let gen = u64::from_be_bytes(take(8)?.try_into().map_err(|_| BadToken)?);
        let exp = u64::from_be_bytes(take(8)?.try_into().map_err(|_| BadToken)?);
        if !r.is_empty() || exp < now_ms {
            return Err(BadToken);
        }
        Ok(Segment {
            tenant,
            pk: PartitionKey(pk),
            owner: Owner(owner),
            sha,
            gen,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg() -> Segment {
        Segment {
            tenant: "acme".into(),
            pk: PartitionKey(7),
            owner: Owner([3; 32]),
            sha: [9; 32],
            gen: 42,
        }
    }

    /// A token opens exactly with the key that signed it, before expiry; any one-byte change,
    /// another key, or expiry ⇒ the same error.
    #[test]
    fn token_round_trips_and_rejects_tampering() {
        let k = ProxyKey::from_material(&[1; 32]);
        let t = Token::seal(&k, &seg(), 1_000);
        assert_eq!(Token::open(&k, &t, 1_000), Ok(seg()));
        assert_eq!(Token::open(&k, &t, 1_001), Err(BadToken), "expired");
        let other = ProxyKey::from_material(&[2; 32]);
        assert_eq!(Token::open(&other, &t, 0), Err(BadToken), "another key");
        let mut raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&t)
            .unwrap();
        for i in 0..raw.len() {
            raw[i] ^= 1;
            let bad = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&raw);
            assert_eq!(Token::open(&k, &bad, 0), Err(BadToken), "byte {i}");
            raw[i] ^= 1;
        }
        assert_eq!(Token::open(&k, "", 0), Err(BadToken));
        assert_eq!(Token::open(&k, "!!", 0), Err(BadToken));
        assert_eq!(Token::open(&k, &"A".repeat(5000), 0), Err(BadToken));
    }

    #[test]
    fn auto_picks_the_path_that_can_work() {
        let secret = crate::auth::ClusterSecret::random();
        let mem = cairn_store::StoreLocation::parse("memory://").unwrap();
        let cfg = |read_path, endpoint: Option<&str>, url: Option<&str>| ValueConfig {
            read_path,
            ttl: None,
            public_endpoint: endpoint.map(str::to_string),
            public_url: url.map(str::to_string),
            prefer_direct: false,
        };
        let pick = |c: &ValueConfig, ingress: &str| {
            ValueUrls::build(c, &mem, &secret, ingress).map(|u| u.name())
        };
        // memory:// cannot sign: an explicit `direct` is an error, `auto` skips it.
        assert!(pick(&cfg(ReadPath::Direct, None, None), "http://127.0.0.1:1").is_err());
        assert_eq!(
            pick(
                &cfg(ReadPath::Auto, Some("https://s3"), None),
                "http://0.0.0.0:1"
            ),
            Ok("inline")
        );
        assert_eq!(
            pick(
                &cfg(ReadPath::Auto, None, Some("https://n.example")),
                "http://0.0.0.0:1"
            ),
            Ok("proxy")
        );
        // `proxy` without a URL: the bound address if specific, otherwise an error.
        assert_eq!(
            pick(&cfg(ReadPath::Proxy, None, None), "http://127.0.0.1:1"),
            Ok("proxy")
        );
        assert!(pick(&cfg(ReadPath::Proxy, None, None), "http://0.0.0.0:1").is_err());
        assert!(pick(
            &cfg(ReadPath::Proxy, None, Some("ftp://x")),
            "http://0.0.0.0:1"
        )
        .is_err());
        assert!(pick(&cfg(ReadPath::Proxy, None, Some("https://x/?a=1")), "x").is_err());
        let ttl = ValueConfig {
            ttl: Some(Duration::from_secs(7200)),
            ..cfg(ReadPath::Inline, None, None)
        };
        assert!(pick(&ttl, "x").is_err());
    }

    #[test]
    fn read_path_parses() {
        assert_eq!("auto".parse(), Ok(ReadPath::Auto));
        assert_eq!("proxy".parse(), Ok(ReadPath::Proxy));
        assert!("x".parse::<ReadPath>().is_err());
    }

    #[tokio::test]
    async fn proxy_urls_wait_for_a_base_and_carry_the_ttl() {
        let k = ProxyKey::from_material(&[1; 32]);
        let u = ValueUrls::proxy(None, k.clone(), Duration::from_secs(60));
        assert!(!u.sends_refs());
        u.bound("127.0.0.1:9".parse().unwrap());
        assert!(u.sends_refs());
        let got = u.urls(&[seg()], 5).await.unwrap();
        let tok = got[0]
            .url
            .strip_prefix("http://127.0.0.1:9/values/v1/")
            .unwrap();
        assert_eq!(Token::open(&k, tok, 60_005), Ok(seg()));
        assert_eq!(Token::open(&k, tok, 60_006), Err(BadToken));
        assert_eq!((got[0].gen, &got[0].sha256[..]), (42, &[9u8; 32][..]));
    }

    fn vars(endpoint: &str) -> Vec<(String, String)> {
        [
            ("T_AWS_ACCESS_KEY_ID", "AKIDEXAMPLE"),
            ("T_AWS_SECRET_ACCESS_KEY", "secret"),
            ("T_AWS_REGION", "auto"),
            ("T_AWS_ENDPOINT", endpoint),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    /// The GET URL signer (the `direct` path): exactly one key,
    /// valid for exactly `ttl`, the public endpoint instead of the node's; cannot sign
    /// with a store that is not S3. Signing is a local computation — the test needs no network.
    #[tokio::test]
    async fn presigner_signs_one_key_for_ttl_on_the_public_endpoint() {
        let loc = cairn_store::StoreLocation::parse_with_vars(
            "s3://bucket/pre",
            "T_",
            vars("http://10.0.0.5:9000"),
        )
        .unwrap();
        let signer = loc
            .presigner(Some("https://public.example"))
            .unwrap()
            .unwrap();
        let url = signer
            .signed_url(
                reqwest::Method::GET,
                &slatedb::object_store::path::Path::from("pre/values/t/x"),
                Duration::from_secs(60),
            )
            .await
            .unwrap();
        let u = url.as_str();
        assert!(u.starts_with("https://public.example/"), "{u}");
        assert!(u.contains("pre/values/t/x"), "{u}");
        assert!(u.contains("X-Amz-Expires=60"), "{u}");
        assert!(!u.contains("10.0.0.5"), "{u}");
        assert!(!u.contains("secret"), "{u}");
        let own = loc.presigner(None).unwrap().unwrap();
        let url = own
            .signed_url(
                reqwest::Method::GET,
                &slatedb::object_store::path::Path::from("a"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert!(url.as_str().starts_with("http://10.0.0.5:9000/"), "{url}");
        assert!(cairn_store::StoreLocation::parse("memory://")
            .unwrap()
            .presigner(None)
            .is_none());
    }
}

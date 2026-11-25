// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The address of one store tier: [`StoreLocation`] (URI → `ObjectStore` + prefix).

use {super::*, std::time::Duration};

/// The address of one store tier: `scheme://authority/prefix`.
///
/// `store` is separate from `prefix` because `object_store` requires it — the URL passed to the
/// resolver must not have a path, and the path belongs to the caller of `Db::open`.
#[derive(Clone)]
pub struct StoreLocation {
    store: Arc<dyn ObjectStore>,
    pub(super) prefix: Path,
    /// The URI verbatim, only for logs and error messages.
    pub(super) uri: String,
    /// The directory of a `file://` store — where the exclusive lock lives
    /// ([`super::ObjectStores::exclusive_local`]). `None` for every other store, and for stores
    /// built with [`Self::from_store`].
    pub(super) local_dir: Option<std::path::PathBuf>,
    /// The **atomic PUT** capability ([`Self::atomic_put`]).
    pub(super) atomic_put: bool,
    /// The client's custom S3 endpoint (`AWS_ENDPOINT`…) — `None` = the client's default (AWS
    /// S3) or a non-S3 store ([`Self::endpoint`]).
    pub(super) endpoint: Option<String>,
    /// `atomic_put` declared by the operator (`--wal-atomic-put`), not the default
    /// ([`Self::atomic_put_declared`]).
    pub(super) atomic_put_declared: bool,
    /// Store clock samples from the HTTP client's `Date` header ([`Self::store_clock`]). `None`
    /// for non-HTTP stores.
    pub(super) store_clock: Option<crate::store_clock::StoreClock>,
    /// URL and S3 client configuration (credentials included) to build a signer of short-lived
    /// GET URLs ([`Self::presigner`]). `None` for non-S3 stores and stores built with
    /// [`Self::from_store`].
    pub(super) s3: Option<Arc<(String, S3Config)>>,
}

/// The `atomic_put` capability **declared** per scheme: a PUT on this store either reads back
/// as the whole object or as nothing — never half of it, even when the client dies mid-PUT
/// (assumption S3-2 in `docs/reliability/irreversible.md` §0.3).
///
/// | Scheme | `atomic_put` | Why |
/// |---|---|---|
/// | `s3`, `s3a` | `true` **only on verified stores** | AWS S3, Tigris, SeaweedFS ([`verified_s3_endpoint`], [`verified_s3_server`]; S3-2 pinned in `s3_store_contract`) |
/// | `memory` | `true` | `InMemory` replaces the object under one lock |
/// | `file` | `true` | `LocalFileSystem` writes a temp file then `rename`s it |
/// | other | `false` | nobody has verified it — keep today's safe behaviour |
///
/// `s3://` on an **unverified** store (MinIO — its erasure coding was measured tearing objects,
/// S3-2 —, Garage, Ceph, unknown gateways): the node **refuses to start** until the operator
/// declares `--wal-atomic-put true|false` (checked in `cairn-server`, `store_check.rs`). This
/// table is only the value once that check has passed. There is no path that **implicitly**
/// changes the capability based on observed behaviour (SlateDB silently drops the read-back
/// when the store does not accept attributes — which we avoid): store detection only chooses
/// between "run with the default" and "require a declaration".
pub fn atomic_put_by_scheme(uri: &str) -> bool {
    matches!(
        uri.split_once("://").map(|(scheme, _)| scheme),
        Some("s3" | "s3a" | "memory" | "file")
    )
}

/// An S3 store verified for S3-2, recognised by the client's **endpoint**: `None` (the client's
/// default endpoint) or a host `*.amazonaws.com` ⇒ AWS S3 (documented S3 behaviour: never adds
/// a partial object); host `t3.storage.dev` / `fly.storage.tigris.dev` (and their subdomains —
/// virtual-hosted-style buckets) ⇒ Tigris. `None` = the `Server` header must be asked
/// ([`verified_s3_server`]).
pub fn verified_s3_endpoint(endpoint: Option<&str>) -> Option<&'static str> {
    let Some(endpoint) = endpoint else {
        return Some("AWS S3");
    };
    let host = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let host = host.rsplit_once('@').map_or(host, |(_, h)| h);
    let host = match host.rsplit_once(':') {
        Some((h, port)) if port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    }
    .trim_end_matches('.')
    .to_ascii_lowercase();
    let under = |zone: &str| host == zone || host.ends_with(&format!(".{zone}"));
    if under("amazonaws.com") || under("amazonaws.com.cn") {
        Some("AWS S3")
    } else if under("t3.storage.dev") || under("fly.storage.tigris.dev") {
        Some("Tigris")
    } else {
        None
    }
}

/// An S3 store verified for S3-2, recognised by the endpoint's `Server` header: `SeaweedFS …`
/// (4.40 returns `SeaweedFS 30GB 4.40`) ⇒ SeaweedFS. Every other value — `MinIO` included — is
/// unverified.
pub fn verified_s3_server(server: &str) -> Option<&'static str> {
    server
        .trim_start()
        .starts_with("SeaweedFS")
        .then_some("SeaweedFS")
}

impl StoreLocation {
    /// Resolves a URI.
    ///
    /// | URI | Result |
    /// |---|---|
    /// | `file:///var/lib/cairnflow` | `LocalFileSystem` rooted at that directory, empty prefix |
    /// | `memory://` | `InMemory`, empty prefix — for tests |
    /// | `s3://bucket/cairnflow` | S3 bucket `bucket`, prefix `cairnflow` |
    ///
    /// `file://` is handled here rather than handed to `Db::resolve_object_store`: that
    /// resolver refuses every URL with a path, and a local path **is** a path. In exchange we
    /// create the directory up front — `LocalFileSystem::new_with_prefix` requires the
    /// directory to exist, and making the operator `mkdir` before running the binary is a trap
    /// with no reason to exist.
    pub fn parse(uri: &str) -> Result<Self> {
        Self::parse_in(uri, None)
    }

    /// Like [`Self::parse`], but the S3 client takes its credentials, endpoint, region…
    /// **only** from the environment variables `<prefix>AWS_*`: a backup target at a different
    /// provider than the source (source on Tigris via `AWS_*`, target on R2 via
    /// `CAIRN_BACKUP_AWS_*`). The rules are in [`s3_config_prefixed`]: no fallback to the
    /// source's `AWS_*`, a missing access key is an error, an unknown `<prefix>AWS_*` variable
    /// is an error; only `s3://`/`s3a://`. Errors name only the **variable**, never its value.
    pub fn parse_with_env_prefix(uri: &str, prefix: &str) -> Result<Self> {
        let mut vars = Vec::new();
        let want = format!("{prefix}AWS_");
        for (key, value) in std::env::vars_os() {
            match (key.into_string(), value.into_string()) {
                (Ok(k), Ok(v)) => vars.push((k, v)),
                // A non-UTF-8 variable in the chosen set: report its name (lossy), do not
                // silently drop it — dropping it changes the client configuration unnoticed.
                (Ok(k), Err(_)) if k.starts_with(&want) => {
                    return Err(StoreError::BadStoreUri {
                        uri: uri.to_string(),
                        reason: format!("environment variable {k} is not UTF-8"),
                    })
                }
                _ => {}
            }
        }
        Self::parse_with_vars(uri, prefix, vars)
    }

    /// [`Self::parse_with_env_prefix`] on a given set of variables instead of the process
    /// environment — for tests (the process's shared environment is not modified).
    pub fn parse_with_vars(
        uri: &str,
        prefix: &str,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self> {
        let config = s3_config_prefixed(prefix, vars).map_err(|reason| StoreError::BadStoreUri {
            uri: uri.to_string(),
            reason,
        });
        Self::parse_in(uri, Some((prefix, config)))
    }

    /// `prefixed`: the chosen separate credential set (prefix, S3 configuration or its error);
    /// `None` = as before, configuration from every environment variable.
    fn parse_in(uri: &str, prefixed: Option<(&str, Result<S3Config>)>) -> Result<Self> {
        let scheme_of = uri.split_once("://").map(|(s, _)| s);
        if let Some((prefix, _)) = &prefixed {
            if !matches!(scheme_of, Some("s3" | "s3a")) {
                return Err(StoreError::BadStoreUri {
                    uri: uri.to_string(),
                    reason: format!(
                        "the separate credential set {prefix}AWS_* is only used with s3:// or s3a://"
                    ),
                });
            }
        }
        let (scheme, rest) = uri
            .split_once("://")
            .ok_or_else(|| StoreError::BadStoreUri {
                uri: uri.to_string(),
                reason:
                    "missing scheme, correct examples: s3://bucket/prefix or file:///var/lib/cairnflow"
                        .into(),
            })?;

        match scheme {
            "file" => {
                let dir = format!("/{}", rest.trim_start_matches('/'));
                std::fs::create_dir_all(&dir).map_err(|e| StoreError::BadStoreUri {
                    uri: uri.to_string(),
                    reason: format!("cannot create directory {dir}: {e}"),
                })?;
                let store = LocalFileSystem::new_with_prefix(&dir).map_err(|e| {
                    StoreError::BadStoreUri {
                        uri: uri.to_string(),
                        reason: e.to_string(),
                    }
                })?;
                Ok(Self {
                    store: Arc::new(store),
                    prefix: Path::default(),
                    uri: uri.to_string(),
                    local_dir: Some(dir.into()),
                    atomic_put: atomic_put_by_scheme(uri),
                    endpoint: None,
                    atomic_put_declared: false,
                    store_clock: None,
                    s3: None,
                })
            }
            "memory" => Ok(Self {
                store: Arc::new(InMemory::new()),
                prefix: Path::from(rest.trim_matches('/')),
                uri: uri.to_string(),
                local_dir: None,
                atomic_put: atomic_put_by_scheme(uri),
                endpoint: None,
                atomic_put_declared: false,
                store_clock: None,
                s3: None,
            }),
            _ => {
                let (authority, prefix) = match rest.split_once('/') {
                    Some((a, p)) => (a, p),
                    None => (rest, ""),
                };
                let url = format!("{scheme}://{authority}");
                let bad = |e: String| StoreError::BadStoreUri {
                    uri: uri.to_string(),
                    reason: e,
                };
                let (store, endpoint, store_clock, s3) = match scheme {
                    "s3" | "s3a" => {
                        let config = match prefixed {
                            None => s3_config_ambient(std::env::vars()),
                            Some((_, config)) => config?,
                        };
                        let clock = crate::store_clock::StoreClock::system();
                        let s3 = Arc::new((url.clone(), config.clone()));
                        let (store, endpoint) =
                            s3_store(&url, &clock, config).map_err(|e| bad(e.to_string()))?;
                        (store, endpoint, Some(clock), Some(s3))
                    }
                    _ => (
                        Db::resolve_object_store(&url).map_err(|e| bad(e.to_string()))?,
                        None,
                        None,
                        None,
                    ),
                };
                Ok(Self {
                    store,
                    prefix: Path::from(prefix.trim_matches('/')),
                    uri: uri.to_string(),
                    local_dir: None,
                    atomic_put: atomic_put_by_scheme(uri),
                    endpoint,
                    atomic_put_declared: false,
                    store_clock,
                    s3,
                })
            }
        }
    }

    /// Builds directly from an existing store. For tests, and for callers that configured
    /// credentials themselves in a way a URI cannot express. The `atomic_put` capability follows
    /// the scheme of `uri` ([`atomic_put_by_scheme`]).
    pub fn from_store(store: Arc<dyn ObjectStore>, prefix: impl Into<Path>, uri: &str) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            uri: uri.to_string(),
            local_dir: None,
            atomic_put: atomic_put_by_scheme(uri),
            endpoint: None,
            atomic_put_declared: false,
            store_clock: None,
            s3: None,
        }
    }

    /// Same address, same capabilities, the store wrapped in one more layer (simulated profile,
    /// failpoints, local CAS). Does not keep `local_dir`, like [`Self::from_store`].
    pub fn wrap(&self, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            local_dir: None,
            ..self.clone()
        }
    }

    /// Like [`Self::wrap`] but **keeps** `local_dir`: for transparent layers (request counting,
    /// [`crate::metered::MeteredStore`]) wrapped before
    /// [`super::ObjectStores::exclusive_local`] — a node on a local directory still takes the
    /// directory lock, and the simulated CAS wraps outside that layer.
    pub fn wrap_transparent(&self, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            ..self.clone()
        }
    }

    /// PUTs on this store are atomic — they read back as the whole object or as nothing
    /// ([`atomic_put_by_scheme`]). The shared WAL resolves the `AlreadyExists` of an ambiguous
    /// log PUT by reading back **only** when this capability is on (NW-2); off, it keeps the
    /// old behaviour: treat it as sealed, fail-stop.
    pub fn atomic_put(&self) -> bool {
        self.atomic_put
    }

    /// The custom S3 endpoint the client uses (`AWS_ENDPOINT`, `AWS_ENDPOINT_URL`… — read from
    /// environment variables the same way as the client). `None` = the client's default
    /// endpoint, i.e. AWS S3, or a non-S3 store.
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    /// Store clock samples from the `Date` header: present for `s3://`/`s3a://` built with
    /// [`Self::parse`]; `None` for other stores.
    pub fn store_clock(&self) -> Option<&crate::store_clock::StoreClock> {
        self.store_clock.as_ref()
    }

    /// Attaches a store clock sampler to a store built with [`Self::from_store`] (tests).
    pub fn with_store_clock(mut self, clock: Option<crate::store_clock::StoreClock>) -> Self {
        self.store_clock = clock;
        self
    }

    /// Sets the known endpoint of a store built with [`Self::from_store`] (tests, or callers
    /// that build their own S3 client).
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Explicitly declares the `atomic_put` capability (`--wal-atomic-put`), overriding the
    /// per-scheme default.
    pub fn with_atomic_put(mut self, atomic: bool) -> Self {
        self.atomic_put = atomic;
        self.atomic_put_declared = true;
        self
    }

    /// `atomic_put` was declared by the operator ([`Self::with_atomic_put`],
    /// `--wal-atomic-put`) rather than being the per-scheme default.
    pub fn atomic_put_declared(&self) -> bool {
        self.atomic_put_declared
    }

    /// An `s3`/`s3a` scheme whose `atomic_put` is undeclared: the `true` default is only right
    /// on verified stores, so the node must identify the store before running
    /// ([`verified_s3_endpoint`], [`verified_s3_server`]).
    pub fn needs_atomic_put_vetting(&self) -> bool {
        !self.atomic_put_declared
            && matches!(
                self.uri.split_once("://").map(|(s, _)| s),
                Some("s3" | "s3a")
            )
    }

    /// Signer of short-lived GET URLs for objects of this store (the `direct` read path): a
    /// separate S3 client with the same configuration, the endpoint overridden by
    /// `public_endpoint` if given (the endpoint the customer's app sees). Signing is a local
    /// computation — this client sends no request when signing with static credentials. `None`
    /// = not an S3 store.
    pub fn presigner(
        &self,
        public_endpoint: Option<&str>,
    ) -> Option<Result<Arc<dyn slatedb::object_store::signer::Signer>>> {
        use slatedb::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
        let (url, config) = &**self.s3.as_ref()?;
        let mut b = config
            .iter()
            .cloned()
            .fold(AmazonS3Builder::new().with_url(url), |b, (k, v)| {
                b.with_config(k, v)
            });
        if let Some(e) = public_endpoint {
            b = b
                .with_config(AmazonS3ConfigKey::Endpoint, e)
                .with_config(AmazonS3ConfigKey::S3Endpoint, e);
        }
        Some(
            b.build()
                .map(|s| Arc::new(s) as Arc<dyn slatedb::object_store::signer::Signer>)
                .map_err(|e| StoreError::BadStoreUri {
                    uri: self.uri.clone(),
                    reason: e.to_string(),
                }),
        )
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl std::fmt::Debug for StoreLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreLocation")
            .field("uri", &self.uri)
            .field("prefix", &self.prefix.as_ref())
            .field("atomic_put", &self.atomic_put)
            .finish()
    }
}

/// Total bound on the 5xx/timeout retry loop **inside** the S3 client
/// (`RetryConfig::retry_timeout`), counted from the first request.
///
/// object_store 0.14's default is 180 s (10 attempts, 100 ms backoff ×2 with jitter, each pause
/// capped at 15 s): every GET/PUT cairn calls directly (owner records, `cluster.conf`, sessions,
/// metering…) could hang for **3 minutes** under a burst of 503s — longer than every bound of
/// the layer calling it. 30 s: equal to the longest bound of the layers that have their own
/// (clone/union `CLONE_BOUND`, the whole `open_io` operation), so inside those layers the
/// client is never the longest wait; still enough for ~8 retries with the default backoff
/// (100 ms ×2: 0.1 + … + 12.8 s), i.e. it absorbs a short S3 throttling burst without pushing
/// 503s up to cairn. Each request still has its own 30 s bound (the default `ClientOptions` —
/// large SSTs need it), so one operation hitting the bound takes at most ~60 s.
///
/// Per-**layer** bounds of each path live at the call sites (`cairn_store::retry::deadline`):
///
/// | Layer | Bound | Where |
/// |---|---|---|
/// | lease/session | renewal: until the self-demotion deadline (< TTL); yield 2 s | `session::renew`, `RELEASE_WRITE_BOUND` |
/// | shared WAL | `put_timeout × put_attempts` (6 s) | `nodewal::stripe::put` |
/// | engine open | 10 s per attempt, 30 s for the whole operation | `node::open_io` |
/// | split/merge | clone/union 30 s, claiming children 10 s | `split::CLONE_BOUND`, `CLAIM_BOUND` |
/// | background (GC, metering, refresh) | this client's bound | — |
///
/// SlateDB retries **forever** on this client (`object_store_max_retries: None`) for every
/// operation through Db/Admin; their bound is the call site's (deadline), not here.
pub const S3_CLIENT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

/// The retry rules of the S3 client cairn builds: object_store's defaults, except
/// [`S3_CLIENT_RETRY_TIMEOUT`].
pub fn s3_client_retry() -> slatedb::object_store::RetryConfig {
    slatedb::object_store::RetryConfig {
        retry_timeout: S3_CLIENT_RETRY_TIMEOUT,
        ..Default::default()
    }
}

/// S3 client configuration: (object_store key, value) pairs.
type S3Config = Vec<(slatedb::object_store::aws::AmazonS3ConfigKey, String)>;

/// The default configuration (old behaviour): every environment variable whose lowercase name is
/// an object_store key — exactly `parse_url_opts` of `Db::resolve_object_store`.
fn s3_config_ambient(vars: impl IntoIterator<Item = (String, String)>) -> S3Config {
    vars.into_iter()
        .filter_map(|(key, value)| Some((key.to_ascii_lowercase().parse().ok()?, value)))
        .collect()
}

/// Configuration from the separate credential set `<prefix>AWS_*`:
///
/// - only variables starting with `<prefix>AWS_`; the part after the prefix (`AWS_ENDPOINT`,
///   `AWS_REGION`…) is read like the default client's variable names. The source's bare `AWS_*`
///   is **never** read — there is no silent fallback to the source's credentials;
/// - an empty value counts as absent;
/// - `<prefix>AWS_ACCESS_KEY_ID` and `<prefix>AWS_SECRET_ACCESS_KEY` are required: without
///   them object_store's default client would go asking the machine's metadata (IMDS,
///   container) — a different credential set the operator did not choose;
/// - `<prefix>AWS_<X>` where `<X>` is no key: an error (a typo like `…_AWS_ENDPONT` must not
///   silently become AWS's default endpoint);
/// - an empty prefix or one with characters outside `A-Z0-9_`: an error.
///
/// Errors name only the variable.
fn s3_config_prefixed(
    prefix: &str,
    vars: impl IntoIterator<Item = (String, String)>,
) -> std::result::Result<S3Config, String> {
    use slatedb::object_store::aws::AmazonS3ConfigKey;
    if prefix.is_empty()
        || !prefix
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(format!(
            "invalid environment variable prefix {prefix:?}: needs A-Z, 0-9, _ and must not be empty \
             (for example CAIRN_BACKUP_)"
        ));
    }
    let want = format!("{prefix}AWS_");
    let mut config = S3Config::new();
    for (key, value) in vars {
        let Some(name) = key.strip_prefix(prefix) else {
            continue;
        };
        if !key.starts_with(&want) || value.is_empty() {
            continue;
        }
        match name.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            Ok(k) => config.push((k, value)),
            Err(_) => {
                return Err(format!(
                    "environment variable {key} is not any S3 setting ({name} is not a key \
                     of the client)"
                ))
            }
        }
    }
    let missing: Vec<String> = [
        (AmazonS3ConfigKey::AccessKeyId, "AWS_ACCESS_KEY_ID"),
        (AmazonS3ConfigKey::SecretAccessKey, "AWS_SECRET_ACCESS_KEY"),
    ]
    .into_iter()
    .filter(|(k, _)| !config.iter().any(|(c, _)| c == k))
    .map(|(_, name)| format!("{prefix}{name}"))
    .collect();
    if !missing.is_empty() {
        return Err(format!(
            "missing environment variable {} — the separate credential set {want}* was chosen, the \
             source's AWS_* is not used",
            missing.join(", ")
        ));
    }
    Ok(config)
}

/// Builds the S3 client like `Db::resolve_object_store` (URL without a path, configuration
/// `config` — by default from lowercase environment variables, exactly `parse_url_opts`,
/// [`s3_config_ambient`]), plus the retry rules [`s3_client_retry`]. `parse_url_opts` does not
/// take a `RetryConfig`, so the builder is assembled here. Also returns the custom endpoint the
/// client will use (`AWS_ENDPOINT_URL_S3` before `AWS_ENDPOINT`/`AWS_ENDPOINT_URL`…, like
/// object_store's `build`) — read from the builder itself, not guessed again from the
/// environment.
///
/// The HTTP client is the default reqwest client wrapped in
/// [`crate::store_clock::DateConnector`]: every response carrying a `Date` header gives `clock`
/// a store clock sample — no extra request.
fn s3_store(
    url: &str,
    clock: &crate::store_clock::StoreClock,
    config: S3Config,
) -> std::result::Result<(Arc<dyn ObjectStore>, Option<String>), slatedb::object_store::Error> {
    use slatedb::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
    let builder = config
        .into_iter()
        .fold(AmazonS3Builder::new().with_url(url), |b, (k, v)| {
            b.with_config(k, v)
        });
    let endpoint = builder
        .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
        .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint));
    Ok((
        Arc::new(
            builder
                .with_retry(s3_client_retry())
                .with_http_connector(crate::store_clock::DateConnector::new(clock.clone()))
                .build()?,
        ),
        endpoint,
    ))
}

/// A normalised object path: strips leading and trailing `/` and empty segments (`a//b`).
pub(crate) fn normalize_path(p: &str) -> String {
    p.split('/')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod atomic_put_tests {
    use super::*;

    /// The capability table of [`atomic_put_by_scheme`]; wrapping keeps the capability, an
    /// explicit declaration overrides the default.
    #[test]
    fn atomic_put_follows_the_declared_table() {
        for (uri, want) in [
            ("s3://b/p", true),
            ("s3a://b", true),
            ("memory://", true),
            ("file:///tmp/x", true),
            ("gs://b/p", false),
            ("az://b", false),
            ("no-scheme", false),
        ] {
            assert_eq!(atomic_put_by_scheme(uri), want, "{uri}");
        }
        let mem = || Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let loc = StoreLocation::from_store(mem(), "p", "memory://p");
        assert!(loc.atomic_put());
        let torn = loc.with_atomic_put(false);
        assert!(!torn.atomic_put());
        assert!(
            !torn.wrap(mem()).atomic_put(),
            "wrapping lost the declaration"
        );
        let stores = ObjectStores::single(StoreLocation::from_store(mem(), "p", "memory://p"))
            .with_wal_atomic_put(Some(false));
        assert!(!stores.wal().atomic_put());
        let stores = ObjectStores::single(StoreLocation::from_store(mem(), "p", "gs://p"))
            .with_wal_atomic_put(None);
        assert!(!stores.wal().atomic_put());
    }

    /// Who must identify the store before running: only an undeclared `s3://`/`s3a://`;
    /// declared (also through `ObjectStores`) or another scheme, no; wrapping keeps both the
    /// declaration and the endpoint.
    #[test]
    fn only_undeclared_s3_needs_vetting() {
        let mem = || Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        for (uri, want) in [
            ("s3://b/p", true),
            ("s3a://b", true),
            ("memory://", false),
            ("file:///tmp/x", false),
            ("gs://b", false),
        ] {
            let loc = StoreLocation::from_store(mem(), "", uri);
            assert_eq!(loc.needs_atomic_put_vetting(), want, "{uri}");
        }
        let s3 = || {
            StoreLocation::from_store(mem(), "", "s3://b").with_endpoint(Some("http://x:1".into()))
        };
        assert!(!s3().with_atomic_put(true).needs_atomic_put_vetting());
        assert!(s3().wrap(mem()).needs_atomic_put_vetting());
        assert_eq!(s3().wrap(mem()).endpoint(), Some("http://x:1"));
        let declared = s3().with_atomic_put(false).wrap(mem());
        assert!(declared.atomic_put_declared() && !declared.atomic_put());
        for declared in [Some(true), Some(false)] {
            let stores = ObjectStores::single(s3()).with_wal_atomic_put(declared);
            assert!(!stores.wal().needs_atomic_put_vetting(), "{declared:?}");
        }
        assert!(ObjectStores::single(s3())
            .with_wal_atomic_put(None)
            .wal()
            .needs_atomic_put_vetting());
    }

    /// The table recognising verified stores.
    #[test]
    fn verified_s3_stores_by_endpoint_and_server() {
        for (endpoint, want) in [
            (None, Some("AWS S3")),
            (Some("https://s3.us-east-1.amazonaws.com"), Some("AWS S3")),
            (
                Some("https://bucket.s3.eu-west-1.amazonaws.com/"),
                Some("AWS S3"),
            ),
            (
                Some("https://s3.cn-north-1.amazonaws.com.cn"),
                Some("AWS S3"),
            ),
            (Some("https://t3.storage.dev"), Some("Tigris")),
            (Some("https://my-bucket.t3.storage.dev:443"), Some("Tigris")),
            (Some("https://fly.storage.tigris.dev"), Some("Tigris")),
            (Some("http://localhost:18333"), None),
            (Some("http://127.0.0.1:19000"), None),
            (Some("https://amazonaws.com.evil.example"), None),
            (Some("https://evilamazonaws.com"), None),
            (Some("https://t3.storage.dev.evil.example"), None),
            (Some("https://user@minio.internal:9000"), None),
        ] {
            assert_eq!(verified_s3_endpoint(endpoint), want, "{endpoint:?}");
        }
        for (server, want) in [
            ("SeaweedFS 30GB 4.40", Some("SeaweedFS")),
            ("SeaweedFS", Some("SeaweedFS")),
            ("MinIO", None),
            ("Garage/v1.0.1", None),
            ("nginx", None),
            ("AmazonS3", None),
            ("", None),
        ] {
            assert_eq!(verified_s3_server(server), want, "{server:?}");
        }
    }
}

#[cfg(test)]
mod env_prefix_tests {
    use {super::*, slatedb::object_store::aws::AmazonS3ConfigKey as K};

    const SRC_SECRET: &str = "source-secret-0123456789";
    const DST_SECRET: &str = "dest-secret-9876543210";

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The environment of a deployment: the source's `AWS_*` (Tigris) and the target's
    /// `CAIRN_BACKUP_AWS_*` (R2).
    fn cell_env() -> Vec<(String, String)> {
        vars(&[
            ("AWS_ACCESS_KEY_ID", "tigris-key"),
            ("AWS_SECRET_ACCESS_KEY", SRC_SECRET),
            ("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev"),
            ("AWS_REGION", "iad"),
            ("AWS_SESSION_TOKEN", "source-session"),
            ("CAIRN_BACKUP_AWS_ACCESS_KEY_ID", "r2-key"),
            ("CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY", DST_SECRET),
            (
                "CAIRN_BACKUP_AWS_ENDPOINT",
                "https://acc.r2.cloudflarestorage.com",
            ),
            ("CAIRN_BACKUP_AWS_REGION", "auto"),
            ("CAIRN_BACKUP_EVERY_MS", "86400000"),
            ("PATH", "/usr/bin"),
        ])
    }

    fn get(c: &S3Config, k: K) -> Option<&str> {
        c.iter().find(|(x, _)| *x == k).map(|(_, v)| v.as_str())
    }

    /// The separate set takes only `CAIRN_BACKUP_AWS_*`: key, secret, endpoint, R2's `auto`
    /// region; nothing of the source — not even `AWS_SESSION_TOKEN` and `AWS_ENDPOINT_URL_S3`
    /// (which takes precedence over `AWS_ENDPOINT`). Other variables with the same prefix that
    /// are not `…AWS_*` are ignored.
    #[test]
    fn prefixed_config_reads_only_its_own_variables() {
        let c = s3_config_prefixed("CAIRN_BACKUP_", cell_env()).unwrap();
        assert_eq!(get(&c, K::AccessKeyId), Some("r2-key"));
        assert_eq!(get(&c, K::SecretAccessKey), Some(DST_SECRET));
        assert_eq!(
            get(&c, K::Endpoint),
            Some("https://acc.r2.cloudflarestorage.com")
        );
        assert_eq!(get(&c, K::Region), Some("auto"));
        assert_eq!(
            get(&c, K::Token),
            None,
            "the source's session leaked into the target"
        );
        assert_eq!(
            get(&c, K::S3Endpoint),
            None,
            "the source's endpoint leaked into the target"
        );
        assert_eq!(
            c.len(),
            4,
            "{:?}",
            c.iter().map(|(k, _)| k).collect::<Vec<_>>()
        );
    }

    /// The default (old behaviour) is unchanged: reads `AWS_*`, and does not take the separate
    /// set's variables.
    #[test]
    fn ambient_config_is_unchanged_and_ignores_the_prefixed_set() {
        let c = s3_config_ambient(cell_env());
        assert_eq!(get(&c, K::AccessKeyId), Some("tigris-key"));
        assert_eq!(get(&c, K::SecretAccessKey), Some(SRC_SECRET));
        assert_eq!(get(&c, K::Token), Some("source-session"));
        assert!(c.iter().all(|(_, v)| v != DST_SECRET && v != "r2-key"));
    }

    /// A missing key or secret in the separate set (absent, or empty) is an error naming exactly
    /// that variable — even when the source's `AWS_*` is complete; no value leaks into the
    /// message.
    #[test]
    fn missing_prefixed_credentials_fail_and_never_fall_back() {
        for drop in [
            "CAIRN_BACKUP_AWS_ACCESS_KEY_ID",
            "CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY",
        ] {
            let env: Vec<_> = cell_env().into_iter().filter(|(k, _)| k != drop).collect();
            let e = s3_config_prefixed("CAIRN_BACKUP_", env).unwrap_err();
            assert!(e.contains(drop), "{e}");
            let mut env = cell_env();
            env.iter_mut().find(|(k, _)| k == drop).unwrap().1.clear();
            let e2 = s3_config_prefixed("CAIRN_BACKUP_", env).unwrap_err();
            assert!(
                e2.contains(drop),
                "an empty value must count as absent: {e2}"
            );
            for e in [e, e2] {
                for secret in [SRC_SECRET, DST_SECRET, "tigris-key", "r2-key"] {
                    assert!(!e.contains(secret), "value exposed in the error: {e}");
                }
            }
        }
        // Only the source's `AWS_*`: an error, no borrowing.
        let only_source = vars(&[
            ("AWS_ACCESS_KEY_ID", "tigris-key"),
            ("AWS_SECRET_ACCESS_KEY", SRC_SECRET),
        ]);
        let e = s3_config_prefixed("CAIRN_BACKUP_", only_source).unwrap_err();
        assert!(
            e.contains("CAIRN_BACKUP_AWS_ACCESS_KEY_ID")
                && e.contains("CAIRN_BACKUP_AWS_SECRET_ACCESS_KEY"),
            "{e}"
        );
    }

    /// An unknown variable in the separate set (a typo) and a broken prefix are errors.
    #[test]
    fn unknown_prefixed_variable_and_bad_prefix_fail() {
        let mut env = cell_env();
        env.push(("CAIRN_BACKUP_AWS_ENDPONT".into(), "https://x".into()));
        let e = s3_config_prefixed("CAIRN_BACKUP_", env).unwrap_err();
        assert!(e.contains("CAIRN_BACKUP_AWS_ENDPONT"), "{e}");
        assert!(!e.contains("https://x"), "{e}");
        for bad in ["", "cairn_backup_", "CAIRN-BACKUP_", "CAIRN BACKUP_"] {
            assert!(s3_config_prefixed(bad, cell_env()).is_err(), "{bad:?}");
        }
    }

    /// Through `StoreLocation`: the endpoint is the separate set's; a non-S3 scheme is refused;
    /// `Debug` and errors contain no secret.
    #[test]
    fn location_uses_the_prefixed_endpoint_and_refuses_other_schemes() {
        let loc = StoreLocation::parse_with_vars("s3://r2-bk/daily", "CAIRN_BACKUP_", cell_env())
            .unwrap();
        assert_eq!(loc.endpoint(), Some("https://acc.r2.cloudflarestorage.com"));
        assert_eq!(loc.prefix().as_ref(), "daily");
        let dbg = format!("{loc:?}");
        assert!(
            !dbg.contains(DST_SECRET) && !dbg.contains(SRC_SECRET),
            "{dbg}"
        );
        for uri in ["file:///tmp/cairn-e4-not-created", "memory://", "gs://b/p"] {
            let e = StoreLocation::parse_with_vars(uri, "CAIRN_BACKUP_", cell_env())
                .unwrap_err()
                .to_string();
            assert!(e.contains("is only used with s3://"), "{uri}: {e}");
        }
        assert!(
            !std::path::Path::new("/tmp/cairn-e4-not-created").exists(),
            "file:// is refused before the directory is created"
        );
        let e = StoreLocation::parse_with_vars("s3://r2-bk", "CAIRN_BACKUP_", vars(&[]))
            .unwrap_err()
            .to_string();
        assert!(e.contains("CAIRN_BACKUP_AWS_ACCESS_KEY_ID"), "{e}");
    }
}

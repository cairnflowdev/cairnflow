// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! Trial replay: re-run the **existing** journal of an invocation on a chosen deployment,
//! read-only.
//!
//! This is the safety gate before moving a revision: "can the new code replay the
//! invocations in flight?" — a question that used to be answerable only by really moving
//! the revision and watching them go `paused`.
//!
//! **Nothing is committed, nothing is delivered.** The opening turn is built by the pure
//! state machine (`cairn_runtime`), not through the engine; the frames the SDK returns are
//! only read to build the report. On the SDK side, the `replay_only` flag of `Start` (wire
//! v3) makes it stop at the first command **outside** the journal — before any closure runs —
//! and return a `ReplayReport`. A deployment that does not speak v3 yet is **refused**: an
//! older SDK ignores that flag and runs the new code for real, with real side effects.
//!
//! Always `REQUEST_RESPONSE` over h1, even for a `BIDI_STREAM` deployment: SDKs serve
//! `/invoke` over h1 in every mode, and a read-only turn needs no bidirectional channel.

use {
    crate::discovery::Deployment,
    cairn_engine::JournalRow,
    cairn_protocol::{pb, Message, MessageType, JOURNAL_MISMATCH},
    cairn_runtime::{Effect, Event, InvocationMachine, Journal, JournalEntry, StartParams},
    cairn_store::InvocationId,
    serde::Serialize,
};

/// Result of replaying one invocation.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayReport {
    pub id: String,
    /// The code matches every command already in the journal.
    pub matches: bool,
    /// Number of commands (not counting `Input`) that matched.
    pub upto: u32,
    /// The divergence: the command index (not counting `Input`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mismatch_at: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub got: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(
        "deployment {0} is pinned to wire v{1}: a replay test needs v{2}; an older SDK \
         ignores `replay_only` and runs the real code"
    )]
    WireTooOld(String, u32, u32),
    #[error("the invocation has no valid journal: {0}")]
    BadJournal(String),
    #[error("calling the SDK failed: {0}")]
    Sdk(String),
    /// The egress policy refused the endpoint.
    #[error("{0}")]
    EgressDenied(crate::egress::Denied),
    #[error("the SDK returned something that is not a replay report: {0}")]
    Unexpected(String),
    /// The journal carries segmented values: the trial replay runs outside the engine and
    /// has no segment read path yet — refuse instead of sending the tail as if it were the
    /// whole value.
    #[error("the journal holds segmented values; replay tests do not read them yet")]
    Segmented,
}

/// Replay `rows` (the journal of `id`) on `deployment`.
pub async fn replay(
    client: &crate::egress::TenantHttp,
    identity: Option<&crate::identity::Identity>,
    value_urls: &crate::value_urls::ValueUrls,
    deployment: &Deployment,
    id: InvocationId,
    rows: Vec<JournalRow>,
    params: StartParams,
) -> Result<ReplayReport, ReplayError> {
    if deployment.wire_version < crate::discovery::WIRE_KEEPALIVE {
        return Err(ReplayError::WireTooOld(
            deployment.id.clone(),
            deployment.wire_version,
            crate::discovery::WIRE_KEEPALIVE,
        ));
    }
    let entries: Vec<JournalEntry> = rows
        .into_iter()
        .map(|(_, ty, payload)| {
            JournalEntry::new(MessageType::from(ty), bytes::Bytes::from(payload))
        })
        .collect();
    let first = entries
        .first()
        .ok_or_else(|| ReplayError::BadJournal("the journal is empty".into()))?;
    let (target, _) = cairn_engine::input_head(first)
        .ok_or_else(|| ReplayError::BadJournal("entry 0 is not a valid Input".into()))?;
    let known_commands = entries
        .iter()
        .skip(1)
        .filter(|e| e.ty.kind() == cairn_protocol::MessageKind::Command)
        .count() as u32;

    let retry = params.retry_count_since_last_stored_entry;
    let mut machine = InvocationMachine::resume(
        id,
        StartParams {
            key: target.object_key.clone(),
            replay_only: true,
            ..params
        },
        Journal::from_entries(entries),
    );
    let mut opening: Vec<Message> = machine
        .handle(Event::Start)
        .map_err(|e| ReplayError::BadJournal(e.to_string()))?
        .into_iter()
        .filter_map(|fx| match fx {
            Effect::Send(m) => Some(m),
            _ => None,
        })
        .collect();
    if opening.iter().any(|m| {
        cairn_protocol::value::values(m)
            .iter()
            .any(|v| !v.segments.is_empty())
    }) {
        // v6 SDKs read segments by URL; below v6, or on a node
        // without a read path, refuse instead of sending the tail as if it were the whole value.
        let owner = cairn_store::value::object_owner(
            &target.tenant,
            target.scope(),
            &target.service,
            &target.object_key,
        );
        let (Some((pk, owner)), true) = (
            owner,
            deployment.wire_version >= crate::discovery::WIRE_SEGMENTED && value_urls.sends_refs(),
        ) else {
            return Err(ReplayError::Segmented);
        };
        let urls = value_urls
            .for_messages(
                target.tenant.as_str(),
                pk,
                owner,
                &opening,
                cairn_store::entropy::now_ms(),
            )
            .await
            .map_err(ReplayError::BadJournal)?;
        match opening.first_mut() {
            Some(Message::Start(s)) => s.segment_urls = urls,
            _ => {
                return Err(ReplayError::BadJournal(
                    "replay does not open with Start".into(),
                ))
            }
        }
    }
    // Identity: sign on behalf of the **deployment's** tenant — the SDK belongs to
    // that tenant; `jti` matches the `Start` the state machine just built.
    let jti = crate::identity::invocation_jti(&id, retry);
    let signing = identity.map(|identity| crate::invoker::Signing {
        identity,
        tenant: &deployment.tenant,
        jti: &jti,
        now_ms: cairn_store::entropy::now_ms(),
    });
    // A bounded wait (a security requirement): a hung SDK must not hold the ingress request
    // forever — the same default limit as a turn.
    let frames = cairn_store::retry::deadline(
        &cairn_store::retry::ops::SDK_REPLAY,
        crate::actor::DEFAULT_SDK_TIMEOUT,
        crate::invoker::call_detached(
            client,
            &deployment.endpoint,
            &target.service,
            &target.handler,
            deployment.wire_version,
            crate::invoker::encode_stream(&opening),
            signing,
            // The operator's trial replay: not added to the tenant's metering.
            None,
            deployment.rr_compression.as_deref() == Some(crate::discovery::RR_GZIP),
        ),
    )
    .await
    .map_err(|e| ReplayError::Sdk(e.to_string()))?
    .map_err(|e| match e {
        crate::invoker::InvokeError::EgressDenied(d) => ReplayError::EgressDenied(d),
        e => ReplayError::Sdk(e.to_string()),
    })?;

    let hex = cairn_store::id_hex(&id);
    for f in &frames {
        if f.ty == MessageType::REPLAY_REPORT {
            let r: pb::ReplayReportMessage = prost::Message::decode(f.payload.clone())
                .map_err(|e| ReplayError::Unexpected(e.to_string()))?;
            return Ok(ReplayReport {
                id: hex,
                matches: !r.mismatch,
                upto: r.matched,
                mismatch_at: r.mismatch.then_some(r.matched),
                expected: (!r.expected.is_empty()).then_some(r.expected),
                got: (!r.got.is_empty()).then_some(r.got),
                message: (!r.message.is_empty()).then_some(r.message),
            });
        }
    }
    // No report: the SDK went through the whole journal without emitting a new command —
    // suspended waiting for a completion, or finished with `Output`/`End`. Both mean
    // everything matched. An `Error` 570 (an SDK without its own report path) is a divergence.
    for f in &frames {
        if let Ok(Message::Error(e)) = cairn_protocol::decode(f) {
            if e.code == JOURNAL_MISMATCH {
                return Ok(ReplayReport {
                    id: hex,
                    matches: false,
                    upto: 0,
                    mismatch_at: None,
                    expected: None,
                    got: None,
                    message: Some(e.message),
                });
            }
            return Err(ReplayError::Unexpected(format!(
                "Error {}: {}",
                e.code, e.message
            )));
        }
    }
    Ok(ReplayReport {
        id: hex,
        matches: true,
        upto: known_commands,
        mismatch_at: None,
        expected: None,
        got: None,
        message: None,
    })
}

/// Trial replay of a **capture bundle** — no cluster needed: the bundle carries the
/// journal, the deployment (endpoint, wire version) and the contract of the captured run.
/// `endpoint` replaces the endpoint recorded in the bundle, for running a local SDK (code
/// being edited) instead of the deployed one.
///
/// Same rules as [`replay`]: writes nothing, the deployment must speak wire v3.
pub async fn replay_bundle(
    client: &crate::egress::TenantHttp,
    bundle: &crate::capture::Bundle,
    endpoint: Option<&str>,
) -> Result<ReplayReport, ReplayError> {
    let mut deployment = bundle.meta.deployment.clone();
    if let Some(e) = endpoint {
        deployment.endpoint = e.to_string();
    }
    let mut rows: Vec<JournalRow> = Vec::with_capacity(bundle.record.journal.len());
    for (index, raw) in &bundle.record.journal {
        let e = JournalEntry::decode(raw.clone())
            .map_err(|e| ReplayError::BadJournal(e.to_string()))?;
        rows.push((*index, e.ty.raw(), e.payload.to_vec()));
    }
    let id = bundle.record.id;
    replay(
        client,
        None,
        // A record outside the cluster: no segment read path — segmented values
        // ⇒ `Segmented` as before.
        &crate::value_urls::ValueUrls::inline(),
        &deployment,
        id,
        rows,
        crate::turn::params_with_retry(&id, 0),
    )
    .await
}

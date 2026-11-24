// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! The journal projection for readers.
//!
//! The journal stores the wire's protobuf messages. The console needs two
//! layers (Restate's approach): the **head** of each entry — kind, name,
//! target, wake-up time, promise/signal name, the completion it waits for or
//! completes — to list the journal without reading payloads, and the **body**
//! (user bytes: input, output, parameter, value, result) loaded when an entry
//! is opened.
//!
//! Only entries that are in the journal (commands and notifications) are
//! projected. Control messages, unknown codes or broken payloads give an
//! [`EntryView`] with only `kind` (the kind name if known) — the reader still
//! sees the code and hex as before. The crate knows no JSON: classifying the
//! body (JSON / UTF-8 / binary) is the business of the layer above.

use {
    crate::{decode, pb, Flags, Frame, Message, MessageType},
    bytes::Bytes,
};

/// A signal by index (awakeable) or by name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignalRef {
    Index(u32),
    Name(String),
}

/// The head of a journal entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntryLite {
    /// The kind name (`Sleep`, `Call`, …); `None` for an unknown code.
    pub kind: Option<&'static str>,
    /// The name the user gave the command (`ctx.run("reserve", …)`); empty ⇒ `None`.
    pub name: Option<String>,
    /// A command: the completion id its result notification will carry.
    pub awaits: Option<u32>,
    /// `Call`/`OneWayCall`: the completion id of the notification carrying the callee id.
    pub awaits_invocation_id: Option<u32>,
    /// A notification: the completion id it completes.
    pub completes: Option<u32>,
    /// The target of `Call`/`OneWayCall`.
    pub service: Option<String>,
    pub handler: Option<String>,
    pub key: Option<String>,
    /// The targeted invocation (`AttachInvocation`, `GetInvocationOutput`,
    /// `SendSignal`) or the callee id (`CallInvocationIdCompletion`).
    pub invocation_id: Option<String>,
    /// `Sleep`: the wake-up time; a scheduled `OneWayCall`: the run time (Unix ms).
    pub wake_up_at_ms: Option<u64>,
    /// The promise key (`GetPromise`, `PeekPromise`, `CompletePromise`).
    pub promise: Option<String>,
    /// The state key (UTF-8, otherwise hex).
    pub state_key: Option<String>,
    /// The signal sent (`SendSignal`) or received (`Signal`).
    pub signal: Option<SignalRef>,
    /// The result is a failure: `(code, message)`.
    pub failure: Option<(u32, String)>,
}

/// A projected entry: head and body (user bytes), if any.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EntryView {
    pub lite: EntryLite,
    pub body: Option<Bytes>,
}

fn opt(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

fn text_or_hex(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => b.iter().map(|x| format!("{x:02x}")).collect(),
    }
}

fn value(v: &Option<pb::Value>) -> Option<Bytes> {
    v.as_ref().and_then(inline_body)
}

/// The displayed body of a `Value`: the whole bytes when inline. Segmented ⇒
/// `None` (the projection does not read segments; the tail is not the value) —
/// like an entry without a body.
fn inline_body(v: &pb::Value) -> Option<Bytes> {
    crate::value::inline_content(Some(v)).ok()
}

fn failure(f: &pb::Failure) -> Option<(u32, String)> {
    Some((f.code, f.message.clone()))
}

/// Projects the journal entry `(ty, payload)`.
pub fn view(ty: MessageType, payload: &Bytes) -> EntryView {
    let mut lite = EntryLite {
        kind: ty.name(),
        ..Default::default()
    };
    let Ok(msg) = decode(&Frame::new(ty, Flags::NONE, payload.clone())) else {
        return EntryView { lite, body: None };
    };
    let body = match msg {
        Message::Input(m) => {
            lite.name = opt(&m.name);
            value(&m.value)
        }
        Message::Output(m) => {
            use pb::output_command_message::Result as R;
            lite.name = opt(&m.name);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::GetState(m) => {
            lite.name = opt(&m.name);
            lite.state_key = Some(text_or_hex(&m.key));
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::SetState(m) => {
            lite.name = opt(&m.name);
            lite.state_key = Some(text_or_hex(&m.key));
            value(&m.value)
        }
        Message::ClearState(m) => {
            lite.name = opt(&m.name);
            lite.state_key = Some(text_or_hex(&m.key));
            None
        }
        Message::ClearAllState(m) => {
            lite.name = opt(&m.name);
            None
        }
        Message::GetStateKeys(m) => {
            lite.name = opt(&m.name);
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::GetEagerState(m) => {
            use pb::get_eager_state_command_message::Result as R;
            lite.name = opt(&m.name);
            lite.state_key = Some(text_or_hex(&m.key));
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                _ => None,
            }
        }
        Message::GetEagerStateKeys(m) => {
            lite.name = opt(&m.name);
            None
        }
        Message::GetPromise(m) => {
            lite.name = opt(&m.name);
            lite.promise = Some(m.key.clone());
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::PeekPromise(m) => {
            lite.name = opt(&m.name);
            lite.promise = Some(m.key.clone());
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::CompletePromise(m) => {
            use pb::complete_promise_command_message::Completion as C;
            lite.name = opt(&m.name);
            lite.promise = Some(m.key.clone());
            lite.awaits = Some(m.result_completion_id);
            match &m.completion {
                Some(C::CompletionValue(v)) => inline_body(v),
                Some(C::CompletionFailure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::Sleep(m) => {
            lite.name = opt(&m.name);
            lite.wake_up_at_ms = Some(m.wake_up_time);
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::Call(m) => {
            lite.name = opt(&m.name);
            lite.service = opt(&m.service_name);
            lite.handler = opt(&m.handler_name);
            lite.key = opt(&m.key);
            lite.awaits = Some(m.result_completion_id);
            lite.awaits_invocation_id = Some(m.invocation_id_notification_idx);
            Some(m.parameter.clone())
        }
        Message::OneWayCall(m) => {
            lite.name = opt(&m.name);
            lite.service = opt(&m.service_name);
            lite.handler = opt(&m.handler_name);
            lite.key = opt(&m.key);
            lite.wake_up_at_ms = (m.invoke_time > 0).then_some(m.invoke_time);
            lite.awaits_invocation_id = Some(m.invocation_id_notification_idx);
            Some(m.parameter.clone())
        }
        Message::SendSignal(m) => {
            use pb::send_signal_command_message::{Result as R, SignalId as S};
            lite.name = opt(&m.entry_name);
            lite.invocation_id = opt(&m.target_invocation_id);
            lite.signal = match &m.signal_id {
                Some(S::Idx(i)) => Some(SignalRef::Index(*i)),
                Some(S::Name(n)) => Some(SignalRef::Name(n.clone())),
                None => None,
            };
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                _ => None,
            }
        }
        Message::Run(m) => {
            lite.name = opt(&m.name);
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::AttachInvocation(m) => {
            lite.name = opt(&m.name);
            lite.invocation_id = opt(&m.invocation_id);
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::GetInvocationOutput(m) => {
            lite.name = opt(&m.name);
            lite.invocation_id = opt(&m.invocation_id);
            lite.awaits = Some(m.result_completion_id);
            None
        }
        Message::GetStateCompletion(m) => {
            use pb::get_state_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                _ => None,
            }
        }
        Message::GetStateKeysCompletion(m) => {
            lite.completes = Some(m.completion_id);
            None
        }
        Message::GetPromiseCompletion(m) => {
            use pb::get_promise_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::PeekPromiseCompletion(m) => {
            use pb::peek_promise_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                _ => None,
            }
        }
        Message::CompletePromiseCompletion(m) => {
            use pb::complete_promise_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            if let Some(R::Failure(f)) = &m.result {
                lite.failure = failure(f);
            }
            None
        }
        Message::SleepCompletion(m) => {
            lite.completes = Some(m.completion_id);
            None
        }
        Message::CallCompletion(m) => {
            use pb::call_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::CallInvocationIdCompletion(m) => {
            lite.completes = Some(m.completion_id);
            lite.invocation_id = opt(&m.invocation_id);
            None
        }
        Message::RunCompletion(m) => {
            use pb::run_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::AttachInvocationCompletion(m) => {
            use pb::attach_invocation_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                None => None,
            }
        }
        Message::GetInvocationOutputCompletion(m) => {
            use pb::get_invocation_output_completion_notification_message::Result as R;
            lite.completes = Some(m.completion_id);
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                _ => None,
            }
        }
        Message::Signal(m) => {
            use pb::signal_notification_message::{Result as R, SignalId as S};
            lite.signal = match &m.signal_id {
                Some(S::Idx(i)) => Some(SignalRef::Index(*i)),
                Some(S::Name(n)) => Some(SignalRef::Name(n.clone())),
                None => None,
            };
            match &m.result {
                Some(R::Value(v)) => inline_body(v),
                Some(R::Failure(f)) => {
                    lite.failure = failure(f);
                    None
                }
                _ => None,
            }
        }
        // Control, side channel, custom: not in the journal, or nothing to
        // project.
        _ => None,
    };
    EntryView { lite, body }
}

/// Which command entries are **waiting** in a journal: commands with `awaits`
/// that no notification `completes`. Returns the indexes (in input order) of
/// those commands. `OneWayCall` does not wait for a result so it is not
/// counted.
pub fn pending(views: &[EntryView]) -> Vec<usize> {
    let done: std::collections::BTreeSet<u32> =
        views.iter().filter_map(|v| v.lite.completes).collect();
    views
        .iter()
        .enumerate()
        .filter(|(_, v)| v.lite.awaits.is_some_and(|c| !done.contains(&c)))
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ty: MessageType, m: &impl prost::Message) -> EntryView {
        view(ty, &Bytes::from(m.encode_to_vec()))
    }

    fn val(b: &'static [u8]) -> Option<pb::Value> {
        Some(crate::value::inline(Bytes::from_static(b)))
    }

    #[test]
    fn input_output_run() {
        let v = entry(
            MessageType::INPUT,
            &pb::InputCommandMessage {
                headers: vec![],
                value: val(b"{\"a\":1}"),
                name: String::new(),
            },
        );
        assert_eq!(v.lite.kind, Some("Input"));
        assert_eq!(v.body.as_deref(), Some(&b"{\"a\":1}"[..]));

        let v = entry(
            MessageType::OUTPUT,
            &pb::OutputCommandMessage {
                result: Some(pb::output_command_message::Result::Failure(pb::Failure {
                    code: 500,
                    message: "card declined".into(),
                    metadata: vec![],
                })),
                name: String::new(),
            },
        );
        assert_eq!(v.lite.failure, Some((500, "card declined".into())));
        assert_eq!(v.body, None);

        let run = entry(
            MessageType::RUN,
            &pb::RunCommandMessage {
                result_completion_id: 3,
                name: "reserve".into(),
            },
        );
        assert_eq!(run.lite.name.as_deref(), Some("reserve"));
        assert_eq!(run.lite.awaits, Some(3));
        let done = entry(
            MessageType::RUN_COMPLETION,
            &pb::RunCompletionNotificationMessage {
                completion_id: 3,
                result: Some(pb::run_completion_notification_message::Result::Value(
                    crate::value::inline(Bytes::from_static(b"ok")),
                )),
            },
        );
        assert_eq!(done.lite.completes, Some(3));
        assert_eq!(done.body.as_deref(), Some(&b"ok"[..]));
        assert_eq!(pending(&[run.clone(), done]), Vec::<usize>::new());
        assert_eq!(pending(&[run]), vec![0]);
    }

    #[test]
    fn calls_carry_target_and_callee() {
        let call = entry(
            MessageType::CALL,
            &pb::CallCommandMessage {
                service_name: "Payment".into(),
                handler_name: "auth".into(),
                parameter: Bytes::from_static(b"{}"),
                key: "k1".into(),
                invocation_id_notification_idx: 4,
                result_completion_id: 5,
                ..Default::default()
            },
        );
        let l = &call.lite;
        assert_eq!(
            (l.service.as_deref(), l.handler.as_deref(), l.key.as_deref()),
            (Some("Payment"), Some("auth"), Some("k1"))
        );
        assert_eq!((l.awaits, l.awaits_invocation_id), (Some(5), Some(4)));
        assert_eq!(call.body.as_deref(), Some(&b"{}"[..]));
        let id = entry(
            MessageType::CALL_INVOCATION_ID_COMPLETION,
            &pb::CallInvocationIdCompletionNotificationMessage {
                completion_id: 4,
                invocation_id: "9be1".into(),
            },
        );
        assert_eq!(id.lite.completes, Some(4));
        assert_eq!(id.lite.invocation_id.as_deref(), Some("9be1"));
        // A callee id without a result yet ⇒ still waiting (completion 5).
        assert_eq!(pending(&[call, id]), vec![0]);

        let later = entry(
            MessageType::ONE_WAY_CALL,
            &pb::OneWayCallCommandMessage {
                service_name: "Mail".into(),
                handler_name: "send".into(),
                invoke_time: 1_700_000_000_000,
                invocation_id_notification_idx: 7,
                ..Default::default()
            },
        );
        assert_eq!(later.lite.wake_up_at_ms, Some(1_700_000_000_000));
        assert_eq!(later.lite.awaits, None, "send does not wait for a result");
        assert_eq!(pending(&[later]), Vec::<usize>::new());
    }

    #[test]
    fn sleep_state_promise_signal() {
        let sleep = entry(
            MessageType::SLEEP,
            &pb::SleepCommandMessage {
                wake_up_time: 42,
                result_completion_id: 1,
                name: String::new(),
            },
        );
        assert_eq!(
            (sleep.lite.wake_up_at_ms, sleep.lite.awaits),
            (Some(42), Some(1))
        );
        let woke = entry(
            MessageType::SLEEP_COMPLETION,
            &pb::SleepCompletionNotificationMessage {
                completion_id: 1,
                void: Some(pb::Void {}),
            },
        );
        assert_eq!(pending(&[sleep, woke]), Vec::<usize>::new());

        let set = entry(
            MessageType::SET_STATE,
            &pb::SetStateCommandMessage {
                key: Bytes::from_static(b"cart"),
                value: val(b"[1]"),
                name: String::new(),
            },
        );
        assert_eq!(set.lite.state_key.as_deref(), Some("cart"));
        assert_eq!(set.body.as_deref(), Some(&b"[1]"[..]));
        let bin = entry(
            MessageType::CLEAR_STATE,
            &pb::ClearStateCommandMessage {
                key: Bytes::from_static(&[0xff, 0x00]),
                name: String::new(),
            },
        );
        assert_eq!(bin.lite.state_key.as_deref(), Some("ff00"));

        let get = entry(
            MessageType::GET_PROMISE,
            &pb::GetPromiseCommandMessage {
                key: "approved".into(),
                result_completion_id: 2,
                name: String::new(),
            },
        );
        assert_eq!(get.lite.promise.as_deref(), Some("approved"));
        assert_eq!(pending(&[get]), vec![0]);

        let sig = entry(
            MessageType::SIGNAL,
            &pb::SignalNotificationMessage {
                signal_id: Some(pb::signal_notification_message::SignalId::Idx(17)),
                result: Some(pb::signal_notification_message::Result::Value(
                    crate::value::inline(Bytes::from_static(b"\"yes\"")),
                )),
            },
        );
        assert_eq!(sig.lite.signal, Some(SignalRef::Index(17)));
        assert_eq!(sig.body.as_deref(), Some(&b"\"yes\""[..]));
        let send = entry(
            MessageType::SEND_SIGNAL,
            &pb::SendSignalCommandMessage {
                target_invocation_id: "abc".into(),
                signal_id: Some(pb::send_signal_command_message::SignalId::Name(
                    "approve".into(),
                )),
                result: None,
                entry_name: "nudge".into(),
            },
        );
        assert_eq!(send.lite.signal, Some(SignalRef::Name("approve".into())));
        assert_eq!(send.lite.invocation_id.as_deref(), Some("abc"));
        assert_eq!(send.lite.name.as_deref(), Some("nudge"));
    }

    #[test]
    fn unknown_and_broken_entries_keep_only_the_kind() {
        let v = view(MessageType(0xFC01), &Bytes::from_static(b"\x01\x02"));
        assert_eq!(v, EntryView::default());
        let v = view(MessageType::SLEEP, &Bytes::from_static(b"\xff\xff\xff"));
        assert_eq!(v.lite.kind, Some("Sleep"));
        assert_eq!(v.lite.awaits, None);
        assert_eq!(v.body, None);
    }
}

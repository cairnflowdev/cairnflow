// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! External promises, idempotency, attach, reply.

use super::*;

impl<S: RangeStore> Engine<S> {
    /// Completes a promise **from outside the system**, through no invocation.
    ///
    /// This is the door for human-in-the-loop: an approval form, a webhook, a
    /// provider callback. Before it existed, users had to write a `SHARED`
    /// handler just to call `ctx.promise(name).resolve()` — it works, but it is
    /// exactly a piece of boilerplate that *every* user must rewrite, i.e. it
    /// belongs in the runtime.
    ///
    /// One commit does three things, just like the `Task::PromiseComplete`
    /// branch: writes the promise, queues the result for **every** waiter, and
    /// removes them from the wait list. Split apart, a death exactly in between
    /// would leave waiters forever on a promise that already has a result.
    ///
    /// **The completer is `(all-zero id, 0)`** — the "from outside" identity.
    /// It collides with no one in practice: the 16 identifying bytes of a real
    /// id are a truncated SHA-256 or 128 random bits, so a real all-zero id has
    /// probability 2⁻¹²⁸. (The earlier version justified it with "the first two
    /// bytes are the partition number, never all zero" — wrong for partition 0
    /// right away, and since 24-byte ids there is no partition byte anymore.)
    /// Thanks to that, the `completer` comparison in the replay branch still
    /// tells "ourselves on the previous run" apart from "an outsider got there
    /// first".
    pub async fn complete_promise_external(
        &mut self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
        result: ReplyResult,
    ) -> Result<PromiseOutcome> {
        let target = Target {
            tenant: tenant.clone(),
            scope: scope.cloned(),
            service: service.to_string(),
            handler: String::new(),
            object_key: object_key.to_string(),
        };
        let owner = target.object_owner().ok_or_else(|| no_owner(&target))?;
        if let Some(rec) = self.read_promise(owner, &target, name).await? {
            // A promise completes only **once**. Say clearly what result it
            // already has, because the caller is usually a retrying webhook —
            // it needs to know it is a duplicate, not get a generic error.
            return Ok(PromiseOutcome::AlreadyCompleted(rec.result));
        }
        let mut muts = Mutations::new();
        let rec = PromiseRecord {
            result: result.clone(),
            completer: ([0u8; 24], 0),
        };
        muts.set_promise(
            owner,
            tenant,
            scope,
            service,
            object_key,
            name,
            &rec.encode(),
        );
        let mut woken = 0usize;
        for (waiter, cid) in self
            .writer
            .promise_waiters_unbounded(owner, tenant, scope, service, object_key, name)
            .await?
        {
            let entry = OutboxEntry::Reply(OutboundReply {
                caller: waiter,
                completion_id: cid,
                result: result.clone(),
            });
            // Owner = the promise's object — the producer of the reply.
            self.stage_outbox(&mut muts, owner, (waiter, cid), &entry.encode())
                .await?;
            muts.remove_promise_waiter(
                owner, tenant, scope, service, object_key, name, &waiter, cid,
            );
            woken += 1;
        }
        self.writer.commit(muts).await?;
        Ok(PromiseOutcome::Completed { woken })
    }

    /// Reads a promise without any invocation — for the introspection path.
    pub async fn peek_promise_external(
        &self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        object_key: &str,
        name: &str,
    ) -> Result<Option<ReplyResult>> {
        let target = Target {
            tenant: tenant.clone(),
            scope: scope.cloned(),
            service: service.to_string(),
            handler: String::new(),
            object_key: object_key.to_string(),
        };
        let owner = target.object_owner().ok_or_else(|| no_owner(&target))?;
        Ok(self
            .read_promise(owner, &target, name)
            .await?
            .map(|r| r.result))
    }

    /// The invocation holding an idempotency key set, if any.
    pub async fn idempotency_target(
        &self,
        tenant: &TenantId,
        scope: Option<&Scope>,
        service: &str,
        handler: &str,
        object_key: &str,
        idem_key: &str,
    ) -> Result<Option<InvocationId>> {
        Ok(self
            .writer
            .idempotency_target(tenant, scope, service, handler, object_key, idem_key)
            .await?)
    }

    /// Records one more waiter for `target`'s result.
    pub async fn attach(
        &mut self,
        target: &InvocationId,
        caller: &InvocationId,
        completion_id: u32,
    ) -> Result<()> {
        let mut muts = Mutations::new();
        muts.add_attachment(target, caller, completion_id);
        self.writer.commit(muts).await?;
        Ok(())
    }

    /// Queues a result in the outbox to return to a waiter.
    ///
    /// `producer` is the **local** entity that produced this result — the
    /// invocation that has the result, or the caller the runtime rejected on
    /// its behalf — and the row lives under its pk, not under the waiter's pk.
    pub async fn enqueue_reply(
        &mut self,
        producer: &InvocationId,
        caller: &InvocationId,
        completion_id: u32,
        result: ReplyResult,
    ) -> Result<()> {
        let entry = OutboxEntry::Reply(OutboundReply {
            caller: *caller,
            completion_id,
            result,
        });
        let mut muts = Mutations::new();
        self.stage_outbox(
            &mut muts,
            partition_key_of(producer),
            (*caller, completion_id),
            &entry.encode(),
        )
        .await?;
        self.writer.commit(muts).await?;
        Ok(())
    }
}

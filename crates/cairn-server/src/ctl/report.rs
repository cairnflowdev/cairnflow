// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
//! `cairn-ctl … --report-to-bucket <id>`: the command's result is written as a JSON
//! report `ops-reports/<id>` in the cluster's bucket, so the control plane — which only
//! holds a `provision` key — can read it through `GET /admin/ops-reports/{id}` when
//! `cairn-ctl` runs in a one-off machine of the cell's app (the bucket credential is
//! the cell's secret, not the control plane's).
//!
//! The report **carries no secrets**: only commands in [`REPORTABLE`] (not
//! `keys create`, no command printing tenant data like `range dump`/`verify`), the
//! command name instead of argv, and exactly the JSON `--json` prints.

use {
    super::{now_ms, Ctl, CtlError, CtlResult},
    cairn_store::ops_report::{self, Pruned, Written},
    serde::Serialize,
};

/// Commands accepting `--report-to-bucket`: backup/restore, reading the cluster version,
/// and the one-way migration gates — the work the control plane triggers in a cell.
pub const REPORTABLE: &[&[&str]] = &[
    &["backup"],
    &["backup", "list"],
    &["backup", "prune"],
    &["restore"],
    &["cluster", "status"],
    &["table", "restore"],
    &["cluster", "enable-quiet-lease"],
    &["cluster", "enable-call-prefix"],
    &["cluster", "enable-streams"],
    &["cluster", "enable-schedules"],
    &["cluster", "enable-key-roles"],
    &["cluster", "enable-tenant-state"],
    &["cluster", "enable-audit"],
    &["cluster", "enable-tenant-purge"],
    &["cluster", "enable-values"],
];

pub fn reportable(cmd: &[&str]) -> bool {
    REPORTABLE.contains(&cmd)
}

/// The JSON content of `ops-reports/<id>`.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsReport {
    pub id: String,
    /// The command name (`backup prune`), not argv.
    pub command: String,
    /// Whether `--apply` was given (repair commands dry-run without it).
    pub apply: bool,
    /// `cairn-ctl <version> (data=…, knows=…)` — like `--version`.
    pub binary: String,
    pub data_format: u32,
    pub knows: u32,
    pub who: String,
    pub started_ms: u64,
    pub finished_ms: u64,
    /// The command's exit code (0 done, 1 verify found errors, 2 arguments, 3 refused, 4 store).
    pub exit_code: i32,
    pub ok: bool,
    pub error: Option<String>,
    /// The JSON `--json` prints for this command; `None` when the command failed.
    pub result: Option<serde_json::Value>,
}

impl OpsReport {
    /// The report of `cmd` (already through [`reportable`]) running from `started_ms` until now.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: &str,
        cmd: &[&str],
        apply: bool,
        who: &str,
        started_ms: u64,
        exit_code: i32,
        error: Option<String>,
        result: Option<serde_json::Value>,
    ) -> Self {
        Self {
            id: id.to_string(),
            command: cmd.join(" "),
            apply,
            binary: crate::version_line("cairn-ctl"),
            data_format: cairn_store::DATA_FORMAT,
            knows: cairn_store::migrate::KNOWS,
            who: who.to_string(),
            started_ms,
            finished_ms: now_ms(),
            exit_code,
            ok: exit_code == 0,
            error,
            result,
        }
    }
}

/// A written report.
#[derive(Clone, Debug)]
pub struct ReportWritten {
    pub path: String,
    pub pruned: Pruned,
}

impl Ctl {
    /// Before running the command: `id` is valid (otherwise [`CtlError::Bad`]) and has no
    /// report yet (otherwise [`CtlError::Refused`] — do not run the command and only then
    /// find it cannot be written). The `Create` write in [`Self::report_write`] is still
    /// what guarantees it.
    pub async fn report_reserve(&self, id: &str) -> CtlResult<()> {
        ops_report::check_id(id).map_err(CtlError::Bad)?;
        let main = self.stores.main();
        if ops_report::exists(main.store(), main.prefix(), id).await? {
            return Err(CtlError::Refused(format!(
                "report ops-reports/{id} already exists — use a new id for each run"
            )));
        }
        Ok(())
    }

    /// Writes the report (`Create`, ambiguity resolved by reading back), then prunes
    /// reports over the cap ([`ops_report::prune`]). An id that already has another
    /// report ⇒ [`CtlError::Refused`]; a failed prune is only a warning in
    /// [`ReportWritten::pruned`].
    pub async fn report_write(&self, r: &OpsReport) -> CtlResult<ReportWritten> {
        let main = self.stores.main();
        let json = serde_json::to_vec(r)
            .map_err(|e| CtlError::Bad(format!("cannot build the report: {e}")))?;
        match ops_report::write(main.store(), main.prefix(), &r.id, &json).await? {
            Written::Written => {}
            Written::Taken => {
                return Err(CtlError::Refused(format!(
                    "ops-reports/{} already holds a different report — not overwriting",
                    r.id
                )))
            }
        }
        let pruned = match ops_report::prune(main.store(), main.prefix(), &r.id).await {
            Ok(p) => p,
            Err(e) => Pruned {
                skipped: Some(format!("cannot list ops-reports/: {e}")),
                ..Pruned::default()
            },
        };
        Ok(ReportWritten {
            path: ops_report::report_path(main.prefix(), &r.id).to_string(),
            pruned,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_bearing_commands_are_not_reportable() {
        assert!(reportable(&["backup", "prune"]));
        assert!(reportable(&["cluster", "enable-key-roles"]));
        for no in [
            &["keys", "create", "default", "provision"][..],
            &["range", "dump", "1"],
            &["verify"],
            &["trash", "list"],
            &["backup", "prune", "x"],
            &[],
        ] {
            assert!(!reportable(no), "{no:?}");
        }
    }
}

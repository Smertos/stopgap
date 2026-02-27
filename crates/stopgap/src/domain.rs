use serde_json::Value;
use serde_json::json;

#[derive(Debug)]
pub(crate) struct FnVersionRow {
    pub(crate) fn_name: String,
    pub(crate) live_fn_schema: String,
    pub(crate) artifact_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveFnRow {
    pub(crate) oid: i64,
    pub(crate) fn_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PruneReport {
    pub(crate) enabled: bool,
    pub(crate) dropped: Vec<String>,
    pub(crate) skipped_with_dependents: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct CandidateFn {
    pub(crate) fn_name: String,
    pub(crate) artifact_hash: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DiffRow {
    pub(crate) fn_name: String,
    pub(crate) change: &'static str,
    pub(crate) active_artifact_hash: Option<String>,
    pub(crate) candidate_artifact_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DiffSummary {
    pub(crate) added: usize,
    pub(crate) changed: usize,
    pub(crate) removed: usize,
    pub(crate) unchanged: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeploymentStatus {
    Open,
    Sealed,
    Active,
    RolledBack,
    Failed,
}

impl DeploymentStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Sealed => "sealed",
            Self::Active => "active",
            Self::RolledBack => "rolled_back",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "sealed" => Some(Self::Sealed),
            "active" => Some(Self::Active),
            "rolled_back" => Some(Self::RolledBack),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

pub(crate) fn prune_manifest_item(report: &PruneReport) -> Value {
    json!({
        "enabled": report.enabled,
        "dropped": report.dropped,
        "skipped_with_dependents": report.skipped_with_dependents
    })
}

pub(crate) fn compute_diff_rows(
    active: &[FnVersionRow],
    candidate: &[CandidateFn],
) -> (Vec<DiffRow>, DiffSummary) {
    let active_by_name = active
        .iter()
        .map(|row| (row.fn_name.as_str(), row.artifact_hash.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let candidate_by_name = candidate
        .iter()
        .map(|row| (row.fn_name.as_str(), row.artifact_hash.as_str()))
        .collect::<std::collections::BTreeMap<_, _>>();

    let all_names = active_by_name
        .keys()
        .chain(candidate_by_name.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>();

    let mut rows = Vec::with_capacity(all_names.len());
    let mut summary = DiffSummary::default();

    for fn_name in all_names {
        let active_hash = active_by_name.get(fn_name).copied();
        let candidate_hash = candidate_by_name.get(fn_name).copied();

        let change = match (active_hash, candidate_hash) {
            (None, Some(_)) => {
                summary.added += 1;
                "added"
            }
            (Some(_), None) => {
                summary.removed += 1;
                "removed"
            }
            (Some(a), Some(c)) if a != c => {
                summary.changed += 1;
                "changed"
            }
            (Some(_), Some(_)) => {
                summary.unchanged += 1;
                "unchanged"
            }
            (None, None) => continue,
        };

        rows.push(DiffRow {
            fn_name: fn_name.to_string(),
            change,
            active_artifact_hash: active_hash.map(str::to_string),
            candidate_artifact_hash: candidate_hash.map(str::to_string),
        });
    }

    (rows, summary)
}

pub(crate) fn fn_manifest_item(
    source_schema: &str,
    live_schema: &str,
    fn_name: &str,
    kind: &str,
    artifact_hash: &str,
) -> Value {
    json!({
        "fn_name": fn_name,
        "source_schema": source_schema,
        "live_schema": live_schema,
        "kind": kind,
        "artifact_hash": artifact_hash,
        "pointer": {
            "plts": 1,
            "kind": "artifact_ptr",
            "artifact_hash": artifact_hash,
            "export": "default",
            "mode": "stopgap_deployed"
        }
    })
}

pub(crate) fn rollback_steps_to_offset(steps: i32) -> Result<i64, String> {
    if steps < 1 {
        return Err("stopgap.rollback requires steps >= 1".to_string());
    }

    Ok(i64::from(steps - 1))
}

pub(crate) fn is_allowed_transition(from: DeploymentStatus, to: DeploymentStatus) -> bool {
    matches!(
        (from, to),
        (DeploymentStatus::Open, DeploymentStatus::Sealed)
            | (DeploymentStatus::Open, DeploymentStatus::Failed)
            | (DeploymentStatus::Sealed, DeploymentStatus::Active)
            | (DeploymentStatus::Sealed, DeploymentStatus::Failed)
            | (DeploymentStatus::Active, DeploymentStatus::RolledBack)
            | (DeploymentStatus::Active, DeploymentStatus::Failed)
            | (DeploymentStatus::RolledBack, DeploymentStatus::Active)
    )
}

pub(crate) fn hash_lock_key(env: &str) -> i64 {
    let mut hash: i64 = 1469598103934665603;
    for b in env.as_bytes() {
        hash ^= i64::from(*b);
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

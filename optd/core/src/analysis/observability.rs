//! Opt-in, in-memory observations for one optimizer run.
//!
//! This deliberately keeps records local to a run.  It is not an exporter or
//! tracing framework; consumers inspect the retained records directly.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use log::warn;

use crate::{EstimateSource, Operator};

const DEFAULT_RECORD_SOFT_LIMIT: usize = 10_000;
const DEFAULT_BYTES_SOFT_LIMIT: usize = 4 * 1024 * 1024;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// The amount of observation collected for an optimization run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationMode {
    /// Retain summaries only for the explicitly finalized selected plan.
    LastPlanSummary,
    /// Also retain estimator and current join-enumeration decisions.
    DetailedSummary,
}

/// A local identity for an analysis context participating in an observed run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObservationContext {
    /// Context identity, unique only within the session.
    pub id: u64,
    /// Cache generation for this context.
    pub generation: u64,
}

/// The lifecycle state of a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationScopeStatus {
    Running,
    Complete,
    Failed,
}

/// One unit of work that produced observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationScope {
    pub id: u64,
    pub context: ObservationContext,
    pub parent: Option<u64>,
    pub kind: ObservationScopeKind,
    pub status: ObservationScopeStatus,
}

/// The actual current optimizer/estimator boundary represented by a scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationScopeKind {
    Operator(Operator),
    Candidate(Operator),
    OptimizerPass(&'static str),
}

/// An append-only observation record.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservationRecord {
    pub id: u64,
    pub scope_id: Option<u64>,
    pub payload: ObservationPayload,
}

/// Typed values retained by the small initial toolkit.
#[derive(Debug, Clone, PartialEq)]
pub enum ObservationPayload {
    /// A cached estimator result was reused rather than derived again.
    CacheReuse { operator: Operator },
    /// A scan column chose this already-computed NDV source and value.
    ScanNdv {
        operator: Operator,
        column: String,
        value: f64,
        source: EstimateSource,
        cap: f64,
    },
    /// A computed operator profile. `post_selection` is true only for an
    /// estimate requested by explicit final-plan finalization.
    OperatorSummary {
        operator: Operator,
        kind: &'static str,
        inputs: Vec<Operator>,
        rows: f64,
        source: EstimateSource,
        columns: usize,
        post_selection: bool,
    },
    /// A join-ordering candidate was materialized and either retained or
    /// discarded by the existing cost comparison. Generic cost types are not
    /// formatted or reconstructed here.
    CandidateDecision { candidate: Operator, retained: bool },
    /// The selected root supplied to explicit finalization.
    SelectedRoot { operator: Operator },
}

/// Metadata and immutable access to a session's retained observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationMetadata {
    pub session_id: u64,
    pub mode: ObservationMode,
    pub record_count: usize,
    pub estimated_bytes: usize,
    pub record_soft_limit_crossed: bool,
    pub bytes_soft_limit_crossed: bool,
}

#[derive(Debug)]
struct SessionState {
    next_context: u64,
    next_scope: u64,
    records: Vec<ObservationRecord>,
    scopes: Vec<ObservationScope>,
    estimated_bytes: usize,
    record_soft_limit_crossed: bool,
    bytes_soft_limit_crossed: bool,
    selected_root: Option<Operator>,
}

/// Run-owned append-only observation storage.
#[derive(Debug, Clone)]
pub struct ObservationSession {
    id: u64,
    mode: ObservationMode,
    limits: (usize, usize),
    state: Rc<RefCell<SessionState>>,
}

impl ObservationSession {
    /// Starts a distinct observed optimization run.
    pub fn new(mode: ObservationMode) -> Self {
        Self::with_limits(mode, DEFAULT_RECORD_SOFT_LIMIT, DEFAULT_BYTES_SOFT_LIMIT)
    }

    #[cfg(test)]
    pub(crate) fn with_soft_limits(mode: ObservationMode, records: usize, bytes: usize) -> Self {
        Self::with_limits(mode, records, bytes)
    }

    fn with_limits(mode: ObservationMode, records: usize, bytes: usize) -> Self {
        Self {
            id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            mode,
            limits: (records, bytes),
            state: Rc::new(RefCell::new(SessionState {
                next_context: 1,
                next_scope: 1,
                records: Vec::new(),
                scopes: Vec::new(),
                estimated_bytes: 0,
                record_soft_limit_crossed: false,
                bytes_soft_limit_crossed: false,
                selected_root: None,
            })),
        }
    }

    pub fn mode(&self) -> ObservationMode {
        self.mode
    }

    /// Returns retained records. The clone is intentional: callers cannot
    /// mutate session storage and this prototype has no borrowed guard API.
    pub fn records(&self) -> Vec<ObservationRecord> {
        self.state.borrow().records.clone()
    }

    pub fn scopes(&self) -> Vec<ObservationScope> {
        self.state.borrow().scopes.clone()
    }

    pub fn metadata(&self) -> ObservationMetadata {
        let state = self.state.borrow();
        ObservationMetadata {
            session_id: self.id,
            mode: self.mode,
            record_count: state.records.len(),
            estimated_bytes: state.estimated_bytes,
            record_soft_limit_crossed: state.record_soft_limit_crossed,
            bytes_soft_limit_crossed: state.bytes_soft_limit_crossed,
        }
    }

    pub(crate) fn new_context(&self) -> ObservationContext {
        let mut state = self.state.borrow_mut();
        let id = state.next_context;
        state.next_context += 1;
        ObservationContext { id, generation: 0 }
    }

    pub(crate) fn begin(&self, context: ObservationContext, kind: ObservationScopeKind) -> u64 {
        let mut state = self.state.borrow_mut();
        let id = state.next_scope;
        state.next_scope += 1;
        let parent = state
            .scopes
            .iter()
            .rev()
            .find(|scope| {
                scope.context == context && scope.status == ObservationScopeStatus::Running
            })
            .map(|scope| scope.id);
        state.scopes.push(ObservationScope {
            id,
            context,
            parent,
            kind,
            status: ObservationScopeStatus::Running,
        });
        id
    }

    pub(crate) fn finish(&self, scope_id: u64, status: ObservationScopeStatus) {
        if let Some(scope) = self
            .state
            .borrow_mut()
            .scopes
            .iter_mut()
            .find(|scope| scope.id == scope_id)
        {
            scope.status = status;
        }
    }

    pub(crate) fn append(&self, scope_id: Option<u64>, payload: ObservationPayload) {
        let mut state = self.state.borrow_mut();
        let id = state.records.len() as u64 + 1;
        state.estimated_bytes += std::mem::size_of::<ObservationRecord>() + payload_bytes(&payload);
        state.records.push(ObservationRecord {
            id,
            scope_id,
            payload,
        });
        self.warn_if_needed(&mut state);
    }

    pub(crate) fn selected_root(&self, root: Operator) {
        self.state.borrow_mut().selected_root = Some(root);
        self.append(None, ObservationPayload::SelectedRoot { operator: root });
    }

    fn warn_if_needed(&self, state: &mut SessionState) {
        if !state.record_soft_limit_crossed && state.records.len() >= self.limits.0 {
            state.record_soft_limit_crossed = true;
            warn!(
                "observation session {} ({:?}) crossed record soft limit {} at {} records; collection continues",
                self.id,
                self.mode,
                self.limits.0,
                state.records.len()
            );
        }
        if !state.bytes_soft_limit_crossed && state.estimated_bytes >= self.limits.1 {
            state.bytes_soft_limit_crossed = true;
            warn!(
                "observation session {} ({:?}) crossed diagnostic-memory soft limit {} at {} bytes; collection continues",
                self.id, self.mode, self.limits.1, state.estimated_bytes
            );
        }
    }
}

fn payload_bytes(payload: &ObservationPayload) -> usize {
    match payload {
        ObservationPayload::ScanNdv { column, .. } => column.capacity(),
        ObservationPayload::OperatorSummary { inputs, .. } => {
            inputs.capacity() * std::mem::size_of::<Operator>()
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_limits_warn_once_and_keep_records() {
        let session = ObservationSession::with_soft_limits(ObservationMode::DetailedSummary, 1, 1);
        session.append(
            None,
            ObservationPayload::SelectedRoot {
                operator: Operator(0),
            },
        );
        session.append(
            None,
            ObservationPayload::SelectedRoot {
                operator: Operator(1),
            },
        );
        let metadata = session.metadata();
        assert_eq!(metadata.record_count, 2);
        assert!(metadata.record_soft_limit_crossed);
        assert!(metadata.bytes_soft_limit_crossed);
    }
}

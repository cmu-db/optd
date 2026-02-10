/// This module contains the main data structure used in decorrelation, the
/// UnnestingInfo structure. In the original paper on unnesting arbitrary
/// subqueries, this is represented as two separate structures, but for
/// simplicity with the borrow checker, this is currently maintained in a single
/// structure. This can be optimised in the future to prevent unnecessary
/// copying.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{
    LogicalJoin, Operator, join::JoinType,
};
use crate::ir::scalar::{BinaryOpKind, ColumnRef, NaryOpKind};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind, ScalarValue};
use crate::utility::union_find::UnionFind;

#[derive(Clone, Debug)]
pub(super) struct UnnestingInfo {

    /// The columns from the outer query that are referenced in this subquery
    pub(super) outer_refs: HashSet<Column>,

    /// Mapping from outer column to the column in the domain (D) (immutable)
    pub(super) domain_repr: HashMap<Column, Column>,
    /// The Domain operator (D) available for joining at leaves
    pub(super) domain_op: Arc<Operator>,

    /// Mapping from outer column to the current representative column (mutable)
    pub(super) repr: HashMap<Column, Column>,
    /// Equivalence classes of columns for attribute minimization
    pub(super) cclasses: UnionFind<Column>,

    /// Parent outer refs for checking deep correlations (flattened from parent chain)
    pub(super) parent_outer_refs: HashSet<Column>,
    /// Parent domain mappings (flattened) for tracking domain columns
    pub(super) parent_domain_repr: HashMap<Column, Column>,
    /// Parent repr mappings for resolving deep correlations
    pub(super) parent_repr: HashMap<Column, Column>,
    /// Parent domain operator (flattened from parent chain)
    pub(super) parent_domain_op: Option<Arc<Operator>>,
    /// Parent outer refs that are actually referenced in this scope
    pub(super) required_parent_refs: HashSet<Column>,

}

impl UnnestingInfo {
    /// Resolve mappings transitively through local and parent mappings.
    pub(super) fn resolve_mapped_column(&self, col: Column) -> Column {
        let mut current = col;
        let mut visited = HashSet::new();
        while visited.insert(current) {
            if let Some(next) = self
                .repr
                .get(&current)
                .copied()
                .or_else(|| self.parent_repr.get(&current).copied())
            {
                if next == current {
                    break;
                }
                current = next;
            } else {
                break;
            }
        }
        current
    }

    /// Create with parent context - inherits outer ref info from parent (but not equivalences)
    pub(super) fn new_with_parent(
        outer_refs: HashSet<Column>,
        domain_repr: HashMap<Column, Column>,
        repr: HashMap<Column, Column>,
        domain_op: Arc<Operator>,
        parent: Option<&UnnestingInfo>,
        required_parent_refs: HashSet<Column>
    ) -> Self {
        let (parent_outer_refs, parent_repr, parent_domain_repr, parent_domain_op) = if let Some(p) = parent {
            // Flatten parent's outer refs (up to the first scope)
            let mut parent_outer_refs = p.outer_refs.clone();
            parent_outer_refs.extend(p.parent_outer_refs.iter().copied());
            // Flatten parent's repr mappings (up to the first scope)
            let mut parent_repr = p.repr.clone();
            parent_repr.extend(p.parent_repr.iter().map(|(k, v)| (*k, *v)));
            // Flatten parent's domain repr mappings
            let mut parent_domain_repr = p.domain_repr.clone();
            parent_domain_repr.extend(p.parent_domain_repr.iter().map(|(k, v)| (*k, *v)));
            let parent_domain_op = if let Some(grandparent_domain) = &p.parent_domain_op {
                Some(
                    LogicalJoin::new(
                        JoinType::Inner,
                        p.domain_op.clone(),
                        grandparent_domain.clone(),
                        crate::ir::scalar::Literal::boolean(true).into_scalar(),
                    )
                    .into_operator(),
                )
            } else {
                Some(p.domain_op.clone())
            };
            (parent_outer_refs, parent_repr, parent_domain_repr, parent_domain_op)
        } else {
            (HashSet::new(), HashMap::new(), HashMap::new(), None)
        };
        Self {
            outer_refs,
            domain_repr,
            repr,
            cclasses: UnionFind::default(),
            domain_op,
            parent_outer_refs,
            parent_repr,
            parent_domain_repr,
            parent_domain_op,
            required_parent_refs
        }
    }

    /// Checks if a column is an outer reference in this scope or any parent scope
    pub(super) fn is_outer_ref(&self, col: &Column) -> bool {
        self.outer_refs.contains(col) || self.parent_outer_refs.contains(col)
    }

    /// Generates the accessing refs of an unnesting context. This is the set of
    /// columns that may count as outer-refs. This includes all outer refs in
    /// this context and parent contexts, as well as domain mappings. This can
    /// be used with compute_accessing_set() to find the operators in a tree
    /// that access some outer refs
    pub(super) fn access_refs(&self) -> HashSet<Column> {
        let mut refs = self.outer_refs.clone();
        refs.extend(self.parent_outer_refs.iter().copied());
        refs.extend(self.domain_repr.values().copied());
        refs.extend(self.parent_domain_repr.values().copied());
        refs
    }

    /// Merge equivalence classes from another UnnestingInfo into this one
    pub(super) fn merge_from(&mut self, other: &UnnestingInfo) {
        for col in other.cclasses.keys() {
            self.cclasses.merge(col, &other.cclasses.find(col));
        }
    }

    /// Create a fresh branch-local state for join-side unnesting while
    /// preserving all shared/global information.
    pub(super) fn fork_for_join_branch(&self) -> Self {
        let mut out = self.clone();
        out.repr = HashMap::new();
        out.cclasses = UnionFind::default();
        out
    }

    /// Update repr mappings for current outer refs if a local equivalent is available
    pub(super) fn update_repr_with_available(&mut self, available: &ColumnSet) {
        let outer_refs: Vec<Column> = self.outer_refs.iter().copied().collect();
        for c in outer_refs {
            // Drop stale local mappings once their representative is no longer
            // produced by the current subtree. Keep domain mappings: they encode
            // that D still has to be injected.
            if let Some(current) = self.repr.get(&c).copied() {
                let is_domain_mapping = self.domain_repr.get(&c).copied() == Some(current);
                if !is_domain_mapping && !available.contains(&current) {
                    if let Some(domain_col) = self.domain_repr.get(&c).copied() {
                        self.repr.insert(c, domain_col);
                    } else {
                        self.repr.remove(&c);
                    }
                }
            }

            let representative = self.cclasses.find(&c);
            if !self.is_outer_ref(&representative) && available.contains(&representative) {
                self.repr.insert(c, representative);
            }
        }
    }

    // Based on operators we traverse, we can update the cclasses structure to
    // take advantage of known equivalences
    pub(super) fn update_equivalences(&mut self, condition: &Arc<Scalar>) {
        match &condition.kind {
            ScalarKind::BinaryOp(bin)
                if bin.op_kind == BinaryOpKind::Eq
                    || bin.op_kind == BinaryOpKind::IsNotDistinctFrom =>
            {
                if let (ScalarKind::ColumnRef(l), ScalarKind::ColumnRef(r)) =
                    (&condition.input_scalars()[0].kind, &condition.input_scalars()[1].kind)
                {
                    if self.is_outer_ref(&l.column) && !self.is_outer_ref(&r.column) {
                        self.cclasses.merge(&r.column, &l.column);
                    } else {
                        self.cclasses.merge(&l.column, &r.column);
                    };
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                for child in condition.input_scalars() {
                    self.update_equivalences(child);
                }
            }
            _ => {}
        }
    }

    // Represents whether any mapping in this UnnestingInfo requires the domain
    // to be injected
    pub(super) fn needs_domain(&self) -> bool {
        self.outer_refs.iter().any(|c| {
            matches!(
                (self.domain_repr.get(c), Some(self.resolve_mapped_column(*c))),
                (Some(dom), Some(cur)) if cur == *dom
            )
        })
    }

    // Represents whether parent domain mappings are still needed in this scope
    // after rewriting.
    pub(super) fn needs_parent_domain(&self) -> bool {
        self.required_parent_refs.iter().any(|c| {
            matches!(
                (self.parent_domain_repr.get(c), Some(self.resolve_mapped_column(*c))),
                (Some(dom), Some(cur)) if cur == *dom
            )
        })
    }

    // Simplifies scalars to get rid of redundant terms
    fn simplify_rewritten_scalar(&self, scalar: Arc<Scalar>) -> Arc<Scalar> {
        match &scalar.kind {
            ScalarKind::BinaryOp(bin)
                if bin.op_kind == BinaryOpKind::Eq
                    || bin.op_kind == BinaryOpKind::IsNotDistinctFrom =>
            {
                let lhs = scalar.input_scalars()[0].clone();
                let rhs = scalar.input_scalars()[1].clone();
                if lhs == rhs {
                    crate::ir::scalar::Literal::boolean(true).into_scalar()
                } else {
                    scalar
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                let mut terms = Vec::new();
                for term in scalar.input_scalars() {
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))) {
                        continue;
                    }
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(false)))) {
                        return crate::ir::scalar::Literal::boolean(false).into_scalar();
                    }
                    terms.push(term.clone());
                }

                if terms.is_empty() {
                    return crate::ir::scalar::Literal::boolean(true).into_scalar();
                }
                if terms.len() == 1 {
                    return terms.pop().unwrap();
                }
                if terms.as_slice() == scalar.input_scalars() {
                    return scalar;
                }

                Arc::new(scalar.clone_with_inputs(Some(Arc::from(terms)), None))
            }
            _ => scalar,
        }
    }

    // Rewrite the scalar using representative columns
    pub(super) fn rewrite_columns(&self, scalar: Arc<Scalar>) -> Arc<Scalar> {
        let rewritten = match &scalar.kind {
            ScalarKind::ColumnRef(cr) => {
                ColumnRef::new(self.resolve_mapped_column(cr.column)).into_scalar()
            }
            _ => {
                let new_inputs: Vec<Arc<Scalar>> = scalar
                    .input_scalars()
                    .iter()
                    .map(|s| self.rewrite_columns(s.clone()))
                    .collect();

                if new_inputs != scalar.input_scalars() {
                    Arc::new(scalar.clone_with_inputs(Some(Arc::from(new_inputs)), None))
                } else {
                    scalar
                }
            }
        };
        self.simplify_rewritten_scalar(rewritten)
    }

    // Helper function to rewrite columns, specifically for join conditions
    // (i.e. unnesting a non-dependent join), since it has different semantics
    pub(super) fn rewrite_join_columns(
        &self,
        scalar: Arc<Scalar>,
        other: &UnnestingInfo,
        left_cols: &ColumnSet,
        right_cols: &ColumnSet,
        join_type: JoinType,
    ) -> Arc<Scalar> {
        let rewritten = match &scalar.kind {
            ScalarKind::ColumnRef(cr) => {
                let in_left = left_cols.contains(&cr.column);
                let in_right = right_cols.contains(&cr.column);

                if in_left ^ in_right {
                    ColumnRef::new(cr.column).into_scalar()
                } else {
                    let mapping = {
                        let left_mapped = self
                            .repr
                            .get(&cr.column)
                            .copied()
                            .or_else(|| self.parent_repr.get(&cr.column).copied())
                            .map(|_| self.resolve_mapped_column(cr.column));
                        let right_mapped = other
                            .repr
                            .get(&cr.column)
                            .copied()
                            .or_else(|| other.parent_repr.get(&cr.column).copied())
                            .map(|_| other.resolve_mapped_column(cr.column));

                        match (left_mapped, right_mapped) {
                            (Some(l), Some(r)) if l == r => Some(l),
                            (Some(l), Some(r)) => {
                                if matches!(join_type, JoinType::Inner) && self.is_outer_ref(&l) && !other.is_outer_ref(&r) {
                                    Some(r)
                                } else {
                                    Some(l)
                                }
                            }
                            (Some(l), None) => Some(l),
                            (None, Some(r)) => Some(r),
                            (None, None) => None,
                        }
                    };

                    if let Some(mapped) = mapping {
                        ColumnRef::new(mapped).into_scalar()
                    } else {
                        ColumnRef::new(cr.column).into_scalar()
                    }
                }
            }
            _ => {
                let new_inputs: Vec<Arc<Scalar>> = scalar
                    .input_scalars()
                    .iter()
                    .map(|s| {
                        self.rewrite_join_columns(
                            s.clone(),
                            other,
                            left_cols,
                            right_cols,
                            join_type,
                        )
                    })
                    .collect();

                if new_inputs != scalar.input_scalars() {
                    Arc::new(scalar.clone_with_inputs(Some(Arc::from(new_inputs)), None))
                } else {
                    scalar
                }
            }
        };
        self.simplify_rewritten_scalar(rewritten)
    }
}

/// Compute the set of operators that access outer references.
/// This is the set of operators whose scalars reference columns in `outer_refs`
/// that are not provided by the operator's inputs.
fn compute_accessing(
    op: &Arc<Operator>,
    outer_refs: &HashSet<Column>,
    result: &mut HashSet<*const Operator>,
    ctx: &IRContext,
) {
    let provided_by_inputs = op
        .input_operators()
        .into_iter()
        .fold(ColumnSet::default(), |acc, child| {
            acc | &child.output_columns(ctx)
        });
    'outer: for scalar in op.input_scalars() {
        for col in scalar.used_columns().iter() {
            if outer_refs.contains(col) && !provided_by_inputs.contains(col) {
                result.insert(Arc::as_ptr(op));
                break 'outer;
            }
        }
    }
    for child in op.input_operators() {
        compute_accessing(child, outer_refs, result, ctx);
    }
}

pub(super) fn compute_accessing_set(
    op: &Arc<Operator>,
    outer_refs: &HashSet<Column>,
    ctx: &IRContext,
) -> HashSet<*const Operator> {
    let mut accessing = HashSet::new();
    compute_accessing(op, outer_refs, &mut accessing, ctx);
    accessing
}

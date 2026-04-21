/// This module contains the main data structures used in decorrelation.
///
/// Following the paper, immutable global state lives in `UnnestingInfo`
/// (outer refs/domain/parent pointer), while mutable fragment-local state lives
/// in `Unnesting` (repr/nested_repr/cclasses).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::Result;
use crate::ir::convert::IntoOperator;
use crate::ir::convert::IntoScalar;
use crate::ir::operator::{Operator, Remap};
use crate::ir::scalar::{BinaryOpKind, ColumnRef, NaryOpKind};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind};
use crate::utility::union_find::UnionFind;

#[derive(Clone, Debug)]
pub(super) struct UnnestingInfo<'a> {
    /// The columns from this dependent join's outer branch referenced by its
    /// inner branch.
    outer_refs: HashSet<Column>,

    /// Local domain for this scope `(domain_repr, domain_op)`.
    /// domain_repr maps each outer column to its domain representation
    /// domain_op is the physical projection operator that projects the domain
    domain: (HashMap<Column, Column>, Arc<Operator>),

    /// Parent unnesting state (if this scope is nested)
    parent: Option<&'a Unnesting<'a>>,
}

impl<'a> UnnestingInfo<'a> {
    pub(super) fn new(
        outer_refs: HashSet<Column>,
        domain: (HashMap<Column, Column>, Arc<Operator>),
        parent: Option<&'a Unnesting<'a>>,
    ) -> Self {
        Self {
            outer_refs,
            domain,
            parent,
        }
    }

    pub(super) fn get_outer_refs(&self) -> &HashSet<Column> {
        &self.outer_refs
    }

    pub(super) fn is_outer_ref(&self, col: &Column) -> bool {
        self.outer_refs.contains(col)
    }

    pub(super) fn get_domain_repr_set(&self) -> &HashMap<Column, Column> {
        &self.domain.0
    }

    pub(super) fn get_domain_repr_of(&self, col: &Column) -> Option<Column> {
        self.domain.0.get(col).cloned()
    }

    pub(super) fn get_domain_op(&self) -> Arc<Operator> {
        self.domain.1.clone()
    }

    pub(super) fn get_parent(&self) -> Option<&'a Unnesting<'a>> {
        self.parent
    }
}

#[derive(Debug)]
pub(super) struct Unnesting<'a> {
    /// Shared immutable info for this unnesting scope
    info: Arc<UnnestingInfo<'a>>,

    /// Equivalence classes of columns for attribute minimization
    cclasses: UnionFind<Column>,

    /// Mapping from outer column to the current representative column, if one
    /// is chosen
    repr: HashMap<Column, Column>,

    /// Representatives discovered for columns that belong to ancestor scopes.
    nested_repr: HashMap<Column, Column>,

    /// Rebuilt operators allocate fresh output column ids. This map remembers
    /// which old output columns are now represented by which fresh columns.
    column_rewrites: HashMap<Column, Column>,
}

impl<'a> Unnesting<'a> {
    pub(super) fn new(info: Arc<UnnestingInfo<'a>>) -> Self {
        Self {
            repr: HashMap::new(),
            nested_repr: HashMap::new(),
            column_rewrites: HashMap::new(),
            cclasses: UnionFind::default(),
            info,
        }
    }

    pub(super) fn get_info(&self) -> Arc<UnnestingInfo<'a>> {
        self.info.clone()
    }

    pub(super) fn get_cclass_of(&self, col: &Column) -> Column {
        self.cclasses.find(col)
    }

    pub(super) fn get_repr_of(&self, col: &Column) -> Option<&Column> {
        self.repr.get(col)
    }

    pub(super) fn get_nested_repr_of(&self, col: &Column) -> Option<&Column> {
        self.nested_repr.get(col)
    }

    pub(super) fn get_resolved_repr_of(&self, col: &Column) -> Option<&Column> {
        self.get_repr_of(col)
            .or_else(|| self.get_nested_repr_of(col))
    }

    pub(super) fn has_resolved_repr_for(&self, col: &Column) -> bool {
        self.repr.contains_key(col) || self.nested_repr.contains_key(col)
    }

    pub(super) fn set_scoped_repr_of(&mut self, key: Column, val: Column) {
        if self.info.is_outer_ref(&key) {
            self.repr.insert(key, val);
        } else if self.is_outer_ref_recursive(&key) {
            self.nested_repr.insert(key, val);
        }
    }

    pub(super) fn clear_scoped_repr_of(&mut self, key: &Column) {
        self.repr.remove(key);
        self.nested_repr.remove(key);
    }

    pub(super) fn collect_outer_refs_recursive(&self) -> Vec<Column> {
        let mut refs = Vec::new();
        let mut scope = Some(self.get_info());
        while let Some(info) = scope {
            refs.extend(info.get_outer_refs().iter().copied());
            scope = info.get_parent().map(|p| p.get_info());
        }
        refs.sort();
        refs.dedup();
        refs
    }

    pub(super) fn remap_repr_values(&mut self, remap: &HashMap<Column, Column>) {
        for val in self.repr.values_mut() {
            if let Some(mapped) = remap.get(val) {
                *val = *mapped;
            }
        }
        for val in self.nested_repr.values_mut() {
            if let Some(mapped) = remap.get(val) {
                *val = *mapped;
            }
        }
        for val in self.column_rewrites.values_mut() {
            if let Some(mapped) = remap.get(val) {
                *val = *mapped;
            }
        }
    }

    fn resolve_column_rewrite(&self, col: Column) -> Option<Column> {
        let mut current = col;
        let mut seen = HashSet::new();
        while let Some(next) = self.column_rewrites.get(&current).copied() {
            if !seen.insert(current) {
                break;
            }
            current = next;
        }
        (current != col).then_some(current)
    }

    pub(super) fn resolve_domain_repr_recursive(&self, col: Column) -> Option<Column> {
        self.info.get_domain_repr_of(&col).or_else(|| {
            self.info
                .parent
                .and_then(|p| p.resolve_domain_repr_recursive(col))
        })
    }

    pub(super) fn resolve_col(&self, col: Column) -> Option<Column> {
        self.resolve_column_rewrite(col)
            .or_else(|| self.get_resolved_repr_of(&col).copied())
            .or_else(|| self.resolve_domain_repr_recursive(col))
    }

    pub(super) fn is_outer_ref_recursive(&self, col: &Column) -> bool {
        self.info.is_outer_ref(col)
            || self
                .info
                .parent
                .as_ref()
                .map(|p| p.is_outer_ref_recursive(col))
                .unwrap_or(false)
    }

    pub(super) fn merge_col_eq_from(&mut self, other: &Unnesting<'_>) {
        for col in other.cclasses.keys() {
            self.cclasses.merge(col, &other.cclasses.find(col));
        }
    }

    // Project/remap/aggregate rebuilds create fresh table-index namespaces in
    // the new IR. Remember which old output column now corresponds to which
    // new one so later expressions do not keep referencing hidden columns.
    pub(super) fn record_column_rewrites(&mut self, mapping: &HashMap<Column, Column>) {
        for (source, target) in mapping {
            self.column_rewrites.insert(*source, *target);
        }
    }

    // Passthrough mappings are stronger than column rewrites: the source input
    // column and target output column are equivalent for representative choice.
    pub(super) fn propagate_passthrough_mapping(&mut self, mapping: &HashMap<Column, Column>) {
        for (source, target) in mapping {
            self.column_rewrites.insert(*source, *target);
            self.cclasses.merge(source, target);
        }

        for outer_col in self.collect_outer_refs_recursive() {
            if let Some(resolved) = self.resolve_col(outer_col)
                && let Some(mapped) = mapping.get(&resolved)
            {
                self.set_scoped_repr_of(outer_col, *mapped);
            }
        }
    }

    // Based on operators we traverse, we can update the cclasses structure to
    // take advantage of known equivalences.
    pub(super) fn update_cclasses_equivalences(&mut self, condition: &Arc<Scalar>) {
        match &condition.kind {
            ScalarKind::BinaryOp(bin) if bin.op_kind == BinaryOpKind::Eq => {
                if let (ScalarKind::ColumnRef(l), ScalarKind::ColumnRef(r)) = (
                    &condition.input_scalars()[0].kind,
                    &condition.input_scalars()[1].kind,
                ) {
                    self.cclasses.merge(&l.column, &r.column);
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                for child in condition.input_scalars() {
                    self.update_cclasses_equivalences(child);
                }
            }
            _ => {}
        }
    }

    // Rewrite the scalar using representative / domain columns.
    pub(super) fn rewrite_columns(&self, scalar: Arc<Scalar>) -> Arc<Scalar> {
        let rewritten = match &scalar.kind {
            ScalarKind::ColumnRef(cr) => {
                ColumnRef::new(self.resolve_col(cr.column).unwrap_or(cr.column)).into_scalar()
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
        rewritten.simplify_nary_scalar()
    }
}

/// Compute the operators that access outer references, and the exact columns
/// referenced from outside the subtree.
pub(super) fn compute_accessing_set(
    op: &Arc<Operator>,
    ctx: &IRContext,
) -> Result<(HashSet<*const Operator>, HashSet<Column>)> {
    fn compute(
        op: &Arc<Operator>,
        operators: &mut HashSet<*const Operator>,
        outer_refs: &mut HashSet<Column>,
        ctx: &IRContext,
    ) -> Result<()> {
        // We say an operator is accessing an outer ref if it uses a column
        // that is not available to it from a downstream operator
        let available = op
            .input_operators()
            .iter()
            .try_fold(ColumnSet::default(), |acc, child| {
                Ok(acc | child.output_columns(ctx)?.as_ref())
            })?;
        let mut non_bound_cols = HashSet::new();
        for scalar in op.input_scalars() {
            non_bound_cols.extend((scalar.used_columns() - &available).as_hash_set());
        }
        if !non_bound_cols.is_empty() {
            operators.insert(Arc::as_ptr(op));
            outer_refs.extend(non_bound_cols);
        }
        for child in op.input_operators() {
            compute(child, operators, outer_refs, ctx)?;
        }
        Ok(())
    }

    let (mut operators, mut outer_refs) = (HashSet::new(), HashSet::new());
    compute(op, &mut operators, &mut outer_refs, ctx)?;
    Ok((operators, outer_refs))
}

/// Check if an operator (by pointer) is contained within a subtree.
pub(super) fn is_contained_in(op_ptr: *const Operator, subtree: &Arc<Operator>) -> bool {
    Arc::as_ptr(subtree) == op_ptr
        || subtree
            .input_operators()
            .iter()
            .any(|c| is_contained_in(op_ptr, c))
}

/// Remap right-side output columns that collide with left-side outputs.
/// Returns the (possibly remapped) right operator and the old->new remap map.
pub(super) fn remap_right_output_collisions(
    left_output_cols: &ColumnSet,
    mut right: Arc<Operator>,
    unnesting: &mut Unnesting<'_>,
    ctx: &IRContext,
) -> Result<(Arc<Operator>, HashMap<Column, Column>)> {
    let right_cols = right.output_columns_in_order(ctx)?;
    if !right_cols.iter().any(|col| left_output_cols.contains(col)) {
        return Ok((right, HashMap::new()));
    }

    // `Remap` is the cheapest way in the current IR to assign a fresh
    // table_index to the entire right subtree while preserving column order.
    let table_index = ctx.add_binding(None, right.output_schema(ctx)?.inner().clone())?;
    let right_remap = right_cols
        .into_iter()
        .enumerate()
        .map(|(idx, col)| (col, Column(table_index, idx)))
        .collect::<HashMap<_, _>>();
    right = Remap::new(table_index, right).into_operator();
    unnesting.remap_repr_values(&right_remap);

    Ok((right, right_remap))
}

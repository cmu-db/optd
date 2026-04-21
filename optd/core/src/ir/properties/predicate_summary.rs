//! Predicate-summary property: lineage + aggregated predicate state per operator.
//!
//! See `docs/predicate-summary-lineage.md.memo` for the design document,
//! transfer-function table, and the list of assumptions/limitations.
//!
//! In short, every operator computes a [`PredicateSummary`] that records:
//! - how each of its output columns traces back to base-table values
//!   ([`PredicateSummary::lineage`]),
//! - equivalence classes over those values
//!   ([`PredicateSummary::eq_classes`]),
//! - per-class literal/range constraints accumulated from `WHERE` / join
//!   conditions seen below,
//! - residual predicates that could not be classified,
//! - a sticky contradiction flag.
//!
//! The advanced cardinality estimator in `super::super::adv_card` consumes
//! this summary to pull base-column statistics through projections, aliases,
//! and joins, and to charge filter/join selectivity against a tighter
//! constrained domain rather than the raw catalog min/max.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc};

use crate::error::{Result, whatever};
use crate::ir::{
    Column, DataType, IRContext, Operator, OperatorKind, Scalar, ScalarKind, ScalarValue,
    convert::IntoScalar,
    operator::{Aggregate, DependentJoin, EnforcerSort, Get, Join, Limit, OrderBy, Project, Remap, Select, Subquery, join::JoinType},
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{BinaryOp, BinaryOpKind, Cast, ColumnRef, Function, List, Literal, NaryOp, NaryOpKind},
};
use crate::utility::union_find::UnionFind;

/// Logical identity of a value flowing through the plan.
///
/// Two columns that map to the same `ValueRef` hold the same value; two
/// columns mapped to *different* `ValueRef`s are not known to be equal (they
/// may still be equal, but the summary has no evidence of it).
///
/// Only the variants backed by a recoverable base column can be used to look
/// up column statistics; [`ValueRef::Derived`] is intentionally opaque.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueRef {
    /// The value is exactly a base-table column. Statistics for this column
    /// can be fetched via `IRContext::column_stats`.
    Base(Column),
    /// The value is `date_part('YEAR', base_col)`. Year bounds are still
    /// recoverable from the base column's `min_value` / `max_value` by
    /// decoding the date, and NDV is `max_year - min_year + 1`.
    ExtractYear(Column),
    /// The value is computed from the named column but the derivation is
    /// opaque to the summary. Distinct `Derived` values are **never** merged
    /// into the same equivalence class: we carry the defining `Column` as a
    /// discriminator so two independent opaque derivations cannot silently
    /// collapse into a single class. No statistics pullback is available.
    Derived(Column),
}

impl ValueRef {
    /// Return the `table_index` (binder binding id) this value originates
    /// from. Used to key `selectivity_groups` so predicates touching the
    /// same underlying table are combined with a correlation-aware decay
    /// rather than pure independence.
    pub fn group_key(&self) -> Option<i64> {
        match self {
            Self::Base(Column(table_index, _))
            | Self::ExtractYear(Column(table_index, _))
            | Self::Derived(Column(table_index, _)) => Some(*table_index),
        }
    }
}

/// Inclusive/exclusive `[lower, upper]` bound accumulated on a single value.
///
/// Each bound is `(value, inclusive_flag)`; `None` means "no bound known on
/// this side". A strict bound (`<`, `>`) is `inclusive = false`; `<=`, `>=`
/// are `inclusive = true`.
#[derive(Debug, Clone, Copy, Default)]
pub struct RangeConstraint {
    pub lower: Option<(f64, bool)>,
    pub upper: Option<(f64, bool)>,
}

impl RangeConstraint {
    /// Tighten the lower bound to at most `value`. If the new value equals
    /// the existing one, we down-grade to strict (non-inclusive) when either
    /// side was strict — `col > 5 AND col >= 5` is still `col > 5`.
    pub fn tighten_lower(&mut self, value: f64, inclusive: bool) {
        match self.lower {
            Some((existing, existing_inclusive)) => {
                if value > existing || ((value - existing).abs() < f64::EPSILON && !inclusive) {
                    self.lower = Some((value, inclusive));
                } else if (value - existing).abs() < f64::EPSILON {
                    self.lower = Some((existing, existing_inclusive && inclusive));
                }
            }
            None => self.lower = Some((value, inclusive)),
        }
    }

    /// Symmetric to `tighten_lower`: tighten upper bound to at most `value`,
    /// preserving strictness when both bounds agree numerically.
    pub fn tighten_upper(&mut self, value: f64, inclusive: bool) {
        match self.upper {
            Some((existing, existing_inclusive)) => {
                if value < existing || ((value - existing).abs() < f64::EPSILON && !inclusive) {
                    self.upper = Some((value, inclusive));
                } else if (value - existing).abs() < f64::EPSILON {
                    self.upper = Some((existing, existing_inclusive && inclusive));
                }
            }
            None => self.upper = Some((value, inclusive)),
        }
    }

    /// Returns true iff the current `[lower, upper]` window contains no
    /// values — either `lower > upper` outright, or `lower == upper` with at
    /// least one side strict (`col > 5 AND col < 5` or `col > 5 AND col <=
    /// 5`).
    pub fn is_empty(&self) -> bool {
        match (self.lower, self.upper) {
            (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
                lower > upper
                    || ((lower - upper).abs() < f64::EPSILON
                        && !(lower_inclusive && upper_inclusive))
            }
            _ => false,
        }
    }
}

/// A predicate attributed to a single `table_index` group. Stored per-group
/// so `summary_selectivity` can combine predicates that plausibly correlate
/// (all on the same underlying table) with a dampening factor rather than
/// pure independence.
#[derive(Debug, Clone)]
pub enum GroupPredicate {
    /// `value = literal`.
    EqLiteral(ValueRef, f64),
    /// `value <op> literal`, op ∈ {Lt, Le, Gt, Ge}.
    Range(ValueRef, BinaryOpKind, f64),
    /// `lhs = rhs` — both sides are known `ValueRef`s in this table.
    ColumnEquality(ValueRef, ValueRef),
    /// Anything we could not decompose; treated as an opaque filter for
    /// selectivity purposes (flat fallback charge per term).
    Residual(Arc<Scalar>),
}

/// Aggregated lineage + predicate state at a given operator.
///
/// `PredicateSummary` is derived bottom-up by the property machinery and
/// cached per operator via the `OnceLock` in `OperatorProperties`. The
/// per-operator transfer functions live in
/// `<Operator as Derive<PredicateSummaryProperty>>::derive_by_compute`.
///
/// All `ValueRef` keys in `eq_classes`, `range_constraints`, and
/// `literal_equalities` are kept **canonicalized** by
/// `normalize_constraints` whenever equivalence classes change, so a lookup
/// on any member of a class resolves to the representative's constraint
/// via [`Self::canonical_value`].
#[derive(Debug, Clone, Default)]
pub struct PredicateSummary {
    /// Output column → value flowing through it. Overwritten on `Project`,
    /// rebuilt on `Remap`, reset-and-rebuilt on `Aggregate`, and inherited
    /// by `Select` / `Limit` / `OrderBy` / etc.
    pub lineage: HashMap<Column, ValueRef>,
    /// Union-find over `ValueRef`s learned from `a = b` equality terms and
    /// equi-join conditions.
    pub eq_classes: UnionFind<ValueRef>,
    /// Tightened `[lo, hi]` per canonical value — consolidates all range
    /// constraints seen on any member of the class.
    pub range_constraints: HashMap<ValueRef, RangeConstraint>,
    /// `canonical_value = literal`. Conflicting literals on the same class
    /// flip `is_contradictory`.
    pub literal_equalities: HashMap<ValueRef, f64>,
    /// Predicates that reference columns from more than one `table_index`
    /// group, or that we otherwise could not attribute to a single table.
    pub residual_predicates: Vec<Arc<Scalar>>,
    /// Predicates attributed to exactly one `table_index`. The key is the
    /// binder binding id; the vec preserves insertion order (only material
    /// for stable reporting — selectivity combines them after sorting).
    pub selectivity_groups: HashMap<i64, Vec<GroupPredicate>>,
    /// Sticky — once set, the plan is unsatisfiable and downstream callers
    /// short-circuit to zero selectivity / zero cardinality.
    pub is_contradictory: bool,
}

impl PredicateSummary {
    /// Fold everything in `other` into `self`. Used by joins to combine the
    /// outer and inner summaries before harvesting equi-conditions from the
    /// join predicate.
    ///
    /// Equivalence-class reconstruction re-merges each UF key against its
    /// root in `other` so the resulting class structure in `self` is the
    /// union of both inputs' classes. Literals and ranges are re-added via
    /// the normal entry points so they get re-keyed to the new
    /// canonical representative in case `other` knows about a class `self`
    /// did not.
    pub fn merge_from(&mut self, other: &PredicateSummary) {
        self.lineage.extend(other.lineage.clone());

        for key in other.eq_classes.keys() {
            let root = other.eq_classes.find(key);
            self.eq_classes.merge(&root, key);
        }

        for (value, literal) in &other.literal_equalities {
            self.add_literal_equality(value.clone(), *literal);
        }

        for (value, range) in &other.range_constraints {
            self.add_range_constraint(value.clone(), *range);
        }

        for (group, predicates) in &other.selectivity_groups {
            self.selectivity_groups
                .entry(*group)
                .or_default()
                .extend(predicates.clone());
        }

        self.residual_predicates
            .extend(other.residual_predicates.iter().cloned());
        self.is_contradictory |= other.is_contradictory;
    }

    /// Resolve a `ValueRef` to the representative of its equivalence class.
    /// All `HashMap` keys in the summary are stored canonicalized, so
    /// callers should always canonicalize before a lookup.
    pub fn canonical_value(&self, value: &ValueRef) -> ValueRef {
        self.eq_classes.find(value)
    }

    /// All canonical `ValueRef`s observed under a given `table_index`
    /// group, collected from every internal collection. Used by
    /// `summary_selectivity` to iterate every class that belongs to the
    /// group, even ones that only appear in `lineage` or `eq_classes` but
    /// do not yet have a literal/range bound.
    pub fn values_in_group(&self, group: i64) -> HashSet<ValueRef> {
        let mut values = HashSet::new();
        for value in self.lineage.values() {
            if value.group_key() == Some(group) {
                values.insert(self.canonical_value(value));
            }
        }
        for value in self.eq_classes.keys() {
            if value.group_key() == Some(group) {
                values.insert(self.canonical_value(value));
            }
        }
        for value in self.literal_equalities.keys() {
            if value.group_key() == Some(group) {
                values.insert(self.canonical_value(value));
            }
        }
        for value in self.range_constraints.keys() {
            if value.group_key() == Some(group) {
                values.insert(self.canonical_value(value));
            }
        }
        values
    }

    /// Ingest a `Select`-style predicate. The predicate is decomposed at the
    /// top-level `AND`; each conjunct is classified by `apply_term`.
    /// Early-out on contradiction — further terms cannot un-contradict.
    pub fn apply_predicate(&mut self, predicate: &Arc<Scalar>, ctx: &IRContext) {
        for term in split_conjuncts(predicate.clone()) {
            self.apply_term(&term, ctx);
            if self.is_contradictory {
                return;
            }
        }
    }

    /// Classify a single atomic predicate and update the summary state.
    ///
    /// Order matters: literal booleans short-circuit first, then we attempt
    /// (in order) value-to-value equality, value-to-literal equality, range
    /// constraints, OR-tautology simplification, and finally a
    /// single-table grouping fallback. Anything left over becomes a
    /// cross-table `residual_predicate`.
    fn apply_term(&mut self, term: &Arc<Scalar>, ctx: &IRContext) {
        if let Some(true_predicate) = literal_bool(term)
            && true_predicate
        {
            return;
        }

        if let Some(false_predicate) = literal_bool(term)
            && !false_predicate
        {
            self.is_contradictory = true;
            return;
        }

        if let Some((lhs, rhs)) = extract_value_equality(term, self, ctx) {
            if lhs == rhs || self.canonical_value(&lhs) == self.canonical_value(&rhs) {
                return;
            }
            let group = lhs.group_key().or(rhs.group_key());
            self.eq_classes.merge(&lhs, &rhs);
            if let Some(group) = group {
                self.selectivity_groups
                    .entry(group)
                    .or_default()
                    .push(GroupPredicate::ColumnEquality(lhs, rhs));
            }
            self.normalize_constraints();
            return;
        }

        if let Some((value, literal)) = extract_literal_equality(term, self, ctx) {
            if let Some(group) = value.group_key() {
                self.selectivity_groups
                    .entry(group)
                    .or_default()
                    .push(GroupPredicate::EqLiteral(value.clone(), literal));
            }
            self.add_literal_equality(value, literal);
            self.normalize_constraints();
            return;
        }

        if let Some((value, op, literal)) = extract_range_constraint(term, self, ctx) {
            if let Some(group) = value.group_key() {
                self.selectivity_groups
                    .entry(group)
                    .or_default()
                    .push(GroupPredicate::Range(value.clone(), op, literal));
            }
            let mut range = RangeConstraint::default();
            match op {
                BinaryOpKind::Lt => range.tighten_upper(literal, false),
                BinaryOpKind::Le => range.tighten_upper(literal, true),
                BinaryOpKind::Gt => range.tighten_lower(literal, false),
                BinaryOpKind::Ge => range.tighten_lower(literal, true),
                _ => {}
            }
            self.add_range_constraint(value, range);
            self.normalize_constraints();
            return;
        }

        if let Some(resolved_or) = rewrite_scalar_through_lineage(term.clone(), self, ctx)
            && or_is_tautology(&resolved_or, ctx)
        {
            return;
        }

        let groups = single_group(term, self, ctx);
        if let Some(group) = groups {
            self.selectivity_groups
                .entry(group)
                .or_default()
                .push(GroupPredicate::Residual(term.clone()));
        } else {
            self.residual_predicates.push(term.clone());
        }
    }

    /// Re-key `literal_equalities` and `range_constraints` against the
    /// current UF representatives and re-check consistency.
    ///
    /// This is called after any operation that can change the class
    /// structure (a new equality merges two classes, or `add_literal`/
    /// `add_range` records a new constraint that may collide with an
    /// existing one on the same class). Because UF merges can re-root a
    /// class under a different representative, entries keyed on an old
    /// non-root `ValueRef` would otherwise become invisible — we rebuild
    /// both maps from scratch keyed on the new canonical form.
    ///
    /// Side effect: any detected inconsistency (two different literals on
    /// one class, an empty `[lo, hi]`, or a literal outside its class's
    /// range) sets `is_contradictory` and the summary is considered dead.
    fn normalize_constraints(&mut self) {
        let literal_entries = self.literal_equalities.clone();
        self.literal_equalities.clear();
        for (value, literal) in literal_entries {
            let canonical = self.canonical_value(&value);
            match self.literal_equalities.get(&canonical) {
                Some(existing) if (*existing - literal).abs() > f64::EPSILON => {
                    self.is_contradictory = true;
                    return;
                }
                None => {
                    self.literal_equalities.insert(canonical.clone(), literal);
                }
                _ => {}
            }
            if let Some(range) = self.range_constraints.get(&canonical)
                && !literal_satisfies_range(literal, range)
            {
                self.is_contradictory = true;
                return;
            }
        }

        let range_entries = self.range_constraints.clone();
        self.range_constraints.clear();
        for (value, range) in range_entries {
            let canonical = self.canonical_value(&value);
            let entry = self.range_constraints.entry(canonical.clone()).or_default();
            if let Some((lower, inclusive)) = range.lower {
                entry.tighten_lower(lower, inclusive);
            }
            if let Some((upper, inclusive)) = range.upper {
                entry.tighten_upper(upper, inclusive);
            }
            if entry.is_empty() {
                self.is_contradictory = true;
                return;
            }
            if let Some(literal) = self.literal_equalities.get(&canonical)
                && !literal_satisfies_range(*literal, entry)
            {
                self.is_contradictory = true;
                return;
            }
        }
    }

    fn add_literal_equality(&mut self, value: ValueRef, literal: f64) {
        let canonical = self.canonical_value(&value);
        match self.literal_equalities.get(&canonical) {
            Some(existing) if (*existing - literal).abs() > f64::EPSILON => {
                self.is_contradictory = true;
            }
            None => {
                self.literal_equalities.insert(canonical, literal);
            }
            _ => {}
        }
    }

    fn add_range_constraint(&mut self, value: ValueRef, range: RangeConstraint) {
        let canonical = self.canonical_value(&value);
        let entry = self.range_constraints.entry(canonical).or_default();
        if let Some((lower, inclusive)) = range.lower {
            entry.tighten_lower(lower, inclusive);
        }
        if let Some((upper, inclusive)) = range.upper {
            entry.tighten_upper(upper, inclusive);
        }
        if entry.is_empty() {
            self.is_contradictory = true;
        }
    }
}

/// Marker type for the predicate-summary operator property.
///
/// The computed value is shared as `Arc<PredicateSummary>` because the
/// summary can be moderately large (union-find + several hash maps) and is
/// read by several estimator paths per operator.
pub struct PredicateSummaryProperty;

impl PropertyMarker for PredicateSummaryProperty {
    type Output = Arc<PredicateSummary>;
}

impl Derive<PredicateSummaryProperty> for Operator {
    fn derive_by_compute(&self, ctx: &IRContext) -> <PredicateSummaryProperty as PropertyMarker>::Output {
        match &self.kind {
            // Group placeholders materialize via the memo's cached property.
            // If the cache is empty, we return a default summary — no lineage,
            // no constraints — which will silently suppress stats-driven
            // estimation for that subtree. See docs for the known gap.
            OperatorKind::Group(_) => Arc::new(PredicateSummary::default()),
            OperatorKind::Get(meta) => {
                let get = Get::borrow_raw_parts(meta, &self.common);
                let lineage = get
                    .projections()
                    .iter()
                    .map(|index| {
                        let column = Column(*get.table_index(), *index);
                        (column, ValueRef::Base(column))
                    })
                    .collect();
                Arc::new(PredicateSummary {
                    lineage,
                    ..PredicateSummary::default()
                })
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &self.common);
                let mut summary = (*select.input().predicate_summary(ctx)).clone();
                summary.apply_predicate(select.predicate(), ctx);
                Arc::new(summary)
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &self.common);
                let input_summary = project.input().predicate_summary(ctx);
                let mut summary = (*input_summary).clone();
                let projections = project.projections().borrow::<List>();
                for (idx, projection) in projections.members().iter().enumerate() {
                    let column = Column(*project.table_index(), idx);
                    let value = derive_value_ref(projection, input_summary.as_ref(), ctx)
                        .unwrap_or(ValueRef::Derived(column));
                    summary.lineage.insert(column, value);
                }
                Arc::new(summary)
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &self.common);
                let input_summary = remap.input().predicate_summary(ctx);
                let mut summary = (*input_summary).clone();
                let input_columns = ordered_output_columns(remap.input(), ctx).unwrap_or_default();
                for (idx, input_column) in input_columns.iter().enumerate() {
                    let output_column = Column(*remap.table_index(), idx);
                    let value = input_summary
                        .lineage
                        .get(input_column)
                        .cloned()
                        .unwrap_or(ValueRef::Base(*input_column));
                    summary.lineage.insert(output_column, value);
                }
                Arc::new(summary)
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &self.common);
                let input_summary = agg.input().predicate_summary(ctx);
                // Aggregates change row semantics (one output row per group),
                // so we drop the input's predicate/constraint state entirely
                // and only carry lineage forward for keys. Grouping keys keep
                // their lineage so higher operators can still use per-column
                // NDV for keys; aggregated exprs are opaque.
                let mut summary = PredicateSummary::default();
                let keys = agg.keys().borrow::<List>();
                for (idx, key) in keys.members().iter().enumerate() {
                    let column = Column(*agg.key_table_index(), idx);
                    let value = derive_value_ref(key, input_summary.as_ref(), ctx)
                        .unwrap_or(ValueRef::Derived(column));
                    summary.lineage.insert(column, value);
                }
                let exprs = agg.exprs().borrow::<List>();
                for idx in 0..exprs.members().len() {
                    let column = Column(*agg.aggregate_table_index(), idx);
                    summary.lineage.insert(column, ValueRef::Derived(column));
                }
                Arc::new(summary)
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &self.common);
                Arc::new(summary_for_join(
                    join.outer(),
                    join.inner(),
                    join.join_cond(),
                    ctx,
                ))
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &self.common);
                Arc::new(summary_for_join(
                    join.outer(),
                    join.inner(),
                    join.join_cond(),
                    ctx,
                ))
            }
            OperatorKind::Limit(meta) => {
                let limit = Limit::borrow_raw_parts(meta, &self.common);
                limit.input().predicate_summary(ctx)
            }
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &self.common);
                order_by.input().predicate_summary(ctx)
            }
            OperatorKind::EnforcerSort(meta) => {
                let sort = EnforcerSort::borrow_raw_parts(meta, &self.common);
                sort.input().predicate_summary(ctx)
            }
            OperatorKind::Subquery(meta) => {
                let subquery = Subquery::borrow_raw_parts(meta, &self.common);
                subquery.input().predicate_summary(ctx)
            }
        }
    }

    fn derive(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <PredicateSummaryProperty as PropertyMarker>::Output {
        if let Some(summary) = self.common.properties.predicate_summary.get() {
            return summary.clone();
        }

        let summary = <Self as Derive<PredicateSummaryProperty>>::derive_by_compute(self, ctx);
        self.common
            .properties
            .predicate_summary
            .get_or_init(|| summary.clone())
            .clone()
    }
}

impl Operator {
    /// Shorthand for `self.get_property::<PredicateSummaryProperty>(ctx)`.
    /// Cheap on repeat calls (returns an `Arc` clone of the cached value).
    pub fn predicate_summary(&self, ctx: &crate::ir::context::IRContext) -> Arc<PredicateSummary> {
        self.get_property::<PredicateSummaryProperty>(ctx)
    }
}

/// Return the operator's output columns in positional (schema) order.
///
/// `OutputColumns` is a `HashSet` and therefore unordered, but `Remap` and
/// `Project` need positional information to stitch lineage to the correct
/// input column. This walks the operator tree directly, mirroring the
/// output-schema construction in the operator definitions. `Group(_)` is
/// treated as an error because the caller only ever calls this during
/// property derivation, where a `Group` placeholder must already have its
/// column set cached — we do not want to silently paper over a missing
/// cache here.
pub fn ordered_output_columns(op: &Operator, ctx: &IRContext) -> Result<Vec<Column>> {
    match &op.kind {
        OperatorKind::Group(_) => whatever!("group output order must be cached"),
        OperatorKind::Get(meta) => {
            let get = Get::borrow_raw_parts(meta, &op.common);
            Ok(get
                .projections()
                .iter()
                .map(|index| Column(*get.table_index(), *index))
                .collect())
        }
        OperatorKind::Select(meta) => {
            let select = Select::borrow_raw_parts(meta, &op.common);
            ordered_output_columns(select.input(), ctx)
        }
        OperatorKind::Limit(meta) => {
            let limit = Limit::borrow_raw_parts(meta, &op.common);
            ordered_output_columns(limit.input(), ctx)
        }
        OperatorKind::OrderBy(meta) => {
            let order_by = OrderBy::borrow_raw_parts(meta, &op.common);
            ordered_output_columns(order_by.input(), ctx)
        }
        OperatorKind::EnforcerSort(meta) => {
            let sort = EnforcerSort::borrow_raw_parts(meta, &op.common);
            ordered_output_columns(sort.input(), ctx)
        }
        OperatorKind::Subquery(meta) => {
            let subquery = Subquery::borrow_raw_parts(meta, &op.common);
            ordered_output_columns(subquery.input(), ctx)
        }
        OperatorKind::Project(meta) => {
            let project = Project::borrow_raw_parts(meta, &op.common);
            let projections = project.projections().borrow::<List>();
            Ok((0..projections.members().len())
                .map(|idx| Column(*project.table_index(), idx))
                .collect())
        }
        OperatorKind::Remap(meta) => {
            let remap = Remap::borrow_raw_parts(meta, &op.common);
            let inputs = ordered_output_columns(remap.input(), ctx)?;
            Ok((0..inputs.len())
                .map(|idx| Column(*remap.table_index(), idx))
                .collect())
        }
        OperatorKind::Aggregate(meta) => {
            let agg = Aggregate::borrow_raw_parts(meta, &op.common);
            let keys = agg.keys().borrow::<List>();
            let exprs = agg.exprs().borrow::<List>();
            Ok((0..keys.members().len())
                .map(|idx| Column(*agg.key_table_index(), idx))
                .chain((0..exprs.members().len()).map(|idx| Column(*agg.aggregate_table_index(), idx)))
                .collect())
        }
        OperatorKind::Join(meta) => {
            let join = Join::borrow_raw_parts(meta, &op.common);
            ordered_join_output_columns(join.outer(), join.inner(), join.join_type(), ctx)
        }
        OperatorKind::DependentJoin(meta) => {
            let join = DependentJoin::borrow_raw_parts(meta, &op.common);
            ordered_join_output_columns(join.outer(), join.inner(), join.join_type(), ctx)
        }
    }
}

/// Resolve a scalar expression to a [`ValueRef`] under the given summary.
///
/// This is the single choke-point that defines what derivations the summary
/// recognizes:
///
/// - `ColumnRef(c)` — look up `c` in `lineage`, falling back to `Base(c)`.
/// - `Cast(expr)` — recurse; preserve the inner `ValueRef` **only** if the
///   cast is type-preserving (`source_ty == target_ty`). A type-changing
///   cast is conservatively treated as opaque because downstream stats
///   pullback parses min/max with the *source* type, so a cast to a
///   different type would make the parsed bounds meaningless.
/// - `date_part('YEAR', col)` — recurse; upgrade `Base(c)` to
///   `ExtractYear(c)`. Only `YEAR` is recognized today.
///
/// Anything else returns `None`. Callers that need a `ValueRef` for a
/// scalar that failed to resolve should substitute `ValueRef::Derived`.
pub fn derive_value_ref(
    scalar: &Scalar,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<ValueRef> {
    match &scalar.kind {
        ScalarKind::ColumnRef(meta) => {
            let column = *ColumnRef::borrow_raw_parts(meta, &scalar.common).column();
            summary
                .lineage
                .get(&column)
                .cloned()
                .or(Some(ValueRef::Base(column)))
        }
        ScalarKind::Cast(_) => {
            let cast = scalar.borrow::<Cast>();
            let value = derive_value_ref(cast.expr(), summary, ctx)?;
            match &value {
                ValueRef::Base(column) | ValueRef::ExtractYear(column) => {
                    let source_ty = ctx.get_column_meta(column).data_type;
                    let target_ty = cast.data_type().clone();
                    if source_ty == target_ty {
                        Some(value)
                    } else {
                        // TODO: Allow order-preserving widening casts
                        // (e.g. Int32 → Int64) so range/min-max statistics
                        // remain usable.
                        None
                    }
                }
                ValueRef::Derived(_) => None,
            }
        }
        ScalarKind::Function(_) => {
            let function = scalar.borrow::<Function>();
            if !function.id().eq_ignore_ascii_case("date_part") || function.params().len() != 2 {
                return None;
            }
            // The first parameter is the "part" name as a string literal.
            // `literal_string` unwraps casts so the TPC-H shape
            // `date_part(YEAR::utf8, col)` still matches.
            let Some(part) = literal_string(function.params()[0].as_ref()) else {
                return None;
            };
            if !part.eq_ignore_ascii_case("year") {
                return None;
            }
            match derive_value_ref(function.params()[1].as_ref(), summary, ctx)? {
                ValueRef::Base(column) => Some(ValueRef::ExtractYear(column)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Convert a `Date32` value (days since 1970-01-01) to its calendar year.
pub fn date_days_to_year(days: i32) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some((epoch + TimeDelta::days(days as i64)).year())
}

/// Convert a `Date64` value (milliseconds since the UNIX epoch) to its
/// calendar year in UTC.
pub fn date_millis_to_year(millis: i64) -> Option<i32> {
    let secs = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) as u32) * 1_000_000;
    Some(DateTime::<Utc>::from_timestamp(secs, nanos)?.year())
}

/// Combine the summaries from both join sides and harvest equi-conditions.
///
/// The inner side is folded into a copy of the outer summary, then every
/// top-level `a = b` conjunct of the join condition merges the two values'
/// equivalence classes. Non-equi terms (e.g. `l_qty < p_qty`) are dropped
/// here — the resulting summary therefore understates constraints on join
/// conditions with range terms. See the limitations section of the README.
fn summary_for_join(
    outer: &Arc<Operator>,
    inner: &Arc<Operator>,
    join_cond: &Arc<Scalar>,
    ctx: &IRContext,
) -> PredicateSummary {
    let mut summary = (*outer.predicate_summary(ctx)).clone();
    summary.merge_from(inner.predicate_summary(ctx).as_ref());

    for term in split_conjuncts(join_cond.clone()) {
        if let Some((lhs, rhs)) = extract_value_equality(&term, &summary, ctx) {
            summary.eq_classes.merge(&lhs, &rhs);
            summary.normalize_constraints();
        }
    }

    summary
}

fn ordered_join_output_columns(
    outer: &Arc<Operator>,
    inner: &Arc<Operator>,
    join_type: &JoinType,
    ctx: &IRContext,
) -> Result<Vec<Column>> {
    let outer_columns = ordered_output_columns(outer, ctx)?;
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => Ok(outer_columns),
        JoinType::Mark(mark_column) => Ok(outer_columns
            .into_iter()
            .chain(std::iter::once(*mark_column))
            .collect()),
        JoinType::Inner | JoinType::LeftOuter | JoinType::Single => {
            let inner_columns = ordered_output_columns(inner, ctx)?;
            Ok(outer_columns
                .into_iter()
                .chain(inner_columns)
                .collect())
        }
    }
}

/// Flatten nested top-level `AND` nodes into a list of atomic terms.
/// Used so `apply_predicate` and `summary_for_join` can classify each
/// conjunct independently.
fn split_conjuncts(predicate: Arc<Scalar>) -> Vec<Arc<Scalar>> {
    if let Ok(nary) = predicate.try_borrow::<NaryOp>()
        && nary.op_kind() == &NaryOpKind::And
    {
        return nary.terms().iter().flat_map(|term| split_conjuncts(term.clone())).collect();
    }
    vec![predicate]
}

fn literal_bool(predicate: &Arc<Scalar>) -> Option<bool> {
    let literal = predicate.try_borrow::<Literal>().ok()?;
    match literal.value() {
        ScalarValue::Boolean(Some(value)) => Some(*value),
        ScalarValue::Boolean(None) => Some(false),
        _ => None,
    }
}

/// Extract a string literal, unwrapping any enclosing `Cast`.
///
/// TPC-H queries often surface `date_part(YEAR::utf8, ...)`, in which case
/// the "YEAR" argument arrives wrapped in a cast from the planner's literal
/// type. Unwrapping here keeps year detection working on those plans.
fn literal_string(scalar: &Scalar) -> Option<String> {
    if let Ok(cast) = scalar.try_borrow::<Cast>() {
        return literal_string(cast.expr());
    }
    let literal = scalar.try_borrow::<Literal>().ok()?;
    match literal.value() {
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn extract_value_equality(
    term: &Arc<Scalar>,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<(ValueRef, ValueRef)> {
    let binary = term.try_borrow::<BinaryOp>().ok()?;
    if binary.op_kind() != &BinaryOpKind::Eq {
        return None;
    }
    let lhs = derive_value_ref(binary.lhs(), summary, ctx)?;
    let rhs = derive_value_ref(binary.rhs(), summary, ctx)?;
    Some((lhs, rhs))
}

fn extract_literal_equality(
    term: &Arc<Scalar>,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<(ValueRef, f64)> {
    let binary = term.try_borrow::<BinaryOp>().ok()?;
    if binary.op_kind() != &BinaryOpKind::Eq {
        return None;
    }
    if let Some(value) = derive_value_ref(binary.lhs(), summary, ctx)
        && let Some(literal) = scalar_value_to_f64(binary.rhs())
    {
        return Some((value, literal));
    }
    if let Some(value) = derive_value_ref(binary.rhs(), summary, ctx)
        && let Some(literal) = scalar_value_to_f64(binary.lhs())
    {
        return Some((value, literal));
    }
    None
}

fn extract_range_constraint(
    term: &Arc<Scalar>,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<(ValueRef, BinaryOpKind, f64)> {
    let binary = term.try_borrow::<BinaryOp>().ok()?;
    let op = *binary.op_kind();
    if !matches!(op, BinaryOpKind::Lt | BinaryOpKind::Le | BinaryOpKind::Gt | BinaryOpKind::Ge) {
        return None;
    }
    if let Some(value) = derive_value_ref(binary.lhs(), summary, ctx)
        && let Some(literal) = scalar_value_to_f64(binary.rhs())
    {
        return Some((value, op, literal));
    }
    if let Some(value) = derive_value_ref(binary.rhs(), summary, ctx)
        && let Some(literal) = scalar_value_to_f64(binary.lhs())
    {
        return Some((value, flip_range_op(op), literal));
    }
    None
}

fn scalar_value_to_f64(scalar: &Scalar) -> Option<f64> {
    let literal = scalar.try_borrow::<Literal>().ok()?;
    match literal.value() {
        ScalarValue::Int8(Some(value)) => Some(*value as f64),
        ScalarValue::Int16(Some(value)) => Some(*value as f64),
        ScalarValue::Int32(Some(value)) => Some(*value as f64),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        ScalarValue::UInt8(Some(value)) => Some(*value as f64),
        ScalarValue::UInt16(Some(value)) => Some(*value as f64),
        ScalarValue::UInt32(Some(value)) => Some(*value as f64),
        ScalarValue::UInt64(Some(value)) => Some(*value as f64),
        ScalarValue::Decimal32(Some(value), _, scale) => Some(*value as f64 / 10f64.powi(*scale as i32)),
        ScalarValue::Decimal64(Some(value), _, scale) => Some(*value as f64 / 10f64.powi(*scale as i32)),
        ScalarValue::Decimal128(Some(value), _, scale) => Some(*value as f64 / 10f64.powi(*scale as i32)),
        _ => None,
    }
}

fn flip_range_op(op: BinaryOpKind) -> BinaryOpKind {
    match op {
        BinaryOpKind::Lt => BinaryOpKind::Gt,
        BinaryOpKind::Le => BinaryOpKind::Ge,
        BinaryOpKind::Gt => BinaryOpKind::Lt,
        BinaryOpKind::Ge => BinaryOpKind::Le,
        _ => op,
    }
}

fn literal_satisfies_range(literal: f64, range: &RangeConstraint) -> bool {
    if let Some((lower, inclusive)) = range.lower
        && (literal < lower || ((literal - lower).abs() < f64::EPSILON && !inclusive))
    {
        return false;
    }
    if let Some((upper, inclusive)) = range.upper
        && (literal > upper || ((literal - upper).abs() < f64::EPSILON && !inclusive))
    {
        return false;
    }
    true
}

/// Return the single `table_index` group every column used by `predicate`
/// belongs to, or `None` if the predicate spans multiple groups (or no
/// columns).
///
/// Cross-table predicates go into `residual_predicates` where they are
/// charged a flat fallback selectivity; same-table predicates are routed
/// into `selectivity_groups` so the group's overall selectivity can apply
/// the correlation-aware decay (see `summary_selectivity`).
fn single_group(predicate: &Arc<Scalar>, summary: &PredicateSummary, ctx: &IRContext) -> Option<i64> {
    let used_columns = predicate.used_columns();
    let mut groups = used_columns
        .iter()
        .filter_map(|column| {
            summary
                .lineage
                .get(column)
                .cloned()
                .or(Some(ValueRef::Base(*column)))
                .and_then(|value| derive_value_group(&value, summary, ctx))
        });
    let group = groups.next()?;
    if groups.all(|candidate| candidate == group) {
        Some(group)
    } else {
        None
    }
}

fn derive_value_group(value: &ValueRef, summary: &PredicateSummary, _ctx: &IRContext) -> Option<i64> {
    summary.canonical_value(value).group_key()
}

/// Rewrite a scalar, replacing every `ColumnRef` with the canonical
/// representative of its equivalence class.
///
/// Used to recognize OR tautologies: by rewriting all terms through
/// lineage, a disjunction like `x.a < 10 OR y.a >= 10` where `x.a = y.a`
/// becomes obvious (`a < 10 OR a >= 10`). Returns `None` if any leaf
/// cannot be represented as a scalar (currently only `Derived`), which
/// disables the tautology shortcut for that term.
fn rewrite_scalar_through_lineage(
    scalar: Arc<Scalar>,
    summary: &PredicateSummary,
    ctx: &IRContext,
) -> Option<Arc<Scalar>> {
    match &scalar.kind {
        ScalarKind::ColumnRef(meta) => {
            let column = *ColumnRef::borrow_raw_parts(meta, &scalar.common).column();
            let value = summary
                .lineage
                .get(&column)
                .cloned()
                .unwrap_or(ValueRef::Base(column));
            value_ref_to_scalar(summary.canonical_value(&value), ctx)
        }
        _ => {
            let rewritten: Vec<_> = scalar
                .input_scalars()
                .iter()
                .map(|child| rewrite_scalar_through_lineage(child.clone(), summary, ctx))
                .collect::<Option<Vec<_>>>()?;
            Some(Arc::new(scalar.clone_with_inputs(Some(Arc::from(rewritten)), None)))
        }
    }
}

fn value_ref_to_scalar(value: ValueRef, _ctx: &IRContext) -> Option<Arc<Scalar>> {
    match value {
        ValueRef::Base(column) => Some(ColumnRef::new(column).into_scalar()),
        ValueRef::ExtractYear(column) => Some(
            Function::new_scalar(
                "date_part",
                Arc::new([
                    Literal::new(ScalarValue::Utf8(Some("YEAR".to_string()))).into_scalar(),
                    ColumnRef::new(column).into_scalar(),
                ]),
                DataType::Int32,
            )
            .into_scalar(),
        ),
        ValueRef::Derived(_) => None,
    }
}

/// Detect whether an `OR` of same-value range terms covers the entire
/// domain of that value — in which case it is only constraining non-null
/// rows.
///
/// Algorithm: every disjunct must be a range term on the same `ValueRef`;
/// each term contributes an interval clamped to the value's `[min, max]`
/// from statistics; intervals are merged by sweep; if the merged coverage
/// spans the whole domain and the column is not all-null, the OR is a
/// tautology for the non-null rows.
///
/// Returns `false` on any shape it does not recognise — the caller then
/// falls back to the normal `OR` handling.
fn or_is_tautology(predicate: &Arc<Scalar>, ctx: &IRContext) -> bool {
    let Ok(nary) = predicate.try_borrow::<NaryOp>() else {
        return false;
    };
    if nary.op_kind() != &NaryOpKind::Or || nary.terms().is_empty() {
        return false;
    }

    let mut value = None;
    let mut intervals = Vec::new();
    let mut null_fraction = 0.0;
    for term in nary.terms() {
        let Some((resolved, op, literal)) =
            extract_range_constraint(term, &PredicateSummary::default(), ctx)
        else {
            return false;
        };
        if value.as_ref().is_some_and(|existing| existing != &resolved) {
            return false;
        }
        value = Some(resolved.clone());
        let Some((min, max, nf)) = value_domain(&resolved, ctx) else {
            return false;
        };
        null_fraction = nf;
        let interval = match op {
            BinaryOpKind::Lt | BinaryOpKind::Le => (min, literal.min(max)),
            BinaryOpKind::Gt | BinaryOpKind::Ge => (literal.max(min), max),
            _ => return false,
        };
        intervals.push(interval);
    }

    intervals.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let Some((min, max, _)) = value.as_ref().and_then(|value| value_domain(value, ctx)) else {
        return false;
    };
    let mut current = intervals[0];
    for interval in intervals.into_iter().skip(1) {
        if interval.0 <= current.1 {
            current.1 = current.1.max(interval.1);
        } else {
            return false;
        }
    }

    current.0 <= min && current.1 >= max && null_fraction < 1.0
}

fn value_domain(value: &ValueRef, ctx: &IRContext) -> Option<(f64, f64, f64)> {
    match value {
        ValueRef::Base(column) => {
            let (stats, offset) = ctx.column_stats(column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            let col_type = &ctx.get_column_meta(column).data_type;
            let min = parse_base_stat_value(col_stats.min_value.as_ref()?, col_type)?;
            let max = parse_base_stat_value(col_stats.max_value.as_ref()?, col_type)?;
            let null_fraction = if stats.row_count > 0 {
                col_stats
                    .null_count
                    .map(|count| count as f64 / stats.row_count as f64)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            Some((min, max, null_fraction))
        }
        ValueRef::ExtractYear(column) => {
            let (stats, offset) = ctx.column_stats(column)?;
            let col_stats = stats.column_statistics.get(offset)?;
            let col_type = &ctx.get_column_meta(column).data_type;
            let min = parse_year_stat_value(col_stats.min_value.as_ref()?, col_type)?;
            let max = parse_year_stat_value(col_stats.max_value.as_ref()?, col_type)?;
            let null_fraction = if stats.row_count > 0 {
                col_stats
                    .null_count
                    .map(|count| count as f64 / stats.row_count as f64)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            Some((min as f64, max as f64, null_fraction))
        }
        ValueRef::Derived(_) => None,
    }
}

fn parse_base_stat_value(value: &str, data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Int8 => value.parse::<i8>().ok().map(|value| value as f64),
        DataType::Int16 => value.parse::<i16>().ok().map(|value| value as f64),
        DataType::Int32 => value.parse::<i32>().ok().map(|value| value as f64),
        DataType::Int64 => value.parse::<i64>().ok().map(|value| value as f64),
        DataType::UInt8 => value.parse::<u8>().ok().map(|value| value as f64),
        DataType::UInt16 => value.parse::<u16>().ok().map(|value| value as f64),
        DataType::UInt32 => value.parse::<u32>().ok().map(|value| value as f64),
        DataType::UInt64 => value.parse::<u64>().ok().map(|value| value as f64),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => value.parse::<f64>().ok(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => value.parse::<f64>().ok(),
        DataType::Date32 => value.parse::<i32>().ok().map(|value| value as f64),
        DataType::Date64 => value.parse::<i64>().ok().map(|value| value as f64),
        _ => None,
    }
}

fn parse_year_stat_value(value: &str, data_type: &DataType) -> Option<i32> {
    match data_type {
        DataType::Date32 => value.parse::<i32>().ok().and_then(date_days_to_year),
        DataType::Date64 => value.parse::<i64>().ok().and_then(date_millis_to_year),
        _ => None,
    }
}

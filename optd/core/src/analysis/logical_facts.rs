use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{
    BinaryOp, Column, Expr, ExprData, JoinType, NaryOp, Operator, OperatorData, QueryContext,
    ScalarValue, TableRef,
};

use super::{
    Analysis, AnalysisContext, AnalysisError, AnalysisResult, Analyzable, CachedAnalysis,
    OperatorAnalysisState, typed_analysis,
};

/// Stable identity of a base-table value within one query.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BaseColumn {
    pub table: TableRef,
    pub name: String,
    pub source: Column,
}

/// Estimator-independent identity of a value flowing through a plan.
///
/// Derived values deliberately do not encode statistics transformations. Their defining expression
/// is retained separately in [`LogicalFacts::definitions`] so a later planner pass or statistics
/// provider can recognize it without changing value identity.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueId {
    Base(BaseColumn),
    Derived(Column),
}

/// Numeric bounds accumulated for one canonical value.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ValueRange {
    pub lower: Option<(f64, bool)>,
    pub upper: Option<(f64, bool)>,
}

impl ValueRange {
    fn tighten_lower(&mut self, value: f64, inclusive: bool) {
        if self.lower.is_none_or(|(current, current_inclusive)| {
            value > current || (value == current && current_inclusive && !inclusive)
        }) {
            self.lower = Some((value, inclusive));
        }
    }

    fn tighten_upper(&mut self, value: f64, inclusive: bool) {
        if self.upper.is_none_or(|(current, current_inclusive)| {
            value < current || (value == current && current_inclusive && !inclusive)
        }) {
            self.upper = Some((value, inclusive));
        }
    }

    pub fn is_empty(&self) -> bool {
        match (self.lower, self.upper) {
            (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) => {
                lower > upper || (lower == upper && !(lower_inclusive && upper_inclusive))
            }
            _ => false,
        }
    }
}

/// Sparse equivalence relation over values mentioned by equality predicates.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValueEquivalenceClasses {
    parents: BTreeMap<ValueId, ValueId>,
}

impl ValueEquivalenceClasses {
    pub fn canonical(&self, value: &ValueId) -> ValueId {
        let mut current = value;
        while let Some(parent) = self.parents.get(current) {
            if parent == current {
                break;
            }
            current = parent;
        }
        current.clone()
    }

    pub fn equivalent(&self, left: &ValueId, right: &ValueId) -> bool {
        self.canonical(left) == self.canonical(right)
    }

    fn insert(&mut self, value: ValueId) {
        self.parents.entry(value.clone()).or_insert(value);
    }

    fn union(&mut self, left: ValueId, right: ValueId) -> bool {
        self.insert(left.clone());
        self.insert(right.clone());
        let left_root = self.canonical(&left);
        let right_root = self.canonical(&right);
        if left_root == right_root {
            return false;
        }
        let (root, child) = if left_root <= right_root {
            (left_root, right_root)
        } else {
            (right_root, left_root)
        };
        self.parents.insert(child, root);
        true
    }

    pub fn classes(&self) -> Vec<BTreeSet<ValueId>> {
        let mut classes = BTreeMap::<ValueId, BTreeSet<ValueId>>::new();
        for value in self.parents.keys() {
            classes
                .entry(self.canonical(value))
                .or_default()
                .insert(value.clone());
        }
        classes
            .into_values()
            .filter(|class| class.len() >= 2)
            .collect()
    }
}

/// Logical predicate facts known to hold for every row emitted by an operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConstraintSet {
    pub equivalence_classes: ValueEquivalenceClasses,
    /// Canonical value classes proven non-null by ordinary comparisons.
    pub non_null_values: BTreeSet<ValueId>,
    pub literal_equalities: BTreeMap<ValueId, ScalarValue>,
    pub ranges: BTreeMap<ValueId, ValueRange>,
    pub residual_predicates: Vec<Expr>,
    pub contradictory: bool,
}

impl ConstraintSet {
    pub fn canonical(&self, value: &ValueId) -> ValueId {
        self.equivalence_classes.canonical(value)
    }

    fn union_values(&mut self, left: ValueId, right: ValueId) {
        if self.equivalence_classes.union(left, right) {
            self.normalize();
        }
    }

    fn normalize(&mut self) {
        self.non_null_values = std::mem::take(&mut self.non_null_values)
            .into_iter()
            .map(|value| self.canonical(&value))
            .collect();

        let mut literals = BTreeMap::<ValueId, ScalarValue>::new();
        for (value, literal) in std::mem::take(&mut self.literal_equalities) {
            let canonical = self.canonical(&value);
            if let Some(existing) = literals.get(&canonical)
                && existing != &literal
            {
                self.contradictory = true;
            } else {
                literals.insert(canonical, literal);
            }
        }
        self.literal_equalities = literals;

        let mut ranges = BTreeMap::<ValueId, ValueRange>::new();
        for (value, range) in std::mem::take(&mut self.ranges) {
            let canonical = self.canonical(&value);
            let target = ranges.entry(canonical).or_default();
            if let Some((bound, inclusive)) = range.lower {
                target.tighten_lower(bound, inclusive);
            }
            if let Some((bound, inclusive)) = range.upper {
                target.tighten_upper(bound, inclusive);
            }
            if target.is_empty() {
                self.contradictory = true;
            }
        }
        self.ranges = ranges;
        self.cross_check_literals_and_ranges();
    }

    fn cross_check_literals_and_ranges(&mut self) {
        for (value, literal) in &self.literal_equalities {
            let Some(number) = scalar_to_f64(literal) else {
                continue;
            };
            let Some(range) = self.ranges.get(value) else {
                continue;
            };
            let below = range.lower.is_some_and(|(bound, inclusive)| {
                number < bound || (number == bound && !inclusive)
            });
            let above = range.upper.is_some_and(|(bound, inclusive)| {
                number > bound || (number == bound && !inclusive)
            });
            if below || above {
                self.contradictory = true;
                return;
            }
        }
    }

    fn merge_from(&mut self, other: &Self) {
        self.contradictory |= other.contradictory;
        for class in other.equivalence_classes.classes() {
            let mut values = class.into_iter();
            let Some(first) = values.next() else {
                continue;
            };
            for value in values {
                self.union_values(first.clone(), value);
            }
        }
        for (value, literal) in &other.literal_equalities {
            self.add_literal(value.clone(), literal.clone());
        }
        for (value, range) in &other.ranges {
            let canonical = self.canonical(value);
            let target = self.ranges.entry(canonical).or_default();
            if let Some((bound, inclusive)) = range.lower {
                target.tighten_lower(bound, inclusive);
            }
            if let Some((bound, inclusive)) = range.upper {
                target.tighten_upper(bound, inclusive);
            }
        }
        for value in &other.non_null_values {
            self.non_null_values.insert(value.clone());
        }
        for predicate in &other.residual_predicates {
            if !self.residual_predicates.contains(predicate) {
                self.residual_predicates.push(*predicate);
            }
        }
        self.normalize();
    }

    fn add_literal(&mut self, value: ValueId, literal: ScalarValue) {
        if matches!(literal, ScalarValue::Null(_)) {
            // In a filter, ordinary equality with NULL evaluates to UNKNOWN and emits no rows.
            self.contradictory = true;
            return;
        }
        let canonical = self.canonical(&value);
        self.non_null_values.insert(canonical.clone());
        if let Some(existing) = self.literal_equalities.get(&canonical)
            && existing != &literal
        {
            self.contradictory = true;
        } else {
            self.literal_equalities.insert(canonical, literal);
        }
        self.cross_check_literals_and_ranges();
    }

    fn add_range(&mut self, value: ValueId, op: BinaryOp, literal: &ScalarValue) -> bool {
        let Some(number) = scalar_to_f64(literal) else {
            return false;
        };
        let canonical = self.canonical(&value);
        self.non_null_values.insert(canonical.clone());
        let range = self.ranges.entry(canonical).or_default();
        match op {
            BinaryOp::Lt => range.tighten_upper(number, false),
            BinaryOp::LtEq => range.tighten_upper(number, true),
            BinaryOp::Gt => range.tighten_lower(number, false),
            BinaryOp::GtEq => range.tighten_lower(number, true),
            _ => return false,
        }
        if range.is_empty() {
            self.contradictory = true;
        }
        self.cross_check_literals_and_ranges();
        true
    }
}

/// Lazily-derived logical value lineage and constraints for one operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogicalFacts {
    pub lineage: BTreeMap<Column, ValueId>,
    /// Opaque definitions for [`ValueId::Derived`] outputs. No statistics semantics are attached.
    pub definitions: BTreeMap<Column, Expr>,
    pub constraints: ConstraintSet,
}

impl LogicalFacts {
    pub fn value_for_column(&self, column: Column) -> Option<&ValueId> {
        self.lineage.get(&column)
    }

    pub fn canonical_value_for_column(&self, column: Column) -> Option<ValueId> {
        self.lineage
            .get(&column)
            .map(|value| self.constraints.canonical(value))
    }

    fn value_for_expr(&self, expr: Expr, ctx: &QueryContext) -> Option<ValueId> {
        match expr.get(ctx) {
            ExprData::ColumnRef(column) => self.lineage.get(column).cloned(),
            _ => None,
        }
    }

    pub(crate) fn apply_predicate(&mut self, predicate: Expr, ctx: &QueryContext) {
        match predicate.get(ctx) {
            ExprData::Literal(ScalarValue::Boolean(true)) => {}
            ExprData::Literal(ScalarValue::Boolean(false) | ScalarValue::Null(_)) => {
                self.constraints.contradictory = true;
            }
            ExprData::Nary {
                op: NaryOp::And,
                exprs,
            } => {
                for term in exprs {
                    self.apply_predicate(*term, ctx);
                }
            }
            ExprData::Binary { op, left, right } => {
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) && matches!(
                    (left.get(ctx), right.get(ctx)),
                    (ExprData::Literal(ScalarValue::Null(_)), _)
                        | (_, ExprData::Literal(ScalarValue::Null(_)))
                ) {
                    // Ordinary SQL comparisons with NULL evaluate to UNKNOWN and cannot pass a
                    // filter. Null-safe comparisons are handled separately.
                    self.constraints.contradictory = true;
                    return;
                }
                let left_value = self.value_for_expr(*left, ctx);
                let right_value = self.value_for_expr(*right, ctx);
                if matches!(op, BinaryOp::Eq | BinaryOp::IsNotDistinctFrom)
                    && let (Some(left_value), Some(right_value)) =
                        (left_value.clone(), right_value.clone())
                {
                    let ordinary_equality = *op == BinaryOp::Eq;
                    self.constraints
                        .union_values(left_value.clone(), right_value);
                    if ordinary_equality {
                        self.constraints
                            .non_null_values
                            .insert(self.constraints.canonical(&left_value));
                    }
                    return;
                }
                if *op == BinaryOp::Eq
                    && let Some((value, literal)) = value_literal(self, *left, *right, ctx)
                {
                    self.constraints.add_literal(value, literal.clone());
                    return;
                }
                if let Some((value, literal, normalized_op)) =
                    value_range(self, *op, *left, *right, ctx)
                    && self.constraints.add_range(value, normalized_op, literal)
                {
                    return;
                }
                if !self.constraints.residual_predicates.contains(&predicate) {
                    self.constraints.residual_predicates.push(predicate);
                }
            }
            _ if !self.constraints.residual_predicates.contains(&predicate) => {
                self.constraints.residual_predicates.push(predicate);
            }
            _ => {}
        }
    }
}

/// Demand-driven, query-local cache of [`LogicalFacts`].
#[derive(Default)]
pub struct LogicalFactsAnalysis {
    state: OperatorAnalysisState<Arc<LogicalFacts>>,
}

impl CachedAnalysis for LogicalFactsAnalysis {
    type Output = Arc<LogicalFacts>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        derive_logical_facts(ctx, analyses, operator).map(Arc::new)
    }
}

impl Analysis for LogicalFactsAnalysis {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for LogicalFactsAnalysis {
    type Value = LogicalFacts;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value> {
        Ok(Self::get_shared(ctx, analyses, op)?.as_ref().clone())
    }
}

impl LogicalFactsAnalysis {
    pub(crate) fn get_shared(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Arc<LogicalFacts>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

fn derive_logical_facts(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator: Operator,
) -> AnalysisResult<LogicalFacts> {
    let facts = match operator.get(ctx) {
        OperatorData::Scan(scan) => {
            let lineage = scan
                .columns
                .iter()
                .map(|column| {
                    (
                        *column,
                        ValueId::Base(BaseColumn {
                            table: scan.table.clone(),
                            name: ctx.column(*column).name.clone(),
                            source: *column,
                        }),
                    )
                })
                .collect();
            LogicalFacts {
                lineage,
                ..LogicalFacts::default()
            }
        }
        OperatorData::ConstScan(scan) => derived_columns(&scan.columns),
        OperatorData::TableFunction(function) => derived_columns(&function.columns),
        OperatorData::Selection(selection) => {
            let mut facts = LogicalFactsAnalysis::get_shared(ctx, analyses, selection.input)?
                .as_ref()
                .clone();
            facts.apply_predicate(selection.predicate, ctx);
            facts
        }
        OperatorData::Projection(projection) => {
            let input = LogicalFactsAnalysis::get_shared(ctx, analyses, projection.input)?;
            let lineage = projection
                .columns
                .iter()
                .filter_map(|column| {
                    input
                        .lineage
                        .get(column)
                        .cloned()
                        .map(|value| (*column, value))
                })
                .collect();
            let definitions = projection
                .columns
                .iter()
                .filter_map(|column| {
                    input
                        .definitions
                        .get(column)
                        .copied()
                        .map(|expr| (*column, expr))
                })
                .collect();
            LogicalFacts {
                lineage,
                definitions,
                constraints: input.constraints.clone(),
            }
        }
        OperatorData::Rename(rename) => {
            let input = LogicalFactsAnalysis::get_shared(ctx, analyses, rename.input)?;
            let mut facts = LogicalFacts {
                constraints: input.constraints.clone(),
                ..LogicalFacts::default()
            };
            for (renamed, original) in &rename.defs {
                if let Some(value) = input.lineage.get(original) {
                    facts.lineage.insert(*renamed, value.clone());
                }
                if let Some(definition) = input.definitions.get(original) {
                    facts.definitions.insert(*renamed, *definition);
                }
            }
            facts
        }
        OperatorData::Map(map) => {
            let mut facts = LogicalFactsAnalysis::get_shared(ctx, analyses, map.input)?
                .as_ref()
                .clone();
            for (column, expression) in &map.computations {
                if let Some(value) = facts.value_for_expr(*expression, ctx) {
                    facts.lineage.insert(*column, value);
                } else {
                    facts.lineage.insert(*column, ValueId::Derived(*column));
                    facts.definitions.insert(*column, *expression);
                }
            }
            facts
        }
        OperatorData::Aggregation(aggregation) => {
            let input = LogicalFactsAnalysis::get_shared(ctx, analyses, aggregation.input)?;
            // Aggregation changes row identity. Input predicates may describe rows that no longer
            // exist (and a scalar aggregate still emits one row for an empty input), so only value
            // lineage for grouping keys survives this boundary.
            let mut facts = LogicalFacts::default();
            for key in &aggregation.keys {
                let ExprData::ColumnRef(column) = key.get(ctx) else {
                    return Err(AnalysisError::UnsupportedAggregationKey {
                        operator,
                        expr: *key,
                    });
                };
                if let Some(value) = input.lineage.get(column) {
                    facts.lineage.insert(*column, value.clone());
                }
            }
            for (column, _) in &aggregation.aggregates {
                facts.lineage.insert(*column, ValueId::Derived(*column));
            }
            facts
        }
        OperatorData::Sort(sort) => LogicalFactsAnalysis::get_shared(ctx, analyses, sort.input)?
            .as_ref()
            .clone(),
        OperatorData::Limit(limit) => LogicalFactsAnalysis::get_shared(ctx, analyses, limit.input)?
            .as_ref()
            .clone(),
        OperatorData::Output(output) => {
            LogicalFactsAnalysis::get_shared(ctx, analyses, output.input)?
                .as_ref()
                .clone()
        }
        OperatorData::CrossProduct(product) => {
            let left = LogicalFactsAnalysis::get_shared(ctx, analyses, product.outer)?;
            let right = LogicalFactsAnalysis::get_shared(ctx, analyses, product.inner)?;
            merge_inputs(&left, &right, true)
        }
        OperatorData::Join(join) => {
            let left = LogicalFactsAnalysis::get_shared(ctx, analyses, join.outer)?;
            let right = LogicalFactsAnalysis::get_shared(ctx, analyses, join.inner)?;
            match join.join_type {
                JoinType::Inner => {
                    let mut facts = merge_inputs(&left, &right, true);
                    facts.apply_predicate(join.on, ctx);
                    facts
                }
                JoinType::LeftSemi | JoinType::LeftAnti => left.as_ref().clone(),
                JoinType::LeftMark { marker, .. } => {
                    let mut facts = left.as_ref().clone();
                    facts.lineage.insert(marker, ValueId::Derived(marker));
                    facts
                }
                JoinType::LeftOuter | JoinType::Single => merge_inputs(&left, &right, false),
                JoinType::RightOuter => {
                    let mut facts = merge_inputs(&left, &right, false);
                    facts.constraints = right.constraints.clone();
                    facts
                }
                JoinType::FullOuter => {
                    let mut facts = merge_inputs(&left, &right, false);
                    facts.constraints = ConstraintSet::default();
                    facts
                }
            }
        }
    };
    Ok(facts)
}

fn derived_columns(columns: &[Column]) -> LogicalFacts {
    LogicalFacts {
        lineage: columns
            .iter()
            .map(|column| (*column, ValueId::Derived(*column)))
            .collect(),
        ..LogicalFacts::default()
    }
}

fn merge_inputs(
    left: &LogicalFacts,
    right: &LogicalFacts,
    merge_constraints: bool,
) -> LogicalFacts {
    let mut facts = left.clone();
    facts.lineage.extend(right.lineage.clone());
    facts.definitions.extend(right.definitions.clone());
    if merge_constraints {
        facts.constraints.merge_from(&right.constraints);
    }
    facts
}

fn value_literal<'a>(
    facts: &LogicalFacts,
    left: Expr,
    right: Expr,
    ctx: &'a QueryContext,
) -> Option<(ValueId, &'a ScalarValue)> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(literal))
        | (ExprData::Literal(literal), ExprData::ColumnRef(column)) => facts
            .lineage
            .get(column)
            .cloned()
            .map(|value| (value, literal)),
        _ => None,
    }
}

fn value_range<'a>(
    facts: &LogicalFacts,
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &'a QueryContext,
) -> Option<(ValueId, &'a ScalarValue, BinaryOp)> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(literal)) => facts
            .lineage
            .get(column)
            .cloned()
            .map(|value| (value, literal, op)),
        (ExprData::Literal(literal), ExprData::ColumnRef(column)) => {
            let reversed = match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                _ => return None,
            };
            facts
                .lineage
                .get(column)
                .cloned()
                .map(|value| (value, literal, reversed))
        }
        _ => None,
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    const MAX_EXACT_INTEGER: u64 = 1_u64 << f64::MANTISSA_DIGITS;
    match value {
        ScalarValue::Int32(value) => Some(f64::from(*value)),
        ScalarValue::Int64(value) if value.unsigned_abs() <= MAX_EXACT_INTEGER => {
            Some(*value as f64)
        }
        ScalarValue::Float64(value) if value.is_finite() => Some(*value),
        // Decimal-to-binary conversion is not generally lossless. Until ranges retain typed
        // bounds, decline to derive proofs from decimals rather than risking a false contradiction.
        ScalarValue::Decimal128 { .. } => None,
        ScalarValue::Date32(value) => Some(f64::from(*value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;
    use crate::{ColumnData, Join, Map, OperatorData, Scan, Selection, test_analyses};

    fn column_ref(ctx: &mut QueryContext, column: Column) -> Expr {
        ExprData::ColumnRef(column).add(ctx)
    }

    fn literal(ctx: &mut QueryContext, value: i64) -> Expr {
        ExprData::Literal(ScalarValue::Int64(value)).add(ctx)
    }

    fn binary(ctx: &mut QueryContext, op: BinaryOp, left: Expr, right: Expr) -> Expr {
        ExprData::Binary { op, left, right }.add(ctx)
    }

    #[test]
    fn lineage_canonicalizes_constraints_and_detects_contradictions() {
        let mut ctx = QueryContext::new();
        let x = ColumnData::new("x", DataType::Int64).add(&mut ctx);
        let y = ColumnData::new("y", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![x, y],
        })
        .add(&mut ctx);
        let x_ref = column_ref(&mut ctx, x);
        let y_ref = column_ref(&mut ctx, y);
        let equality = binary(&mut ctx, BinaryOp::Eq, x_ref, y_ref);
        let equal = OperatorData::Selection(Selection {
            predicate: equality,
            input: scan,
        })
        .add(&mut ctx);
        let one = literal(&mut ctx, 1);
        let x_eq_one = binary(&mut ctx, BinaryOp::Eq, x_ref, one);
        let first = OperatorData::Selection(Selection {
            predicate: x_eq_one,
            input: equal,
        })
        .add(&mut ctx);
        let two = literal(&mut ctx, 2);
        let y_eq_two = binary(&mut ctx, BinaryOp::Eq, y_ref, two);
        let contradictory = OperatorData::Selection(Selection {
            predicate: y_eq_two,
            input: first,
        })
        .add(&mut ctx);
        ctx.set_root(contradictory);

        let mut analyses = test_analyses(&ctx);
        let facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, contradictory)
            .expect("logical facts should derive");
        assert!(facts.constraints.contradictory);
        assert!(facts.constraints.equivalence_classes.equivalent(
            facts.value_for_column(x).expect("x lineage"),
            facts.value_for_column(y).expect("y lineage"),
        ));
        let profile = analyses
            .get::<crate::CardinalityEstimationV1>(&ctx, contradictory)
            .expect("cardinality should consume contradiction facts");
        assert_eq!(profile.rows.value, 0.0);
    }

    #[test]
    fn ordinary_equality_strengthens_null_safe_equivalence_with_non_nullness() {
        let mut ctx = QueryContext::new();
        let x = ColumnData::new("x", DataType::Int64).add(&mut ctx);
        let y = ColumnData::new("y", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![x, y],
        })
        .add(&mut ctx);
        let x_ref = column_ref(&mut ctx, x);
        let y_ref = column_ref(&mut ctx, y);
        let null_safe = binary(&mut ctx, BinaryOp::IsNotDistinctFrom, x_ref, y_ref);
        let first = OperatorData::Selection(Selection {
            predicate: null_safe,
            input: scan,
        })
        .add(&mut ctx);
        let ordinary = binary(&mut ctx, BinaryOp::Eq, x_ref, y_ref);
        let second = OperatorData::Selection(Selection {
            predicate: ordinary,
            input: first,
        })
        .add(&mut ctx);

        let mut analyses = test_analyses(&ctx);
        let null_safe_facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, first)
            .expect("logical facts should derive");
        assert!(null_safe_facts.constraints.non_null_values.is_empty());
        let ordinary_facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, second)
            .expect("logical facts should derive");
        let canonical = ordinary_facts
            .canonical_value_for_column(x)
            .expect("x lineage");
        assert!(
            ordinary_facts
                .constraints
                .non_null_values
                .contains(&canonical)
        );
    }

    #[test]
    fn large_integer_rounding_cannot_prove_a_false_contradiction() {
        let mut ctx = QueryContext::new();
        let x = ColumnData::new("x", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![x],
        })
        .add(&mut ctx);
        let x_ref = column_ref(&mut ctx, x);
        let exact = literal(&mut ctx, 9_007_199_254_740_993);
        let equality = binary(&mut ctx, BinaryOp::Eq, x_ref, exact);
        let equal = OperatorData::Selection(Selection {
            predicate: equality,
            input: scan,
        })
        .add(&mut ctx);
        let lower = literal(&mut ctx, 9_007_199_254_740_992);
        let greater = binary(&mut ctx, BinaryOp::Gt, x_ref, lower);
        let filtered = OperatorData::Selection(Selection {
            predicate: greater,
            input: equal,
        })
        .add(&mut ctx);
        ctx.set_root(filtered);

        let mut analyses = test_analyses(&ctx);
        let facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, filtered)
            .expect("logical facts should derive");
        assert!(!facts.constraints.contradictory);
    }

    #[test]
    fn map_keeps_direct_aliases_but_treats_computations_as_opaque() {
        let mut ctx = QueryContext::new();
        let source = ColumnData::new("source", DataType::Int64).add(&mut ctx);
        let alias = ColumnData::new("alias", DataType::Int64).add(&mut ctx);
        let shifted = ColumnData::new("shifted", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![source],
        })
        .add(&mut ctx);
        let source_ref = column_ref(&mut ctx, source);
        let one = literal(&mut ctx, 1);
        let shifted_expr = binary(&mut ctx, BinaryOp::Add, source_ref, one);
        let map = OperatorData::Map(Map {
            computations: vec![(alias, source_ref), (shifted, shifted_expr)],
            input: scan,
        })
        .add(&mut ctx);
        ctx.set_root(map);

        let mut analyses = test_analyses(&ctx);
        let facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, map)
            .expect("logical facts should derive");
        assert_eq!(
            facts.value_for_column(source),
            facts.value_for_column(alias)
        );
        assert_eq!(
            facts.value_for_column(shifted),
            Some(&ValueId::Derived(shifted))
        );
        assert_eq!(facts.definitions.get(&shifted), Some(&shifted_expr));
    }

    #[test]
    fn outer_join_does_not_claim_on_predicate_equivalence() {
        let mut ctx = QueryContext::new();
        let left_column = ColumnData::new("left_id", DataType::Int64).add(&mut ctx);
        let right_column = ColumnData::new("right_id", DataType::Int64).add(&mut ctx);
        let left = OperatorData::Scan(Scan {
            table: TableRef::bare("left_table"),
            columns: vec![left_column],
        })
        .add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("right_table"),
            columns: vec![right_column],
        })
        .add(&mut ctx);
        let left_ref = column_ref(&mut ctx, left_column);
        let right_ref = column_ref(&mut ctx, right_column);
        let on = binary(&mut ctx, BinaryOp::Eq, left_ref, right_ref);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        ctx.set_root(join);

        let mut analyses = test_analyses(&ctx);
        let facts = analyses
            .get::<LogicalFactsAnalysis>(&ctx, join)
            .expect("logical facts should derive");
        assert!(!facts.constraints.equivalence_classes.equivalent(
            facts.value_for_column(left_column).expect("left lineage"),
            facts.value_for_column(right_column).expect("right lineage"),
        ));
    }
}

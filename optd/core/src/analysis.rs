use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::{
    AggregateExpr, AggregateFunction, BinaryOp, Catalog, Column, ColumnStatistics, Expr, ExprData,
    HypergraphOf, JoinType, NaryOp, NodeSet, Operator, OperatorData, QueryContext, QueryHypergraph,
    Relation, ScalarValue, Scan, TableStatistics, UnaryOp,
};

/// Result type used by query analyses.
pub type AnalysisResult<T> = Result<T, AnalysisError>;

/// Error produced while running query analyses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisError {
    /// The current IR cannot expose an expression aggregation key as an output column.
    UnsupportedAggregationKey { operator: Operator, expr: Expr },
    /// The operator graph contained a cycle for the requested analysis.
    CyclicDependency {
        analysis: &'static str,
        operator: Operator,
    },
    /// A registered analysis had an unexpected concrete type.
    AnalysisTypeMismatch(&'static str),
}

impl fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAggregationKey { operator, expr } => write!(
                f,
                "aggregation {operator:?} has non-column key expression {expr:?}"
            ),
            Self::CyclicDependency { analysis, operator } => {
                write!(f, "cyclic dependency for {analysis} at {operator:?}")
            }
            Self::AnalysisTypeMismatch(analysis) => {
                write!(f, "registered analysis had the wrong type for {analysis}")
            }
        }
    }
}

impl std::error::Error for AnalysisError {}

/// Umbrella trait for all analyses. Owns the query interface.
///
/// Each analysis decides its own computation strategy and caching.
/// Bottom-up analyses recurse into inputs via [`AnalysisContext::get`].
/// Top-down analyses use [`AnalysisContext::get::<ParentsOf>`] to walk from root.
pub trait Analyzable: 'static {
    /// The value produced for one operator.
    type Value: Clone;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value>;
}

/// Object-safe base trait for analysis instances stored in an [`AnalysisContext`].
pub trait Analysis: Any {
    /// Returns this analysis as [`Any`] for typed lookup.
    fn as_any(&self) -> &dyn Any;

    /// Clears analysis-owned cache state.
    fn clear(&self);
}

/// Registry of lazily-created analysis instances.
pub type AnalysisRegistry = HashMap<TypeId, Rc<dyn Analysis>>;

/// Analysis that computes and caches one output value per operator.
/// Internal trait for analyses that cache results per operator handle.
/// Used by the existing bottom-up analyses.
trait CachedAnalysis: Analysis {
    type Output: Clone + 'static;

    fn state(&self) -> &OperatorAnalysisState<Self::Output>;

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output>;

    fn get_cached(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output>
    where
        Self: Sized,
    {
        if let Some(value) = self.state().values.borrow().get(&operator) {
            return Ok(value.clone());
        }

        if !self.state().in_progress.borrow_mut().insert(operator) {
            return Err(AnalysisError::CyclicDependency {
                analysis: type_name::<Self>(),
                operator,
            });
        }

        let result = self.compute(ctx, analyses, operator);
        self.state().in_progress.borrow_mut().remove(&operator);

        let value = result?;
        self.state()
            .values
            .borrow_mut()
            .insert(operator, value.clone());
        Ok(value)
    }

    fn clear_cache(&self) {
        self.state().clear();
    }
}

/// Cache state for an operator analysis.
pub struct OperatorAnalysisState<T> {
    values: RefCell<HashMap<Operator, T>>,
    in_progress: RefCell<HashSet<Operator>>,
}

impl<T> Default for OperatorAnalysisState<T> {
    fn default() -> Self {
        Self {
            values: RefCell::new(HashMap::new()),
            in_progress: RefCell::new(HashSet::new()),
        }
    }
}

impl<T> OperatorAnalysisState<T> {
    /// Clears cached values and in-progress markers.
    pub fn clear(&self) {
        self.values.borrow_mut().clear();
        self.in_progress.borrow_mut().clear();
    }
}

/// Provenance for a cardinality or distinct-value estimate.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateSource {
    Exact,
    Catalog,
    Derived,
    Mock,
    Default,
}

/// A point estimate plus optional conservative bounds.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    pub value: f64,
    pub lower: Option<f64>,
    pub upper: Option<f64>,
    pub source: EstimateSource,
}

impl Estimate {
    pub fn exact(value: f64) -> Self {
        Self {
            value,
            lower: Some(value),
            upper: Some(value),
            source: EstimateSource::Exact,
        }
    }

    pub fn derived(value: f64, lower: Option<f64>, upper: Option<f64>) -> Self {
        Self {
            value: clamp_to_bounds(value, lower, upper),
            lower,
            upper,
            source: EstimateSource::Derived,
        }
    }

    pub fn catalog(value: f64) -> Self {
        Self {
            value,
            lower: Some(value),
            upper: Some(value),
            source: EstimateSource::Catalog,
        }
    }

    pub fn mock(value: f64) -> Self {
        Self {
            value,
            lower: Some(value),
            upper: Some(value),
            source: EstimateSource::Mock,
        }
    }

    pub fn default(value: f64) -> Self {
        Self {
            value,
            lower: Some(0.0),
            upper: None,
            source: EstimateSource::Default,
        }
    }

    pub fn scale(&self, factor: f64, source: EstimateSource) -> Self {
        Self {
            value: (self.value * factor).max(0.0),
            lower: self.lower.map(|v| (v * factor).max(0.0)),
            upper: self.upper.map(|v| (v * factor).max(0.0)),
            source,
        }
    }

    pub fn cap(&self, cap: f64, source: EstimateSource) -> Self {
        Self {
            value: self.value.min(cap).max(0.0),
            lower: self.lower.map(|v| v.min(cap).max(0.0)),
            upper: Some(self.upper.map_or(cap, |v| v.min(cap)).max(0.0)),
            source,
        }
    }

    fn max_by_value(&self, other: Self) -> Self {
        if self.value >= other.value {
            self.clone()
        } else {
            other
        }
    }
}

impl std::fmt::Display for Estimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 1. Print the core value
        write!(f, "{}", self.value)?;

        // 2. Conditionally print the bounds using '?' for None
        match (self.lower, self.upper) {
            (Some(l), Some(u)) => write!(f, " [{}, {}]", l, u)?,
            (Some(l), None) => write!(f, " [{}, ?]", l)?,
            (None, Some(u)) => write!(f, " [?, {}]", u)?,
            (None, None) => {} // Do nothing if there are no bounds
        }

        // 3. Print the source tag
        write!(f, " ({:?})", self.source)
    }
}

/// Per-column cardinality profile.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProfile {
    pub lower_bound: Option<ScalarValue>,
    pub upper_bound: Option<ScalarValue>,
    pub frequency: Estimate,
    pub distinct: Estimate,
}

impl ColumnProfile {
    pub fn unknown(rows: &Estimate) -> Self {
        Self {
            lower_bound: None,
            upper_bound: None,
            frequency: rows.clone(),
            distinct: Estimate::derived(rows.value.min(100.0), Some(0.0), rows.upper),
        }
    }

    fn scale_frequency(&self, factor: f64, rows: &Estimate) -> Self {
        let mut profile = self.clone();
        profile.frequency = profile.frequency.scale(factor, EstimateSource::Derived);
        profile.frequency = profile.frequency.cap(rows.value, EstimateSource::Derived);
        profile.distinct = profile
            .distinct
            .cap(profile.frequency.value, EstimateSource::Derived);
        profile
    }

    fn cap_by_rows(&self, rows: &Estimate) -> Self {
        let mut profile = self.clone();
        profile.frequency = profile.frequency.cap(rows.value, EstimateSource::Derived);
        profile.distinct = profile
            .distinct
            .cap(profile.frequency.value, EstimateSource::Derived);
        profile
    }
}

/// Estimated row and column profiles for an operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct CardinalityProfile {
    pub rows: Estimate,
    pub columns: BTreeMap<Column, ColumnProfile>,
    pub equivalence_classes: Vec<ColumnEquivalenceClass>,
}

/// Columns known equal in a derived profile, with the best known class NDV.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnEquivalenceClass {
    pub columns: BTreeSet<Column>,
    pub distinct: Estimate,
}

impl CardinalityProfile {
    pub fn unknown_for_columns(row_count: f64, columns: impl IntoIterator<Item = Column>) -> Self {
        let rows = Estimate::default(row_count);
        let columns = columns
            .into_iter()
            .map(|column| (column, ColumnProfile::unknown(&rows)))
            .collect::<BTreeMap<_, _>>();
        Self::new(rows, columns)
    }

    pub fn new(rows: Estimate, columns: BTreeMap<Column, ColumnProfile>) -> Self {
        let equivalence_classes = singleton_equivalence_classes(&columns);
        Self {
            rows,
            columns,
            equivalence_classes,
        }
    }

    fn cap_by_rows(&self, rows: Estimate) -> Self {
        let columns = self
            .columns
            .iter()
            .map(|(&column, profile)| (column, profile.cap_by_rows(&rows)))
            .collect();
        let equivalence_classes = filter_equivalence_classes(&self.equivalence_classes, &columns);
        Self {
            rows,
            columns,
            equivalence_classes,
        }
    }
}

fn singleton_equivalence_classes(
    columns: &BTreeMap<Column, ColumnProfile>,
) -> Vec<ColumnEquivalenceClass> {
    columns
        .iter()
        .map(|(&column, profile)| ColumnEquivalenceClass {
            columns: BTreeSet::from([column]),
            distinct: profile.distinct.clone(),
        })
        .collect()
}

fn filter_equivalence_classes(
    classes: &[ColumnEquivalenceClass],
    columns: &BTreeMap<Column, ColumnProfile>,
) -> Vec<ColumnEquivalenceClass> {
    let mut filtered = Vec::new();
    for class in classes {
        let kept = class
            .columns
            .iter()
            .copied()
            .filter(|column| columns.contains_key(column))
            .collect::<BTreeSet<_>>();
        if kept.is_empty() {
            continue;
        }
        let distinct = kept
            .iter()
            .filter_map(|column| columns.get(column))
            .map(|profile| profile.distinct.clone())
            .max_by(|a, b| a.value.total_cmp(&b.value))
            .unwrap_or_else(|| class.distinct.clone());
        filtered.push(ColumnEquivalenceClass {
            columns: kept,
            distinct,
        });
    }
    filtered
}

fn rename_equivalence_classes(
    classes: &[ColumnEquivalenceClass],
    rename_map: &BTreeMap<Column, Column>,
) -> Vec<ColumnEquivalenceClass> {
    classes
        .iter()
        .filter_map(|class| {
            let columns = class
                .columns
                .iter()
                .filter_map(|column| rename_map.get(column).copied())
                .collect::<BTreeSet<_>>();
            (!columns.is_empty()).then(|| ColumnEquivalenceClass {
                columns,
                distinct: class.distinct.clone(),
            })
        })
        .collect()
}

fn merge_equivalence_class_lists(
    left: &[ColumnEquivalenceClass],
    right: &[ColumnEquivalenceClass],
) -> Vec<ColumnEquivalenceClass> {
    let mut merged = left.to_vec();
    merged.extend_from_slice(right);
    merged
}

/// Input profiles supplied when estimating a virtual join-ordering DP state.
pub struct JoinInputProfiles<'a> {
    pub left: &'a CardinalityProfile,
    pub right: &'a CardinalityProfile,
}

/// Cardinality and column-profile analysis for one operator.
#[derive(Default)]
pub struct CardinalityEstimationV1 {
    state: OperatorAnalysisState<CardinalityProfile>,
}

/// Registry of lazily-created analysis instances.
#[derive(Default)]
pub struct AnalysisContext {
    pub analyses: AnalysisRegistry,
    pub catalog: Option<Arc<dyn Catalog>>,
}

impl AnalysisContext {
    /// Creates an empty analysis context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an analysis context that can consult `catalog` during analysis.
    pub fn with_catalog(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog: Some(catalog),
        }
    }

    /// Replaces the catalog used by catalog-aware analyses.
    pub fn set_catalog(&mut self, catalog: Option<Arc<dyn Catalog>>) {
        self.catalog = catalog;
        self.clear();
    }

    /// Clears every registered analysis cache while preserving analysis instances.
    pub fn clear(&self) {
        for analysis in self.analyses.values() {
            analysis.clear();
        }
    }

    /// Returns analysis output for `operator`.
    pub fn get<A: Analyzable>(
        &mut self,
        ctx: &QueryContext,
        op: Operator,
    ) -> AnalysisResult<A::Value> {
        A::get(ctx, self, op)
    }

    pub(crate) fn registry_entry<A>(&mut self) -> Rc<dyn Analysis>
    where
        A: Analysis + Default + 'static,
    {
        let id = TypeId::of::<A>();
        self.analyses
            .entry(id)
            .or_insert_with(|| Rc::new(A::default()) as Rc<dyn Analysis>)
            .clone()
    }
}

fn typed_analysis<A>(analysis: &Rc<dyn Analysis>) -> AnalysisResult<&A>
where
    A: Analysis + 'static,
{
    analysis
        .as_any()
        .downcast_ref::<A>()
        .ok_or(AnalysisError::AnalysisTypeMismatch(type_name::<A>()))
}

/// Returns columns referenced by an expression.
pub fn expr_used_columns(ctx: &QueryContext, expr: Expr) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();
    let mut analyses = AnalysisContext::new();
    collect_expr_used_columns(ctx, &mut analyses, expr, &mut columns)?;
    Ok(columns)
}

fn collect_expr_used_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    expr: Expr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match expr.get(ctx) {
        ExprData::Literal(_) => {}
        ExprData::ColumnRef(column) => push_unique_column(columns, *column),
        ExprData::Unary { expr, .. } => collect_expr_used_columns(ctx, analyses, *expr, columns)?,
        ExprData::Binary { left, right, .. } => {
            collect_expr_used_columns(ctx, analyses, *left, columns)?;
            collect_expr_used_columns(ctx, analyses, *right, columns)?;
        }
        ExprData::Nary { exprs, .. } => {
            for expr in exprs {
                collect_expr_used_columns(ctx, analyses, *expr, columns)?;
            }
        }
        ExprData::Cast { expr, .. } => collect_expr_used_columns(ctx, analyses, *expr, columns)?,
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                collect_expr_used_columns(ctx, analyses, *when, columns)?;
                collect_expr_used_columns(ctx, analyses, *then, columns)?;
            }
            if let Some(else_expr) = else_expr {
                collect_expr_used_columns(ctx, analyses, *else_expr, columns)?;
            }
        }
        ExprData::ScalarFunction { args, .. } => {
            for arg in args {
                collect_expr_used_columns(ctx, analyses, *arg, columns)?;
            }
        }
        ExprData::Exists { subquery, .. } | ExprData::ScalarSubquery { subquery } => {
            collect_subquery_free_columns(ctx, analyses, *subquery, columns)?;
        }
        ExprData::InSubquery { expr, subquery, .. } => {
            collect_expr_used_columns(ctx, analyses, *expr, columns)?;
            collect_subquery_free_columns(ctx, analyses, *subquery, columns)?;
        }
        ExprData::Like { expr, pattern, .. } => {
            collect_expr_used_columns(ctx, analyses, *expr, columns)?;
            collect_expr_used_columns(ctx, analyses, *pattern, columns)?;
        }
    }

    Ok(())
}

fn collect_subquery_free_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator: Operator,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    extend_unique_columns(columns, analyses.get::<FreeColumns>(ctx, operator)?);
    Ok(())
}

fn collect_aggregate_expr_used_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    aggregate: &AggregateExpr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match aggregate {
        AggregateExpr::CountStar => Ok(()),
        AggregateExpr::Func { arg, .. } => collect_expr_used_columns(ctx, analyses, *arg, columns),
    }
}

fn push_unique_column(columns: &mut Vec<Column>, column: Column) {
    if !columns.contains(&column) {
        columns.push(column);
    }
}

fn extend_unique_columns(columns: &mut Vec<Column>, incoming: impl IntoIterator<Item = Column>) {
    for column in incoming {
        push_unique_column(columns, column);
    }
}

fn directly_created_columns(operator: &OperatorData) -> Vec<Column> {
    match operator {
        OperatorData::Scan(operator) => operator.columns.clone(),
        OperatorData::TableFunction(operator) => operator.columns.clone(),
        OperatorData::Map(operator) => operator
            .computations
            .iter()
            .map(|(column, _)| *column)
            .collect(),
        OperatorData::Aggregation(operator) => operator
            .aggregates
            .iter()
            .map(|(column, _)| *column)
            .collect(),
        OperatorData::Join(operator) => match operator.join_type {
            JoinType::LeftMark { marker: column, .. } => vec![column],
            _ => Vec::new(),
        },
        OperatorData::Selection(_)
        | OperatorData::CrossProduct(_)
        | OperatorData::Sort(_)
        | OperatorData::Limit(_)
        | OperatorData::Projection(_)
        | OperatorData::Output(_) => Vec::new(),
        OperatorData::ConstScan(operator) => operator.columns.clone(),
        OperatorData::Rename(r) => r.defs.iter().map(|(renamed, _)| *renamed).collect(),
    }
}

/// Returns columns referenced directly by an operator.
fn directly_used_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator_data: &OperatorData,
) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();

    match operator_data {
        OperatorData::Scan(_)
        | OperatorData::CrossProduct(_)
        | OperatorData::Limit(_)
        | OperatorData::ConstScan(_) => {}
        OperatorData::Sort(data) => {
            for key in &data.keys {
                collect_expr_used_columns(ctx, analyses, key.expr, &mut columns)?;
            }
        }
        OperatorData::Selection(data) => {
            collect_expr_used_columns(ctx, analyses, data.predicate, &mut columns)?;
        }
        OperatorData::Map(data) => {
            for (_, expr) in &data.computations {
                collect_expr_used_columns(ctx, analyses, *expr, &mut columns)?;
            }
        }
        OperatorData::TableFunction(data) => {
            for arg in &data.args {
                collect_expr_used_columns(ctx, analyses, *arg, &mut columns)?;
            }
        }
        OperatorData::Join(data) => {
            collect_expr_used_columns(ctx, analyses, data.on, &mut columns)?;
        }
        OperatorData::Aggregation(data) => {
            for key in &data.keys {
                collect_expr_used_columns(ctx, analyses, *key, &mut columns)?;
            }
            for (_, aggregate) in &data.aggregates {
                collect_aggregate_expr_used_columns(ctx, analyses, aggregate, &mut columns)?;
            }
        }
        OperatorData::Projection(data) => {
            for column in &data.columns {
                push_unique_column(&mut columns, *column);
            }
        }
        OperatorData::Output(data) => {
            columns.extend(analyses.get::<AvailableColumns>(ctx, data.input)?);
        }
        OperatorData::Rename(_) => {} // no expressions — column mapping only
    }

    Ok(columns)
}

fn input_available_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator_data: &OperatorData,
) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();

    match operator_data {
        OperatorData::Scan(_) | OperatorData::TableFunction(_) | OperatorData::ConstScan(_) => {}
        OperatorData::Selection(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Map(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Aggregation(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Projection(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Sort(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Limit(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Output(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.input)?,
            );
        }
        OperatorData::Join(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.outer)?,
            );
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.inner)?,
            );
        }
        OperatorData::CrossProduct(data) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.outer)?,
            );
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, data.inner)?,
            );
        }
        OperatorData::Rename(r) => {
            extend_unique_columns(
                &mut columns,
                analyses.get::<AvailableColumns>(ctx, r.input)?,
            );
        }
    }

    Ok(columns)
}

fn free_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator: Operator,
) -> AnalysisResult<Vec<Column>> {
    let operator_data = operator.get(ctx);
    let input_columns = input_available_columns(ctx, analyses, operator_data)?;
    let mut columns = Vec::new();

    // Columns used directly by this operator but not available from its inputs.
    for column in analyses.get::<UsedColumns>(ctx, operator)? {
        if !input_columns.contains(&column) {
            push_unique_column(&mut columns, column);
        }
    }

    // Bubble up free columns from inputs that are also not available here.
    for input in operator_data.inputs() {
        for column in analyses.get::<FreeColumns>(ctx, input)? {
            if !input_columns.contains(&column) {
                push_unique_column(&mut columns, column);
            }
        }
    }

    Ok(columns)
}

fn push_unique_nullability(columns: &mut Vec<(Column, bool)>, column: Column, nullable: bool) {
    if !columns.iter().any(|(existing, _)| *existing == column) {
        columns.push((column, nullable));
    }
}

fn lookup_nullability(nullability: &[(Column, bool)], column: Column) -> Option<bool> {
    nullability
        .iter()
        .find_map(|(candidate, nullable)| (*candidate == column).then_some(*nullable))
}

fn mark_non_null(columns: &mut [(Column, bool)], column: Column) {
    if let Some((_, nullable)) = columns
        .iter_mut()
        .find(|(candidate, _)| *candidate == column)
    {
        *nullable = false;
    }
}

fn expr_nullability(
    ctx: &QueryContext,
    input_nullability: &[(Column, bool)],
    expr: Expr,
) -> AnalysisResult<bool> {
    match expr.get(ctx) {
        ExprData::Literal(value) => Ok(matches!(value, crate::ScalarValue::Null(_))),
        ExprData::ColumnRef(column) => {
            Ok(lookup_nullability(input_nullability, *column).unwrap_or(true))
        }
        ExprData::Unary { op, expr } => match op {
            crate::UnaryOp::IsNull | crate::UnaryOp::IsNotNull => Ok(false),
            crate::UnaryOp::Not | crate::UnaryOp::Negate => {
                expr_nullability(ctx, input_nullability, *expr)
            }
        },
        ExprData::Binary { op, left, right } => match op {
            crate::BinaryOp::IsNotDistinctFrom => Ok(false),
            _ => Ok(expr_nullability(ctx, input_nullability, *left)?
                || expr_nullability(ctx, input_nullability, *right)?),
        },
        ExprData::Nary { exprs, .. } => {
            for expr in exprs {
                if expr_nullability(ctx, input_nullability, *expr)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ExprData::Cast { expr, .. } => expr_nullability(ctx, input_nullability, *expr),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (_, then) in when_then {
                if expr_nullability(ctx, input_nullability, *then)? {
                    return Ok(true);
                }
            }
            if let Some(else_expr) = else_expr {
                expr_nullability(ctx, input_nullability, *else_expr)
            } else {
                Ok(true)
            }
        }
        ExprData::ScalarFunction { .. } | ExprData::ScalarSubquery { .. } => Ok(true),
        ExprData::Exists { .. } | ExprData::InSubquery { .. } => Ok(false),
        ExprData::Like { .. } => Ok(false), // LIKE returns boolean, never null
    }
}

fn collect_non_null_columns_from_predicate(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    expr: Expr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match expr.get(ctx) {
        ExprData::Literal(_)
        | ExprData::ColumnRef(_)
        | ExprData::Cast { .. }
        | ExprData::CaseWhen { .. }
        | ExprData::ScalarFunction { .. }
        | ExprData::Like { .. }
        | ExprData::Exists { .. }
        | ExprData::InSubquery { .. }
        | ExprData::ScalarSubquery { .. } => {}
        ExprData::Unary { op, expr } => match op {
            crate::UnaryOp::IsNotNull => {
                collect_expr_used_columns(ctx, analyses, *expr, columns)?;
            }
            crate::UnaryOp::Not => {
                if let ExprData::Unary {
                    op: crate::UnaryOp::IsNull,
                    expr,
                } = expr.get(ctx)
                {
                    collect_expr_used_columns(ctx, analyses, *expr, columns)?;
                }
            }
            crate::UnaryOp::IsNull | crate::UnaryOp::Negate => {}
        },
        ExprData::Binary { op, left, right } => match op {
            crate::BinaryOp::Eq
            | crate::BinaryOp::IsNotDistinctFrom
            | crate::BinaryOp::NotEq
            | crate::BinaryOp::Lt
            | crate::BinaryOp::LtEq
            | crate::BinaryOp::Gt
            | crate::BinaryOp::GtEq => {
                collect_expr_used_columns(ctx, analyses, *left, columns)?;
                collect_expr_used_columns(ctx, analyses, *right, columns)?;
            }
            crate::BinaryOp::Add
            | crate::BinaryOp::Subtract
            | crate::BinaryOp::Multiply
            | crate::BinaryOp::Divide => {}
        },
        ExprData::Nary { op, exprs } => match op {
            crate::NaryOp::And => {
                for expr in exprs {
                    collect_non_null_columns_from_predicate(ctx, analyses, *expr, columns)?;
                }
            }
            crate::NaryOp::Or => {
                let mut iter = exprs.iter();
                let Some(first) = iter.next() else {
                    return Ok(());
                };

                let mut intersection = non_null_columns_from_predicate(ctx, analyses, *first)?;
                for expr in iter {
                    let branch = non_null_columns_from_predicate(ctx, analyses, *expr)?;
                    intersection.retain(|column| branch.contains(column));
                }

                extend_unique_columns(columns, intersection);
            }
        },
    }

    Ok(())
}

fn non_null_columns_from_predicate(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    expr: Expr,
) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();
    collect_non_null_columns_from_predicate(ctx, analyses, expr, &mut columns)?;
    Ok(columns)
}

fn aggregate_nullability(
    ctx: &QueryContext,
    input_nullability: &[(Column, bool)],
    aggregate: &AggregateExpr,
) -> AnalysisResult<bool> {
    match aggregate {
        AggregateExpr::CountStar => Ok(false),
        AggregateExpr::Func { func, arg, .. } => match func {
            AggregateFunction::Count => Ok(false),
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Min
            | AggregateFunction::Max
            | AggregateFunction::Extension(_) => expr_nullability(ctx, input_nullability, *arg),
        },
    }
}

fn output_column_nullability(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    operator: Operator,
) -> AnalysisResult<Vec<(Column, bool)>> {
    match operator.get(ctx) {
        OperatorData::Scan(data) => Ok(scan_column_nullability(data, analyses)),
        OperatorData::TableFunction(_) | OperatorData::ConstScan(_) => Ok(analyses
            .get::<CreatedColumns>(ctx, operator)?
            .into_iter()
            .map(|column| (column, true))
            .collect()),
        OperatorData::Selection(data) => {
            let mut columns = analyses.get::<ColumnNullability>(ctx, data.input)?;
            for column in non_null_columns_from_predicate(ctx, analyses, data.predicate)? {
                mark_non_null(&mut columns, column);
            }
            Ok(columns)
        }
        OperatorData::Map(data) => {
            let mut columns = analyses.get::<ColumnNullability>(ctx, data.input)?;
            for (column, expr) in &data.computations {
                let nullable = expr_nullability(ctx, &columns, *expr)?;
                push_unique_nullability(&mut columns, *column, nullable);
            }
            Ok(columns)
        }
        OperatorData::Join(data) => {
            let outer = analyses.get::<ColumnNullability>(ctx, data.outer)?;
            let inner = analyses.get::<ColumnNullability>(ctx, data.inner)?;
            let mut columns = Vec::new();

            for (column, nullable) in outer {
                let nullable = match data.join_type {
                    JoinType::RightOuter | JoinType::FullOuter => true,
                    _ => nullable,
                };
                push_unique_nullability(&mut columns, column, nullable);
            }

            for (column, nullable) in inner {
                let nullable = match data.join_type {
                    JoinType::LeftOuter | JoinType::FullOuter | JoinType::Single => true,
                    _ => nullable,
                };
                if !matches!(data.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    push_unique_nullability(&mut columns, column, nullable);
                }
            }

            if let JoinType::LeftMark {
                marker: column,
                nullable,
            } = data.join_type
            {
                push_unique_nullability(&mut columns, column, nullable);
            }

            if matches!(data.join_type, JoinType::Inner) {
                for column in non_null_columns_from_predicate(ctx, analyses, data.on)? {
                    mark_non_null(&mut columns, column);
                }
            }

            Ok(columns)
        }
        OperatorData::CrossProduct(data) => {
            let mut columns = analyses.get::<ColumnNullability>(ctx, data.outer)?;
            for (column, nullable) in analyses.get::<ColumnNullability>(ctx, data.inner)? {
                push_unique_nullability(&mut columns, column, nullable);
            }
            Ok(columns)
        }
        OperatorData::Aggregation(data) => {
            let input_nullability = analyses.get::<ColumnNullability>(ctx, data.input)?;
            let mut columns = Vec::new();

            for expr in &data.keys {
                match expr.get(ctx) {
                    ExprData::ColumnRef(column) => push_unique_nullability(
                        &mut columns,
                        *column,
                        lookup_nullability(&input_nullability, *column).unwrap_or(true),
                    ),
                    _ => {
                        return Err(AnalysisError::UnsupportedAggregationKey {
                            operator,
                            expr: *expr,
                        });
                    }
                }
            }

            for (column, aggregate) in &data.aggregates {
                push_unique_nullability(
                    &mut columns,
                    *column,
                    aggregate_nullability(ctx, &input_nullability, aggregate)?,
                );
            }

            Ok(columns)
        }
        OperatorData::Projection(data) => {
            let input_nullability = analyses.get::<ColumnNullability>(ctx, data.input)?;
            Ok(data
                .columns
                .iter()
                .map(|column| {
                    (
                        *column,
                        lookup_nullability(&input_nullability, *column).unwrap_or(true),
                    )
                })
                .collect())
        }
        OperatorData::Sort(data) => analyses.get::<ColumnNullability>(ctx, data.input),
        OperatorData::Limit(data) => analyses.get::<ColumnNullability>(ctx, data.input),
        OperatorData::Output(data) => analyses.get::<ColumnNullability>(ctx, data.input),
        OperatorData::Rename(r) => {
            let input_nullability = analyses.get::<ColumnNullability>(ctx, r.input)?;
            Ok(r.defs
                .iter()
                .map(|(renamed, original)| {
                    (
                        *renamed,
                        lookup_nullability(&input_nullability, *original).unwrap_or(true),
                    )
                })
                .collect())
        }
    }
}

fn scan_column_nullability(scan: &Scan, analyses: &AnalysisContext) -> Vec<(Column, bool)> {
    let Some(catalog) = &analyses.catalog else {
        return scan.columns.iter().map(|column| (*column, true)).collect();
    };

    let Ok(metadata) = catalog.table_by_ref(&scan.table) else {
        return scan.columns.iter().map(|column| (*column, true)).collect();
    };

    scan.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let nullable = metadata
                .schema
                .fields()
                .get(index)
                .map(|field| field.is_nullable())
                .unwrap_or(true);
            (*column, nullable)
        })
        .collect()
}

impl CachedAnalysis for CardinalityEstimationV1 {
    type Output = CardinalityProfile;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        cardinality_profile(operator, ctx, analyses)
    }
}

impl Analysis for CardinalityEstimationV1 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for CardinalityEstimationV1 {
    type Value = CardinalityProfile;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

fn cardinality_profile(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
) -> AnalysisResult<CardinalityProfile> {
    match operator.get(ctx) {
        OperatorData::Scan(scan) => Ok(scan_profile(scan, ctx, analyses)),
        OperatorData::ConstScan(data) => Ok(const_scan_profile(data, ctx)),
        OperatorData::TableFunction(data) => Ok(CardinalityProfile::unknown_for_columns(
            1000.0,
            data.columns.clone(),
        )),
        OperatorData::Selection(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            Ok(apply_selection_profile(input, data.predicate, ctx))
        }
        OperatorData::Projection(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            Ok(project_profile(&input, &data.columns))
        }
        OperatorData::Output(data) => analyses.get::<CardinalityEstimationV1>(ctx, data.input),
        OperatorData::Sort(data) => analyses.get::<CardinalityEstimationV1>(ctx, data.input),
        OperatorData::Limit(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            let rows = match data.fetch {
                Some(fetch) => input.rows.cap(fetch as f64, EstimateSource::Derived),
                None => input.rows.clone(),
            };
            Ok(input.cap_by_rows(rows))
        }
        OperatorData::Rename(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            Ok(rename_profile(&input, &data.defs))
        }
        OperatorData::Map(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            Ok(map_profile(&input, &data.computations, ctx))
        }
        OperatorData::Aggregation(data) => {
            let input = analyses.get::<CardinalityEstimationV1>(ctx, data.input)?;
            Ok(aggregation_profile(&input, data, ctx))
        }
        OperatorData::CrossProduct(data) => {
            let left = analyses.get::<CardinalityEstimationV1>(ctx, data.outer)?;
            let right = analyses.get::<CardinalityEstimationV1>(ctx, data.inner)?;
            Ok(cross_product_profile(&left, &right))
        }
        OperatorData::Join(data) => {
            let left = analyses.get::<CardinalityEstimationV1>(ctx, data.outer)?;
            let right = analyses.get::<CardinalityEstimationV1>(ctx, data.inner)?;
            let predicates = analyses
                .get::<HypergraphOf>(ctx, operator)?
                .map(|hg| {
                    hg.edges
                        .iter()
                        .filter(|edge| edge.source == operator)
                        .filter_map(|edge| edge.predicate)
                        .collect::<Vec<_>>()
                })
                .filter(|predicates| !predicates.is_empty())
                .unwrap_or_else(|| vec![data.on]);
            Ok(join_profile_from_predicates(
                &left,
                &right,
                data.join_type.clone(),
                &predicates,
                ctx,
            ))
        }
    }
}

fn scan_profile(scan: &Scan, ctx: &QueryContext, analyses: &AnalysisContext) -> CardinalityProfile {
    let catalog_stats = analyses
        .catalog
        .as_ref()
        .and_then(|catalog| catalog.table_by_ref(&scan.table).ok())
        .and_then(|metadata| metadata.statistics);
    let mock_stats = mock_table_statistics(scan.table.table());

    let row_estimate = catalog_stats
        .as_ref()
        .and_then(|stats| stats.row_count)
        .map(|rows| Estimate::catalog(rows as f64))
        .or_else(|| {
            mock_stats
                .as_ref()
                .and_then(|stats| stats.row_count)
                .map(|rows| Estimate::mock(rows as f64))
        })
        .unwrap_or_else(|| Estimate::default(1000.0));

    let mut columns = BTreeMap::new();
    for column in &scan.columns {
        let column_name = &ctx.column(*column).name;
        let catalog_column = catalog_stats
            .as_ref()
            .and_then(|stats| stats.column_statistics.get(column_name));
        let mock_column = mock_stats
            .as_ref()
            .and_then(|stats| stats.column_statistics.get(column_name));
        let profile = catalog_column
            .map(|stats| column_profile_from_stats(stats, &row_estimate, EstimateSource::Catalog))
            .or_else(|| {
                mock_column.map(|stats| {
                    column_profile_from_stats(stats, &row_estimate, EstimateSource::Mock)
                })
            })
            .unwrap_or_else(|| ColumnProfile::unknown(&row_estimate));
        columns.insert(*column, profile);
    }

    CardinalityProfile::new(row_estimate, columns)
}

fn column_profile_from_stats(
    stats: &ColumnStatistics,
    rows: &Estimate,
    source: EstimateSource,
) -> ColumnProfile {
    let frequency_value = stats.frequency.map_or(rows.value, |value| value as f64);
    let distinct_value = stats
        .distinct
        .map_or(frequency_value.min(rows.value), |value| value as f64);
    ColumnProfile {
        lower_bound: stats.lower_bound.clone(),
        upper_bound: stats.upper_bound.clone(),
        frequency: Estimate {
            value: frequency_value,
            lower: Some(frequency_value),
            upper: Some(frequency_value),
            source,
        },
        distinct: Estimate {
            value: distinct_value.min(frequency_value),
            lower: Some(distinct_value.min(frequency_value)),
            upper: Some(distinct_value.min(frequency_value)),
            source,
        },
    }
}

fn const_scan_profile(data: &crate::ConstScan, ctx: &QueryContext) -> CardinalityProfile {
    let rows = Estimate::exact(data.rows.len() as f64);
    let mut columns = BTreeMap::new();
    for (idx, column) in data.columns.iter().enumerate() {
        let mut values = Vec::new();
        for row in &data.rows {
            if let Some(expr) = row.get(idx)
                && let ExprData::Literal(value) = expr.get(ctx)
                && !matches!(value, ScalarValue::Null(_))
            {
                values.push(value.clone());
            }
        }
        let distinct = distinct_scalar_count(&values) as f64;
        columns.insert(
            *column,
            ColumnProfile {
                lower_bound: scalar_min(&values),
                upper_bound: scalar_max(&values),
                frequency: Estimate::exact(values.len() as f64),
                distinct: Estimate::exact(distinct),
            },
        );
    }
    CardinalityProfile::new(rows, columns)
}

fn project_profile(input: &CardinalityProfile, columns: &[Column]) -> CardinalityProfile {
    let columns = columns
        .iter()
        .filter_map(|column| {
            input
                .columns
                .get(column)
                .cloned()
                .map(|profile| (*column, profile))
        })
        .collect();
    let equivalence_classes = filter_equivalence_classes(&input.equivalence_classes, &columns);
    CardinalityProfile {
        rows: input.rows.clone(),
        columns,
        equivalence_classes,
    }
}

fn rename_profile(input: &CardinalityProfile, defs: &[(Column, Column)]) -> CardinalityProfile {
    let rename_map = defs
        .iter()
        .map(|(renamed, original)| (*original, *renamed))
        .collect::<BTreeMap<_, _>>();
    let columns = defs
        .iter()
        .filter_map(|(renamed, original)| {
            input
                .columns
                .get(original)
                .cloned()
                .map(|profile| (*renamed, profile))
        })
        .collect();
    CardinalityProfile {
        rows: input.rows.clone(),
        columns,
        equivalence_classes: rename_equivalence_classes(&input.equivalence_classes, &rename_map),
    }
}

fn map_profile(
    input: &CardinalityProfile,
    computations: &[(Column, Expr)],
    ctx: &QueryContext,
) -> CardinalityProfile {
    let mut output = input.clone();
    for (column, expr) in computations {
        let profile = profile_for_computation(input, *expr, ctx)
            .unwrap_or_else(|| ColumnProfile::unknown(&input.rows));
        output
            .columns
            .insert(*column, profile.cap_by_rows(&input.rows));
    }
    output
}

fn profile_for_computation(
    input: &CardinalityProfile,
    expr: Expr,
    ctx: &QueryContext,
) -> Option<ColumnProfile> {
    match expr.get(ctx) {
        ExprData::ColumnRef(column) => input.columns.get(column).cloned(),
        ExprData::Cast { expr, .. } => profile_for_computation(input, *expr, ctx),
        ExprData::Binary {
            op: op @ (BinaryOp::Add | BinaryOp::Subtract),
            left,
            right,
        } => shifted_profile(input, *op, *left, *right, ctx),
        _ => None,
    }
}

fn shifted_profile(
    input: &CardinalityProfile,
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Option<ColumnProfile> {
    let (column, literal, literal_on_right) = match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(value)) => (*column, value, true),
        (ExprData::Literal(value), ExprData::ColumnRef(column)) => (*column, value, false),
        _ => return None,
    };
    let delta = scalar_to_f64(literal)?;
    let mut profile = input.columns.get(&column)?.clone();
    match op {
        BinaryOp::Add => {
            profile.lower_bound = shift_scalar(profile.lower_bound.as_ref(), delta);
            profile.upper_bound = shift_scalar(profile.upper_bound.as_ref(), delta);
        }
        BinaryOp::Subtract if literal_on_right => {
            profile.lower_bound = shift_scalar(profile.lower_bound.as_ref(), -delta);
            profile.upper_bound = shift_scalar(profile.upper_bound.as_ref(), -delta);
        }
        _ => return None,
    }
    Some(profile)
}

fn aggregation_profile(
    input: &CardinalityProfile,
    data: &crate::Aggregation,
    ctx: &QueryContext,
) -> CardinalityProfile {
    let mut group_rows = if data.keys.is_empty() {
        Estimate::exact(1.0)
    } else {
        let product = data.keys.iter().fold(1.0, |acc, expr| {
            if let ExprData::ColumnRef(column) = expr.get(ctx) {
                acc * input
                    .columns
                    .get(column)
                    .map_or(input.rows.value.min(100.0), |profile| {
                        profile.distinct.value
                    })
            } else {
                acc * input.rows.value.min(100.0)
            }
        });
        Estimate::derived(product.min(input.rows.value), Some(0.0), input.rows.upper)
    };
    group_rows = group_rows.cap(input.rows.value, EstimateSource::Derived);

    let mut columns = BTreeMap::new();
    for expr in &data.keys {
        if let ExprData::ColumnRef(column) = expr.get(ctx)
            && let Some(profile) = input.columns.get(column)
        {
            columns.insert(*column, profile.cap_by_rows(&group_rows));
        }
    }
    for (column, aggregate) in &data.aggregates {
        let profile = match aggregate {
            AggregateExpr::CountStar => ColumnProfile {
                lower_bound: Some(ScalarValue::Int64(0)),
                upper_bound: input
                    .rows
                    .upper
                    .map(|upper| ScalarValue::Int64(upper as i64)),
                frequency: group_rows.clone(),
                distinct: group_rows.cap(group_rows.value, EstimateSource::Derived),
            },
            AggregateExpr::Func {
                func: AggregateFunction::Count,
                ..
            } => ColumnProfile {
                lower_bound: Some(ScalarValue::Int64(0)),
                upper_bound: input
                    .rows
                    .upper
                    .map(|upper| ScalarValue::Int64(upper as i64)),
                frequency: group_rows.clone(),
                distinct: group_rows.cap(group_rows.value, EstimateSource::Derived),
            },
            _ => ColumnProfile::unknown(&group_rows),
        };
        columns.insert(*column, profile);
    }
    CardinalityProfile::new(group_rows, columns)
}

fn apply_selection_profile(
    mut profile: CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
) -> CardinalityProfile {
    for conjunct in conjuncts(predicate, ctx) {
        let selectivity = filter_selectivity(&profile, conjunct, ctx);
        profile = scale_profile(profile, selectivity.value);
        tighten_filter_columns(&mut profile, conjunct, ctx);
    }
    profile
}

fn scale_profile(profile: CardinalityProfile, factor: f64) -> CardinalityProfile {
    let factor = factor.clamp(0.0, 1.0);
    let rows = profile.rows.scale(factor, EstimateSource::Derived);
    let columns = profile
        .columns
        .iter()
        .map(|(&column, column_profile)| (column, column_profile.scale_frequency(factor, &rows)))
        .collect();
    let equivalence_classes = filter_equivalence_classes(&profile.equivalence_classes, &columns);
    CardinalityProfile {
        rows,
        columns,
        equivalence_classes,
    }
}

fn cross_product_profile(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
) -> CardinalityProfile {
    let rows = Estimate::derived(
        left.rows.value * right.rows.value,
        Some(0.0),
        multiply_options(left.rows.upper, right.rows.upper),
    );
    let mut columns = BTreeMap::new();
    for (&column, profile) in &left.columns {
        let mut output = profile.clone();
        output.frequency = output
            .frequency
            .scale(right.rows.value, EstimateSource::Derived);
        output.frequency = output.frequency.cap(rows.value, EstimateSource::Derived);
        output.distinct = output
            .distinct
            .cap(output.frequency.value, EstimateSource::Derived);
        columns.insert(column, output);
    }
    for (&column, profile) in &right.columns {
        let mut output = profile.clone();
        output.frequency = output
            .frequency
            .scale(left.rows.value, EstimateSource::Derived);
        output.frequency = output.frequency.cap(rows.value, EstimateSource::Derived);
        output.distinct = output
            .distinct
            .cap(output.frequency.value, EstimateSource::Derived);
        columns.insert(column, output);
    }
    CardinalityProfile {
        rows,
        columns,
        equivalence_classes: merge_equivalence_class_lists(
            &left.equivalence_classes,
            &right.equivalence_classes,
        ),
    }
}

fn join_profile_from_predicates(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    predicates: &[Expr],
    ctx: &QueryContext,
) -> CardinalityProfile {
    let estimate = join_selectivity_with_classes(left, right, predicates, ctx);
    join_profile_with_selectivity_and_classes(left, right, join_type, estimate)
}

pub(crate) fn join_profile_with_selectivity(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    selectivity: Estimate,
) -> CardinalityProfile {
    let equivalence_classes =
        merge_equivalence_class_lists(&left.equivalence_classes, &right.equivalence_classes);
    join_profile_with_selectivity_and_classes(
        left,
        right,
        join_type,
        JoinSelectivityEstimate {
            match_probability: Estimate::derived(
                (right.rows.value * selectivity.value).clamp(0.0, 1.0),
                Some(0.0),
                Some(1.0),
            ),
            selectivity,
            equivalence_classes,
        },
    )
}

fn join_profile_with_selectivity_and_classes(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    estimate: JoinSelectivityEstimate,
) -> CardinalityProfile {
    let selectivity = estimate.selectivity;
    let inner_rows = Estimate::derived(
        left.rows.value * right.rows.value * selectivity.value,
        Some(0.0),
        multiply_options(left.rows.upper, right.rows.upper).map(|v| v * selectivity.value),
    );
    match join_type {
        JoinType::LeftSemi => semi_join_profile(left, estimate.match_probability.value),
        JoinType::LeftAnti => anti_join_profile(left, estimate.match_probability.value),
        JoinType::Single => left.cap_by_rows(left.rows.clone()),
        JoinType::LeftOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(left.rows.value),
            estimate.equivalence_classes,
        ),
        JoinType::RightOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(right.rows.value),
            estimate.equivalence_classes,
        ),
        JoinType::FullOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(left.rows.value.max(right.rows.value)),
            estimate.equivalence_classes,
        ),
        JoinType::LeftMark {
            marker: column,
            nullable,
        } => {
            let mut profile = left.cap_by_rows(left.rows.clone());
            let max_distinct: f64 = if nullable { 3.0 } else { 2.0 };
            profile.columns.insert(
                column,
                ColumnProfile {
                    lower_bound: Some(ScalarValue::Boolean(false)),
                    upper_bound: Some(ScalarValue::Boolean(true)),
                    frequency: profile.rows.clone(),
                    distinct: Estimate::derived(
                        max_distinct.min(profile.rows.value),
                        Some(0.0),
                        Some(max_distinct),
                    ),
                },
            );
            profile
        }
        JoinType::Inner => {
            combine_join_columns(left, right, inner_rows, None, estimate.equivalence_classes)
        }
    }
}

fn semi_join_profile(input: &CardinalityProfile, match_probability: f64) -> CardinalityProfile {
    let rows = Estimate::derived(
        (input.rows.value * match_probability.clamp(0.0, 1.0)).clamp(0.0, input.rows.value),
        Some(0.0),
        input.rows.upper,
    );
    input.cap_by_rows(rows)
}

fn anti_join_profile(input: &CardinalityProfile, match_probability: f64) -> CardinalityProfile {
    let rows = Estimate::derived(
        (input.rows.value * (1.0 - match_probability.clamp(0.0, 1.0))).clamp(0.0, input.rows.value),
        Some(0.0),
        input.rows.upper,
    );
    input.cap_by_rows(rows)
}

fn combine_join_columns(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    mut rows: Estimate,
    lower_bound: Option<f64>,
    equivalence_classes: Vec<ColumnEquivalenceClass>,
) -> CardinalityProfile {
    if let Some(min_rows) = lower_bound {
        rows.value = rows.value.max(min_rows);
        rows.lower = Some(rows.lower.unwrap_or(0.0).max(min_rows));
    }
    let mut columns = BTreeMap::new();
    for (&column, profile) in &left.columns {
        columns.insert(column, profile.cap_by_rows(&rows));
    }
    for (&column, profile) in &right.columns {
        columns.insert(column, profile.cap_by_rows(&rows));
    }
    let equivalence_classes = filter_equivalence_classes(&equivalence_classes, &columns);
    CardinalityProfile {
        rows,
        columns,
        equivalence_classes,
    }
}

#[cfg(test)]
pub(crate) fn join_selectivity(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    predicates: &[Expr],
    ctx: &QueryContext,
) -> Estimate {
    join_selectivity_with_classes(left, right, predicates, ctx).selectivity
}

struct JoinSelectivityEstimate {
    selectivity: Estimate,
    equivalence_classes: Vec<ColumnEquivalenceClass>,
    match_probability: Estimate,
}

fn join_selectivity_with_classes(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    predicates: &[Expr],
    ctx: &QueryContext,
) -> JoinSelectivityEstimate {
    let mut classes = EquivalenceClassState::from_profiles(left, right);
    let mut equality_edges = Vec::new();
    let mut residual_selectivity = 1.0;
    for predicate in predicates
        .iter()
        .flat_map(|predicate| conjuncts(*predicate, ctx))
    {
        if let Some((left_col, right_col)) = column_equality(predicate, ctx) {
            equality_edges.push(EqualityEdge {
                left: left_col,
                right: right_col,
                chosen_ndv: classes
                    .class_distinct(left_col)
                    .max_by_value(classes.class_distinct(right_col)),
            });
        } else {
            residual_selectivity *=
                filter_selectivity_for_predicate(left, right, predicate, ctx).value;
        }
    }

    equality_edges.sort_by(|a, b| b.chosen_ndv.value.total_cmp(&a.chosen_ndv.value));
    let mut equality_selectivity = 1.0;
    let mut match_probability = 0.0_f64;
    for edge in equality_edges {
        if classes.equivalent(edge.left, edge.right) {
            continue;
        }
        let connects_inputs =
            column_sides(edge.left, left, right) != column_sides(edge.right, left, right);
        let chosen_ndv = edge.chosen_ndv.value.max(1.0);
        if connects_inputs {
            equality_selectivity *= 1.0 / chosen_ndv;
            let left_match = right.rows.value / chosen_ndv;
            let right_match = left.rows.value / chosen_ndv;
            match_probability = match_probability.max(left_match.min(right_match).clamp(0.0, 1.0));
        }
        classes.union(edge.left, edge.right, edge.chosen_ndv);
    }

    let selectivity = (equality_selectivity * residual_selectivity).clamp(0.0, 1.0);
    JoinSelectivityEstimate {
        selectivity: Estimate::derived(selectivity, Some(0.0), Some(1.0)),
        equivalence_classes: classes.into_classes(),
        match_probability: Estimate::derived(
            (if match_probability == 0.0 && equality_selectivity == 1.0 {
                residual_selectivity
            } else {
                match_probability * residual_selectivity
            })
            .clamp(0.0, 1.0),
            Some(0.0),
            Some(1.0),
        ),
    }
}

pub(crate) fn connecting_edge_indices(
    left_nodes: NodeSet,
    right_nodes: NodeSet,
    hg: &QueryHypergraph,
) -> Vec<usize> {
    hg.edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| {
            ((edge.left & left_nodes == edge.left) && (edge.right & right_nodes == edge.right))
                || ((edge.left & right_nodes == edge.left)
                    && (edge.right & left_nodes == edge.right))
        })
        .map(|(idx, _)| idx)
        .collect()
}

fn filter_selectivity(
    profile: &CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
) -> Estimate {
    filter_selectivity_for_predicate(profile, profile, predicate, ctx)
}

fn filter_selectivity_for_predicate(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
) -> Estimate {
    match predicate.get(ctx) {
        ExprData::Literal(ScalarValue::Boolean(true)) => Estimate::exact(1.0),
        ExprData::Literal(ScalarValue::Boolean(false)) => Estimate::exact(0.0),
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            let value = exprs
                .iter()
                .map(|expr| filter_selectivity_for_predicate(left, right, *expr, ctx).value)
                .product::<f64>();
            Estimate::derived(value, Some(0.0), Some(1.0))
        }
        ExprData::Nary {
            op: NaryOp::Or,
            exprs,
        } => {
            let mut not_selected = 1.0;
            for expr in exprs {
                not_selected *=
                    1.0 - filter_selectivity_for_predicate(left, right, *expr, ctx).value;
            }
            Estimate::derived(1.0 - not_selected, Some(0.0), Some(1.0))
        }
        ExprData::Binary {
            op,
            left: l,
            right: r,
        } => binary_selectivity(left, right, *op, *l, *r, ctx),
        ExprData::Unary {
            op: UnaryOp::IsNull,
            expr,
        } => {
            let column = column_ref(*expr, ctx);
            let frequency = column.and_then(|column| {
                left.columns
                    .get(&column)
                    .or_else(|| right.columns.get(&column))
                    .map(|profile| profile.frequency.value)
            });
            let rows = left.rows.value.max(right.rows.value).max(1.0);
            Estimate::derived(
                frequency.map_or(0.1, |f| 1.0 - (f / rows).clamp(0.0, 1.0)),
                Some(0.0),
                Some(1.0),
            )
        }
        ExprData::Unary {
            op: UnaryOp::IsNotNull,
            expr,
        } => {
            let column = column_ref(*expr, ctx);
            let frequency = column.and_then(|column| {
                left.columns
                    .get(&column)
                    .or_else(|| right.columns.get(&column))
                    .map(|profile| profile.frequency.value)
            });
            let rows = left.rows.value.max(right.rows.value).max(1.0);
            Estimate::derived(
                frequency.map_or(0.9, |f| (f / rows).clamp(0.0, 1.0)),
                Some(0.0),
                Some(1.0),
            )
        }
        ExprData::Like { .. } => Estimate::derived(0.1, Some(0.0), Some(1.0)),
        _ => Estimate::derived(0.25, Some(0.0), Some(1.0)),
    }
}

fn binary_selectivity(
    left_profile: &CardinalityProfile,
    right_profile: &CardinalityProfile,
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Estimate {
    if let Some((column, literal)) = column_literal(left, right, ctx)
        && let Some(profile) = left_profile
            .columns
            .get(&column)
            .or_else(|| right_profile.columns.get(&column))
    {
        return match op {
            BinaryOp::Eq => {
                Estimate::derived(1.0 / profile.distinct.value.max(1.0), Some(0.0), Some(1.0))
            }
            BinaryOp::NotEq => Estimate::derived(
                1.0 - (1.0 / profile.distinct.value.max(1.0)),
                Some(0.0),
                Some(1.0),
            ),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                range_selectivity(profile, literal, op)
            }
            _ => Estimate::derived(0.25, Some(0.0), Some(1.0)),
        };
    }
    if let Some((left_col, right_col)) = column_equality_from_parts(op, left, right, ctx) {
        let left_ndv = left_profile
            .columns
            .get(&left_col)
            .or_else(|| right_profile.columns.get(&left_col))
            .map_or(left_profile.rows.value.max(1.0), |profile| {
                profile.distinct.value
            });
        let right_ndv = left_profile
            .columns
            .get(&right_col)
            .or_else(|| right_profile.columns.get(&right_col))
            .map_or(right_profile.rows.value.max(1.0), |profile| {
                profile.distinct.value
            });
        return Estimate::derived(1.0 / left_ndv.max(right_ndv).max(1.0), Some(0.0), Some(1.0));
    }
    Estimate::derived(0.25, Some(0.0), Some(1.0))
}

fn tighten_filter_columns(profile: &mut CardinalityProfile, predicate: Expr, ctx: &QueryContext) {
    if let ExprData::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = predicate.get(ctx)
        && let Some((column, literal)) = column_literal(*left, *right, ctx)
        && let Some(column_profile) = profile.columns.get_mut(&column)
    {
        column_profile.lower_bound = Some(literal.clone());
        column_profile.upper_bound = Some(literal.clone());
        column_profile.distinct = Estimate::derived(1.0, Some(0.0), Some(1.0));
        column_profile.frequency = column_profile
            .frequency
            .cap(profile.rows.value, EstimateSource::Derived);
    }
}

fn range_selectivity(profile: &ColumnProfile, literal: &ScalarValue, op: BinaryOp) -> Estimate {
    let Some(min) = profile.lower_bound.as_ref().and_then(scalar_to_f64) else {
        return Estimate::derived(0.33, Some(0.0), Some(1.0));
    };
    let Some(max) = profile.upper_bound.as_ref().and_then(scalar_to_f64) else {
        return Estimate::derived(0.33, Some(0.0), Some(1.0));
    };
    let Some(value) = scalar_to_f64(literal) else {
        return Estimate::derived(0.33, Some(0.0), Some(1.0));
    };
    let width = (max - min).abs().max(1.0);
    let selected = match op {
        BinaryOp::Lt | BinaryOp::LtEq => ((value - min) / width).clamp(0.0, 1.0),
        BinaryOp::Gt | BinaryOp::GtEq => ((max - value) / width).clamp(0.0, 1.0),
        _ => 0.33,
    };
    Estimate::derived(selected, Some(0.0), Some(1.0))
}

fn conjuncts(expr: Expr, ctx: &QueryContext) -> Vec<Expr> {
    match expr.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .iter()
            .flat_map(|expr| conjuncts(*expr, ctx))
            .collect(),
        _ => vec![expr],
    }
}

fn column_ref(expr: Expr, ctx: &QueryContext) -> Option<Column> {
    match expr.get(ctx) {
        ExprData::ColumnRef(column) => Some(*column),
        _ => None,
    }
}

fn column_literal(left: Expr, right: Expr, ctx: &QueryContext) -> Option<(Column, &ScalarValue)> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(value))
        | (ExprData::Literal(value), ExprData::ColumnRef(column)) => Some((*column, value)),
        _ => None,
    }
}

fn column_equality(expr: Expr, ctx: &QueryContext) -> Option<(Column, Column)> {
    let ExprData::Binary { op, left, right } = expr.get(ctx) else {
        return None;
    };
    column_equality_from_parts(*op, *left, *right, ctx)
}

fn column_equality_from_parts(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Option<(Column, Column)> {
    if op != BinaryOp::Eq {
        return None;
    }
    let ExprData::ColumnRef(left_col) = left.get(ctx) else {
        return None;
    };
    let ExprData::ColumnRef(right_col) = right.get(ctx) else {
        return None;
    };
    Some((*left_col, *right_col))
}

struct EqualityEdge {
    left: Column,
    right: Column,
    chosen_ndv: Estimate,
}

#[derive(Clone, Default)]
struct EquivalenceClassState {
    parent: HashMap<Column, Column>,
    distinct: HashMap<Column, Estimate>,
}

impl EquivalenceClassState {
    fn from_profiles(left: &CardinalityProfile, right: &CardinalityProfile) -> Self {
        let mut state = Self::default();
        for profile in [left, right] {
            for (&column, column_profile) in &profile.columns {
                state.parent.entry(column).or_insert(column);
                state
                    .distinct
                    .entry(column)
                    .or_insert_with(|| column_profile.distinct.clone());
            }
            for class in &profile.equivalence_classes {
                let mut iter = class.columns.iter().copied();
                let Some(first) = iter.next() else {
                    continue;
                };
                let first_root = state.find(first);
                state.distinct.insert(first_root, class.distinct.clone());
                for column in iter {
                    state.union(first, column, class.distinct.clone());
                }
            }
        }
        state
    }

    fn equivalent(&mut self, left: Column, right: Column) -> bool {
        self.find(left) == self.find(right)
    }

    fn union(&mut self, left: Column, right: Column, distinct: Estimate) -> bool {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            false
        } else {
            self.parent.insert(right_root, left_root);
            let left_distinct = self
                .distinct
                .remove(&left_root)
                .unwrap_or_else(|| distinct.clone());
            let right_distinct = self
                .distinct
                .remove(&right_root)
                .unwrap_or_else(|| distinct.clone());
            self.distinct.insert(
                left_root,
                left_distinct
                    .max_by_value(right_distinct)
                    .max_by_value(distinct),
            );
            true
        }
    }

    fn class_distinct(&mut self, column: Column) -> Estimate {
        let root = self.find(column);
        self.distinct
            .get(&root)
            .cloned()
            .unwrap_or_else(|| Estimate::default(100.0))
    }

    fn find(&mut self, column: Column) -> Column {
        let parent = *self.parent.entry(column).or_insert(column);
        if parent == column {
            self.distinct
                .entry(column)
                .or_insert_with(|| Estimate::default(100.0));
            column
        } else {
            let root = self.find(parent);
            self.parent.insert(column, root);
            root
        }
    }

    fn into_classes(mut self) -> Vec<ColumnEquivalenceClass> {
        let columns = self.parent.keys().copied().collect::<Vec<_>>();
        let mut grouped = BTreeMap::<Column, BTreeSet<Column>>::new();
        for column in columns {
            let root = self.find(column);
            grouped.entry(root).or_default().insert(column);
        }
        grouped
            .into_iter()
            .map(|(root, columns)| ColumnEquivalenceClass {
                columns,
                distinct: self
                    .distinct
                    .get(&root)
                    .cloned()
                    .unwrap_or_else(|| Estimate::default(100.0)),
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnSide {
    Left,
    Right,
    Both,
    Neither,
}

fn column_sides(
    column: Column,
    left: &CardinalityProfile,
    right: &CardinalityProfile,
) -> ColumnSide {
    match (
        left.columns.contains_key(&column),
        right.columns.contains_key(&column),
    ) {
        (true, false) => ColumnSide::Left,
        (false, true) => ColumnSide::Right,
        (true, true) => ColumnSide::Both,
        (false, false) => ColumnSide::Neither,
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int32(value) => Some(*value as f64),
        ScalarValue::Int64(value) => Some(*value as f64),
        ScalarValue::Float64(value) => Some(*value),
        ScalarValue::Date32(value) => Some(*value as f64),
        ScalarValue::Decimal128 { value, scale, .. } => {
            Some(*value as f64 / 10_f64.powi(*scale as i32))
        }
        _ => None,
    }
}

fn shift_scalar(value: Option<&ScalarValue>, delta: f64) -> Option<ScalarValue> {
    match value? {
        ScalarValue::Int32(value) => Some(ScalarValue::Int32((*value as f64 + delta) as i32)),
        ScalarValue::Int64(value) => Some(ScalarValue::Int64((*value as f64 + delta) as i64)),
        ScalarValue::Float64(value) => Some(ScalarValue::Float64(*value + delta)),
        ScalarValue::Date32(value) => Some(ScalarValue::Date32((*value as f64 + delta) as i32)),
        _ => None,
    }
}

fn scalar_min(values: &[ScalarValue]) -> Option<ScalarValue> {
    values
        .iter()
        .filter_map(|value| scalar_to_f64(value).map(|as_f64| (as_f64, value)))
        .min_by(|(a, _), (b, _)| a.total_cmp(b))
        .map(|(_, value)| value.clone())
}

fn scalar_max(values: &[ScalarValue]) -> Option<ScalarValue> {
    values
        .iter()
        .filter_map(|value| scalar_to_f64(value).map(|as_f64| (as_f64, value)))
        .max_by(|(a, _), (b, _)| a.total_cmp(b))
        .map(|(_, value)| value.clone())
}

fn distinct_scalar_count(values: &[ScalarValue]) -> usize {
    let mut distinct = Vec::<&ScalarValue>::new();
    for value in values {
        if !distinct.contains(&value) {
            distinct.push(value);
        }
    }
    distinct.len()
}

fn multiply_options(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left * right),
        _ => None,
    }
}

fn clamp_to_bounds(value: f64, lower: Option<f64>, upper: Option<f64>) -> f64 {
    let mut value = value.max(0.0);
    if let Some(lower) = lower {
        value = value.max(lower);
    }
    if let Some(upper) = upper {
        value = value.min(upper);
    }
    value
}

fn mock_table_statistics(table: &str) -> Option<TableStatistics> {
    let row_count = match table {
        "region" => 5,
        "nation" => 25,
        "supplier" => 10_000,
        "customer" => 150_000,
        "part" => 200_000,
        "partsupp" => 800_000,
        "orders" => 1_500_000,
        "lineitem" => 6_001_215,
        "aka_name" => 901_343,
        "aka_title" => 361_472,
        "cast_info" => 36_244_344,
        "char_name" => 3_140_339,
        "comp_cast_type" => 4,
        "company_name" => 234_997,
        "company_type" => 4,
        "complete_cast" => 135_086,
        "info_type" => 113,
        "keyword" => 134_170,
        "kind_type" => 7,
        "link_type" => 18,
        "movie_companies" => 2_609_129,
        "movie_info" => 14_835_720,
        "movie_info_idx" => 1_380_035,
        "movie_keyword" => 4_523_930,
        "movie_link" => 29_997,
        "name" => 4_167_491,
        "person_info" => 2_963_664,
        "role_type" => 12,
        "title" => 2_528_312,
        _ => return None,
    };
    Some(TableStatistics {
        row_count: Some(row_count),
        size_bytes: None,
        column_statistics: mock_column_statistics(table, row_count),
    })
}

fn mock_column_statistics(table: &str, row_count: usize) -> BTreeMap<String, ColumnStatistics> {
    let mut stats = BTreeMap::new();
    stats.insert("id".to_string(), mock_column_stat(row_count, row_count));

    let referenced = match table {
        "aka_title" | "cast_info" | "complete_cast" | "movie_companies" | "movie_info"
        | "movie_info_idx" | "movie_keyword" | "movie_link" => Some(("movie_id", 2_528_312)),
        _ => None,
    };
    if let Some((column, distinct)) = referenced {
        stats.insert(column.to_string(), mock_column_stat(row_count, distinct));
    }
    if table == "movie_link" {
        stats.insert(
            "linked_movie_id".to_string(),
            mock_column_stat(row_count, 2_528_312),
        );
    }

    for (column, distinct) in [
        ("keyword_id", 134_170),
        ("company_id", 234_997),
        ("company_type_id", 4),
        ("info_type_id", 113),
        ("kind_id", 7),
        ("link_type_id", 18),
        ("person_id", 4_167_491),
        ("person_role_id", 4_167_491),
        ("role_id", 12),
        ("role_type_id", 12),
        ("subject_id", 4),
        ("status_id", 4),
    ] {
        stats.insert(column.to_string(), mock_column_stat(row_count, distinct));
    }

    for (column, distinct) in [
        ("r_regionkey", 5),
        ("n_nationkey", 25),
        ("n_regionkey", 5),
        ("s_suppkey", 10_000),
        ("s_nationkey", 25),
        ("c_custkey", 150_000),
        ("c_nationkey", 25),
        ("p_partkey", 200_000),
        ("ps_partkey", 200_000),
        ("ps_suppkey", 10_000),
        ("o_orderkey", 1_500_000),
        ("o_custkey", 150_000),
        ("l_orderkey", 1_500_000),
        ("l_partkey", 200_000),
        ("l_suppkey", 10_000),
    ] {
        stats.insert(column.to_string(), mock_column_stat(row_count, distinct));
    }

    stats
}

fn mock_column_stat(row_count: usize, distinct: usize) -> ColumnStatistics {
    ColumnStatistics {
        lower_bound: None,
        upper_bound: None,
        frequency: Some(row_count),
        distinct: Some(distinct.min(row_count)),
    }
}

/// Analysis that records columns introduced directly by an operator.
#[derive(Default)]
pub struct CreatedColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl CachedAnalysis for CreatedColumns {
    type Output = Vec<Column>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        _analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        Ok(directly_created_columns(operator.get(ctx)))
    }
}

impl Analysis for CreatedColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for CreatedColumns {
    type Value = Vec<Column>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<Column>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

/// Analysis that records columns referenced directly by an operator.
#[derive(Default)]
pub struct UsedColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl CachedAnalysis for UsedColumns {
    type Output = Vec<Column>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        directly_used_columns(ctx, analyses, operator.get(ctx))
    }
}

impl Analysis for UsedColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for UsedColumns {
    type Value = Vec<Column>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<Column>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

/// Analysis that records directly-used columns not available from operator inputs.
#[derive(Default)]
pub struct FreeColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl CachedAnalysis for FreeColumns {
    type Output = Vec<Column>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        free_columns(ctx, analyses, operator)
    }
}

impl Analysis for FreeColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for FreeColumns {
    type Value = Vec<Column>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<Column>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

/// Analysis that records whether each output column may contain null values.
#[derive(Default)]
pub struct ColumnNullability {
    state: OperatorAnalysisState<Vec<(Column, bool)>>,
}

impl CachedAnalysis for ColumnNullability {
    type Output = Vec<(Column, bool)>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        output_column_nullability(ctx, analyses, operator)
    }
}

impl Analysis for ColumnNullability {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for ColumnNullability {
    type Value = Vec<(Column, bool)>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<(Column, bool)>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

/// Analysis that records columns available in an operator's output.
#[derive(Default)]
pub struct AvailableColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl CachedAnalysis for AvailableColumns {
    type Output = Vec<Column>;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Output> {
        match op.get(ctx) {
            OperatorData::Scan(_) | OperatorData::TableFunction(_) | OperatorData::ConstScan(_) => {
                analyses.get::<CreatedColumns>(ctx, op)
            }
            OperatorData::Selection(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Map(data) => {
                let mut columns = analyses.get::<AvailableColumns>(ctx, data.input)?;
                columns.extend(analyses.get::<CreatedColumns>(ctx, op)?);
                Ok(columns)
            }
            OperatorData::Join(data) => {
                let mut columns = analyses.get::<AvailableColumns>(ctx, data.outer)?;
                if !matches!(data.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    columns.extend(analyses.get::<AvailableColumns>(ctx, data.inner)?);
                }
                columns.extend(analyses.get::<CreatedColumns>(ctx, op)?);
                Ok(columns)
            }
            OperatorData::CrossProduct(data) => {
                let mut columns = analyses.get::<AvailableColumns>(ctx, data.outer)?;
                columns.extend(analyses.get::<AvailableColumns>(ctx, data.inner)?);
                Ok(columns)
            }
            OperatorData::Aggregation(data) => {
                analyses.get::<AvailableColumns>(ctx, data.input)?;
                let mut columns = Vec::new();
                for expr in &data.keys {
                    match expr.get(ctx) {
                        ExprData::ColumnRef(column) => columns.push(*column),
                        _ => {
                            return Err(AnalysisError::UnsupportedAggregationKey {
                                operator: op,
                                expr: *expr,
                            });
                        }
                    }
                }
                columns.extend(analyses.get::<CreatedColumns>(ctx, op)?);
                Ok(columns)
            }
            OperatorData::Projection(data) => {
                analyses.get::<AvailableColumns>(ctx, data.input)?;
                Ok(data.columns.clone())
            }
            OperatorData::Sort(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Limit(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Output(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Rename(r) => {
                analyses.get::<AvailableColumns>(ctx, r.input)?;
                Ok(r.defs.iter().map(|(renamed, _)| *renamed).collect())
            }
        }
    }
}

impl Analysis for AvailableColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for AvailableColumns {
    type Value = Vec<Column>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<Column>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

/// Parent index for one reachable plan version.
///
/// The index is derived from a single root. It may contain multiple parents for
/// an operator when the reachable plan is DAG-shaped.
#[derive(Debug, Clone)]
pub struct ParentIndex {
    root: Operator,
    parents: HashMap<Operator, Vec<Operator>>,
}

impl ParentIndex {
    /// Builds a parent index for the reachable plan under `root`.
    pub fn build(ctx: &QueryContext, root: Operator) -> Self {
        let mut parents: HashMap<Operator, Vec<Operator>> = HashMap::new();
        let mut stack = vec![root];
        let mut visited = HashSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            for child in relational_inputs(current, ctx) {
                parents.entry(child).or_default().push(current);
                stack.push(child);
            }
        }

        Self { root, parents }
    }

    /// Returns the root this index was built from.
    pub fn root(&self) -> Operator {
        self.root
    }

    /// Returns all immediate parents of `op` in this reachable plan.
    pub fn parents(&self, op: Operator) -> &[Operator] {
        self.parents.get(&op).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Analysis that returns all immediate parents of an operator in the reachable plan.
///
/// Returns an empty vector for the root or for unreachable operators. Builds and
/// caches a full parent index on first call. The cache is keyed on the root at
/// time of computation; call [`AnalysisContext::clear`] if the root changes.
#[derive(Default)]
pub struct ParentsOf {
    /// Cached parent index. Invalidated when root changes.
    cache: RefCell<Option<ParentIndex>>,
}

impl Analysis for ParentsOf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        *self.cache.borrow_mut() = None;
    }
}

impl Analyzable for ParentsOf {
    /// Immediate parents of `op` in the reachable plan.
    type Value = Vec<Operator>;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Vec<Operator>> {
        let Some(root) = ctx.root() else {
            return Ok(Vec::new());
        };

        let entry = analyses.registry_entry::<Self>();
        let analysis = typed_analysis::<Self>(&entry)?;

        {
            let cache = analysis.cache.borrow();
            if let Some(ref index) = *cache
                && index.root() == root
            {
                return Ok(index.parents(op).to_vec());
            }
        }

        let index = ParentIndex::build(ctx, root);
        let parents = index.parents(op).to_vec();
        *analysis.cache.borrow_mut() = Some(index);
        Ok(parents)
    }
}

fn relational_inputs(op: Operator, ctx: &QueryContext) -> Vec<Operator> {
    op.get(ctx).inputs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Catalog, ColumnData, ExprData,
        Join, JoinType, Map, MemoryCatalog, OperatorData, Output, Projection, ScalarValue, Scan,
        Selection, TableFunction, TableFunctionDef, TableRef,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn created_columns_tracks_columns_introduced_by_operators() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let is_adult = ColumnData::new("is_adult", DataType::Boolean).add(&mut ctx);

        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);

        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let adult_expr = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: scan,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![id, is_adult],
            input: map,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, scan).unwrap(),
            vec![id, age]
        );
        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, map).unwrap(),
            vec![is_adult]
        );
        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, projection).unwrap(),
            vec![]
        );
        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, output).unwrap(),
            vec![]
        );
    }

    #[test]
    fn available_columns_tracks_columns_visible_at_each_operator() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);
        let order_total = ColumnData::new("order_total", DataType::Float64).add(&mut ctx);
        let is_adult = ColumnData::new("is_adult", DataType::Boolean).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id, age],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id, order_total],
        })
        .add(&mut ctx);

        let left = ExprData::ColumnRef(user_id).add(&mut ctx);
        let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        })
        .add(&mut ctx);

        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let adult_expr = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: join,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![user_id, order_total, is_adult],
            input: map,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, users).unwrap(),
            vec![user_id, age]
        );
        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, join).unwrap(),
            vec![user_id, age, order_user_id, order_total]
        );
        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, map).unwrap(),
            vec![user_id, age, order_user_id, order_total, is_adult]
        );
        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, projection).unwrap(),
            vec![user_id, order_total, is_adult]
        );
        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, output).unwrap(),
            vec![user_id, order_total, is_adult]
        );
    }

    #[test]
    fn used_columns_tracks_columns_referenced_by_each_operator() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);
        let order_total = ColumnData::new("order_total", DataType::Float64).add(&mut ctx);
        let is_adult = ColumnData::new("is_adult", DataType::Boolean).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id, age],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id, order_total],
        })
        .add(&mut ctx);

        let left = ExprData::ColumnRef(user_id).add(&mut ctx);
        let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        })
        .add(&mut ctx);

        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let adult_expr = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: join,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![user_id, order_total, is_adult],
            input: map,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(analyses.get::<UsedColumns>(&ctx, users).unwrap(), vec![]);
        assert_eq!(
            analyses.get::<UsedColumns>(&ctx, join).unwrap(),
            vec![user_id, order_user_id]
        );
        assert_eq!(analyses.get::<UsedColumns>(&ctx, map).unwrap(), vec![age]);
        assert_eq!(
            analyses.get::<UsedColumns>(&ctx, projection).unwrap(),
            vec![user_id, order_total, is_adult]
        );
        assert_eq!(
            analyses.get::<UsedColumns>(&ctx, output).unwrap(),
            vec![user_id, order_total, is_adult]
        );
    }

    #[test]
    fn free_columns_tracks_used_columns_missing_from_inputs() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let missing = ColumnData::new("missing", DataType::Int64).add(&mut ctx);
        let computed = ColumnData::new("computed", DataType::Int64).add(&mut ctx);

        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);

        let missing_ref = ExprData::ColumnRef(missing).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate: missing_ref,
            input: scan,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![id, missing],
            input: scan,
        })
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(computed, missing_ref)],
            input: scan,
        })
        .add(&mut ctx);
        let table_function = OperatorData::TableFunction(TableFunction {
            function: TableFunctionDef::extension("read_from_column_path"),
            args: vec![missing_ref],
            columns: vec![id],
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(analyses.get::<FreeColumns>(&ctx, scan).unwrap(), vec![]);
        assert_eq!(
            analyses.get::<FreeColumns>(&ctx, selection).unwrap(),
            vec![missing]
        );
        assert_eq!(
            analyses.get::<FreeColumns>(&ctx, projection).unwrap(),
            vec![missing]
        );
        assert_eq!(
            analyses.get::<FreeColumns>(&ctx, map).unwrap(),
            vec![missing]
        );
        assert_eq!(
            analyses.get::<FreeColumns>(&ctx, table_function).unwrap(),
            vec![missing]
        );
    }

    #[test]
    fn free_columns_for_subquery_expressions_only_bubble_correlations() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);
        let order_total = ColumnData::new("order_total", DataType::Float64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id, order_total],
        })
        .add(&mut ctx);

        let order_user_ref = ExprData::ColumnRef(order_user_id).add(&mut ctx);
        let user_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
        let correlated = ExprData::Binary {
            op: BinaryOp::Eq,
            left: order_user_ref,
            right: user_ref,
        }
        .add(&mut ctx);
        let subquery = OperatorData::Selection(Selection {
            predicate: correlated,
            input: orders,
        })
        .add(&mut ctx);

        let user_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
        let subquery_expr = ExprData::ScalarSubquery { subquery }.add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: user_ref,
            right: subquery_expr,
        }
        .add(&mut ctx);
        let parent = OperatorData::Selection(Selection {
            predicate,
            input: users,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<UsedColumns>(&ctx, parent).unwrap(),
            vec![user_id]
        );
        assert_eq!(analyses.get::<FreeColumns>(&ctx, parent).unwrap(), vec![]);
        assert_eq!(
            analyses.get::<FreeColumns>(&ctx, subquery).unwrap(),
            vec![user_id]
        );
    }

    #[test]
    fn column_nullability_tracks_output_columns() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let is_age_null = ColumnData::new("is_age_null", DataType::Boolean).add(&mut ctx);
        let age_plus_one = ColumnData::new("age_plus_one", DataType::Int32).add(&mut ctx);

        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let is_null = ExprData::Unary {
            op: crate::UnaryOp::IsNull,
            expr: age_ref,
        }
        .add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int32(1)).add(&mut ctx);
        let age_plus_one_expr = ExprData::Binary {
            op: BinaryOp::Add,
            left: age_ref,
            right: one,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(is_age_null, is_null), (age_plus_one, age_plus_one_expr)],
            input: scan,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![id, is_age_null, age_plus_one],
            input: map,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, scan).unwrap(),
            vec![(id, true), (age, true)]
        );
        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, map).unwrap(),
            vec![
                (id, true),
                (age, true),
                (is_age_null, false),
                (age_plus_one, true)
            ]
        );
        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, projection).unwrap(),
            vec![(id, true), (is_age_null, false), (age_plus_one, true)]
        );
    }

    #[test]
    fn column_nullability_uses_catalog_scan_schema_when_available() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("users"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("age", DataType::Int32, true),
                ])),
                None,
            )
            .unwrap();

        let mut ctx = QueryContext::new();
        let scan = ctx
            .add_scan_from_catalog(catalog.as_ref(), TableRef::bare("users"))
            .unwrap();

        let mut analyses = AnalysisContext::with_catalog(catalog);

        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            panic!("catalog scan should create a scan operator");
        };
        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, scan).unwrap(),
            vec![(scan_data.columns[0], false), (scan_data.columns[1], true)]
        );
    }

    #[test]
    fn column_nullability_uses_selection_null_rejection() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);

        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let predicate = ExprData::Unary {
            op: crate::UnaryOp::IsNotNull,
            expr: age_ref,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, selection).unwrap(),
            vec![(id, true), (age, false)]
        );
    }

    #[test]
    fn column_nullability_uses_inner_join_null_rejection() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id],
        })
        .add(&mut ctx);
        let left = ExprData::ColumnRef(user_id).add(&mut ctx);
        let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
            vec![(user_id, false), (order_user_id, false)]
        );
    }

    #[test]
    fn column_nullability_tracks_outer_join_null_extension() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id],
        })
        .add(&mut ctx);
        let left = ExprData::ColumnRef(user_id).add(&mut ctx);
        let right = ExprData::ColumnRef(order_user_id).add(&mut ctx);
        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on,
            outer: users,
            inner: orders,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
            vec![(user_id, true), (order_user_id, true)]
        );
    }

    #[test]
    fn column_nullability_uses_mark_join_nullable_flag() {
        for (marker_nullable, expected_nullable) in [(false, false), (true, true)] {
            let mut ctx = QueryContext::new();
            let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
            let order_user_id = ColumnData::new("order_user_id", DataType::Int64).add(&mut ctx);
            let marker = ColumnData::new("mark", DataType::Boolean).add(&mut ctx);

            let users = OperatorData::Scan(Scan {
                table: TableRef::bare("users"),
                columns: vec![user_id],
            })
            .add(&mut ctx);
            let orders = OperatorData::Scan(Scan {
                table: TableRef::bare("orders"),
                columns: vec![order_user_id],
            })
            .add(&mut ctx);
            let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
            let join = OperatorData::Join(Join {
                join_type: JoinType::LeftMark {
                    marker,
                    nullable: marker_nullable,
                },
                on,
                outer: users,
                inner: orders,
            })
            .add(&mut ctx);

            let mut analyses = ctx.analyze();

            assert_eq!(
                analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
                vec![
                    (user_id, true),
                    (order_user_id, true),
                    (marker, expected_nullable)
                ]
            );
        }
    }

    #[test]
    fn expr_used_columns_deduplicates_by_first_use() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);

        let a_left = ExprData::ColumnRef(a).add(&mut ctx);
        let b_ref = ExprData::ColumnRef(b).add(&mut ctx);
        let a_right = ExprData::ColumnRef(a).add(&mut ctx);
        let expr = ExprData::Nary {
            op: crate::NaryOp::And,
            exprs: vec![a_left, b_ref, a_right],
        }
        .add(&mut ctx);

        assert_eq!(expr_used_columns(&ctx, expr).unwrap(), vec![a, b]);
    }

    #[test]
    fn available_columns_rejects_expression_aggregation_keys() {
        let mut ctx = QueryContext::new();
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let count = ColumnData::new("count", DataType::Int64).add(&mut ctx);

        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int32(1)).add(&mut ctx);
        let key = ExprData::Binary {
            op: BinaryOp::Add,
            left: age_ref,
            right: one,
        }
        .add(&mut ctx);
        let aggregation = OperatorData::Aggregation(Aggregation {
            keys: vec![key],
            aggregates: vec![(
                count,
                AggregateExpr::Func {
                    func: AggregateFunction::Count,
                    arg: age_ref,
                    distinct: false,
                },
            )],
            input: scan,
        })
        .add(&mut ctx);

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, aggregation),
            Err(AnalysisError::UnsupportedAggregationKey {
                operator: aggregation,
                expr: key,
            })
        );
    }

    #[test]
    fn analysis_context_clear_invalidates_analysis_caches() {
        let mut ctx = QueryContext::new();
        let first = ColumnData::new("first", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![first],
        })
        .add(&mut ctx);
        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, scan).unwrap(),
            vec![first]
        );

        let second = ColumnData::new("second", DataType::Int64).add(&mut ctx);
        *ctx.operator_mut(scan) = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![first, second],
        });

        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, scan).unwrap(),
            vec![first]
        );
        analyses.clear();
        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, scan).unwrap(),
            vec![first, second]
        );
    }

    #[test]
    fn parents_of_returns_multiple_immediate_parents() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![id],
        })
        .add(&mut ctx);
        let left_predicate = ExprData::ColumnRef(id).add(&mut ctx);
        let left = OperatorData::Selection(crate::Selection {
            predicate: left_predicate,
            input: scan,
        })
        .add(&mut ctx);
        let right_predicate = ExprData::ColumnRef(id).add(&mut ctx);
        let right = OperatorData::Selection(crate::Selection {
            predicate: right_predicate,
            input: scan,
        })
        .add(&mut ctx);
        let on = ExprData::Literal(crate::ScalarValue::Boolean(true)).add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        ctx.set_root(join);

        let mut analyses = AnalysisContext::new();
        let parents = analyses.get::<ParentsOf>(&ctx, scan).unwrap();

        assert_eq!(parents.len(), 2);
        assert!(parents.contains(&left));
        assert!(parents.contains(&right));
    }

    #[test]
    fn cardinality_estimation_uses_catalog_column_statistics() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("users"),
                TableStatistics {
                    row_count: Some(100),
                    size_bytes: None,
                    column_statistics: [(
                        "id".to_string(),
                        ColumnStatistics {
                            lower_bound: Some(ScalarValue::Int64(1)),
                            upper_bound: Some(ScalarValue::Int64(100)),
                            frequency: Some(100),
                            distinct: Some(100),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, scan).unwrap();

        assert_eq!(profile.rows.value, 100.0);
        assert_eq!(profile.columns[&id].distinct.value, 100.0);
        assert_eq!(
            profile.columns[&id].lower_bound,
            Some(ScalarValue::Int64(1))
        );
    }

    #[test]
    fn cardinality_estimation_applies_equality_filter() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);
        let id_ref = ExprData::ColumnRef(id).add(&mut ctx);
        let literal = ExprData::Literal(ScalarValue::Int64(7)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: id_ref,
            right: literal,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("users"),
                TableStatistics {
                    row_count: Some(100),
                    size_bytes: None,
                    column_statistics: [(
                        "id".to_string(),
                        ColumnStatistics {
                            lower_bound: Some(ScalarValue::Int64(1)),
                            upper_bound: Some(ScalarValue::Int64(100)),
                            frequency: Some(100),
                            distinct: Some(100),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, selection)
            .unwrap();

        assert_eq!(profile.rows.value, 1.0);
        assert_eq!(profile.columns[&id].distinct.value, 1.0);
        assert_eq!(
            profile.columns[&id].lower_bound,
            Some(ScalarValue::Int64(7))
        );
    }

    #[test]
    fn cardinality_estimation_shifts_simple_map_bounds() {
        let mut ctx = QueryContext::new();
        let value = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let shifted = ColumnData::new("shifted", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![value],
        })
        .add(&mut ctx);
        let value_ref = ExprData::ColumnRef(value).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let expr = ExprData::Binary {
            op: BinaryOp::Add,
            left: value_ref,
            right: one,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(shifted, expr)],
            input: scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("users"),
                TableStatistics {
                    row_count: Some(10),
                    size_bytes: None,
                    column_statistics: [(
                        "value".to_string(),
                        ColumnStatistics {
                            lower_bound: Some(ScalarValue::Int64(3)),
                            upper_bound: Some(ScalarValue::Int64(9)),
                            frequency: Some(10),
                            distinct: Some(7),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, map).unwrap();

        assert_eq!(
            profile.columns[&shifted].lower_bound,
            Some(ScalarValue::Int64(4))
        );
        assert_eq!(
            profile.columns[&shifted].upper_bound,
            Some(ScalarValue::Int64(10))
        );
        assert_eq!(profile.columns[&shifted].distinct.value, 7.0);
    }

    #[test]
    fn cardinality_estimation_caps_aggregation_by_group_ndv() {
        let mut ctx = QueryContext::new();
        let key = ColumnData::new("key", DataType::Int64).add(&mut ctx);
        let count = ColumnData::new("count", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![key],
        })
        .add(&mut ctx);
        let key_ref = ExprData::ColumnRef(key).add(&mut ctx);
        let aggregation = OperatorData::Aggregation(Aggregation {
            keys: vec![key_ref],
            aggregates: vec![(count, AggregateExpr::CountStar)],
            input: scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("users"),
                TableStatistics {
                    row_count: Some(100),
                    size_bytes: None,
                    column_statistics: [(
                        "key".to_string(),
                        ColumnStatistics {
                            lower_bound: None,
                            upper_bound: None,
                            frequency: Some(100),
                            distinct: Some(10),
                        },
                    )]
                    .into_iter()
                    .collect(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, aggregation)
            .unwrap();

        assert_eq!(profile.rows.value, 10.0);
        assert_eq!(profile.columns[&key].distinct.value, 10.0);
        assert_eq!(profile.columns[&count].frequency.value, 10.0);
    }

    #[test]
    fn cardinality_estimation_estimates_simple_equality_join() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let left_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("left_t"),
            columns: vec![left_key],
        })
        .add(&mut ctx);
        let right_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("right_t"),
            columns: vec![right_key],
        })
        .add(&mut ctx);
        let on = equality_expr(&mut ctx, left_key, right_key);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("left_t"), schema(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("right_t"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("left_t"),
                table_stats_for_column("left_key", 100, 100),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("right_t"),
                table_stats_for_column("right_key", 50, 50),
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, join).unwrap();

        assert_eq!(profile.rows.value, 50.0);
    }

    #[test]
    fn cardinality_estimation_uses_mark_join_nullable_distinct_bound() {
        for (marker_nullable, expected_upper) in [(false, Some(2.0)), (true, Some(3.0))] {
            let mut ctx = QueryContext::new();
            let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
            let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
            let marker = ColumnData::new("mark", DataType::Boolean).add(&mut ctx);
            let left_scan = OperatorData::Scan(Scan {
                table: TableRef::bare("left_t"),
                columns: vec![left_key],
            })
            .add(&mut ctx);
            let right_scan = OperatorData::Scan(Scan {
                table: TableRef::bare("right_t"),
                columns: vec![right_key],
            })
            .add(&mut ctx);
            let on = equality_expr(&mut ctx, left_key, right_key);
            let join = OperatorData::Join(Join {
                join_type: JoinType::LeftMark {
                    marker,
                    nullable: marker_nullable,
                },
                on,
                outer: left_scan,
                inner: right_scan,
            })
            .add(&mut ctx);

            let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
            catalog
                .create_table(TableRef::bare("left_t"), schema(), None)
                .unwrap();
            catalog
                .create_table(TableRef::bare("right_t"), schema(), None)
                .unwrap();
            catalog
                .set_table_statistics(
                    TableRef::bare("left_t"),
                    table_stats_for_column("left_key", 100, 100),
                )
                .unwrap();
            catalog
                .set_table_statistics(
                    TableRef::bare("right_t"),
                    table_stats_for_column("right_key", 50, 50),
                )
                .unwrap();
            let mut analyses = AnalysisContext::with_catalog(catalog);

            let profile = analyses.get::<CardinalityEstimationV1>(&ctx, join).unwrap();

            assert_eq!(profile.columns[&marker].distinct.upper, expected_upper);
        }
    }

    #[test]
    fn cardinality_estimation_ignores_redundant_equality_edges() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let rows = Estimate::exact(100.0);
        let profile = ColumnProfile {
            lower_bound: None,
            upper_bound: None,
            frequency: rows.clone(),
            distinct: Estimate::exact(100.0),
        };
        let left = CardinalityProfile::new(
            rows.clone(),
            [(a, profile.clone()), (b, profile.clone())]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(rows, [(c, profile)].into_iter().collect());

        let ab = equality_expr(&mut ctx, a, b);
        let bc = equality_expr(&mut ctx, b, c);
        let ac = equality_expr(&mut ctx, a, c);

        let selectivity = join_selectivity(&left, &right, &[ab, bc, ac], &ctx);

        assert_eq!(selectivity.value, 0.01);
    }

    #[test]
    fn cardinality_estimation_uses_pk_fk_containment_for_two_fk_tables() {
        let mut ctx = QueryContext::new();
        let title_id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let mi_movie_id = ColumnData::new("movie_id", DataType::Int64).add(&mut ctx);
        let mk_movie_id = ColumnData::new("movie_id", DataType::Int64).add(&mut ctx);
        let title = OperatorData::Scan(Scan {
            table: TableRef::bare("title"),
            columns: vec![title_id],
        })
        .add(&mut ctx);
        let mi = OperatorData::Scan(Scan {
            table: TableRef::bare("movie_info"),
            columns: vec![mi_movie_id],
        })
        .add(&mut ctx);
        let mk = OperatorData::Scan(Scan {
            table: TableRef::bare("movie_keyword"),
            columns: vec![mk_movie_id],
        })
        .add(&mut ctx);

        let on_title_mi = equality_expr(&mut ctx, title_id, mi_movie_id);
        let title_mi = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_title_mi,
            outer: title,
            inner: mi,
        })
        .add(&mut ctx);
        let on_title_mk = equality_expr(&mut ctx, title_id, mk_movie_id);
        let on_mi_mk = equality_expr(&mut ctx, mi_movie_id, mk_movie_id);
        let on = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![on_title_mk, on_mi_mk],
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: title_mi,
            inner: mk,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        for table in ["title", "movie_info", "movie_keyword"] {
            catalog
                .create_table(TableRef::bare(table), schema(), None)
                .unwrap();
        }
        catalog
            .set_table_statistics(
                TableRef::bare("title"),
                table_stats_for_column("id", 100, 100),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("movie_info"),
                table_stats_for_column("movie_id", 1_000, 100),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("movie_keyword"),
                table_stats_for_column("movie_id", 2_000, 100),
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, join).unwrap();

        assert_eq!(profile.rows.value, 20_000.0);
    }

    #[test]
    fn cardinality_estimation_picks_largest_ndv_for_parallel_edges() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let rows = Estimate::exact(10_000.0);
        let mut left = CardinalityProfile::new(
            rows.clone(),
            [
                (a, test_column_profile(10_000.0, 100.0)),
                (b, test_column_profile(10_000.0, 1_000.0)),
            ]
            .into_iter()
            .collect(),
        );
        left.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([a, b]),
            distinct: Estimate::exact(1_000.0),
        }];
        let right = CardinalityProfile::new(
            rows,
            [(c, test_column_profile(10_000.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let ac = equality_expr(&mut ctx, a, c);
        let bc = equality_expr(&mut ctx, b, c);

        let selectivity = join_selectivity(&left, &right, &[ac, bc], &ctx);

        assert_eq!(selectivity.value, 0.001);
    }

    #[test]
    fn cardinality_estimation_uses_match_probability_for_semi_and_anti() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let left_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("left_t"),
            columns: vec![left_key],
        })
        .add(&mut ctx);
        let right_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("right_t"),
            columns: vec![right_key],
        })
        .add(&mut ctx);
        let on = equality_expr(&mut ctx, left_key, right_key);
        let semi = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);
        let on = equality_expr(&mut ctx, left_key, right_key);
        let anti = OperatorData::Join(Join {
            join_type: JoinType::LeftAnti,
            on,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("left_t"), schema(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("right_t"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("left_t"),
                table_stats_for_column("left_key", 1_000, 1_000),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("right_t"),
                table_stats_for_column("right_key", 100, 1_000),
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, semi)
                .unwrap()
                .rows
                .value,
            100.0
        );
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, anti)
                .unwrap()
                .rows
                .value,
            900.0
        );
    }

    #[test]
    fn cardinality_estimation_preserves_outer_join_lower_bound() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let left_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("left_t"),
            columns: vec![left_key],
        })
        .add(&mut ctx);
        let right_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("right_t"),
            columns: vec![right_key],
        })
        .add(&mut ctx);
        let on = equality_expr(&mut ctx, left_key, right_key);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("left_t"), schema(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("right_t"), schema(), None)
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("left_t"),
                table_stats_for_column("left_key", 100, 1_000),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("right_t"),
                table_stats_for_column("right_key", 10, 1_000),
            )
            .unwrap();
        let mut analyses = AnalysisContext::with_catalog(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, join).unwrap();

        assert_eq!(profile.rows.value, 100.0);
        assert_eq!(profile.rows.lower, Some(100.0));
    }

    fn equality_expr(ctx: &mut QueryContext, left: Column, right: Column) -> Expr {
        let left = ExprData::ColumnRef(left).add(ctx);
        let right = ExprData::ColumnRef(right).add(ctx);
        ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(ctx)
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn table_stats_for_column(column: &str, rows: usize, distinct: usize) -> TableStatistics {
        TableStatistics {
            row_count: Some(rows),
            size_bytes: None,
            column_statistics: [(
                column.to_string(),
                ColumnStatistics {
                    lower_bound: None,
                    upper_bound: None,
                    frequency: Some(rows),
                    distinct: Some(distinct),
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    fn test_column_profile(rows: f64, distinct: f64) -> ColumnProfile {
        ColumnProfile {
            lower_bound: None,
            upper_bound: None,
            frequency: Estimate::exact(rows),
            distinct: Estimate::exact(distinct),
        }
    }
}

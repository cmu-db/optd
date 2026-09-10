use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::{
    AggregateExpr, AggregateFunction, BinaryOp, Catalog, Column, ColumnStatistics, Expr, ExprData,
    JoinType, NaryOp, NodeSet, Operator, OperatorData, QueryContext, QueryHypergraph, Relation,
    ScalarValue, Scan, UnaryOp,
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
    /// A catalog-aware analysis could not resolve a referenced table.
    Catalog(String),
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
            Self::Catalog(error) => write!(f, "catalog analysis failed: {error}"),
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
        Self::unknown_with_ndv_cap(rows, 100.0)
    }

    fn unknown_with_ndv_cap(rows: &Estimate, ndv_cap: f64) -> Self {
        Self {
            lower_bound: None,
            upper_bound: None,
            frequency: rows.clone(),
            distinct: Estimate::derived(rows.value.min(ndv_cap), Some(0.0), rows.upper),
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

    /// Returns a capped clone after an operator reduces its output row count.
    fn cap_by_rows(&self, rows: &Estimate) -> Self {
        let mut profile = self.clone();
        profile.cap_by_rows_mut(rows);
        profile
    }

    /// Restores `frequency <= rows` and `distinct <= frequency` on an owned output profile.
    ///
    /// `frequency` counts non-null values, so neither it nor the number of distinct non-null
    /// values can exceed the enclosing row count. [`Estimate::cap`] also tightens estimate bounds
    /// and marks their provenance as derived because the operator's row bound, rather than the
    /// original statistic alone, now constrains them. In-place mutation is safe only for a newly
    /// constructed or cloned output profile; cached `Arc` profiles are never mutated.
    fn cap_by_rows_mut(&mut self, rows: &Estimate) {
        self.frequency = self.frequency.cap(rows.value, EstimateSource::Derived);
        self.distinct = self
            .distinct
            .cap(self.frequency.value, EstimateSource::Derived);
    }
}

/// Estimated row and column profiles for an operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub struct CardinalityProfile {
    pub rows: Estimate,
    pub columns: BTreeMap<Column, ColumnProfile>,
    /// Nontrivial equality classes whose columns all occur in [`Self::columns`].
    ///
    /// Singleton classes are intentionally omitted: a single column's NDV is already stored in
    /// its [`ColumnProfile`]. Every stored class therefore contains at least two columns.
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
        Self::unknown_for_columns_with_ndv_cap(row_count, columns, 100.0)
    }

    fn unknown_for_columns_with_ndv_cap(
        row_count: f64,
        columns: impl IntoIterator<Item = Column>,
        ndv_cap: f64,
    ) -> Self {
        let rows = Estimate::default(row_count);
        let columns = columns
            .into_iter()
            .map(|column| (column, ColumnProfile::unknown_with_ndv_cap(&rows, ndv_cap)))
            .collect::<BTreeMap<_, _>>();
        Self::new(rows, columns)
    }

    pub fn new(rows: Estimate, columns: BTreeMap<Column, ColumnProfile>) -> Self {
        Self {
            rows,
            columns,
            equivalence_classes: Vec::new(),
        }
    }

    /// Clones a profile with a lower row estimate and reestablishes all column/class invariants.
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

/// Restricts equality classes to output columns and drops classes that become singletons.
///
/// Recomputing the class NDV from retained columns prevents a removed column's statistic from
/// continuing to constrain the projected profile.
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
        if kept.len() < 2 {
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

/// Filters owned equality classes in place.
///
/// Join-profile construction owns the newly derived classes, so rebuilding every `BTreeSet` and
/// the outer vector only creates allocator traffic. Other analysis paths still use the borrowed
/// helper because their input classes must remain intact. Retained class NDVs are recomputed for
/// the same reason as in [`filter_equivalence_classes`].
fn filter_owned_equivalence_classes(
    mut classes: Vec<ColumnEquivalenceClass>,
    columns: &BTreeMap<Column, ColumnProfile>,
) -> Vec<ColumnEquivalenceClass> {
    classes.retain_mut(|class| {
        class.columns.retain(|column| columns.contains_key(column));
        if class.columns.len() < 2 {
            return false;
        }
        class.distinct = class
            .columns
            .iter()
            .filter_map(|column| columns.get(column))
            .map(|profile| profile.distinct.clone())
            .max_by(|a, b| a.value.total_cmp(&b.value))
            .unwrap_or_else(|| class.distinct.clone());
        true
    });
    classes
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
            (columns.len() >= 2).then(|| ColumnEquivalenceClass {
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

/// Cardinality and column-profile analysis for one operator.
#[derive(Default)]
pub struct CardinalityEstimationV1 {
    state: OperatorAnalysisState<Arc<CardinalityProfile>>,
}

/// Tunable assumptions and opt-in predicate semantics used by [`CardinalityEstimationV1`].
///
/// Every predicate-semantics switch defaults to `false`, while the numeric
/// values retain the constants already used by V1. The complete default is
/// therefore behaviorally identical to the estimator before configuration was
/// introduced.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CardinalityEstimationConfig {
    /// Looks through proven injective, order-preserving casts and evaluates
    /// supported typed literal arithmetic while recognizing predicates.
    pub normalize_filter_operands: bool,
    /// Partitions the known non-null population between `LIKE` and `NOT LIKE`
    /// using [`Self::like_selectivity`].
    pub honor_negated_like: bool,
    /// Combines range conjuncts on one column into one interval estimate.
    pub correlate_same_column_ranges: bool,
    /// Marks columns as non-null after predicates that reject nulls.
    pub honor_filter_null_rejection: bool,
    pub like_selectivity: f64,
    pub default_predicate_selectivity: f64,
    pub range_fallback_selectivity: f64,
    pub unknown_scan_rows: f64,
    pub unknown_column_ndv_cap: f64,
}

/// Invalid cardinality-estimation configuration supplied by a caller.
#[derive(Debug, Clone, PartialEq)]
pub enum CardinalityEstimationConfigError {
    /// A selectivity was not finite or fell outside the inclusive `[0, 1]` range.
    InvalidSelectivity { field: &'static str, value: f64 },
    /// A row-count or NDV assumption was not finite or was negative.
    InvalidNonNegative { field: &'static str, value: f64 },
}

impl fmt::Display for CardinalityEstimationConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSelectivity { field, value } => write!(
                f,
                "cardinality configuration {field} must be finite and in [0, 1], got {value}"
            ),
            Self::InvalidNonNegative { field, value } => write!(
                f,
                "cardinality configuration {field} must be finite and nonnegative, got {value}"
            ),
        }
    }
}

impl std::error::Error for CardinalityEstimationConfigError {}

impl CardinalityEstimationConfig {
    /// Validates every numeric assumption before it is installed on an analysis context.
    ///
    /// Selectivities must be finite and within `[0, 1]`. Row-count and NDV
    /// assumptions must be finite and nonnegative. Invalid values are rejected;
    /// they are never silently clamped.
    pub fn validate(&self) -> Result<(), CardinalityEstimationConfigError> {
        for (field, value) in [
            ("like_selectivity", self.like_selectivity),
            (
                "default_predicate_selectivity",
                self.default_predicate_selectivity,
            ),
            (
                "range_fallback_selectivity",
                self.range_fallback_selectivity,
            ),
        ] {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(CardinalityEstimationConfigError::InvalidSelectivity { field, value });
            }
        }
        for (field, value) in [
            ("unknown_scan_rows", self.unknown_scan_rows),
            ("unknown_column_ndv_cap", self.unknown_column_ndv_cap),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(CardinalityEstimationConfigError::InvalidNonNegative { field, value });
            }
        }
        Ok(())
    }
}

impl Default for CardinalityEstimationConfig {
    fn default() -> Self {
        Self {
            normalize_filter_operands: false,
            honor_negated_like: false,
            correlate_same_column_ranges: false,
            honor_filter_null_rejection: false,
            like_selectivity: 0.1,
            default_predicate_selectivity: 0.25,
            range_fallback_selectivity: 0.33,
            unknown_scan_rows: 1000.0,
            unknown_column_ndv_cap: 100.0,
        }
    }
}

/// Exact logical proof that an operator can produce at most one row.
///
/// Unlike cardinality estimation, this property never relies on catalog
/// statistics or heuristic selectivity.
#[derive(Default)]
pub struct AtMostOneRow {
    state: OperatorAnalysisState<bool>,
}

/// Registry of lazily-created analysis instances.
pub struct AnalysisContext {
    analyses: AnalysisRegistry,
    catalog: Arc<dyn Catalog>,
    cardinality_config: CardinalityEstimationConfig,
}

impl AnalysisContext {
    /// Creates analysis state backed by `catalog`.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog,
            cardinality_config: CardinalityEstimationConfig::default(),
        }
    }

    /// Uses explicit V1 cardinality configuration for this analysis context.
    ///
    /// Returns an error when any value violates
    /// [`CardinalityEstimationConfig::validate`].
    pub fn with_cardinality_estimation_config(
        mut self,
        config: CardinalityEstimationConfig,
    ) -> Result<Self, CardinalityEstimationConfigError> {
        self.set_cardinality_estimation_config(config)?;
        Ok(self)
    }

    /// Replaces the V1 cardinality configuration used by this analysis context.
    ///
    /// Validation happens before mutation, so an invalid configuration leaves
    /// both the current configuration and all cached analysis results intact.
    /// Changing to a valid configuration invalidates every existing cache.
    pub fn set_cardinality_estimation_config(
        &mut self,
        config: CardinalityEstimationConfig,
    ) -> Result<(), CardinalityEstimationConfigError> {
        config.validate()?;
        if self.cardinality_config != config {
            self.clear();
        }
        self.cardinality_config = config;
        Ok(())
    }

    /// Returns the configuration used by cardinality estimation.
    pub fn cardinality_estimation_config(&self) -> CardinalityEstimationConfig {
        self.cardinality_config
    }

    /// Returns the catalog used by every analysis in this context.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Creates a fresh cache that uses the same catalog as this context.
    pub fn fork(&self) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog: Arc::clone(&self.catalog),
            cardinality_config: self.cardinality_config,
        }
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
pub fn expr_used_columns(
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
    expr: Expr,
) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();
    collect_expr_used_columns(ctx, analyses, expr, &mut columns)?;
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
        OperatorData::Scan(data) => scan_column_nullability(data, analyses),
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

fn scan_column_nullability(
    scan: &Scan,
    analyses: &AnalysisContext,
) -> AnalysisResult<Vec<(Column, bool)>> {
    let metadata = analyses
        .catalog
        .table_by_ref(&scan.table)
        .map_err(|error| AnalysisError::Catalog(error.to_string()))?;

    Ok(scan
        .columns
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
        .collect())
}

impl CachedAnalysis for AtMostOneRow {
    type Output = bool;

    fn state(&self) -> &OperatorAnalysisState<Self::Output> {
        &self.state
    }

    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output> {
        let at_most_one = match operator.get(ctx) {
            OperatorData::Scan(_) | OperatorData::TableFunction(_) => false,
            OperatorData::ConstScan(data) => data.rows.len() <= 1,
            OperatorData::Selection(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Projection(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Output(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Sort(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Limit(data) => {
                data.fetch.is_some_and(|fetch| fetch <= 1)
                    || analyses.get::<Self>(ctx, data.input)?
            }
            OperatorData::Rename(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Map(data) => analyses.get::<Self>(ctx, data.input)?,
            OperatorData::Aggregation(data) => {
                data.keys.is_empty() || analyses.get::<Self>(ctx, data.input)?
            }
            OperatorData::CrossProduct(data) => {
                analyses.get::<Self>(ctx, data.outer)? && analyses.get::<Self>(ctx, data.inner)?
            }
            OperatorData::Join(data) => match data.join_type {
                JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark { .. }
                | JoinType::Single => analyses.get::<Self>(ctx, data.outer)?,
                JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter => {
                    analyses.get::<Self>(ctx, data.outer)?
                        && analyses.get::<Self>(ctx, data.inner)?
                }
                JoinType::FullOuter => false,
            },
        };
        Ok(at_most_one)
    }
}

impl Analysis for AtMostOneRow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_cache();
    }
}

impl Analyzable for AtMostOneRow {
    type Value = bool;

    fn get(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Self::Value> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

impl CachedAnalysis for CardinalityEstimationV1 {
    type Output = Arc<CardinalityProfile>;

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
        Ok(Self::get_shared(ctx, analyses, op)?.as_ref().clone())
    }
}

impl CardinalityEstimationV1 {
    /// Returns a shared cardinality profile for internal consumers that only need to inspect it.
    ///
    /// [`Analyzable::get`] deliberately preserves the public owned-value API, which costing still
    /// uses. Recursive cardinality estimation uses this path to avoid cloning every column profile
    /// on a cache hit.
    pub(crate) fn get_shared(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        op: Operator,
    ) -> AnalysisResult<Arc<CardinalityProfile>> {
        let analysis = analyses.registry_entry::<Self>();
        typed_analysis::<Self>(&analysis)?.get_cached(ctx, analyses, op)
    }
}

fn cardinality_profile(
    operator: Operator,
    ctx: &QueryContext,
    analyses: &mut AnalysisContext,
) -> AnalysisResult<Arc<CardinalityProfile>> {
    let config = analyses.cardinality_estimation_config();
    match operator.get(ctx) {
        OperatorData::Scan(scan) => scan_profile(scan, ctx, analyses, &config).map(Arc::new),
        OperatorData::ConstScan(data) => Ok(Arc::new(const_scan_profile(data, ctx))),
        OperatorData::TableFunction(data) => Ok(Arc::new(
            CardinalityProfile::unknown_for_columns_with_ndv_cap(
                config.unknown_scan_rows,
                data.columns.clone(),
                config.unknown_column_ndv_cap,
            ),
        )),
        OperatorData::Selection(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(apply_selection_profile(
                input.as_ref().clone(),
                data.predicate,
                ctx,
                &config,
            )))
        }
        OperatorData::Projection(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(project_profile(&input, &data.columns)))
        }
        OperatorData::Output(data) => {
            CardinalityEstimationV1::get_shared(ctx, analyses, data.input)
        }
        OperatorData::Sort(data) => CardinalityEstimationV1::get_shared(ctx, analyses, data.input),
        OperatorData::Limit(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            let rows = match data.fetch {
                Some(fetch) => input.rows.cap(fetch as f64, EstimateSource::Derived),
                None => input.rows.clone(),
            };
            Ok(Arc::new(input.cap_by_rows(rows)))
        }
        OperatorData::Rename(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(rename_profile(&input, &data.defs)))
        }
        OperatorData::Map(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(map_profile(
                &input,
                &data.computations,
                ctx,
                &config,
            )))
        }
        OperatorData::Aggregation(data) => {
            let input = CardinalityEstimationV1::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(aggregation_profile(&input, data, ctx, &config)))
        }
        OperatorData::CrossProduct(data) => {
            let left = CardinalityEstimationV1::get_shared(ctx, analyses, data.outer)?;
            let right = CardinalityEstimationV1::get_shared(ctx, analyses, data.inner)?;
            Ok(Arc::new(cross_product_profile(&left, &right)))
        }
        OperatorData::Join(data) => {
            let left = CardinalityEstimationV1::get_shared(ctx, analyses, data.outer)?;
            let right = CardinalityEstimationV1::get_shared(ctx, analyses, data.inner)?;
            Ok(Arc::new(join_profile_from_predicate_with_config(
                &left,
                &right,
                data.join_type.clone(),
                data.on,
                ctx,
                &config,
            )))
        }
    }
}

fn scan_profile(
    scan: &Scan,
    ctx: &QueryContext,
    analyses: &AnalysisContext,
    config: &CardinalityEstimationConfig,
) -> AnalysisResult<CardinalityProfile> {
    let catalog_stats = analyses
        .catalog
        .table_by_ref(&scan.table)
        .map_err(|error| AnalysisError::Catalog(error.to_string()))?
        .statistics;

    let row_estimate = catalog_stats
        .as_ref()
        .and_then(|stats| stats.row_count)
        .map(|rows| Estimate::catalog(rows as f64))
        .unwrap_or_else(|| Estimate::default(config.unknown_scan_rows));

    let mut columns = BTreeMap::new();
    for column in &scan.columns {
        let column_name = &ctx.column(*column).name;
        let catalog_column = catalog_stats
            .as_ref()
            .and_then(|stats| stats.column_statistics.get(column_name));
        let profile = catalog_column
            .map(|stats| column_profile_from_stats(stats, &row_estimate))
            .unwrap_or_else(|| default_scan_column_profile(&row_estimate, config));
        columns.insert(*column, profile);
    }

    Ok(CardinalityProfile::new(row_estimate, columns))
}

fn column_profile_from_stats(stats: &ColumnStatistics, rows: &Estimate) -> ColumnProfile {
    let frequency = stats
        .frequency
        .map(|value| Estimate::catalog(value as f64))
        .unwrap_or_else(|| Estimate::default(rows.value));
    let distinct_value = stats
        .distinct
        .map_or(frequency.value.min(rows.value), |value| value as f64);
    let distinct_value = distinct_value.min(frequency.value);
    let distinct = stats
        .distinct
        .map(|_| Estimate::catalog(distinct_value))
        .unwrap_or_else(|| Estimate::default(distinct_value));
    ColumnProfile {
        lower_bound: stats.lower_bound.clone(),
        upper_bound: stats.upper_bound.clone(),
        frequency,
        distinct,
    }
}

fn default_scan_column_profile(
    rows: &Estimate,
    config: &CardinalityEstimationConfig,
) -> ColumnProfile {
    ColumnProfile {
        lower_bound: None,
        upper_bound: None,
        frequency: Estimate::default(rows.value),
        distinct: Estimate::default(rows.value.min(config.unknown_column_ndv_cap)),
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
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    let mut output = input.clone();
    for (column, expr) in computations {
        let profile = profile_for_computation(input, *expr, ctx).unwrap_or_else(|| {
            ColumnProfile::unknown_with_ndv_cap(&input.rows, config.unknown_column_ndv_cap)
        });
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
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    let mut group_rows = if data.keys.is_empty() {
        Estimate::exact(1.0)
    } else {
        let product = data.keys.iter().fold(1.0, |acc, expr| {
            if let ExprData::ColumnRef(column) = expr.get(ctx) {
                acc * input.columns.get(column).map_or(
                    input.rows.value.min(config.unknown_column_ndv_cap),
                    |profile| profile.distinct.value,
                )
            } else {
                acc * input.rows.value.min(config.unknown_column_ndv_cap)
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
            _ => ColumnProfile::unknown_with_ndv_cap(&group_rows, config.unknown_column_ndv_cap),
        };
        columns.insert(*column, profile);
    }
    CardinalityProfile::new(group_rows, columns)
}

fn apply_selection_profile(
    mut profile: CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    // The caller passes an owned clone of the cached input, so each conjunct can safely tighten it
    // in place while later conjuncts observe the preceding selectivity and bounds.
    let conjuncts = conjuncts(predicate, ctx);
    let (range_selectivities, grouped_range_indices) = if config.correlate_same_column_ranges {
        same_column_range_selectivities(&profile, &conjuncts, ctx, config)
    } else {
        (BTreeMap::new(), HashSet::new())
    };
    for (index, &conjunct) in conjuncts.iter().enumerate() {
        if let Some(selectivity) = range_selectivities.get(&index) {
            profile = scale_profile(profile, *selectivity);
        } else if !grouped_range_indices.contains(&index) {
            let selectivity = filter_selectivity(&profile, conjunct, ctx, config);
            profile = scale_profile(profile, selectivity.value);
        }
        tighten_filter_columns(&mut profile, conjunct, ctx, config);
    }
    profile
}

fn same_column_range_selectivities(
    profile: &CardinalityProfile,
    conjuncts: &[Expr],
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> (BTreeMap<usize, f64>, HashSet<usize>) {
    let mut constraints = BTreeMap::<Column, Vec<RangeConstraint>>::new();
    for (index, &conjunct) in conjuncts.iter().enumerate() {
        let ExprData::Binary { op, left, right } = conjunct.get(ctx) else {
            continue;
        };
        let Some((column, literal, normalized_op)) =
            column_literal_predicate(*op, *left, *right, ctx, config)
        else {
            continue;
        };
        if !matches!(
            normalized_op,
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
        ) {
            continue;
        }
        if scalar_to_f64(&literal).is_none() {
            continue;
        }
        constraints
            .entry(column)
            .or_default()
            .push(RangeConstraint {
                index,
                op: normalized_op,
                literal,
            });
    }

    let mut selectivities = BTreeMap::new();
    let mut grouped_indices = HashSet::new();
    for (column, constraints) in constraints {
        if constraints.len() < 2 {
            continue;
        }
        let Some(column_profile) = profile.columns.get(&column) else {
            continue;
        };
        let selectivity = range_interval_selectivity(column_profile, &constraints, config);
        let first_index = constraints
            .iter()
            .map(|constraint| constraint.index)
            .min()
            .expect("range group is non-empty");
        selectivities.insert(first_index, selectivity);
        grouped_indices.extend(constraints.into_iter().map(|constraint| constraint.index));
    }
    (selectivities, grouped_indices)
}

fn range_interval_selectivity(
    profile: &ColumnProfile,
    constraints: &[RangeConstraint],
    config: &CardinalityEstimationConfig,
) -> f64 {
    let Some(domain_min) = profile.lower_bound.as_ref() else {
        return config.range_fallback_selectivity;
    };
    let Some(domain_max) = profile.upper_bound.as_ref() else {
        return config.range_fallback_selectivity;
    };
    if scalar_cmp(domain_min, domain_max) == Some(std::cmp::Ordering::Greater) {
        return config.range_fallback_selectivity;
    }

    let mut lower = RangeEndpoint {
        value: domain_min.clone(),
        inclusive: true,
    };
    let mut upper = RangeEndpoint {
        value: domain_max.clone(),
        inclusive: true,
    };
    for constraint in constraints {
        if scalar_cmp(domain_min, &constraint.literal).is_none()
            || scalar_cmp(domain_max, &constraint.literal).is_none()
        {
            return config.range_fallback_selectivity;
        }
        let endpoint = RangeEndpoint {
            value: constraint.literal.clone(),
            inclusive: matches!(constraint.op, BinaryOp::GtEq | BinaryOp::LtEq),
        };
        match constraint.op {
            BinaryOp::Lt | BinaryOp::LtEq => tighten_upper_endpoint(&mut upper, endpoint),
            BinaryOp::Gt | BinaryOp::GtEq => tighten_lower_endpoint(&mut lower, endpoint),
            _ => unreachable!("only range constraints are grouped"),
        }
    }

    let Some(ordering) = scalar_cmp(&lower.value, &upper.value) else {
        return config.range_fallback_selectivity;
    };
    if ordering == std::cmp::Ordering::Greater
        || (ordering == std::cmp::Ordering::Equal && !(lower.inclusive && upper.inclusive))
    {
        return 0.0;
    }
    if ordering == std::cmp::Ordering::Equal {
        if scalar_cmp(domain_min, domain_max) == Some(std::cmp::Ordering::Equal) {
            return 1.0;
        }
        return (1.0 / profile.distinct.value.max(1.0)).clamp(0.0, 1.0);
    }

    let Some(min) = scalar_to_f64(domain_min) else {
        return config.range_fallback_selectivity;
    };
    let Some(max) = scalar_to_f64(domain_max) else {
        return config.range_fallback_selectivity;
    };
    let Some(lower) = scalar_to_f64(&lower.value) else {
        return config.range_fallback_selectivity;
    };
    let Some(upper) = scalar_to_f64(&upper.value) else {
        return config.range_fallback_selectivity;
    };
    let width = (max - min).abs();
    if width == 0.0 || !width.is_finite() {
        return config.range_fallback_selectivity;
    }
    ((upper - lower) / width).clamp(0.0, 1.0)
}

#[derive(Clone)]
struct RangeConstraint {
    index: usize,
    op: BinaryOp,
    literal: ScalarValue,
}

struct RangeEndpoint {
    value: ScalarValue,
    inclusive: bool,
}

fn tighten_lower_endpoint(current: &mut RangeEndpoint, candidate: RangeEndpoint) {
    match scalar_cmp(&candidate.value, &current.value) {
        Some(std::cmp::Ordering::Greater) => *current = candidate,
        Some(std::cmp::Ordering::Equal) => current.inclusive &= candidate.inclusive,
        _ => {}
    }
}

fn tighten_upper_endpoint(current: &mut RangeEndpoint, candidate: RangeEndpoint) {
    match scalar_cmp(&candidate.value, &current.value) {
        Some(std::cmp::Ordering::Less) => *current = candidate,
        Some(std::cmp::Ordering::Equal) => current.inclusive &= candidate.inclusive,
        _ => {}
    }
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

pub(crate) fn cross_product_profile(
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

/// Estimates a join profile from an arbitrary predicate expression.
///
/// This is the expression-tree entry point: it flattens nested conjunctions exactly once before
/// delegating to [`join_profile_from_conjuncts`].
#[cfg(test)]
fn join_profile_from_predicate(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    predicate: Expr,
    ctx: &QueryContext,
) -> CardinalityProfile {
    join_profile_from_predicate_with_config(
        left,
        right,
        join_type,
        predicate,
        ctx,
        &CardinalityEstimationConfig::default(),
    )
}

fn join_profile_from_predicate_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    let conjuncts = conjuncts(predicate, ctx);
    join_profile_from_conjuncts_with_config(left, right, join_type, &conjuncts, ctx, config)
}

/// Estimates a join profile from predicates that are already flattened into atomic conjuncts.
///
/// Join enumeration can use this entry point when hypergraph edges already carry individual
/// conjuncts, avoiding expression reconstruction and another flattening pass.
#[cfg(test)]
pub(crate) fn join_profile_from_conjuncts(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    conjuncts: &[Expr],
    ctx: &QueryContext,
) -> CardinalityProfile {
    join_profile_from_conjuncts_with_config(
        left,
        right,
        join_type,
        conjuncts,
        ctx,
        &CardinalityEstimationConfig::default(),
    )
}

fn join_profile_from_conjuncts_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    conjuncts: &[Expr],
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    debug_assert!(
        conjuncts.iter().all(|predicate| !matches!(
            predicate.get(ctx),
            ExprData::Nary {
                op: NaryOp::And,
                ..
            }
        )),
        "join_profile_from_conjuncts requires atomic conjuncts"
    );
    let estimate = join_selectivity_from_conjuncts_with_config(left, right, conjuncts, ctx, config);
    join_profile_with_selectivity_and_classes(left, right, join_type, estimate)
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
            // ON equalities hold only for matched rows. Null extension therefore preserves
            // equality knowledge from the non-null-supplying input, but not from the right input
            // or across the join boundary.
            left.equivalence_classes.clone(),
        ),
        JoinType::RightOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(right.rows.value),
            right.equivalence_classes.clone(),
        ),
        JoinType::FullOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(left.rows.value.max(right.rows.value)),
            Vec::new(),
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
        rows.upper = rows.upper.map(|upper| upper.max(min_rows));
    }
    let mut columns = left.columns.clone();
    let mut right_columns = right.columns.clone();
    // Preserve the established right-input precedence if malformed/derived plans reuse a column
    // handle across inputs. Both maps are owned here, so append and subsequent capping cannot
    // mutate either cached input profile.
    columns.append(&mut right_columns);
    columns
        .values_mut()
        .for_each(|profile| profile.cap_by_rows_mut(&rows));
    let equivalence_classes = filter_owned_equivalence_classes(equivalence_classes, &columns);
    CardinalityProfile {
        rows,
        columns,
        equivalence_classes,
    }
}

#[cfg(test)]
fn join_selectivity(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    conjuncts: &[Expr],
    ctx: &QueryContext,
) -> Estimate {
    join_selectivity_from_conjuncts_with_config(
        left,
        right,
        conjuncts,
        ctx,
        &CardinalityEstimationConfig::default(),
    )
    .selectivity
}

// ---------------------------------------------------------------------------
// Selectivity estimation
// ---------------------------------------------------------------------------

struct JoinSelectivityEstimate {
    selectivity: Estimate,
    equivalence_classes: Vec<ColumnEquivalenceClass>,
    match_probability: Estimate,
    #[cfg(test)]
    equivalence_state_columns: usize,
}

#[cfg(test)]
fn join_selectivity_from_conjuncts(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    conjuncts: &[Expr],
    ctx: &QueryContext,
) -> JoinSelectivityEstimate {
    join_selectivity_from_conjuncts_with_config(
        left,
        right,
        conjuncts,
        ctx,
        &CardinalityEstimationConfig::default(),
    )
}

fn join_selectivity_from_conjuncts_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    conjuncts: &[Expr],
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> JoinSelectivityEstimate {
    let mut equality_pairs = Vec::new();
    let mut residual_selectivity = 1.0;
    for &predicate in conjuncts {
        if let Some((left_col, right_col)) = column_equality(predicate, ctx, config) {
            equality_pairs.push((left_col, right_col));
        } else {
            residual_selectivity *=
                filter_selectivity_for_predicate(left, right, predicate, ctx, config).value;
        }
    }

    let mut classes = EquivalenceClassState::from_profiles_and_equalities(
        left,
        right,
        &equality_pairs,
        config.unknown_column_ndv_cap,
    );
    let mut equality_edges = equality_pairs
        .into_iter()
        .map(|(left_col, right_col)| EqualityEdge {
            left: left_col,
            right: right_col,
            chosen_ndv: classes
                .class_distinct(left_col)
                .max_by_value(classes.class_distinct(right_col)),
        })
        .collect::<Vec<_>>();

    // Process the most selective equality first, then union equivalent columns.
    // This avoids multiplying selectivity again for transitive predicates such
    // as a = b AND b = c AND a = c.
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
    #[cfg(test)]
    let equivalence_state_columns = classes.tracked_column_count();
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
        #[cfg(test)]
        equivalence_state_columns,
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
    config: &CardinalityEstimationConfig,
) -> Estimate {
    filter_selectivity_for_predicate(profile, profile, predicate, ctx, config)
}

fn filter_selectivity_for_predicate(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
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
                .map(|expr| filter_selectivity_for_predicate(left, right, *expr, ctx, config).value)
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
                    1.0 - filter_selectivity_for_predicate(left, right, *expr, ctx, config).value;
            }
            Estimate::derived(1.0 - not_selected, Some(0.0), Some(1.0))
        }
        ExprData::Binary {
            op,
            left: l,
            right: r,
        } => binary_selectivity(left, right, *op, *l, *r, ctx, config),
        ExprData::Unary {
            op: UnaryOp::IsNull,
            expr,
        } => {
            let column = predicate_column_ref(*expr, ctx, config);
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
            let column = predicate_column_ref(*expr, ctx, config);
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
        ExprData::Like { negated, expr, .. } => {
            let selectivity = if config.honor_negated_like {
                let non_null_fraction =
                    predicate_non_null_fraction(left, right, *expr, ctx, config);
                non_null_fraction
                    * if *negated {
                        1.0 - config.like_selectivity
                    } else {
                        config.like_selectivity
                    }
            } else {
                config.like_selectivity
            };
            Estimate::derived(selectivity, Some(0.0), Some(1.0))
        }
        _ => Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0)),
    }
}

fn predicate_non_null_fraction(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    expr: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> f64 {
    let Some(column) = predicate_column_ref(expr, ctx, config) else {
        return 1.0;
    };
    for profile in [left, right] {
        if let Some(column_profile) = profile.columns.get(&column) {
            return (column_profile.frequency.value / profile.rows.value.max(1.0)).clamp(0.0, 1.0);
        }
    }
    1.0
}

fn binary_selectivity(
    left_profile: &CardinalityProfile,
    right_profile: &CardinalityProfile,
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Estimate {
    if let Some((column, literal, normalized_op)) =
        column_literal_predicate(op, left, right, ctx, config)
        && let Some(profile) = left_profile
            .columns
            .get(&column)
            .or_else(|| right_profile.columns.get(&column))
    {
        return match normalized_op {
            BinaryOp::Eq => {
                Estimate::derived(1.0 / profile.distinct.value.max(1.0), Some(0.0), Some(1.0))
            }
            BinaryOp::NotEq => Estimate::derived(
                1.0 - (1.0 / profile.distinct.value.max(1.0)),
                Some(0.0),
                Some(1.0),
            ),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                range_selectivity(profile, &literal, normalized_op, config)
            }
            _ => Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0)),
        };
    }
    if let Some((left_col, right_col)) = column_equality_from_parts(op, left, right, ctx, config) {
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
    Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0))
}

fn tighten_filter_columns(
    profile: &mut CardinalityProfile,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) {
    if let ExprData::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = predicate.get(ctx)
        && let Some((column, literal, _)) =
            column_literal_predicate(BinaryOp::Eq, *left, *right, ctx, config)
        && let Some(column_profile) = profile.columns.get_mut(&column)
    {
        column_profile.lower_bound = Some(literal.clone());
        column_profile.upper_bound = Some(literal);
        column_profile.distinct = Estimate::derived(1.0, Some(0.0), Some(1.0));
        column_profile.frequency = column_profile
            .frequency
            .cap(profile.rows.value, EstimateSource::Derived);
    }
    if config.honor_filter_null_rejection {
        for column in filter_null_rejected_columns(predicate, ctx, config) {
            if let Some(column_profile) = profile.columns.get_mut(&column) {
                column_profile.frequency = profile.rows.clone();
                column_profile.distinct = column_profile
                    .distinct
                    .cap(profile.rows.value, EstimateSource::Derived);
            }
        }
    }
}

fn filter_null_rejected_columns(
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> BTreeSet<Column> {
    match predicate.get(ctx) {
        ExprData::Binary {
            op:
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq,
            left,
            right,
        } => [
            predicate_column_ref(*left, ctx, config),
            predicate_column_ref(*right, ctx, config),
        ]
        .into_iter()
        .flatten()
        .collect(),
        ExprData::Unary {
            op: UnaryOp::IsNotNull,
            expr,
        } => predicate_column_ref(*expr, ctx, config)
            .into_iter()
            .collect(),
        ExprData::Unary {
            op: UnaryOp::Not,
            expr,
        } => match expr.get(ctx) {
            ExprData::Unary {
                op: UnaryOp::IsNull,
                expr,
            } => predicate_column_ref(*expr, ctx, config)
                .into_iter()
                .collect(),
            _ => BTreeSet::new(),
        },
        ExprData::Like { expr, .. } => predicate_column_ref(*expr, ctx, config)
            .into_iter()
            .collect(),
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .iter()
            .flat_map(|expr| filter_null_rejected_columns(*expr, ctx, config))
            .collect(),
        ExprData::Nary {
            op: NaryOp::Or,
            exprs,
        } => {
            let mut iter = exprs.iter();
            let Some(first) = iter.next() else {
                return BTreeSet::new();
            };
            let mut intersection = filter_null_rejected_columns(*first, ctx, config);
            for expr in iter {
                let branch = filter_null_rejected_columns(*expr, ctx, config);
                intersection.retain(|column| branch.contains(column));
            }
            intersection
        }
        _ => BTreeSet::new(),
    }
}

fn range_selectivity(
    profile: &ColumnProfile,
    literal: &ScalarValue,
    op: BinaryOp,
    config: &CardinalityEstimationConfig,
) -> Estimate {
    if !config.correlate_same_column_ranges {
        let Some(min) = profile.lower_bound.as_ref().and_then(scalar_to_f64) else {
            return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
        };
        let Some(max) = profile.upper_bound.as_ref().and_then(scalar_to_f64) else {
            return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
        };
        let Some(value) = scalar_to_f64(literal) else {
            return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
        };
        let width = (max - min).abs().max(1.0);
        let selected = match op {
            BinaryOp::Lt | BinaryOp::LtEq => ((value - min) / width).clamp(0.0, 1.0),
            BinaryOp::Gt | BinaryOp::GtEq => ((max - value) / width).clamp(0.0, 1.0),
            _ => config.range_fallback_selectivity,
        };
        return Estimate::derived(selected, Some(0.0), Some(1.0));
    }
    let constraint = RangeConstraint {
        index: 0,
        op,
        literal: literal.clone(),
    };
    let selected = range_interval_selectivity(profile, &[constraint], config);
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

fn predicate_column_ref(
    expr: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<Column> {
    normalized_column_ref(expr, ctx, config).map(|normalized| normalized.column)
}

struct NormalizedColumnRef {
    column: Column,
    effective_type: arrow_schema::DataType,
    stripped_cast: bool,
}

fn normalized_column_ref(
    expr: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<NormalizedColumnRef> {
    match expr.get(ctx) {
        ExprData::ColumnRef(column) => Some(NormalizedColumnRef {
            column: *column,
            effective_type: ctx.column(*column).ty.clone(),
            stripped_cast: false,
        }),
        ExprData::Cast { expr, ty } if config.normalize_filter_operands => {
            let mut normalized = normalized_column_ref(*expr, ctx, config)?;
            estimation_safe_column_cast(&normalized.effective_type, ty)?;
            normalized.effective_type = ty.clone();
            normalized.stripped_cast = true;
            Some(normalized)
        }
        _ => None,
    }
}

fn estimation_safe_column_cast(
    source: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
) -> Option<()> {
    use arrow_schema::DataType;

    let safe = source == target
        || matches!(
            (source, target),
            (DataType::Int32, DataType::Int64 | DataType::Float64)
        )
        || matches!(
            (source, target),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        )
        || matches!(
            (source, target),
            (DataType::Decimal128(source_precision, source_scale), DataType::Decimal128(target_precision, target_scale))
                if source_scale == target_scale && source_precision <= target_precision
        );
    safe.then_some(())
}

fn predicate_literal(
    expr: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<ScalarValue> {
    if !config.normalize_filter_operands {
        return match expr.get(ctx) {
            ExprData::Literal(value) => Some(value.clone()),
            _ => None,
        };
    }
    evaluate_literal_expr(expr, ctx).map(|evaluated| evaluated.value)
}

struct EvaluatedLiteral {
    value: ScalarValue,
    ty: arrow_schema::DataType,
}

fn evaluate_literal_expr(expr: Expr, ctx: &QueryContext) -> Option<EvaluatedLiteral> {
    match expr.get(ctx) {
        ExprData::Literal(value) => Some(EvaluatedLiteral {
            value: value.clone(),
            ty: value.data_type(),
        }),
        ExprData::Cast { expr, ty } => {
            let input = evaluate_literal_expr(*expr, ctx)?;
            let value = cast_literal(input.value, &input.ty, ty)?;
            Some(EvaluatedLiteral {
                value,
                ty: ty.clone(),
            })
        }
        ExprData::Unary {
            op: UnaryOp::Negate,
            expr,
        } => {
            let input = evaluate_literal_expr(*expr, ctx)?;
            let value = negate_literal(input.value)?;
            Some(EvaluatedLiteral {
                value,
                ty: input.ty,
            })
        }
        ExprData::Binary {
            op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide),
            left,
            right,
        } => {
            let left = evaluate_literal_expr(*left, ctx)?;
            let right = evaluate_literal_expr(*right, ctx)?;
            evaluate_literal_arithmetic(*op, left, right)
        }
        _ => None,
    }
}

fn cast_literal(
    value: ScalarValue,
    source: &arrow_schema::DataType,
    target: &arrow_schema::DataType,
) -> Option<ScalarValue> {
    use arrow_schema::DataType;

    if source == target {
        return Some(value);
    }
    match (value, source, target) {
        (ScalarValue::Int32(value), DataType::Int32, DataType::Int64) => {
            Some(ScalarValue::Int64(i64::from(value)))
        }
        (ScalarValue::Int32(value), DataType::Int32, DataType::Float64) => {
            Some(ScalarValue::Float64(f64::from(value)))
        }
        (ScalarValue::Int64(value), DataType::Int64, DataType::Int32) => {
            i32::try_from(value).ok().map(ScalarValue::Int32)
        }
        (ScalarValue::Int64(value), DataType::Int64, DataType::Float64) => {
            Some(ScalarValue::Float64(value as f64))
        }
        (ScalarValue::Int32(value), DataType::Int32, DataType::Decimal128(precision, scale)) => {
            let value = i128::from(value).checked_mul(decimal_scale_factor(*scale)?)?;
            decimal_literal(value, *precision, *scale)
        }
        (ScalarValue::Int64(value), DataType::Int64, DataType::Decimal128(precision, scale)) => {
            let value = i128::from(value).checked_mul(decimal_scale_factor(*scale)?)?;
            decimal_literal(value, *precision, *scale)
        }
        (
            ScalarValue::Decimal128 {
                value,
                precision: _,
                scale: source_scale,
            },
            DataType::Decimal128(_, _),
            DataType::Decimal128(target_precision, target_scale),
        ) => {
            let value = rescale_decimal(value, source_scale, *target_scale)?;
            decimal_literal(value, *target_precision, *target_scale)
        }
        (
            ScalarValue::Utf8(value),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => Some(ScalarValue::Utf8(value)),
        (ScalarValue::Null(_), _, target) => Some(ScalarValue::Null(target.clone())),
        // Float-to-integer casts and other conversions with engine-specific
        // rounding, saturation, or loss semantics are deliberately declined.
        _ => None,
    }
}

fn negate_literal(value: ScalarValue) -> Option<ScalarValue> {
    match value {
        ScalarValue::Int32(value) => value.checked_neg().map(ScalarValue::Int32),
        ScalarValue::Int64(value) => value.checked_neg().map(ScalarValue::Int64),
        ScalarValue::Float64(value) => (-value).is_finite().then_some(ScalarValue::Float64(-value)),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => decimal_literal(value.checked_neg()?, precision, scale),
        _ => None,
    }
}

fn evaluate_literal_arithmetic(
    op: BinaryOp,
    left: EvaluatedLiteral,
    right: EvaluatedLiteral,
) -> Option<EvaluatedLiteral> {
    if left.ty != right.ty {
        return None;
    }
    let ty = left.ty;
    let value = match (left.value, right.value) {
        (ScalarValue::Int32(left), ScalarValue::Int32(right)) => ScalarValue::Int32(match op {
            BinaryOp::Add => left.checked_add(right)?,
            BinaryOp::Subtract => left.checked_sub(right)?,
            BinaryOp::Multiply => left.checked_mul(right)?,
            BinaryOp::Divide => left.checked_div(right)?,
            _ => unreachable!("matched arithmetic operator"),
        }),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => ScalarValue::Int64(match op {
            BinaryOp::Add => left.checked_add(right)?,
            BinaryOp::Subtract => left.checked_sub(right)?,
            BinaryOp::Multiply => left.checked_mul(right)?,
            BinaryOp::Divide => left.checked_div(right)?,
            _ => unreachable!("matched arithmetic operator"),
        }),
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
            if op == BinaryOp::Divide && right == 0.0 {
                return None;
            }
            let value = match op {
                BinaryOp::Add => left + right,
                BinaryOp::Subtract => left - right,
                BinaryOp::Multiply => left * right,
                BinaryOp::Divide => left / right,
                _ => unreachable!("matched arithmetic operator"),
            };
            value.is_finite().then_some(ScalarValue::Float64(value))?
        }
        (
            ScalarValue::Decimal128 {
                value: left,
                precision: left_precision,
                scale: left_scale,
            },
            ScalarValue::Decimal128 {
                value: right,
                precision: right_precision,
                scale: right_scale,
            },
        ) => {
            return evaluate_decimal_arithmetic(
                op,
                left,
                left_precision,
                left_scale,
                right,
                right_precision,
                right_scale,
            );
        }
        _ => return None,
    };
    Some(EvaluatedLiteral { value, ty })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_decimal_arithmetic(
    op: BinaryOp,
    left: i128,
    left_precision: u8,
    left_scale: i8,
    right: i128,
    right_precision: u8,
    right_scale: i8,
) -> Option<EvaluatedLiteral> {
    let (value, precision, scale) = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let scale = left_scale.max(right_scale);
            let left = rescale_decimal(left, left_scale, scale)?;
            let right = rescale_decimal(right, right_scale, scale)?;
            let value = if op == BinaryOp::Add {
                left.checked_add(right)?
            } else {
                left.checked_sub(right)?
            };
            let integer_digits = (i16::from(left_precision) - i16::from(left_scale))
                .max(i16::from(right_precision) - i16::from(right_scale));
            let precision = i16::from(scale)
                .checked_add(integer_digits)?
                .checked_add(1)?
                .clamp(1, 38) as u8;
            (value, precision, scale)
        }
        BinaryOp::Multiply => {
            let precision = left_precision
                .saturating_add(right_precision.saturating_add(1))
                .min(38);
            let scale = left_scale.checked_add(right_scale)?;
            if scale > 38 {
                return None;
            }
            (left.checked_mul(right)?, precision, scale)
        }
        BinaryOp::Divide => {
            if right == 0 {
                return None;
            }
            let scale = left_scale.saturating_add(4).min(38);
            let scale_delta = scale.checked_sub(left_scale)?.checked_add(right_scale)?;
            let (numerator, denominator) = if scale_delta >= 0 {
                (left.checked_mul(decimal_scale_factor(scale_delta)?)?, right)
            } else {
                (
                    left,
                    right.checked_mul(decimal_scale_factor(scale_delta.checked_neg()?)?)?,
                )
            };
            let precision = i16::from(scale_delta)
                .checked_add(i16::from(left_precision))?
                .clamp(1, 38) as u8;
            (numerator.checked_div(denominator)?, precision, scale)
        }
        _ => unreachable!("matched arithmetic operator"),
    };
    Some(EvaluatedLiteral {
        value: decimal_literal(value, precision, scale)?,
        ty: arrow_schema::DataType::Decimal128(precision, scale),
    })
}

fn decimal_scale_factor(scale: i8) -> Option<i128> {
    (scale >= 0).then_some(())?;
    10_i128.checked_pow(u32::try_from(scale).ok()?)
}

fn rescale_decimal(value: i128, source_scale: i8, target_scale: i8) -> Option<i128> {
    let delta = target_scale.checked_sub(source_scale)?;
    if delta >= 0 {
        value.checked_mul(decimal_scale_factor(delta)?)
    } else {
        let divisor = decimal_scale_factor(delta.checked_neg()?)?;
        (value % divisor == 0).then_some(value / divisor)
    }
}

fn decimal_literal(value: i128, precision: u8, scale: i8) -> Option<ScalarValue> {
    let digits = if value == 0 {
        1
    } else {
        value.checked_abs()?.ilog10() + 1
    };
    (digits <= u32::from(precision)).then_some(ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    })
}

fn column_literal_predicate(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<(Column, ScalarValue, BinaryOp)> {
    if let Some(column) = normalized_column_ref(left, ctx, config) {
        let literal = predicate_literal_for_column(right, &column, ctx, config)?;
        return Some((column.column, literal, op));
    }
    let column = normalized_column_ref(right, ctx, config)?;
    let literal = predicate_literal_for_column(left, &column, ctx, config)?;
    let op = if config.normalize_filter_operands {
        reverse_comparison(op)
    } else {
        // Preserve V1's historical operand-orientation behavior unless
        // predicate normalization is explicitly enabled.
        op
    };
    Some((column.column, literal, op))
}

fn predicate_literal_for_column(
    expr: Expr,
    column: &NormalizedColumnRef,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<ScalarValue> {
    if !config.normalize_filter_operands {
        return predicate_literal(expr, ctx, config);
    }
    let literal = evaluate_literal_expr(expr, ctx)?;
    comparison_types_compatible(&column.effective_type, &literal.ty).then_some(literal.value)
}

fn comparison_types_compatible(
    left: &arrow_schema::DataType,
    right: &arrow_schema::DataType,
) -> bool {
    use arrow_schema::DataType;
    left == right
        || matches!(
            (left, right),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        )
}

fn reverse_comparison(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        _ => op,
    }
}

fn column_equality(
    expr: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<(Column, Column)> {
    let ExprData::Binary { op, left, right } = expr.get(ctx) else {
        return None;
    };
    column_equality_from_parts(*op, *left, *right, ctx, config)
}

fn column_equality_from_parts(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> Option<(Column, Column)> {
    if op != BinaryOp::Eq {
        return None;
    }
    let left = normalized_column_ref(left, ctx, config)?;
    let right = normalized_column_ref(right, ctx, config)?;
    if config.normalize_filter_operands
        && (left.stripped_cast || right.stripped_cast)
        && !comparison_types_compatible(&left.effective_type, &right.effective_type)
    {
        return None;
    }
    Some((left.column, right.column))
}

struct EqualityEdge {
    left: Column,
    right: Column,
    chosen_ndv: Estimate,
}

#[derive(Clone)]
struct EquivalenceClassNode {
    column: Column,
    parent: usize,
    distinct: Estimate,
}

/// Compact union-find state for columns that participate in equality reasoning.
///
/// Wide profiles commonly contribute no equality columns at all. Relevant columns are sorted
/// once, making this vector cheaper to initialize than maps spanning every output column while
/// retaining logarithmic column lookup for large equivalence classes.
///
/// This remains local and specialized because it lazily maps participating [`Column`] handles to
/// dense indices, stores per-root [`Estimate`] metadata for class NDVs, and preserves deterministic
/// root precedence and serialized class order. A generic parent/rank utility could be extracted
/// later, but doing so would broaden this cardinality-only refactor; generic disjoint-set work is
/// deliberately excluded here.
#[derive(Clone, Default)]
struct EquivalenceClassState {
    nodes: Vec<EquivalenceClassNode>,
}

impl EquivalenceClassState {
    fn from_profiles_and_equalities(
        left: &CardinalityProfile,
        right: &CardinalityProfile,
        equality_pairs: &[(Column, Column)],
        unknown_column_ndv_cap: f64,
    ) -> Self {
        debug_assert!(
            [left, right]
                .into_iter()
                .flat_map(|profile| &profile.equivalence_classes)
                .all(|class| class.columns.len() >= 2),
            "cardinality profiles must omit singleton equivalence classes",
        );
        let mut tracked_columns = Vec::new();
        for profile in [left, right] {
            for class in &profile.equivalence_classes {
                if class.columns.len() < 2 {
                    continue;
                }
                tracked_columns.extend(class.columns.iter().copied());
            }
        }
        for &(left_col, right_col) in equality_pairs {
            tracked_columns.extend([left_col, right_col]);
        }
        tracked_columns.sort_unstable();
        tracked_columns.dedup();

        let nodes = tracked_columns
            .into_iter()
            .enumerate()
            .map(|(parent, column)| EquivalenceClassNode {
                column,
                parent,
                distinct: left
                    .columns
                    .get(&column)
                    .or_else(|| right.columns.get(&column))
                    .map(|profile| profile.distinct.clone())
                    .unwrap_or_else(|| Estimate::default(unknown_column_ndv_cap)),
            })
            .collect();
        let mut state = Self { nodes };

        for profile in [left, right] {
            for class in &profile.equivalence_classes {
                if class.columns.len() < 2 {
                    continue;
                }
                let mut iter = class.columns.iter().copied();
                let Some(first) = iter.next() else {
                    continue;
                };
                let first_root = state.find(first);
                state.nodes[first_root].distinct = class.distinct.clone();
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
            let left_distinct = self.nodes[left_root].distinct.clone();
            let right_distinct = self.nodes[right_root].distinct.clone();
            self.nodes[right_root].parent = left_root;
            self.nodes[left_root].distinct = left_distinct
                .max_by_value(right_distinct)
                .max_by_value(distinct);
            true
        }
    }

    fn class_distinct(&mut self, column: Column) -> Estimate {
        let root = self.find(column);
        self.nodes[root].distinct.clone()
    }

    fn find(&mut self, column: Column) -> usize {
        let index = self
            .nodes
            .binary_search_by_key(&column, |node| node.column)
            .expect("equivalence-class columns must be pretracked before union-find lookup");
        self.find_index(index)
    }

    fn find_index(&mut self, index: usize) -> usize {
        // Preserve the historical left-root union rule (and therefore NDV/root semantics), but
        // avoid recursive lookup: adversarially oriented equalities can form a parent chain whose
        // depth is proportional to the number of participating columns.
        let mut root = index;
        while self.nodes[root].parent != root {
            root = self.nodes[root].parent;
        }

        let mut current = index;
        while self.nodes[current].parent != current {
            let parent = self.nodes[current].parent;
            self.nodes[current].parent = root;
            current = parent;
        }
        root
    }

    #[cfg(test)]
    fn tracked_column_count(&self) -> usize {
        self.nodes.len()
    }

    fn into_classes(mut self) -> Vec<ColumnEquivalenceClass> {
        let mut grouped = BTreeMap::<Column, (usize, BTreeSet<Column>)>::new();
        for index in 0..self.nodes.len() {
            let root = self.find_index(index);
            let root_column = self.nodes[root].column;
            grouped
                .entry(root_column)
                .or_insert_with(|| (root, BTreeSet::new()))
                .1
                .insert(self.nodes[index].column);
        }
        grouped
            .into_iter()
            .filter_map(|(_, (root, columns))| {
                (columns.len() >= 2).then(|| ColumnEquivalenceClass {
                    columns,
                    distinct: self.nodes[root].distinct.clone(),
                })
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

fn scalar_cmp(left: &ScalarValue, right: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (ScalarValue::Int32(left), ScalarValue::Int32(right)) => Some(left.cmp(right)),
        (ScalarValue::Int64(left), ScalarValue::Int64(right)) => Some(left.cmp(right)),
        (ScalarValue::Int32(left), ScalarValue::Int64(right)) => Some(i64::from(*left).cmp(right)),
        (ScalarValue::Int64(left), ScalarValue::Int32(right)) => Some(left.cmp(&i64::from(*right))),
        (ScalarValue::Int32(left), ScalarValue::Float64(right)) => {
            f64::from(*left).partial_cmp(right)
        }
        (ScalarValue::Float64(left), ScalarValue::Int32(right)) => {
            left.partial_cmp(&f64::from(*right))
        }
        (ScalarValue::Float64(left), ScalarValue::Float64(right)) => left.partial_cmp(right),
        (ScalarValue::Date32(left), ScalarValue::Date32(right)) => Some(left.cmp(right)),
        (
            ScalarValue::Decimal128 {
                value: left,
                scale: left_scale,
                ..
            },
            ScalarValue::Decimal128 {
                value: right,
                scale: right_scale,
                ..
            },
        ) => compare_scaled_integers(*left, *left_scale, *right, *right_scale),
        (
            ScalarValue::Int32(left),
            ScalarValue::Decimal128 {
                value: right,
                scale,
                ..
            },
        ) => compare_scaled_integers(i128::from(*left), 0, *right, *scale),
        (
            ScalarValue::Decimal128 {
                value: left, scale, ..
            },
            ScalarValue::Int32(right),
        ) => compare_scaled_integers(*left, *scale, i128::from(*right), 0),
        (
            ScalarValue::Int64(left),
            ScalarValue::Decimal128 {
                value: right,
                scale,
                ..
            },
        ) => compare_scaled_integers(i128::from(*left), 0, *right, *scale),
        (
            ScalarValue::Decimal128 {
                value: left, scale, ..
            },
            ScalarValue::Int64(right),
        ) => compare_scaled_integers(*left, *scale, i128::from(*right), 0),
        _ => None,
    }
}

fn compare_scaled_integers(
    left: i128,
    left_scale: i8,
    right: i128,
    right_scale: i8,
) -> Option<std::cmp::Ordering> {
    let common_scale = left_scale.max(right_scale);
    let left = rescale_decimal(left, left_scale, common_scale)?;
    let right = rescale_decimal(right, right_scale, common_scale)?;
    Some(left.cmp(&right))
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
                if !matches!(
                    data.join_type,
                    JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark { .. }
                ) {
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
        Join, JoinType, Limit, Map, MemoryCatalog, OperatorData, Output, Projection, ScalarValue,
        Scan, Selection, TableFunction, TableFunctionDef, TableRef, TableStatistics,
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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = AnalysisContext::new(catalog);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);

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

            let mut analyses = crate::test_analyses(&ctx);

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

        let mut analyses = crate::test_analyses(&ctx);
        assert_eq!(
            expr_used_columns(&ctx, &mut analyses, expr).unwrap(),
            vec![a, b]
        );
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

        let mut analyses = crate::test_analyses(&ctx);

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
        let mut analyses = crate::test_analyses(&ctx);

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
    fn explicit_default_cardinality_config_preserves_v1_estimates() {
        let (ctx, selection) = fallback_selection_query();
        let catalog = crate::test_catalog(&ctx);
        let mut implicit = AnalysisContext::new(Arc::clone(&catalog));
        let mut explicit = AnalysisContext::new(catalog)
            .with_cardinality_estimation_config(CardinalityEstimationConfig::default())
            .unwrap();

        let implicit_profile = implicit
            .get::<CardinalityEstimationV1>(&ctx, selection)
            .unwrap();
        let explicit_profile = explicit
            .get::<CardinalityEstimationV1>(&ctx, selection)
            .unwrap();

        assert_eq!(implicit_profile, explicit_profile);
        assert_eq!(
            implicit_profile.rows,
            Estimate {
                value: 250.0,
                lower: Some(0.0),
                upper: None,
                source: EstimateSource::Derived,
            }
        );
    }

    #[test]
    fn cardinality_config_survives_clear_fork_and_repeated_queries() {
        let (first_query, first_selection) = fallback_selection_query();
        let catalog = crate::test_catalog(&first_query);
        let config = CardinalityEstimationConfig {
            default_predicate_selectivity: 0.5,
            ..CardinalityEstimationConfig::default()
        };
        let mut analyses = AnalysisContext::new(catalog)
            .with_cardinality_estimation_config(config)
            .unwrap();

        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&first_query, first_selection)
                .unwrap()
                .rows
                .value,
            500.0
        );

        analyses.clear();
        assert_eq!(analyses.cardinality_estimation_config(), config);
        let (second_query, second_selection) = fallback_selection_query();
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&second_query, second_selection)
                .unwrap()
                .rows
                .value,
            500.0
        );

        let mut fork = analyses.fork();
        assert_eq!(fork.cardinality_estimation_config(), config);
        assert_eq!(
            fork.get::<CardinalityEstimationV1>(&second_query, second_selection)
                .unwrap()
                .rows
                .value,
            500.0
        );
    }

    #[test]
    fn changing_cardinality_config_invalidates_cached_estimates() {
        let (ctx, selection) = fallback_selection_query();
        let catalog = crate::test_catalog(&ctx);
        let mut analyses = AnalysisContext::new(catalog);
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, selection)
                .unwrap()
                .rows
                .value,
            250.0
        );

        assert!(
            analyses
                .set_cardinality_estimation_config(CardinalityEstimationConfig {
                    unknown_scan_rows: f64::NAN,
                    ..CardinalityEstimationConfig::default()
                })
                .is_err()
        );
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, selection)
                .unwrap()
                .rows
                .value,
            250.0
        );

        analyses
            .set_cardinality_estimation_config(CardinalityEstimationConfig {
                default_predicate_selectivity: 0.5,
                ..CardinalityEstimationConfig::default()
            })
            .unwrap();

        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, selection)
                .unwrap()
                .rows
                .value,
            500.0
        );
    }

    #[test]
    fn cardinality_config_rejects_invalid_numeric_assumptions_at_installation() {
        let invalid = [
            CardinalityEstimationConfig {
                like_selectivity: -0.01,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                default_predicate_selectivity: 1.01,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                range_fallback_selectivity: f64::INFINITY,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                like_selectivity: f64::NAN,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                unknown_scan_rows: -1.0,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                unknown_scan_rows: f64::NEG_INFINITY,
                ..CardinalityEstimationConfig::default()
            },
            CardinalityEstimationConfig {
                unknown_column_ndv_cap: f64::INFINITY,
                ..CardinalityEstimationConfig::default()
            },
        ];

        for config in invalid {
            assert!(config.validate().is_err());
            let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("memory", "public"));
            let mut analyses = AnalysisContext::new(Arc::clone(&catalog));
            assert!(analyses.set_cardinality_estimation_config(config).is_err());
            assert_eq!(
                analyses.cardinality_estimation_config(),
                CardinalityEstimationConfig::default()
            );
            assert!(
                AnalysisContext::new(Arc::clone(&catalog))
                    .with_cardinality_estimation_config(config)
                    .is_err()
            );
            assert!(
                crate::PlannedQuery::new(QueryContext::new(), catalog)
                    .with_cardinality_estimation_config(config)
                    .is_err()
            );
        }
    }

    #[test]
    fn cardinality_config_accepts_valid_boundaries_at_both_entry_paths() {
        for selectivity in [0.0, 1.0] {
            let config = CardinalityEstimationConfig {
                normalize_filter_operands: selectivity == 1.0,
                honor_negated_like: selectivity == 1.0,
                correlate_same_column_ranges: selectivity == 1.0,
                honor_filter_null_rejection: selectivity == 1.0,
                like_selectivity: selectivity,
                default_predicate_selectivity: selectivity,
                range_fallback_selectivity: selectivity,
                unknown_scan_rows: 0.0,
                unknown_column_ndv_cap: 0.0,
            };
            let catalog: Arc<dyn Catalog> = Arc::new(MemoryCatalog::new("memory", "public"));
            let mut analyses = AnalysisContext::new(Arc::clone(&catalog));
            analyses.set_cardinality_estimation_config(config).unwrap();
            assert_eq!(analyses.cardinality_estimation_config(), config);

            assert_eq!(
                AnalysisContext::new(Arc::clone(&catalog))
                    .with_cardinality_estimation_config(config)
                    .unwrap()
                    .cardinality_estimation_config(),
                config
            );
            assert_eq!(
                crate::PlannedQuery::new(QueryContext::new(), catalog)
                    .with_cardinality_estimation_config(config)
                    .unwrap()
                    .cardinality_estimation_config(),
                config
            );
        }
    }

    #[test]
    fn custom_config_controls_like_range_and_unknown_ndv_fallbacks() {
        let config = CardinalityEstimationConfig {
            like_selectivity: 0.4,
            range_fallback_selectivity: 0.6,
            unknown_scan_rows: 50.0,
            unknown_column_ndv_cap: 7.0,
            ..CardinalityEstimationConfig::default()
        };

        let (like_query, like_scan, like_selection) = like_selection_query();
        let mut like_analyses = AnalysisContext::new(crate::test_catalog(&like_query))
            .with_cardinality_estimation_config(config)
            .unwrap();
        let scan_profile = like_analyses
            .get::<CardinalityEstimationV1>(&like_query, like_scan)
            .unwrap();
        assert_eq!(scan_profile.rows.value, 50.0);
        assert_eq!(
            scan_profile.columns.values().next().unwrap().distinct.value,
            7.0
        );
        assert_eq!(
            like_analyses
                .get::<CardinalityEstimationV1>(&like_query, like_selection)
                .unwrap()
                .rows
                .value,
            20.0
        );

        let (range_query, range_selection) = range_selection_query();
        let mut range_analyses = AnalysisContext::new(crate::test_catalog(&range_query))
            .with_cardinality_estimation_config(config)
            .unwrap();
        assert_eq!(
            range_analyses
                .get::<CardinalityEstimationV1>(&range_query, range_selection)
                .unwrap()
                .rows
                .value,
            30.0
        );
    }

    #[test]
    fn cardinality_estimation_can_normalize_casts_and_literal_arithmetic() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let id_ref = ExprData::ColumnRef(id).add(&mut ctx);
        let cast_id = ExprData::Cast {
            expr: id_ref,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let three = ExprData::Literal(ScalarValue::Int64(3)).add(&mut ctx);
        let four = ExprData::Literal(ScalarValue::Int64(4)).add(&mut ctx);
        let arithmetic = ExprData::Binary {
            op: BinaryOp::Add,
            left: three,
            right: four,
        }
        .add(&mut ctx);
        let cast_literal = ExprData::Cast {
            expr: arithmetic,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: cast_id,
            right: cast_literal,
        }
        .add(&mut ctx);
        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(id, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );

        let legacy = apply_selection_profile(
            profile.clone(),
            predicate,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        let normalized = apply_selection_profile(
            profile,
            predicate,
            &ctx,
            &CardinalityEstimationConfig {
                normalize_filter_operands: true,
                ..CardinalityEstimationConfig::default()
            },
        );

        assert_eq!(legacy.rows.value, 25.0);
        assert_eq!(normalized.rows.value, 1.0);
        assert_eq!(normalized.columns[&id].distinct.value, 1.0);
        assert_eq!(
            normalized.columns[&id].lower_bound,
            Some(ScalarValue::Int64(7))
        );
    }

    #[test]
    fn predicate_normalization_declines_float_to_integer_casts() {
        let mut ctx = QueryContext::new();
        let integer = ColumnData::new("integer", DataType::Int64).add(&mut ctx);
        let integer_ref = ExprData::ColumnRef(integer).add(&mut ctx);
        let float = ExprData::Literal(ScalarValue::Float64(1.9)).add(&mut ctx);
        let cast_float = ExprData::Cast {
            expr: float,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let literal_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: integer_ref,
            right: cast_float,
        }
        .add(&mut ctx);

        let floating = ColumnData::new("floating", DataType::Float64).add(&mut ctx);
        let floating_ref = ExprData::ColumnRef(floating).add(&mut ctx);
        let cast_column = ExprData::Cast {
            expr: floating_ref,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let column_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: cast_column,
            right: one,
        }
        .add(&mut ctx);
        let forty_two = ExprData::Literal(ScalarValue::Int64(42)).add(&mut ctx);
        let checked_narrowing = ExprData::Cast {
            expr: forty_two,
            ty: DataType::Int32,
        }
        .add(&mut ctx);
        let too_large =
            ExprData::Literal(ScalarValue::Int64(i64::from(i32::MAX) + 1)).add(&mut ctx);
        let overflowing_narrowing = ExprData::Cast {
            expr: too_large,
            ty: DataType::Int32,
        }
        .add(&mut ctx);
        let config = CardinalityEstimationConfig {
            normalize_filter_operands: true,
            ..CardinalityEstimationConfig::default()
        };

        assert!(predicate_literal(cast_float, &ctx, &config).is_none());
        assert_eq!(
            predicate_literal(checked_narrowing, &ctx, &config),
            Some(ScalarValue::Int32(42))
        );
        assert!(predicate_literal(overflowing_narrowing, &ctx, &config).is_none());
        assert!(
            column_literal_predicate(BinaryOp::Eq, integer_ref, cast_float, &ctx, &config)
                .is_none()
        );
        assert!(column_literal_predicate(BinaryOp::Eq, cast_column, one, &ctx, &config).is_none());

        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [
                (integer, test_column_profile(100.0, 100.0)),
                (floating, test_column_profile(100.0, 100.0)),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(
            apply_selection_profile(profile.clone(), literal_predicate, &ctx, &config)
                .rows
                .value,
            25.0
        );
        assert_eq!(
            apply_selection_profile(profile, column_predicate, &ctx, &config)
                .rows
                .value,
            25.0
        );
    }

    #[test]
    fn casted_join_equality_requires_injective_order_preserving_casts() {
        let mut ctx = QueryContext::new();
        let int32 = ColumnData::new("int32", DataType::Int32).add(&mut ctx);
        let int64 = ColumnData::new("int64", DataType::Int64).add(&mut ctx);
        let float64 = ColumnData::new("float64", DataType::Float64).add(&mut ctx);
        let int32_ref = ExprData::ColumnRef(int32).add(&mut ctx);
        let safe_cast = ExprData::Cast {
            expr: int32_ref,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let int64_ref = ExprData::ColumnRef(int64).add(&mut ctx);
        let safe_equality = ExprData::Binary {
            op: BinaryOp::Eq,
            left: safe_cast,
            right: int64_ref,
        }
        .add(&mut ctx);
        let float64_ref = ExprData::ColumnRef(float64).add(&mut ctx);
        let unsafe_cast = ExprData::Cast {
            expr: float64_ref,
            ty: DataType::Int64,
        }
        .add(&mut ctx);
        let int64_ref = ExprData::ColumnRef(int64).add(&mut ctx);
        let unsafe_equality = ExprData::Binary {
            op: BinaryOp::Eq,
            left: unsafe_cast,
            right: int64_ref,
        }
        .add(&mut ctx);
        let config = CardinalityEstimationConfig {
            normalize_filter_operands: true,
            ..CardinalityEstimationConfig::default()
        };

        assert_eq!(
            column_equality(safe_equality, &ctx, &config),
            Some((int32, int64))
        );
        assert_eq!(column_equality(unsafe_equality, &ctx, &config), None);
    }

    #[test]
    fn literal_arithmetic_is_typed_checked_and_precision_preserving() {
        let mut ctx = QueryContext::new();
        let seven = ExprData::Literal(ScalarValue::Int64(7)).add(&mut ctx);
        let two = ExprData::Literal(ScalarValue::Int64(2)).add(&mut ctx);
        let integer_division = ExprData::Binary {
            op: BinaryOp::Divide,
            left: seven,
            right: two,
        }
        .add(&mut ctx);
        let large = ExprData::Literal(ScalarValue::Int64(9_007_199_254_740_993)).add(&mut ctx);
        let two = ExprData::Literal(ScalarValue::Int64(2)).add(&mut ctx);
        let precise_addition = ExprData::Binary {
            op: BinaryOp::Add,
            left: large,
            right: two,
        }
        .add(&mut ctx);
        let max = ExprData::Literal(ScalarValue::Int64(i64::MAX)).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let overflow = ExprData::Binary {
            op: BinaryOp::Add,
            left: max,
            right: one,
        }
        .add(&mut ctx);
        let decimal_left = ExprData::Literal(ScalarValue::Decimal128 {
            value: 125,
            precision: 5,
            scale: 2,
        })
        .add(&mut ctx);
        let decimal_right = ExprData::Literal(ScalarValue::Decimal128 {
            value: 250,
            precision: 5,
            scale: 2,
        })
        .add(&mut ctx);
        let decimal_addition = ExprData::Binary {
            op: BinaryOp::Add,
            left: decimal_left,
            right: decimal_right,
        }
        .add(&mut ctx);
        let decimal_left = ExprData::Literal(ScalarValue::Decimal128 {
            value: 125,
            precision: 5,
            scale: 2,
        })
        .add(&mut ctx);
        let decimal_right = ExprData::Literal(ScalarValue::Decimal128 {
            value: 250,
            precision: 5,
            scale: 2,
        })
        .add(&mut ctx);
        let decimal_division = ExprData::Binary {
            op: BinaryOp::Divide,
            left: decimal_left,
            right: decimal_right,
        }
        .add(&mut ctx);

        assert_eq!(
            evaluate_literal_expr(integer_division, &ctx).map(|value| value.value),
            Some(ScalarValue::Int64(3))
        );
        assert_eq!(
            evaluate_literal_expr(precise_addition, &ctx).map(|value| value.value),
            Some(ScalarValue::Int64(9_007_199_254_740_995))
        );
        assert!(evaluate_literal_expr(overflow, &ctx).is_none());
        assert_eq!(
            evaluate_literal_expr(decimal_addition, &ctx).map(|value| value.value),
            Some(ScalarValue::Decimal128 {
                value: 375,
                precision: 6,
                scale: 2,
            })
        );
        assert_eq!(
            evaluate_literal_expr(decimal_division, &ctx).map(|value| value.value),
            Some(ScalarValue::Decimal128 {
                value: 500_000,
                precision: 11,
                scale: 6,
            })
        );
    }

    #[test]
    fn cardinality_estimation_can_honor_negated_like() {
        let mut ctx = QueryContext::new();
        let name = ColumnData::new("name", DataType::Utf8).add(&mut ctx);
        let name_ref = ExprData::ColumnRef(name).add(&mut ctx);
        let pattern = ExprData::Literal(ScalarValue::Utf8("prefix%".to_string())).add(&mut ctx);
        let predicate = ExprData::Like {
            negated: true,
            expr: name_ref,
            pattern,
            case_insensitive: false,
        }
        .add(&mut ctx);
        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(name, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );

        let legacy = apply_selection_profile(
            profile.clone(),
            predicate,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        let corrected = apply_selection_profile(
            profile,
            predicate,
            &ctx,
            &CardinalityEstimationConfig {
                honor_negated_like: true,
                ..CardinalityEstimationConfig::default()
            },
        );

        assert_eq!(legacy.rows.value, 10.0);
        assert_eq!(corrected.rows.value, 90.0);
    }

    #[test]
    fn negated_like_complements_only_the_non_null_population() {
        let mut ctx = QueryContext::new();
        let name = ColumnData::new("name", DataType::Utf8).add(&mut ctx);
        let name_ref = ExprData::ColumnRef(name).add(&mut ctx);
        let pattern = ExprData::Literal(ScalarValue::Utf8("prefix%".to_string())).add(&mut ctx);
        let negated = ExprData::Like {
            negated: true,
            expr: name_ref,
            pattern,
            case_insensitive: false,
        }
        .add(&mut ctx);
        let name_ref = ExprData::ColumnRef(name).add(&mut ctx);
        let pattern = ExprData::Literal(ScalarValue::Utf8("prefix%".to_string())).add(&mut ctx);
        let positive = ExprData::Like {
            negated: false,
            expr: name_ref,
            pattern,
            case_insensitive: false,
        }
        .add(&mut ctx);
        let mixed = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(name, test_column_profile(60.0, 50.0))]
                .into_iter()
                .collect(),
        );
        let estimate = |predicate, honor_negated_like, honor_filter_null_rejection| {
            apply_selection_profile(
                mixed.clone(),
                predicate,
                &ctx,
                &CardinalityEstimationConfig {
                    honor_negated_like,
                    honor_filter_null_rejection,
                    ..CardinalityEstimationConfig::default()
                },
            )
        };

        let legacy = estimate(negated, false, false);
        let complement_only = estimate(negated, true, false);
        let rejection_only = estimate(negated, false, true);
        let corrected = estimate(negated, true, true);
        let positive = estimate(positive, true, true);

        assert_eq!(legacy.rows.value, 10.0);
        assert_eq!(legacy.columns[&name].frequency.value, 6.0);
        assert_eq!(complement_only.rows.value, 54.0);
        assert!((complement_only.columns[&name].frequency.value - 32.4).abs() < 1e-12);
        assert_eq!(rejection_only.rows.value, 10.0);
        assert_eq!(rejection_only.columns[&name].frequency.value, 10.0);
        assert_eq!(corrected.rows.value, 54.0);
        assert_eq!(corrected.columns[&name].frequency.value, 54.0);
        assert_eq!(positive.rows.value, 6.0);
        assert_eq!(positive.columns[&name].frequency.value, 6.0);
        assert_eq!(corrected.rows.value + positive.rows.value, 60.0);

        let all_null = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(name, test_column_profile(0.0, 0.0))]
                .into_iter()
                .collect(),
        );
        let all_null = apply_selection_profile(
            all_null,
            negated,
            &ctx,
            &CardinalityEstimationConfig {
                honor_negated_like: true,
                honor_filter_null_rejection: true,
                ..CardinalityEstimationConfig::default()
            },
        );
        assert_eq!(all_null.rows.value, 0.0);
        assert_eq!(all_null.columns[&name].frequency.value, 0.0);
    }

    #[test]
    fn cardinality_estimation_can_correlate_same_column_ranges() {
        let mut ctx = QueryContext::new();
        let value = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let value_ref = ExprData::ColumnRef(value).add(&mut ctx);
        let lower = ExprData::Literal(ScalarValue::Int64(20)).add(&mut ctx);
        let greater = ExprData::Binary {
            op: BinaryOp::Gt,
            left: value_ref,
            right: lower,
        }
        .add(&mut ctx);
        let value_ref = ExprData::ColumnRef(value).add(&mut ctx);
        let upper = ExprData::Literal(ScalarValue::Int64(30)).add(&mut ctx);
        let less = ExprData::Binary {
            op: BinaryOp::Lt,
            left: value_ref,
            right: upper,
        }
        .add(&mut ctx);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![greater, less],
        }
        .add(&mut ctx);
        let mut column = test_column_profile(100.0, 100.0);
        column.lower_bound = Some(ScalarValue::Int64(0));
        column.upper_bound = Some(ScalarValue::Int64(100));
        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(value, column)].into_iter().collect(),
        );

        let legacy = apply_selection_profile(
            profile.clone(),
            predicate,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        let correlated = apply_selection_profile(
            profile,
            predicate,
            &ctx,
            &CardinalityEstimationConfig {
                correlate_same_column_ranges: true,
                ..CardinalityEstimationConfig::default()
            },
        );

        assert_eq!(legacy.rows.value, 24.0);
        assert_eq!(correlated.rows.value, 10.0);
    }

    #[test]
    fn range_intersection_tracks_inclusivity_contradictions_and_constant_domains() {
        let config = CardinalityEstimationConfig {
            correlate_same_column_ranges: true,
            ..CardinalityEstimationConfig::default()
        };
        let mut ranged = test_column_profile(100.0, 100.0);
        ranged.lower_bound = Some(ScalarValue::Int64(0));
        ranged.upper_bound = Some(ScalarValue::Int64(100));
        let inclusive_singleton = [
            RangeConstraint {
                index: 0,
                op: BinaryOp::GtEq,
                literal: ScalarValue::Int64(20),
            },
            RangeConstraint {
                index: 1,
                op: BinaryOp::LtEq,
                literal: ScalarValue::Int64(20),
            },
        ];
        let strict_contradiction = [
            RangeConstraint {
                index: 0,
                op: BinaryOp::Gt,
                literal: ScalarValue::Int64(20),
            },
            RangeConstraint {
                index: 1,
                op: BinaryOp::Lt,
                literal: ScalarValue::Int64(20),
            },
        ];
        let ordered_contradiction = [
            RangeConstraint {
                index: 0,
                op: BinaryOp::GtEq,
                literal: ScalarValue::Int64(30),
            },
            RangeConstraint {
                index: 1,
                op: BinaryOp::LtEq,
                literal: ScalarValue::Int64(20),
            },
        ];

        assert_eq!(
            range_interval_selectivity(&ranged, &inclusive_singleton, &config),
            0.01
        );
        assert_eq!(
            range_interval_selectivity(&ranged, &strict_contradiction, &config),
            0.0
        );
        assert_eq!(
            range_interval_selectivity(&ranged, &ordered_contradiction, &config),
            0.0
        );

        let mut constant = test_column_profile(100.0, 1.0);
        constant.lower_bound = Some(ScalarValue::Int64(20));
        constant.upper_bound = Some(ScalarValue::Int64(20));
        assert_eq!(
            range_interval_selectivity(
                &constant,
                &[RangeConstraint {
                    index: 0,
                    op: BinaryOp::GtEq,
                    literal: ScalarValue::Int64(20),
                }],
                &config
            ),
            1.0
        );
        assert_eq!(
            range_interval_selectivity(
                &constant,
                &[RangeConstraint {
                    index: 0,
                    op: BinaryOp::Gt,
                    literal: ScalarValue::Int64(20),
                }],
                &config
            ),
            0.0
        );
        assert_eq!(
            range_interval_selectivity(
                &constant,
                &[RangeConstraint {
                    index: 0,
                    op: BinaryOp::Lt,
                    literal: ScalarValue::Int64(10),
                }],
                &config
            ),
            0.0
        );
    }

    #[test]
    fn reversed_range_operands_respect_independent_normalization_flags() {
        let mut ctx = QueryContext::new();
        let value = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let twenty = ExprData::Literal(ScalarValue::Int64(20)).add(&mut ctx);
        let value_ref = ExprData::ColumnRef(value).add(&mut ctx);
        let reversed_lower = ExprData::Binary {
            op: BinaryOp::LtEq,
            left: twenty,
            right: value_ref,
        }
        .add(&mut ctx);
        let value_ref = ExprData::ColumnRef(value).add(&mut ctx);
        let thirty = ExprData::Literal(ScalarValue::Int64(30)).add(&mut ctx);
        let upper = ExprData::Binary {
            op: BinaryOp::LtEq,
            left: value_ref,
            right: thirty,
        }
        .add(&mut ctx);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![reversed_lower, upper],
        }
        .add(&mut ctx);
        let mut column = test_column_profile(100.0, 100.0);
        column.lower_bound = Some(ScalarValue::Int64(0));
        column.upper_bound = Some(ScalarValue::Int64(100));
        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(value, column)].into_iter().collect(),
        );

        let estimate = |normalize_filter_operands, correlate_same_column_ranges| {
            apply_selection_profile(
                profile.clone(),
                predicate,
                &ctx,
                &CardinalityEstimationConfig {
                    normalize_filter_operands,
                    correlate_same_column_ranges,
                    ..CardinalityEstimationConfig::default()
                },
            )
            .rows
            .value
        };

        assert_eq!(estimate(false, false), 6.0);
        assert_eq!(estimate(true, false), 24.0);
        assert_eq!(estimate(false, true), 20.0);
        assert_eq!(estimate(true, true), 10.0);
    }

    #[test]
    fn cardinality_estimation_can_propagate_filter_null_rejection() {
        let mut ctx = QueryContext::new();
        let key = ColumnData::new("key", DataType::Int64).add(&mut ctx);
        let key_ref = ExprData::ColumnRef(key).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: key_ref,
            right: one,
        }
        .add(&mut ctx);
        let profile = CardinalityProfile::new(
            Estimate::catalog(100.0),
            [(key, test_column_profile(50.0, 10.0))]
                .into_iter()
                .collect(),
        );

        let legacy = apply_selection_profile(
            profile.clone(),
            predicate,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        let corrected = apply_selection_profile(
            profile,
            predicate,
            &ctx,
            &CardinalityEstimationConfig {
                honor_filter_null_rejection: true,
                ..CardinalityEstimationConfig::default()
            },
        );

        assert_eq!(legacy.rows.value, 10.0);
        assert_eq!(legacy.columns[&key].frequency.value, 5.0);
        assert_eq!(corrected.rows.value, 10.0);
        assert_eq!(corrected.columns[&key].frequency.value, 10.0);
    }

    #[test]
    fn filter_null_rejection_intersects_or_branches() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let two = ExprData::Literal(ScalarValue::Int64(2)).add(&mut ctx);
        let a_eq_one = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(a).add(&mut ctx),
            right: one,
        }
        .add(&mut ctx);
        let a_eq_two = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(a).add(&mut ctx),
            right: two,
        }
        .add(&mut ctx);
        let b_eq_two = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(b).add(&mut ctx),
            right: two,
        }
        .add(&mut ctx);
        let same_column_or = ExprData::Nary {
            op: NaryOp::Or,
            exprs: vec![a_eq_one, a_eq_two],
        }
        .add(&mut ctx);
        let different_columns_or = ExprData::Nary {
            op: NaryOp::Or,
            exprs: vec![a_eq_one, b_eq_two],
        }
        .add(&mut ctx);
        let config = CardinalityEstimationConfig {
            honor_filter_null_rejection: true,
            ..CardinalityEstimationConfig::default()
        };

        assert_eq!(
            filter_null_rejected_columns(same_column_or, &ctx, &config),
            BTreeSet::from([a])
        );
        assert!(filter_null_rejected_columns(different_columns_or, &ctx, &config).is_empty());
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

        let mut analyses = crate::test_analyses(&ctx);
        let parents = analyses.get::<ParentsOf>(&ctx, scan).unwrap();

        assert_eq!(parents.len(), 2);
        assert!(parents.contains(&left));
        assert!(parents.contains(&right));
    }

    #[test]
    fn cardinality_shared_lookup_reuses_cached_profile() {
        let (ctx, scan) = single_column_scan();
        let mut analyses = crate::test_analyses(&ctx);

        let first = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, scan).unwrap();
        let second = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, scan).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn cardinality_passthrough_operator_reuses_input_profile() {
        let (mut ctx, scan) = single_column_scan();
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        let mut analyses = crate::test_analyses(&ctx);

        let input = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, scan).unwrap();
        let passthrough = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, output).unwrap();

        assert!(Arc::ptr_eq(&input, &passthrough));
    }

    #[test]
    fn cardinality_public_lookup_stays_owned_and_clear_recomputes() {
        let (ctx, scan) = single_column_scan();
        let mut analyses = crate::test_analyses(&ctx);
        let before = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, scan).unwrap();

        let owned: CardinalityProfile =
            analyses.get::<CardinalityEstimationV1>(&ctx, scan).unwrap();
        assert_eq!(&owned, before.as_ref());

        analyses.clear();
        let after = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, scan).unwrap();
        assert_eq!(before.as_ref(), after.as_ref());
        assert!(!Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn cardinality_profile_omits_singleton_equivalence_classes() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let rows = Estimate::exact(100.0);
        let profile = CardinalityProfile::new(
            rows,
            [
                (a, test_column_profile(100.0, 100.0)),
                (b, test_column_profile(100.0, 50.0)),
            ]
            .into_iter()
            .collect(),
        );

        assert!(profile.equivalence_classes.is_empty());
    }

    #[test]
    fn inner_join_records_only_nontrivial_equivalence_classes() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let left_payload = ColumnData::new("left_payload", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let right_payload = ColumnData::new("right_payload", DataType::Int64).add(&mut ctx);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [
                (a, test_column_profile(100.0, 100.0)),
                (left_payload, test_column_profile(100.0, 25.0)),
            ]
            .into_iter()
            .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(50.0),
            [
                (b, test_column_profile(50.0, 50.0)),
                (right_payload, test_column_profile(50.0, 10.0)),
            ]
            .into_iter()
            .collect(),
        );
        let predicate = equality_expr(&mut ctx, a, b);

        let output =
            join_profile_from_conjuncts(&left, &right, JoinType::Inner, &[predicate], &ctx);

        assert_eq!(output.equivalence_classes.len(), 1);
        assert_eq!(
            output.equivalence_classes[0].columns,
            BTreeSet::from([a, b])
        );
    }

    #[test]
    fn projection_drops_equivalence_classes_reduced_to_one_column() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let mut input = CardinalityProfile::new(
            Estimate::exact(100.0),
            [a, b, c]
                .into_iter()
                .map(|column| (column, test_column_profile(100.0, 50.0)))
                .collect(),
        );
        input.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([a, b, c]),
            distinct: Estimate::exact(50.0),
        }];

        let pair = project_profile(&input, &[a, b]);
        let singleton = project_profile(&input, &[a]);

        assert_eq!(pair.equivalence_classes.len(), 1);
        assert_eq!(pair.equivalence_classes[0].columns, BTreeSet::from([a, b]));
        assert!(singleton.equivalence_classes.is_empty());
    }

    #[test]
    fn owned_equivalence_filter_matches_borrowed_filter() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let missing = ColumnData::new("missing", DataType::Int64).add(&mut ctx);
        let columns = [
            (a, test_column_profile(100.0, 10.0)),
            (b, test_column_profile(100.0, 20.0)),
            (c, test_column_profile(100.0, 30.0)),
        ]
        .into_iter()
        .collect();
        let classes = vec![
            ColumnEquivalenceClass {
                columns: BTreeSet::from([a, b, missing]),
                distinct: Estimate::exact(100.0),
            },
            ColumnEquivalenceClass {
                columns: BTreeSet::from([c, missing]),
                distinct: Estimate::exact(100.0),
            },
        ];

        assert_eq!(
            filter_owned_equivalence_classes(classes.clone(), &columns),
            filter_equivalence_classes(&classes, &columns),
        );
    }

    #[test]
    fn join_column_merge_preserves_right_input_precedence() {
        let mut ctx = QueryContext::new();
        let shared = ColumnData::new("shared", DataType::Int64).add(&mut ctx);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(shared, test_column_profile(100.0, 10.0))]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(shared, test_column_profile(100.0, 20.0))]
                .into_iter()
                .collect(),
        );

        let output = combine_join_columns(&left, &right, Estimate::exact(50.0), None, Vec::new());

        assert_eq!(output.columns[&shared].frequency.value, 50.0);
        assert_eq!(output.columns[&shared].distinct.value, 20.0);
    }

    #[test]
    fn rename_preserves_only_nontrivial_equivalence_classes() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let renamed_a = ColumnData::new("renamed_a", DataType::Int64).add(&mut ctx);
        let renamed_b = ColumnData::new("renamed_b", DataType::Int64).add(&mut ctx);
        let mut input = CardinalityProfile::new(
            Estimate::exact(100.0),
            [a, b]
                .into_iter()
                .map(|column| (column, test_column_profile(100.0, 50.0)))
                .collect(),
        );
        input.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([a, b]),
            distinct: Estimate::exact(50.0),
        }];

        let pair = rename_profile(&input, &[(renamed_a, a), (renamed_b, b)]);
        let singleton = rename_profile(&input, &[(renamed_a, a)]);

        assert_eq!(pair.equivalence_classes.len(), 1);
        assert_eq!(
            pair.equivalence_classes[0].columns,
            BTreeSet::from([renamed_a, renamed_b])
        );
        assert!(singleton.equivalence_classes.is_empty());
    }

    #[test]
    fn sparse_equivalence_classes_preserve_transitive_join_estimates() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let unrelated = ColumnData::new("unrelated", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let rows = Estimate::exact(100.0);
        let left = CardinalityProfile::new(
            rows.clone(),
            [a, b, unrelated]
                .into_iter()
                .map(|column| (column, test_column_profile(100.0, 100.0)))
                .collect(),
        );
        let right = CardinalityProfile::new(
            rows,
            [(c, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let ab = equality_expr(&mut ctx, a, b);
        let bc = equality_expr(&mut ctx, b, c);
        let ac = equality_expr(&mut ctx, a, c);

        let estimate = join_selectivity_from_conjuncts(&left, &right, &[ab, bc, ac], &ctx);

        assert_eq!(estimate.selectivity.value, 0.01);
        assert_eq!(estimate.equivalence_classes.len(), 1);
        assert_eq!(
            estimate.equivalence_classes[0].columns,
            BTreeSet::from([a, b, c])
        );
    }

    #[test]
    fn overlapping_column_uses_left_profile_ndv_for_equality_domain() {
        let mut ctx = QueryContext::new();
        let shared = ColumnData::new("shared", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(shared, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [
                (shared, test_column_profile(100.0, 10.0)),
                (right_key, test_column_profile(100.0, 50.0)),
            ]
            .into_iter()
            .collect(),
        );
        let predicate = equality_expr(&mut ctx, shared, right_key);

        // This documents the specialized state's intentional left-before-right lookup when a
        // column handle appears in both inputs; it is not a parity assertion against main.
        let estimate = join_selectivity_from_conjuncts(&left, &right, &[predicate], &ctx);

        assert_eq!(estimate.selectivity.value, 0.01);
        assert_eq!(estimate.equivalence_classes.len(), 1);
        assert_eq!(estimate.equivalence_classes[0].distinct.value, 100.0);
        assert_eq!(
            estimate.equivalence_classes[0].columns,
            BTreeSet::from([shared, right_key]),
        );
    }

    #[test]
    fn sparse_equivalence_state_merges_overlapping_inherited_classes() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let mut left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [
                (a, test_column_profile(100.0, 10.0)),
                (b, test_column_profile(100.0, 20.0)),
            ]
            .into_iter()
            .collect(),
        );
        left.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([a, b]),
            distinct: Estimate::exact(80.0),
        }];
        let mut right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [
                (b, test_column_profile(100.0, 5.0)),
                (c, test_column_profile(100.0, 60.0)),
            ]
            .into_iter()
            .collect(),
        );
        right.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([b, c]),
            distinct: Estimate::exact(40.0),
        }];

        let estimate = join_selectivity_from_conjuncts(&left, &right, &[], &ctx);

        assert_eq!(estimate.selectivity.value, 1.0);
        assert_eq!(estimate.equivalence_classes.len(), 1);
        assert_eq!(
            estimate.equivalence_classes[0],
            ColumnEquivalenceClass {
                columns: BTreeSet::from([a, b, c]),
                // The historical left-then-right overwrite/max sequence first installs 40 for
                // the right class, then retains c's larger column NDV.
                distinct: Estimate::exact(60.0),
            },
        );
    }

    #[test]
    fn sparse_equivalence_state_orders_disjoint_classes_by_root_column() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let d = ColumnData::new("d", DataType::Int64).add(&mut ctx);
        let mut left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [a, b, c, d]
                .into_iter()
                .map(|column| (column, test_column_profile(100.0, 50.0)))
                .collect(),
        );
        left.equivalence_classes = vec![
            ColumnEquivalenceClass {
                columns: BTreeSet::from([c, d]),
                distinct: Estimate::exact(40.0),
            },
            ColumnEquivalenceClass {
                columns: BTreeSet::from([a, b]),
                distinct: Estimate::exact(30.0),
            },
        ];
        let right = CardinalityProfile::new(Estimate::exact(1.0), BTreeMap::new());

        let estimate = join_selectivity_from_conjuncts(&left, &right, &[], &ctx);

        assert_eq!(
            estimate.equivalence_classes,
            vec![
                ColumnEquivalenceClass {
                    columns: BTreeSet::from([a, b]),
                    distinct: Estimate::exact(50.0),
                },
                ColumnEquivalenceClass {
                    columns: BTreeSet::from([c, d]),
                    distinct: Estimate::exact(50.0),
                },
            ],
        );
    }

    #[test]
    fn large_equality_class_tracks_each_participating_column_once() {
        const COLUMN_COUNT: usize = 512;

        let mut ctx = QueryContext::new();
        let columns = (0..COLUMN_COUNT)
            .map(|index| ColumnData::new(format!("key_{index}"), DataType::Int64).add(&mut ctx))
            .collect::<Vec<_>>();
        let midpoint = COLUMN_COUNT / 2;
        let left = CardinalityProfile::new(
            Estimate::exact(COLUMN_COUNT as f64),
            columns[..midpoint]
                .iter()
                .copied()
                .map(|column| {
                    (
                        column,
                        test_column_profile(COLUMN_COUNT as f64, COLUMN_COUNT as f64),
                    )
                })
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(COLUMN_COUNT as f64),
            columns[midpoint..]
                .iter()
                .copied()
                .map(|column| {
                    (
                        column,
                        test_column_profile(COLUMN_COUNT as f64, COLUMN_COUNT as f64),
                    )
                })
                .collect(),
        );
        let equalities = columns
            .windows(2)
            .map(|pair| equality_expr(&mut ctx, pair[0], pair[1]))
            .collect::<Vec<_>>();

        let estimate = join_selectivity_from_conjuncts(&left, &right, &equalities, &ctx);

        assert_eq!(estimate.equivalence_state_columns, COLUMN_COUNT);
        assert_eq!(estimate.equivalence_classes.len(), 1);
        assert_eq!(estimate.equivalence_classes[0].columns.len(), COLUMN_COUNT);
        assert_eq!(
            estimate.equivalence_classes[0].columns,
            columns.into_iter().collect()
        );
    }

    #[test]
    fn reverse_oriented_equality_chain_uses_iterative_path_compression() {
        const COLUMN_COUNT: usize = 16_384;

        let mut ctx = QueryContext::new();
        let columns = (0..COLUMN_COUNT)
            .map(|index| ColumnData::new(format!("key_{index}"), DataType::Int64).add(&mut ctx))
            .collect::<Vec<_>>();
        let equality_pairs = columns
            .windows(2)
            .map(|pair| (pair[1], pair[0]))
            .collect::<Vec<_>>();
        let empty = CardinalityProfile::new(Estimate::exact(1.0), BTreeMap::new());
        let mut state = EquivalenceClassState::from_profiles_and_equalities(
            &empty,
            &empty,
            &equality_pairs,
            CardinalityEstimationConfig::default().unknown_column_ndv_cap,
        );

        for &(left, right) in &equality_pairs {
            assert!(state.union(left, right, Estimate::exact(100.0)));
        }

        let mut depth = 0;
        let mut current = 0;
        while state.nodes[current].parent != current {
            current = state.nodes[current].parent;
            depth += 1;
        }
        assert_eq!(depth, COLUMN_COUNT - 1);

        let root = state.find_index(0);
        assert_eq!(root, COLUMN_COUNT - 1);
        assert_eq!(state.nodes[0].parent, root);
    }

    #[test]
    fn residual_only_wide_profiles_do_not_populate_equivalence_state() {
        let mut ctx = QueryContext::new();
        let left_columns = (0..128)
            .map(|index| ColumnData::new(format!("left_{index}"), DataType::Int64).add(&mut ctx))
            .collect::<Vec<_>>();
        let right_columns = (0..128)
            .map(|index| ColumnData::new(format!("right_{index}"), DataType::Int64).add(&mut ctx))
            .collect::<Vec<_>>();
        let left = CardinalityProfile::new(
            Estimate::exact(1_000.0),
            left_columns
                .iter()
                .copied()
                .map(|column| (column, test_column_profile(1_000.0, 500.0)))
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(1_000.0),
            right_columns
                .iter()
                .copied()
                .map(|column| (column, test_column_profile(1_000.0, 500.0)))
                .collect(),
        );
        let left_ref = ExprData::ColumnRef(left_columns[0]).add(&mut ctx);
        let right_ref = ExprData::ColumnRef(right_columns[0]).add(&mut ctx);
        let residual = ExprData::Binary {
            op: BinaryOp::Gt,
            left: left_ref,
            right: right_ref,
        }
        .add(&mut ctx);

        let estimate = join_selectivity_from_conjuncts(&left, &right, &[residual], &ctx);

        assert_eq!(estimate.equivalence_state_columns, 0);
        assert!(estimate.equivalence_classes.is_empty());
    }

    #[test]
    fn residual_join_preserves_existing_equivalence_classes() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let payload = ColumnData::new("payload", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let mut left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [a, b, payload]
                .into_iter()
                .map(|column| (column, test_column_profile(100.0, 50.0)))
                .collect(),
        );
        left.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from([a, b]),
            distinct: Estimate::exact(50.0),
        }];
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(c, test_column_profile(100.0, 25.0))]
                .into_iter()
                .collect(),
        );
        let a_ref = ExprData::ColumnRef(a).add(&mut ctx);
        let c_ref = ExprData::ColumnRef(c).add(&mut ctx);
        let residual = ExprData::Binary {
            op: BinaryOp::Gt,
            left: a_ref,
            right: c_ref,
        }
        .add(&mut ctx);

        let estimate = join_selectivity_from_conjuncts(&left, &right, &[residual], &ctx);

        assert_eq!(estimate.equivalence_state_columns, 2);
        assert_eq!(estimate.equivalence_classes.len(), 1);
        assert_eq!(
            estimate.equivalence_classes[0],
            ColumnEquivalenceClass {
                columns: BTreeSet::from([a, b]),
                distinct: Estimate::exact(50.0),
            }
        );
    }

    #[test]
    fn nested_and_join_profile_matches_preflattened_conjuncts() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(a, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(50.0),
            [(b, test_column_profile(50.0, 50.0))].into_iter().collect(),
        );
        let equality = equality_expr(&mut ctx, a, b);
        let a_ref = ExprData::ColumnRef(a).add(&mut ctx);
        let ten = ExprData::Literal(ScalarValue::Int64(10)).add(&mut ctx);
        let range = ExprData::Binary {
            op: BinaryOp::Gt,
            left: a_ref,
            right: ten,
        }
        .add(&mut ctx);
        let always_true = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let inner_and = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![equality, range],
        }
        .add(&mut ctx);
        let nested_and = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![inner_and, always_true],
        }
        .add(&mut ctx);

        let from_expression =
            join_profile_from_predicate(&left, &right, JoinType::Inner, nested_and, &ctx);
        let from_conjuncts = join_profile_from_conjuncts(
            &left,
            &right,
            JoinType::Inner,
            &[equality, range, always_true],
            &ctx,
        );

        assert_eq!(from_expression, from_conjuncts);
        assert_eq!(from_expression.equivalence_classes.len(), 1);
        assert_eq!(
            from_expression.equivalence_classes[0].columns,
            BTreeSet::from([a, b])
        );
    }

    #[test]
    fn join_types_propagate_only_sound_equivalence_classes() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let left_equal = ColumnData::new("left_equal", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let right_equal = ColumnData::new("right_equal", DataType::Int64).add(&mut ctx);
        let marker = ColumnData::new("marker", DataType::Boolean).add(&mut ctx);
        let left = test_equivalent_profile(100.0, [left_key, left_equal]);
        let right = test_equivalent_profile(100.0, [right_key, right_equal]);
        let predicate = equality_expr(&mut ctx, left_key, right_key);
        let left_class = BTreeSet::from([left_key, left_equal]);
        let right_class = BTreeSet::from([right_key, right_equal]);
        let merged_class = BTreeSet::from([left_key, left_equal, right_key, right_equal]);
        let left_columns = left_class.clone();
        let both_columns = merged_class.clone();
        let mut mark_columns = left_class.clone();
        mark_columns.insert(marker);

        let cases = [
            (
                JoinType::Inner,
                vec![merged_class],
                both_columns.clone(),
                100.0,
            ),
            (
                JoinType::LeftOuter,
                vec![left_class.clone()],
                both_columns.clone(),
                100.0,
            ),
            (
                JoinType::RightOuter,
                vec![right_class],
                both_columns.clone(),
                100.0,
            ),
            (JoinType::FullOuter, Vec::new(), both_columns, 100.0),
            (
                JoinType::LeftSemi,
                vec![left_class.clone()],
                left_columns.clone(),
                100.0,
            ),
            (
                JoinType::LeftAnti,
                vec![left_class.clone()],
                left_columns.clone(),
                0.0,
            ),
            (
                JoinType::Single,
                vec![left_class.clone()],
                left_columns.clone(),
                100.0,
            ),
            (
                JoinType::LeftMark {
                    marker,
                    nullable: false,
                },
                vec![left_class],
                mark_columns,
                100.0,
            ),
        ];

        for (join_type, expected_classes, expected_columns, expected_rows) in cases {
            let output =
                join_profile_from_conjuncts(&left, &right, join_type.clone(), &[predicate], &ctx);
            let actual_classes = output
                .equivalence_classes
                .iter()
                .map(|class| class.columns.clone())
                .collect::<Vec<_>>();
            let actual_columns = output.columns.keys().copied().collect::<BTreeSet<_>>();

            assert_eq!(actual_classes, expected_classes, "{join_type:?}");
            assert_eq!(actual_columns, expected_columns, "{join_type:?}");
            assert_eq!(output.rows.value, expected_rows, "{join_type:?}");
        }
    }

    #[test]
    fn outer_join_row_bounds_include_null_extended_rows() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let predicate = equality_expr(&mut ctx, left_key, right_key);
        let cases = [
            (JoinType::LeftOuter, 100.0, 10.0, 100.0),
            (JoinType::RightOuter, 10.0, 100.0, 100.0),
            (JoinType::FullOuter, 100.0, 10.0, 100.0),
        ];

        for (join_type, left_rows, right_rows, expected_minimum) in cases {
            let left = CardinalityProfile::new(
                Estimate::exact(left_rows),
                [(left_key, test_column_profile(left_rows, left_rows))]
                    .into_iter()
                    .collect(),
            );
            let right = CardinalityProfile::new(
                Estimate::exact(right_rows),
                [(right_key, test_column_profile(right_rows, right_rows))]
                    .into_iter()
                    .collect(),
            );

            let output =
                join_profile_from_conjuncts(&left, &right, join_type.clone(), &[predicate], &ctx);
            let lower = output.rows.lower.expect("exact inputs have a lower bound");
            let upper = output.rows.upper.expect("exact inputs have an upper bound");

            assert_eq!(lower, expected_minimum, "{join_type:?}");
            assert_eq!(output.rows.value, expected_minimum, "{join_type:?}");
            assert_eq!(upper, expected_minimum, "{join_type:?}");
            assert!(lower <= output.rows.value, "{join_type:?}");
            assert!(output.rows.value <= upper, "{join_type:?}");
        }
    }

    #[test]
    fn outer_join_equality_is_not_redundant_in_a_downstream_join() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let left = CardinalityProfile::new(
            Estimate::default(100.0),
            [(a, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::default(10.0),
            [(b, test_column_profile(10.0, 10.0))].into_iter().collect(),
        );
        let third = CardinalityProfile::new(
            Estimate::default(100.0),
            [(c, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let ab = equality_expr(&mut ctx, a, b);
        let outer = join_profile_from_conjuncts(&left, &right, JoinType::LeftOuter, &[ab], &ctx);
        let ac = equality_expr(&mut ctx, a, c);
        let bc = equality_expr(&mut ctx, b, c);
        let downstream_selectivity =
            join_selectivity_from_conjuncts(&outer, &third, &[ac, bc], &ctx).selectivity;

        let downstream =
            join_profile_from_conjuncts(&outer, &third, JoinType::Inner, &[ac, bc], &ctx);

        assert_eq!(outer.rows.value, 100.0);
        assert_eq!(third.rows.value, 100.0);
        assert!(outer.equivalence_classes.is_empty());
        assert_eq!(outer.columns[&a].distinct.value, 100.0);
        assert_eq!(outer.columns[&b].distinct.value, 10.0);
        assert_eq!(downstream_selectivity.value, 0.0001);
        assert_eq!(downstream.rows.value, 1.0);
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
                            distribution: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    constraints: Default::default(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::new(catalog);

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
                            distribution: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    constraints: Default::default(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::new(catalog);

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
                            distribution: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    constraints: Default::default(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::new(catalog);

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
                            distribution: None,
                        },
                    )]
                    .into_iter()
                    .collect(),
                    constraints: Default::default(),
                },
            )
            .unwrap();
        let mut analyses = AnalysisContext::new(catalog);

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
        let mut analyses = AnalysisContext::new(catalog);

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
            let mut analyses = AnalysisContext::new(catalog);

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
        let mut analyses = AnalysisContext::new(catalog);

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
        let mut analyses = AnalysisContext::new(catalog);

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
        let mut analyses = AnalysisContext::new(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, join).unwrap();

        assert_eq!(profile.rows.value, 100.0);
        assert_eq!(profile.rows.lower, Some(100.0));
    }

    #[test]
    fn cardinality_estimation_for_non_root_join_uses_only_its_predicates() {
        let mut ctx = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let b = ColumnData::new("b", DataType::Int64).add(&mut ctx);
        let c = ColumnData::new("c", DataType::Int64).add(&mut ctx);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("a_table"),
            columns: vec![a],
        })
        .add(&mut ctx);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("b_table"),
            columns: vec![b],
        })
        .add(&mut ctx);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("c_table"),
            columns: vec![c],
        })
        .add(&mut ctx);
        let ab_predicate = equality_expr(&mut ctx, a, b);
        let ab = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: ab_predicate,
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut ctx);
        let ac_predicate = equality_expr(&mut ctx, a, c);
        let root = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: ac_predicate,
            outer: ab,
            inner: scan_c,
        })
        .add(&mut ctx);
        ctx.set_root(root);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        for (table, column) in [("a_table", "a"), ("b_table", "b"), ("c_table", "c")] {
            catalog
                .create_table(TableRef::bare(table), schema(), None)
                .unwrap();
            catalog
                .set_table_statistics(
                    TableRef::bare(table),
                    table_stats_for_column(column, 100, 100),
                )
                .unwrap();
        }
        let mut analyses = AnalysisContext::new(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, ab).unwrap();

        assert_eq!(profile.rows.value, 100.0);
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

    #[test]
    fn at_most_one_row_is_an_exact_structural_property() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let two = ExprData::Literal(ScalarValue::Int64(2)).add(&mut ctx);
        let input = OperatorData::ConstScan(crate::ConstScan {
            columns: vec![column],
            rows: vec![vec![one], vec![two]],
        })
        .add(&mut ctx);
        let false_predicate = ExprData::Literal(ScalarValue::Boolean(false)).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate: false_predicate,
            input,
        })
        .add(&mut ctx);
        let limit = OperatorData::Limit(Limit {
            offset: 0,
            fetch: Some(1),
            input,
        })
        .add(&mut ctx);

        let mut analyses = crate::test_analyses(&ctx);
        let estimated = analyses
            .get::<CardinalityEstimationV1>(&ctx, selection)
            .unwrap();
        assert_eq!(estimated.rows.upper, Some(0.0));
        assert!(!analyses.get::<AtMostOneRow>(&ctx, selection).unwrap());
        assert!(analyses.get::<AtMostOneRow>(&ctx, limit).unwrap());
    }

    #[test]
    fn catalog_aware_cardinality_does_not_silently_fall_back() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("missing"),
            columns: vec![column],
        })
        .add(&mut ctx);
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let mut analyses = AnalysisContext::new(catalog);

        let error = analyses
            .get::<CardinalityEstimationV1>(&ctx, scan)
            .unwrap_err();

        assert!(matches!(error, AnalysisError::Catalog(_)));
        let error = analyses.get::<ColumnNullability>(&ctx, scan).unwrap_err();
        assert!(matches!(error, AnalysisError::Catalog(_)));
    }

    #[test]
    fn resolved_scan_without_statistics_uses_explicit_defaults() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(TableRef::bare("users"), schema(), None)
            .unwrap();
        let mut ctx = QueryContext::new();
        let scan = ctx
            .add_scan_from_catalog(catalog.as_ref(), TableRef::bare("users"))
            .unwrap();
        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            panic!("catalog scan should create a scan operator");
        };
        let mut analyses = AnalysisContext::new(catalog);

        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, scan).unwrap();

        assert_eq!(profile.rows.value, 1000.0);
        assert_eq!(profile.rows.source, EstimateSource::Default);
        for column in &scan_data.columns {
            assert_eq!(
                profile.columns[column].frequency.source,
                EstimateSource::Default
            );
            assert_eq!(
                profile.columns[column].distinct.source,
                EstimateSource::Default
            );
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn single_column_scan() -> (QueryContext, Operator) {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![column],
        })
        .add(&mut ctx);
        (ctx, scan)
    }

    fn fallback_selection_query() -> (QueryContext, Operator) {
        let (mut ctx, scan) = single_column_scan();
        let predicate = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        ctx.set_root(selection);
        (ctx, selection)
    }

    fn like_selection_query() -> (QueryContext, Operator, Operator) {
        let (mut ctx, scan) = single_column_scan();
        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            unreachable!("single_column_scan returns a scan")
        };
        let column = scan_data.columns[0];
        let expr = ExprData::ColumnRef(column).add(&mut ctx);
        let pattern = ExprData::Literal(ScalarValue::Utf8("%x%".into())).add(&mut ctx);
        let predicate = ExprData::Like {
            negated: false,
            expr,
            pattern,
            case_insensitive: false,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        ctx.set_root(selection);
        (ctx, scan, selection)
    }

    fn range_selection_query() -> (QueryContext, Operator) {
        let (mut ctx, scan) = single_column_scan();
        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            unreachable!("single_column_scan returns a scan")
        };
        let column = scan_data.columns[0];
        let left = ExprData::ColumnRef(column).add(&mut ctx);
        let right = ExprData::Literal(ScalarValue::Int64(10)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Lt,
            left,
            right,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        ctx.set_root(selection);
        (ctx, selection)
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
                    distribution: None,
                },
            )]
            .into_iter()
            .collect(),
            constraints: Default::default(),
        }
    }

    fn test_equivalent_profile(rows: f64, columns: [Column; 2]) -> CardinalityProfile {
        let mut profile = CardinalityProfile::new(
            Estimate::exact(rows),
            columns
                .into_iter()
                .map(|column| (column, test_column_profile(rows, rows)))
                .collect(),
        );
        profile.equivalence_classes = vec![ColumnEquivalenceClass {
            columns: BTreeSet::from(columns),
            distinct: Estimate::exact(rows),
        }];
        profile
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

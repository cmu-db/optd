use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::{
    AggregateExpr, AggregateFunction, BinaryOp, Catalog, Column, ColumnSketches, ColumnStatistics,
    Expr, ExprData, JoinType, NaryOp, NodeSet, Operator, OperatorData, QueryContext,
    QueryHypergraph, Relation, ScalarValue, Scan, TableId, TableRef, UnaryOp,
};

mod logical_facts;

pub use logical_facts::{
    BaseColumn, ConstraintSet, LogicalFacts, LogicalFactsAnalysis, ValueEquivalenceClasses,
    ValueId, ValueRange,
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
    /// Query-local column sketches violated compatibility or population invariants.
    InvalidColumnSketch(String),
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
            Self::InvalidColumnSketch(error) => write!(f, "invalid column sketch: {error}"),
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
    /// Estimate produced directly by a query-local probabilistic sketch.
    Sketch,
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

    fn min_by_value(&self, other: Self) -> Self {
        if self.value <= other.value {
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
    /// Logical value whose statistics this profile describes.
    pub value: Option<ValueId>,
    /// Base-population sketches. Operators that change the represented population invalidate this
    /// field unless they can derive a sound replacement.
    pub sketches: Option<Arc<ColumnSketches>>,
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
            value: None,
            sketches: None,
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
        self.sketches = None;
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
/// continuing to constrain the projected profile. Equal values cannot have more distinct values
/// than the smallest member domain.
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
            .min_by(|a, b| a.value.total_cmp(&b.value))
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
            .min_by(|a, b| a.value.total_cmp(&b.value))
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

/// Tunable fallback assumptions used by [`CardinalityEstimationV1`].
///
/// These values expose the constants already used by the V1 estimator; they do
/// not enable any additional statistics or estimation models. The default is
/// therefore behaviorally identical to the estimator before configuration was
/// introduced.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CardinalityEstimationConfig {
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

pub(crate) type ColumnSketchStore = BTreeMap<(TableId, String), Arc<ColumnSketches>>;

/// Registry of lazily-created analysis instances.
pub struct AnalysisContext {
    analyses: AnalysisRegistry,
    catalog: Arc<dyn Catalog>,
    cardinality_config: CardinalityEstimationConfig,
    /// Query-local base-column sketches. Derived operator statistics remain owned by their
    /// demand-driven analyses; a future catalog cache can populate this same boundary.
    column_sketches: ColumnSketchStore,
}

impl AnalysisContext {
    /// Creates analysis state backed by `catalog`.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog,
            cardinality_config: CardinalityEstimationConfig::default(),
            column_sketches: BTreeMap::new(),
        }
    }

    /// Uses explicit V1 fallback assumptions for this analysis context.
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

    /// Replaces the V1 fallback assumptions used by this analysis context.
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

    /// Returns the fallback assumptions used by cardinality estimation.
    pub fn cardinality_estimation_config(&self) -> CardinalityEstimationConfig {
        self.cardinality_config
    }

    /// Returns the catalog used by every analysis in this context.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Installs query-local sketches for a base column and invalidates derived analyses.
    ///
    /// Sketches are keyed by resolved table identity and catalog column name. The payload is shared
    /// by derived profiles and remains available when analysis caches are cleared.
    pub fn set_column_sketches(
        &mut self,
        table: TableRef,
        column: impl Into<String>,
        sketches: ColumnSketches,
    ) -> AnalysisResult<()> {
        sketches
            .validate()
            .map_err(AnalysisError::InvalidColumnSketch)?;
        let table_id = self
            .catalog
            .table_by_ref(&table)
            .map_err(|error| AnalysisError::Catalog(error.to_string()))?
            .id;
        self.column_sketches
            .insert((table_id, column.into()), Arc::new(sketches));
        self.clear();
        Ok(())
    }

    /// Returns query-local sketches for a base column, when collected.
    pub fn column_sketches(
        &self,
        table: &TableRef,
        column: &str,
    ) -> AnalysisResult<Option<Arc<ColumnSketches>>> {
        let table_id = self
            .catalog
            .table_by_ref(table)
            .map_err(|error| AnalysisError::Catalog(error.to_string()))?
            .id;
        Ok(self
            .column_sketches
            .get(&(table_id, column.to_owned()))
            .cloned())
    }

    pub(crate) fn from_planned_parts(
        catalog: Arc<dyn Catalog>,
        cardinality_config: CardinalityEstimationConfig,
        column_sketches: ColumnSketchStore,
    ) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog,
            cardinality_config,
            column_sketches,
        }
    }

    pub(crate) fn planned_sketches(&self) -> ColumnSketchStore {
        self.column_sketches.clone()
    }

    /// Creates a fresh derived-analysis cache with the same catalog, configuration, and base
    /// sketches as this context.
    pub fn fork(&self) -> Self {
        Self {
            analyses: AnalysisRegistry::new(),
            catalog: Arc::clone(&self.catalog),
            cardinality_config: self.cardinality_config,
            column_sketches: self.column_sketches.clone(),
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
            | crate::BinaryOp::NotEq
            | crate::BinaryOp::Lt
            | crate::BinaryOp::LtEq
            | crate::BinaryOp::Gt
            | crate::BinaryOp::GtEq => {
                collect_expr_used_columns(ctx, analyses, *left, columns)?;
                collect_expr_used_columns(ctx, analyses, *right, columns)?;
            }
            crate::BinaryOp::IsNotDistinctFrom
            | crate::BinaryOp::Add
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
        let mut profile = cardinality_profile(operator, ctx, analyses)?;
        let facts = LogicalFactsAnalysis::get_shared(ctx, analyses, operator)?;

        let mut classes = BTreeMap::<ValueId, BTreeSet<Column>>::new();
        for (&column, value) in &facts.lineage {
            if profile.columns.contains_key(&column) {
                classes
                    .entry(facts.constraints.canonical(value))
                    .or_default()
                    .insert(column);
            }
        }
        let equivalence_classes = classes
            .into_values()
            .filter(|columns| columns.len() >= 2)
            .map(|columns| {
                let distinct = columns
                    .iter()
                    .filter_map(|column| profile.columns.get(column))
                    .map(|column| column.distinct.clone())
                    .min_by(|left, right| left.value.total_cmp(&right.value))
                    .unwrap_or_else(|| Estimate::default(0.0));
                ColumnEquivalenceClass { columns, distinct }
            })
            .collect::<Vec<_>>();
        let lineage_changed = profile.columns.iter().any(|(column, column_profile)| {
            column_profile.value.as_ref() != facts.lineage.get(column)
        });
        let contradiction_changed = facts.constraints.contradictory && profile.rows.value != 0.0;
        if !lineage_changed
            && profile.equivalence_classes == equivalence_classes
            && !contradiction_changed
        {
            return Ok(profile);
        }

        let mutable_profile = Arc::make_mut(&mut profile);
        for (column, column_profile) in &mut mutable_profile.columns {
            column_profile.value = facts.lineage.get(column).cloned();
        }
        mutable_profile.equivalence_classes = equivalence_classes;
        cap_equivalent_column_distinct_counts(mutable_profile);
        if contradiction_changed {
            *mutable_profile = mutable_profile.cap_by_rows(Estimate::exact(0.0));
        }
        Ok(profile)
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
    let catalog = Arc::clone(analyses.catalog());
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
            let input_facts = LogicalFactsAnalysis::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(apply_selection_profile(
                input.as_ref().clone(),
                input_facts.as_ref().clone(),
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
            let offset = data.offset as f64;
            let remaining = Estimate::derived(
                (input.rows.value - offset).max(0.0),
                input.rows.lower.map(|lower| (lower - offset).max(0.0)),
                input.rows.upper.map(|upper| (upper - offset).max(0.0)),
            );
            let rows = match data.fetch {
                Some(fetch) => remaining.cap(fetch as f64, EstimateSource::Derived),
                None => remaining,
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
            let input_facts = LogicalFactsAnalysis::get_shared(ctx, analyses, data.input)?;
            Ok(Arc::new(aggregation_profile(
                &input,
                &input_facts,
                data,
                ctx,
                &config,
            )))
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
                Some(catalog.as_ref()),
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
        let sketches = analyses.column_sketches(&scan.table, column_name)?;
        let profile = catalog_column
            .map(|stats| column_profile_from_stats(stats, &row_estimate, sketches.clone()))
            .unwrap_or_else(|| {
                default_scan_column_profile(&row_estimate, config, sketches.clone())
            });
        columns.insert(*column, profile);
    }

    Ok(CardinalityProfile::new(row_estimate, columns))
}

fn column_profile_from_stats(
    stats: &ColumnStatistics,
    rows: &Estimate,
    sketches: Option<Arc<ColumnSketches>>,
) -> ColumnProfile {
    let frequency = stats
        .frequency
        .map(|value| Estimate::catalog(value as f64))
        .unwrap_or_else(|| Estimate::default(rows.value));
    let sketch_distinct = sketches
        .as_ref()
        .and_then(|sketches| sketches.distinct_values.as_ref())
        .map(|hll| hll.estimate().min(frequency.value));
    let distinct_value = stats.distinct.map(|value| value as f64).or(sketch_distinct);
    let distinct = match (stats.distinct, distinct_value) {
        (Some(_), Some(value)) => Estimate::catalog(value.min(frequency.value)),
        (None, Some(value)) => Estimate {
            value: value.min(frequency.value),
            lower: Some(0.0),
            upper: Some(frequency.value),
            source: EstimateSource::Sketch,
        },
        (_, None) => Estimate::default(frequency.value.min(rows.value)),
    };
    ColumnProfile {
        lower_bound: stats.lower_bound.clone(),
        upper_bound: stats.upper_bound.clone(),
        frequency,
        distinct,
        value: None,
        sketches,
    }
}

fn default_scan_column_profile(
    rows: &Estimate,
    config: &CardinalityEstimationConfig,
    sketches: Option<Arc<ColumnSketches>>,
) -> ColumnProfile {
    let distinct = sketches
        .as_ref()
        .and_then(|sketches| sketches.distinct_values.as_ref())
        .map(|hll| Estimate {
            value: hll.estimate().min(rows.value),
            lower: Some(0.0),
            upper: Some(rows.value),
            source: EstimateSource::Sketch,
        })
        .unwrap_or_else(|| Estimate::default(rows.value.min(config.unknown_column_ndv_cap)));
    ColumnProfile {
        lower_bound: None,
        upper_bound: None,
        frequency: Estimate::default(rows.value),
        distinct,
        value: None,
        sketches,
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
                value: None,
                sketches: None,
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
        output.columns.insert(*column, profile);
    }
    output
}

fn profile_for_computation(
    input: &CardinalityProfile,
    expr: Expr,
    ctx: &QueryContext,
) -> Option<ColumnProfile> {
    // Only direct aliases inherit statistics. Every other expression remains opaque until a
    // generic statistics-transform provider can prove how its value domain changes.
    let ExprData::ColumnRef(column) = expr.get(ctx) else {
        return None;
    };
    input.columns.get(column).cloned()
}

fn aggregation_profile(
    input: &CardinalityProfile,
    input_facts: &LogicalFacts,
    data: &crate::Aggregation,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    let group_rows = if data.keys.is_empty() {
        // SQL scalar aggregation emits exactly one row even for an empty input.
        Estimate::exact(1.0)
    } else {
        // TODO(statistics): Once sampling is available, use sampled multi-column NDV for
        // non-equivalent grouping keys instead of multiplying independent single-column NDVs.
        let mut seen_values = BTreeSet::new();
        let product = data.keys.iter().fold(1.0, |acc, expr| {
            if let ExprData::ColumnRef(column) = expr.get(ctx) {
                if let Some(value) = input_facts.canonical_value_for_column(*column)
                    && !seen_values.insert(value)
                {
                    return acc;
                }
                acc * input.columns.get(column).map_or(
                    input.rows.value.min(config.unknown_column_ndv_cap),
                    |profile| profile.distinct.value,
                )
            } else {
                acc * input.rows.value.min(config.unknown_column_ndv_cap)
            }
        });
        Estimate::derived(product.min(input.rows.value), Some(0.0), input.rows.upper)
            .cap(input.rows.value, EstimateSource::Derived)
    };

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
                value: None,
                sketches: None,
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
                value: None,
                sketches: None,
            },
            _ => {
                let mut profile =
                    ColumnProfile::unknown_with_ndv_cap(&group_rows, config.unknown_column_ndv_cap);
                // Non-COUNT aggregates are nullable: empty scalar input and all-NULL groups
                // produce NULL. Keep the estimate explicit rather than claiming every output is
                // non-null.
                let non_null_fraction = if input.rows.value == 0.0 { 0.0 } else { 0.9 };
                profile.frequency = Estimate::derived(
                    group_rows.value * non_null_fraction,
                    Some(0.0),
                    group_rows.upper,
                );
                profile.distinct = profile
                    .distinct
                    .cap(profile.frequency.value, EstimateSource::Derived);
                profile
            }
        };
        columns.insert(*column, profile);
    }
    CardinalityProfile::new(group_rows, columns)
}

fn apply_selection_profile(
    mut profile: CardinalityProfile,
    mut facts: LogicalFacts,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
) -> CardinalityProfile {
    // Charge only information newly introduced by each conjunct. This handles both predicates
    // repeated across stacked selections and redundant equalities inside one conjunction.
    for conjunct in conjuncts(predicate, ctx) {
        let before = facts.constraints.clone();
        facts.apply_predicate(conjunct, ctx);
        let selectivity = if facts.constraints == before {
            1.0
        } else if facts.constraints.contradictory {
            0.0
        } else {
            filter_selectivity(&profile, conjunct, ctx, config).value
        };
        let value_restriction = value_restricted_filter_column(conjunct, ctx);
        profile = scale_profile(profile, selectivity, value_restriction);
        tighten_filter_columns(&mut profile, conjunct, ctx);
    }
    profile
}

fn scale_profile(
    profile: CardinalityProfile,
    factor: f64,
    value_restriction: Option<(Column, BinaryOp)>,
) -> CardinalityProfile {
    let factor = factor.clamp(0.0, 1.0);
    let rows = profile.rows.scale(factor, EstimateSource::Derived);
    let columns = profile
        .columns
        .iter()
        .map(|(&column, column_profile)| {
            let mut output = column_profile.scale_frequency(factor, &rows);
            if let Some((_, restricted_op)) =
                value_restriction.filter(|(restricted, _)| *restricted == column)
            {
                // A direct comparison with a literal removes values from this column's domain,
                // rather than independently thinning every value's rows. Remove the input null
                // fraction from the row selectivity before applying it to the non-null NDV.
                let non_null_fraction = if profile.rows.value <= 0.0 {
                    0.0
                } else {
                    (column_profile.frequency.value / profile.rows.value).clamp(0.0, 1.0)
                };
                let domain_fraction = if non_null_fraction <= 0.0 {
                    0.0
                } else {
                    (factor / non_null_fraction).clamp(0.0, 1.0)
                };
                output.distinct = if restricted_op == BinaryOp::NotEq {
                    Estimate::derived(
                        (column_profile.distinct.value - 1.0)
                            .max(0.0)
                            .min(rows.value),
                        Some(0.0),
                        Some(
                            (column_profile.distinct.value - 1.0)
                                .max(0.0)
                                .min(rows.value),
                        ),
                    )
                } else {
                    column_profile
                        .distinct
                        .scale(domain_fraction, EstimateSource::Derived)
                        .cap(rows.value, EstimateSource::Derived)
                };
                // Ordinary comparisons reject NULL for the constrained column.
                output.frequency = rows.clone();
            } else {
                output.distinct = surviving_distinct_after_row_filter(
                    column_profile,
                    factor,
                    output.frequency.value,
                );
            }
            output.sketches = None;
            (column, output)
        })
        .collect();
    let equivalence_classes = filter_equivalence_classes(&profile.equivalence_classes, &columns);
    CardinalityProfile {
        rows,
        columns,
        equivalence_classes,
    }
}

/// Expected surviving NDV after independently retaining each non-null row with `factor`.
///
/// Under uniform multiplicity, one value has `frequency / distinct` opportunities to survive, so
/// its survival probability is `1 - (1 - factor)^(frequency / distinct)`. Direct value-domain
/// predicates use proportional domain scaling above instead of this occupancy model.
fn surviving_distinct_after_row_filter(
    profile: &ColumnProfile,
    factor: f64,
    output_frequency: f64,
) -> Estimate {
    if profile.distinct.value <= 0.0 || profile.frequency.value <= 0.0 || factor <= 0.0 {
        return Estimate::derived(0.0, Some(0.0), Some(0.0));
    }
    if factor >= 1.0 {
        return profile
            .distinct
            .cap(output_frequency, EstimateSource::Derived);
    }
    let average_multiplicity = profile.frequency.value / profile.distinct.value;
    let survival_probability = -(average_multiplicity * (-factor).ln_1p()).exp_m1();
    Estimate::derived(
        (profile.distinct.value * survival_probability).min(output_frequency),
        Some(0.0),
        Some(profile.distinct.value.min(output_frequency)),
    )
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
        output.sketches = None;
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
        output.sketches = None;
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
        None,
    )
}

fn join_profile_from_predicate_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    predicate: Expr,
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
    catalog: Option<&dyn Catalog>,
) -> CardinalityProfile {
    let conjuncts = conjuncts(predicate, ctx);
    join_profile_from_conjuncts_with_config(
        left, right, join_type, &conjuncts, ctx, config, catalog,
    )
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
        None,
    )
}

fn join_profile_from_conjuncts_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    conjuncts: &[Expr],
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
    catalog: Option<&dyn Catalog>,
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
    let estimate =
        join_selectivity_from_conjuncts_with_config(left, right, conjuncts, ctx, config, catalog);
    join_profile_with_selectivity_and_classes(left, right, join_type, estimate)
}

fn join_profile_with_selectivity_and_classes(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    join_type: JoinType,
    estimate: JoinSelectivityEstimate,
) -> CardinalityProfile {
    let non_null_equality_columns = estimate.non_null_equality_columns.clone();
    let left_equality_coverages = estimate.left_equality_coverages.clone();
    let residual_selectivity = estimate.residual_selectivity;
    let selectivity = estimate.selectivity;
    let inner_rows = Estimate::derived(
        left.rows.value * right.rows.value * selectivity.value,
        Some(0.0),
        multiply_options(left.rows.upper, right.rows.upper).map(|v| v * selectivity.value),
    );
    match join_type {
        JoinType::LeftSemi => semi_join_profile(
            left,
            estimate.match_probability.value,
            &non_null_equality_columns,
            &left_equality_coverages,
        ),
        JoinType::LeftAnti => anti_join_profile(
            left,
            estimate.match_probability.value,
            &left_equality_coverages,
            residual_selectivity,
        ),
        JoinType::Single => single_join_profile(left, right),
        JoinType::LeftOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(left.rows.value),
            // ON equalities hold only for matched rows. Null extension therefore preserves
            // equality knowledge from the non-null-supplying input, but not from the right input
            // or across the join boundary.
            left.equivalence_classes.clone(),
            true,
            false,
        ),
        JoinType::RightOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(right.rows.value),
            right.equivalence_classes.clone(),
            false,
            true,
        ),
        JoinType::FullOuter => combine_join_columns(
            left,
            right,
            inner_rows,
            Some(if selectivity.value == 0.0 {
                left.rows.value + right.rows.value
            } else {
                left.rows.value.max(right.rows.value)
            }),
            Vec::new(),
            true,
            true,
        ),
        JoinType::LeftMark {
            marker: column,
            nullable,
        } => {
            let mut profile = left.cap_by_rows(left.rows.clone());
            let max_distinct = 2.0_f64;
            let frequency = if nullable {
                Estimate::derived(profile.rows.value * 0.9, Some(0.0), profile.rows.upper)
            } else {
                profile.rows.clone()
            };
            profile.columns.insert(
                column,
                ColumnProfile {
                    lower_bound: Some(ScalarValue::Boolean(false)),
                    upper_bound: Some(ScalarValue::Boolean(true)),
                    frequency,
                    distinct: Estimate::derived(
                        max_distinct.min(profile.rows.value),
                        Some(0.0),
                        Some(max_distinct),
                    ),
                    value: None,
                    sketches: None,
                },
            );
            profile
        }
        JoinType::Inner => {
            let mut profile = combine_join_columns(
                left,
                right,
                inner_rows,
                None,
                estimate.equivalence_classes,
                false,
                false,
            );
            mark_columns_non_null(&mut profile, &non_null_equality_columns);
            profile
        }
    }
}

fn single_join_profile(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
) -> CardinalityProfile {
    let mut columns = left.columns.clone();
    for (&column, right_profile) in &right.columns {
        let mut output = right_profile.clone();
        let non_null_fraction = if right.rows.value <= 0.0 {
            0.0
        } else {
            (right_profile.frequency.value / right.rows.value).clamp(0.0, 1.0)
        };
        let present_fraction = right.rows.value.clamp(0.0, 1.0) * non_null_fraction;
        output.frequency = Estimate::derived(
            left.rows.value * present_fraction,
            Some(0.0),
            left.rows.upper,
        );
        output.distinct = output
            .distinct
            .cap(output.frequency.value, EstimateSource::Derived);
        output.sketches = None;
        columns.insert(column, output);
    }
    CardinalityProfile {
        rows: left.rows.clone(),
        columns,
        equivalence_classes: left.equivalence_classes.clone(),
    }
}

fn semi_join_profile(
    input: &CardinalityProfile,
    match_probability: f64,
    non_null_equality_columns: &BTreeSet<Column>,
    left_equality_coverages: &[LeftEqualityCoverage],
) -> CardinalityProfile {
    let match_probability = match_probability.clamp(0.0, 1.0);
    let rows = Estimate::derived(
        (input.rows.value * match_probability).clamp(0.0, input.rows.value),
        Some(0.0),
        input.rows.upper,
    );
    let mut profile = scale_profile(input.clone(), match_probability, None);
    profile.rows = rows.clone();
    for column_profile in profile.columns.values_mut() {
        column_profile.cap_by_rows_mut(&rows);
    }
    for coverage in left_equality_coverages {
        let Some(column_profile) = profile.columns.get_mut(&coverage.column) else {
            continue;
        };
        column_profile.distinct = column_profile
            .distinct
            .cap(coverage.intersection_distinct, EstimateSource::Derived);
        if coverage.ordinary_non_null {
            column_profile.frequency = rows.clone();
        } else {
            let edge_match = coverage.non_null_match_probability + coverage.null_match_probability;
            let conditional_non_null = if edge_match <= 0.0 {
                0.0
            } else {
                (coverage.non_null_match_probability / edge_match).clamp(0.0, 1.0)
            };
            column_profile.frequency =
                Estimate::derived(rows.value * conditional_non_null, Some(0.0), rows.upper);
        }
        column_profile.cap_by_rows_mut(&rows);
    }
    mark_columns_non_null(&mut profile, non_null_equality_columns);
    profile
}

fn mark_columns_non_null(profile: &mut CardinalityProfile, columns: &BTreeSet<Column>) {
    for column in columns {
        if let Some(column_profile) = profile.columns.get_mut(column) {
            column_profile.frequency = profile.rows.clone();
        }
    }
}

fn anti_join_profile(
    input: &CardinalityProfile,
    match_probability: f64,
    left_equality_coverages: &[LeftEqualityCoverage],
    residual_selectivity: f64,
) -> CardinalityProfile {
    let unmatched_probability = 1.0 - match_probability.clamp(0.0, 1.0);
    let rows = Estimate::derived(
        (input.rows.value * unmatched_probability).clamp(0.0, input.rows.value),
        Some(0.0),
        input.rows.upper,
    );
    let mut profile = scale_profile(input.clone(), unmatched_probability, None);
    profile.rows = rows.clone();
    for column_profile in profile.columns.values_mut() {
        column_profile.cap_by_rows_mut(&rows);
    }

    // For one pure equality, unmatched non-null mass and domain are the complement of directional
    // coverage. With multiple keys or residual predicates, a row can fail for another reason, so
    // subtracting every key domain would be unsound and the occupancy fallback is retained.
    if left_equality_coverages.len() == 1 && residual_selectivity == 1.0 {
        let coverage = left_equality_coverages[0];
        if let Some(column_profile) = profile.columns.get_mut(&coverage.column) {
            let unmatched_non_null_probability =
                (coverage.input_non_null_fraction - coverage.non_null_match_probability).max(0.0);
            column_profile.frequency = Estimate::derived(
                (input.rows.value * unmatched_non_null_probability).min(rows.value),
                Some(0.0),
                rows.upper,
            );
            let unmatched_distinct =
                (coverage.input_distinct - coverage.intersection_distinct).max(0.0);
            column_profile.distinct = Estimate::derived(
                unmatched_distinct.min(column_profile.frequency.value),
                Some(0.0),
                Some(unmatched_distinct.min(column_profile.frequency.value)),
            );
            column_profile.cap_by_rows_mut(&rows);
        }
    }
    profile
}

fn combine_join_columns(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    mut rows: Estimate,
    lower_bound: Option<f64>,
    equivalence_classes: Vec<ColumnEquivalenceClass>,
    preserve_left: bool,
    preserve_right: bool,
) -> CardinalityProfile {
    let matched_rows = rows.value;
    if let Some(min_rows) = lower_bound {
        rows.value = rows.value.max(min_rows);
        rows.lower = Some(rows.lower.unwrap_or(0.0).max(min_rows));
        rows.upper = rows.upper.map(|upper| upper.max(min_rows));
    }
    let mut columns = derive_join_side_columns(left, matched_rows, &rows, preserve_left);
    // Preserve the established right-input precedence if malformed/derived plans reuse a column
    // handle across inputs.
    columns.append(&mut derive_join_side_columns(
        right,
        matched_rows,
        &rows,
        preserve_right,
    ));
    let equivalence_classes = filter_owned_equivalence_classes(equivalence_classes, &columns);
    let mut output = CardinalityProfile {
        rows,
        columns,
        equivalence_classes,
    };
    cap_equivalent_column_distinct_counts(&mut output);
    output
}

fn derive_join_side_columns(
    input: &CardinalityProfile,
    matched_rows: f64,
    output_rows: &Estimate,
    preserve_input: bool,
) -> BTreeMap<Column, ColumnProfile> {
    input
        .columns
        .iter()
        .map(|(&column, profile)| {
            let non_null_fraction = if input.rows.value <= 0.0 {
                0.0
            } else {
                (profile.frequency.value / input.rows.value).clamp(0.0, 1.0)
            };
            let represented_rows = matched_rows
                + if preserve_input {
                    (input.rows.value - matched_rows).max(0.0)
                } else {
                    0.0
                };
            let mut output = profile.clone();
            output.frequency = Estimate::derived(
                (represented_rows * non_null_fraction).min(output_rows.value),
                Some(0.0),
                output_rows.upper,
            );
            output.distinct = output
                .distinct
                .cap(output.frequency.value, EstimateSource::Derived);
            output.sketches = None;
            (column, output)
        })
        .collect()
}

fn cap_equivalent_column_distinct_counts(profile: &mut CardinalityProfile) {
    for class in &profile.equivalence_classes {
        for column in &class.columns {
            if let Some(column_profile) = profile.columns.get_mut(column) {
                column_profile.distinct = column_profile
                    .distinct
                    .cap(class.distinct.value, EstimateSource::Derived);
            }
        }
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
        None,
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
    left_equality_coverages: Vec<LeftEqualityCoverage>,
    residual_selectivity: f64,
    non_null_equality_columns: BTreeSet<Column>,
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
        None,
    )
}

fn join_selectivity_from_conjuncts_with_config(
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    conjuncts: &[Expr],
    ctx: &QueryContext,
    config: &CardinalityEstimationConfig,
    catalog: Option<&dyn Catalog>,
) -> JoinSelectivityEstimate {
    let mut equality_pairs = Vec::new();
    let mut residual_selectivity = 1.0;
    for &predicate in conjuncts {
        if let Some((left_col, right_col)) = column_equality(predicate, ctx) {
            let null_safe = matches!(
                predicate.get(ctx),
                ExprData::Binary {
                    op: BinaryOp::IsNotDistinctFrom,
                    ..
                }
            );
            equality_pairs.push((left_col, right_col, null_safe));
        } else {
            residual_selectivity *=
                filter_selectivity_for_predicate(left, right, predicate, ctx, config).value;
        }
    }

    let class_pairs = equality_pairs
        .iter()
        .map(|(left, right, _)| (*left, *right))
        .collect::<Vec<_>>();
    let mut classes = EquivalenceClassState::from_profiles_and_equalities(
        left,
        right,
        &class_pairs,
        config.unknown_column_ndv_cap,
    );
    let mut non_null_equality_columns = equality_pairs
        .iter()
        .filter(|(_, _, null_safe)| !null_safe)
        .flat_map(|(left, right, _)| [*left, *right])
        .collect::<BTreeSet<_>>();
    // Non-nullness propagates through null-safe equality once any member of the connected value
    // class participates in ordinary equality.
    loop {
        let previous_len = non_null_equality_columns.len();
        for (left, right, _) in &equality_pairs {
            if non_null_equality_columns.contains(left) || non_null_equality_columns.contains(right)
            {
                non_null_equality_columns.extend([*left, *right]);
            }
        }
        if non_null_equality_columns.len() == previous_len {
            break;
        }
    }
    let mut equality_edges = equality_pairs
        .into_iter()
        .map(|(left_col, right_col, null_safe)| {
            let left_ndv = classes.class_distinct(left_col);
            let right_ndv = classes.class_distinct(right_col);
            EqualityEdge {
                left: left_col,
                right: right_col,
                null_safe: null_safe && !non_null_equality_columns.contains(&left_col),
                chosen_ndv: left_ndv.max_by_value(right_ndv.clone()),
                left_ndv,
                right_ndv,
            }
        })
        .collect::<Vec<_>>();

    // Process the most selective equality first, then union equivalent columns.
    // This avoids multiplying selectivity again for transitive predicates such
    // as a = b AND b = c AND a = c.
    equality_edges.sort_by(|a, b| {
        b.chosen_ndv
            .value
            .total_cmp(&a.chosen_ndv.value)
            .then_with(|| a.null_safe.cmp(&b.null_safe))
    });
    // TODO(statistics): Once sampling is available, replace products across composite equality
    // keys with sampled multi-column NDV and overlap statistics.
    let mut equality_selectivity = 1.0;
    let mut equality_match_probability = 1.0;
    let mut left_equality_coverages = Vec::new();
    let mut has_cross_input_equality = false;
    for edge in equality_edges {
        let connects_inputs =
            column_sides(edge.left, left, right) != column_sides(edge.right, left, right);
        let edge_estimate =
            connects_inputs.then(|| equality_edge_estimate(&edge, left, right, catalog));

        // Every equality remains a real constraint even when it closes a transitive cycle. A
        // disjoint ordered domain therefore proves the whole conjunction unsatisfiable.
        if edge_estimate
            .as_ref()
            .is_some_and(|estimate| estimate.domains_disjoint)
        {
            equality_selectivity = 0.0;
            equality_match_probability = 0.0;
        }
        if classes.equivalent(edge.left, edge.right) {
            continue;
        }
        if let Some(edge_estimate) = edge_estimate {
            has_cross_input_equality = true;
            equality_selectivity *= edge_estimate.pair_selectivity;
            equality_match_probability *= edge_estimate.left_match_probability;
            if let Some(coverage) = edge_estimate.left_coverage {
                left_equality_coverages.push(coverage);
            }
        }
        classes.union(edge.left, edge.right, edge.chosen_ndv);
    }

    let selectivity = (equality_selectivity * residual_selectivity).clamp(0.0, 1.0);
    let first_moment_upper = (right.rows.value * selectivity).clamp(0.0, 1.0);
    let match_probability = if has_cross_input_equality {
        // TODO(statistics): Model residual predicates as probability that at least one candidate
        // partner survives. Linear scaling is conservative in implementation complexity but loses
        // fanout/correlation information.
        (equality_match_probability * residual_selectivity).clamp(0.0, 1.0)
    } else {
        // With no equality-domain coverage information, retain the first-moment heuristic. It is
        // an upper bound when pair selectivity is exact, not a calibrated existence probability.
        first_moment_upper
    };
    #[cfg(test)]
    let equivalence_state_columns = classes.tracked_column_count();
    JoinSelectivityEstimate {
        selectivity: Estimate::derived(selectivity, Some(0.0), Some(1.0)),
        equivalence_classes: classes.into_classes(),
        match_probability: Estimate::derived(
            match_probability,
            Some(0.0),
            Some(first_moment_upper.max(match_probability)),
        ),
        left_equality_coverages,
        residual_selectivity,
        non_null_equality_columns,
        #[cfg(test)]
        equivalence_state_columns,
    }
}

#[derive(Debug, Clone, Copy)]
struct LeftEqualityCoverage {
    column: Column,
    input_distinct: f64,
    input_non_null_fraction: f64,
    intersection_distinct: f64,
    non_null_match_probability: f64,
    null_match_probability: f64,
    ordinary_non_null: bool,
}

#[derive(Debug, Clone, Copy)]
struct EqualityEdgeEstimate {
    pair_selectivity: f64,
    left_match_probability: f64,
    left_coverage: Option<LeftEqualityCoverage>,
    domains_disjoint: bool,
}

fn equality_edge_estimate(
    edge: &EqualityEdge,
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    catalog: Option<&dyn Catalog>,
) -> EqualityEdgeEstimate {
    let first = edge.left;
    let second = edge.right;
    let null_safe = edge.null_safe;
    let Some((left_column, right_column, left_profile, right_profile)) =
        oriented_join_columns(first, second, left, right)
    else {
        let pair_selectivity = 1.0
            / join_column_distinct(first, left, right)
                .max(join_column_distinct(second, left, right))
                .max(1.0);
        return EqualityEdgeEstimate {
            pair_selectivity,
            left_match_probability: (right.rows.value * pair_selectivity).clamp(0.0, 1.0),
            left_coverage: None,
            domains_disjoint: false,
        };
    };

    let left_base = base_column(left_profile);
    let right_base = base_column(right_profile);
    let (left_distinct_hint, right_distinct_hint) = if left_column == first {
        (edge.left_ndv.value, edge.right_ndv.value)
    } else {
        (edge.right_ndv.value, edge.left_ndv.value)
    };
    let left_distinct =
        effective_join_distinct(left, left_profile, left_base, catalog).max(left_distinct_hint);
    let right_distinct =
        effective_join_distinct(right, right_profile, right_base, catalog).max(right_distinct_hint);
    let intersection =
        estimated_distinct_intersection(left_profile, right_profile, left_distinct, right_distinct);
    let left_non_null = join_column_non_null_fraction(left_column, left, right);
    let right_non_null = join_column_non_null_fraction(right_column, left, right);
    let left_has_null = left.rows.value > left_profile.frequency.value;
    let right_has_null = right.rows.value > right_profile.frequency.value;
    let null_pair_selectivity = if null_safe && left_has_null && right_has_null {
        (1.0 - left_non_null) * (1.0 - right_non_null)
    } else {
        0.0
    };
    let null_match_probability = if null_safe && right_has_null {
        1.0 - left_non_null
    } else {
        0.0
    };
    if intersection.domains_disjoint {
        let nulls_can_match = null_pair_selectivity > 0.0;
        return EqualityEdgeEstimate {
            pair_selectivity: null_pair_selectivity,
            left_match_probability: null_match_probability,
            left_coverage: Some(LeftEqualityCoverage {
                column: left_column,
                input_distinct: left_distinct,
                input_non_null_fraction: left_non_null,
                intersection_distinct: 0.0,
                non_null_match_probability: 0.0,
                null_match_probability,
                ordinary_non_null: !null_safe,
            }),
            domains_disjoint: !nulls_can_match,
        };
    }

    if !null_safe
        && right.rows.value > 0.0
        && let (Some(catalog), Some(left_base), Some(right_base)) = (catalog, left_base, right_base)
        && single_column_foreign_key(left_base, right_base, catalog)
        && profile_has_complete_base_population(right, right_base, catalog)
    {
        // Every surviving non-null FK row has exactly one partner in the complete referenced key.
        return EqualityEdgeEstimate {
            pair_selectivity: if right.rows.value <= 0.0 {
                0.0
            } else {
                left_non_null / right.rows.value
            },
            left_match_probability: left_non_null,
            left_coverage: Some(LeftEqualityCoverage {
                column: left_column,
                input_distinct: left_distinct,
                input_non_null_fraction: left_non_null,
                intersection_distinct: left_distinct,
                non_null_match_probability: left_non_null,
                null_match_probability: 0.0,
                ordinary_non_null: true,
            }),
            domains_disjoint: false,
        };
    }

    let equal_non_null = if left_distinct <= 0.0 || right_distinct <= 0.0 {
        0.0
    } else {
        left_non_null * right_non_null * intersection.distinct / (left_distinct * right_distinct)
    };
    let pair_selectivity = if !null_safe {
        equal_non_null
    } else {
        equal_non_null + null_pair_selectivity
    };
    let uniform_non_null_match_probability = if left_distinct <= 0.0 {
        0.0
    } else {
        left_non_null * intersection.distinct / left_distinct
    };
    let non_null_match_probability = uniform_non_null_match_probability;
    EqualityEdgeEstimate {
        pair_selectivity: pair_selectivity.clamp(0.0, 1.0),
        left_match_probability: (non_null_match_probability + null_match_probability)
            .clamp(0.0, 1.0),
        left_coverage: Some(LeftEqualityCoverage {
            column: left_column,
            input_distinct: left_distinct,
            input_non_null_fraction: left_non_null,
            intersection_distinct: intersection.distinct,
            non_null_match_probability,
            null_match_probability,
            ordinary_non_null: !null_safe,
        }),
        domains_disjoint: false,
    }
}

fn oriented_join_columns<'a>(
    first: Column,
    second: Column,
    left: &'a CardinalityProfile,
    right: &'a CardinalityProfile,
) -> Option<(Column, Column, &'a ColumnProfile, &'a ColumnProfile)> {
    match (
        left.columns.get(&first),
        right.columns.get(&second),
        left.columns.get(&second),
        right.columns.get(&first),
    ) {
        (Some(left_profile), Some(right_profile), _, _) => {
            Some((first, second, left_profile, right_profile))
        }
        (_, _, Some(left_profile), Some(right_profile)) => {
            Some((second, first, left_profile, right_profile))
        }
        _ => None,
    }
}

fn join_column_distinct(
    column: Column,
    left: &CardinalityProfile,
    right: &CardinalityProfile,
) -> f64 {
    left.columns
        .get(&column)
        .or_else(|| right.columns.get(&column))
        .map_or(1.0, |profile| profile.distinct.value)
}

fn base_column(profile: &ColumnProfile) -> Option<&BaseColumn> {
    match profile.value.as_ref() {
        Some(ValueId::Base(base)) => Some(base),
        _ => None,
    }
}

fn effective_join_distinct(
    relation: &CardinalityProfile,
    profile: &ColumnProfile,
    base: Option<&BaseColumn>,
    catalog: Option<&dyn Catalog>,
) -> f64 {
    if let (Some(base), Some(catalog)) = (base, catalog)
        && single_column_unique(base, catalog)
        && profile_has_complete_base_population(relation, base, catalog)
    {
        // A complete base population cannot duplicate a provider-asserted unique value. A later
        // join may duplicate it, so uniqueness is not reused once row provenance becomes derived.
        profile.frequency.value
    } else {
        profile.distinct.value
    }
}

fn single_column_unique(base: &BaseColumn, catalog: &dyn Catalog) -> bool {
    catalog
        .table_by_ref(&base.table)
        .ok()
        .and_then(|metadata| metadata.statistics)
        .is_some_and(|statistics| {
            statistics
                .constraints
                .unique_keys
                .iter()
                .any(|key| key.columns() == [base.name.as_str()])
        })
}

fn single_column_foreign_key(
    local: &BaseColumn,
    referenced: &BaseColumn,
    catalog: &dyn Catalog,
) -> bool {
    let Ok(local_metadata) = catalog.table_by_ref(&local.table) else {
        return false;
    };
    let Ok(referenced_metadata) = catalog.table_by_ref(&referenced.table) else {
        return false;
    };
    local_metadata
        .statistics
        .as_ref()
        .is_some_and(|statistics| {
            statistics
                .constraints
                .foreign_keys
                .iter()
                .any(|foreign_key| {
                    foreign_key.columns() == [local.name.as_str()]
                        && foreign_key.referenced_columns() == [referenced.name.as_str()]
                        && catalog
                            .table_by_ref(&foreign_key.referenced_table)
                            .is_ok_and(|metadata| metadata.id == referenced_metadata.id)
                })
        })
}

fn profile_has_complete_base_population(
    profile: &CardinalityProfile,
    base: &BaseColumn,
    catalog: &dyn Catalog,
) -> bool {
    if profile.rows.source != EstimateSource::Catalog {
        return false;
    }
    catalog
        .table_by_ref(&base.table)
        .ok()
        .and_then(|metadata| metadata.statistics)
        .and_then(|statistics| statistics.row_count)
        .is_some_and(|row_count| profile.rows.value == row_count as f64)
}

#[derive(Debug, Clone, Copy)]
struct DistinctIntersection {
    distinct: f64,
    domains_disjoint: bool,
}

fn estimated_distinct_intersection(
    left: &ColumnProfile,
    right: &ColumnProfile,
    left_distinct: f64,
    right_distinct: f64,
) -> DistinctIntersection {
    if left_distinct <= 0.0 || right_distinct <= 0.0 {
        return DistinctIntersection {
            distinct: 0.0,
            domains_disjoint: false,
        };
    }
    let Some((left_lower, left_upper, right_lower, right_upper)) = left
        .lower_bound
        .as_ref()
        .zip(left.upper_bound.as_ref())
        .zip(right.lower_bound.as_ref().zip(right.upper_bound.as_ref()))
        .map(|((left_lower, left_upper), (right_lower, right_upper))| {
            (left_lower, left_upper, right_lower, right_upper)
        })
    else {
        return DistinctIntersection {
            distinct: left_distinct.min(right_distinct),
            domains_disjoint: false,
        };
    };

    if crate::catalog::scalar_order(left_upper, right_lower).is_some_and(|order| order.is_lt())
        || crate::catalog::scalar_order(right_upper, left_lower).is_some_and(|order| order.is_lt())
    {
        return DistinctIntersection {
            distinct: 0.0,
            domains_disjoint: true,
        };
    }

    let overlap_lower = if crate::catalog::scalar_order(left_lower, right_lower)
        .is_some_and(|order| order.is_lt())
    {
        right_lower
    } else {
        left_lower
    };
    let overlap_upper = if crate::catalog::scalar_order(left_upper, right_upper)
        .is_some_and(|order| order.is_gt())
    {
        right_upper
    } else {
        left_upper
    };
    let left_fraction =
        scalar_interval_fraction(left_lower, left_upper, overlap_lower, overlap_upper);
    let right_fraction =
        scalar_interval_fraction(right_lower, right_upper, overlap_lower, overlap_upper);
    let distinct = match (left_fraction, right_fraction) {
        (Some(left_fraction), Some(right_fraction)) => {
            let estimate = (left_distinct * left_fraction)
                .min(right_distinct * right_fraction)
                .min(left_distinct.min(right_distinct));
            if crate::catalog::scalar_order(overlap_lower, overlap_upper)
                .is_some_and(|order| order.is_eq())
            {
                // A point overlap is a shared observed endpoint, so both domains contain it.
                estimate.max(1.0)
            } else {
                estimate
            }
        }
        _ => left_distinct.min(right_distinct),
    };

    // TODO(statistics): Replace uniform ordered-range overlap with histogram intersection and
    // sample/set-sketch overlap once those statistics are collected and persisted.
    DistinctIntersection {
        distinct,
        domains_disjoint: false,
    }
}

fn scalar_interval_fraction(
    lower: &ScalarValue,
    upper: &ScalarValue,
    overlap_lower: &ScalarValue,
    overlap_upper: &ScalarValue,
) -> Option<f64> {
    fn discrete(lower: i128, upper: i128, overlap_lower: i128, overlap_upper: i128) -> Option<f64> {
        let width = upper.checked_sub(lower)?.checked_add(1)? as f64;
        let overlap = overlap_upper.checked_sub(overlap_lower)?.checked_add(1)? as f64;
        (width > 0.0).then(|| (overlap / width).clamp(0.0, 1.0))
    }
    fn continuous(lower: f64, upper: f64, overlap_lower: f64, overlap_upper: f64) -> Option<f64> {
        let width = upper - lower;
        if !width.is_finite() || width < 0.0 {
            return None;
        }
        if width == 0.0 {
            return Some(1.0);
        }
        Some(((overlap_upper - overlap_lower) / width).clamp(0.0, 1.0))
    }
    fn continuous_i128(
        lower: i128,
        upper: i128,
        overlap_lower: i128,
        overlap_upper: i128,
    ) -> Option<f64> {
        let width = upper.checked_sub(lower)?;
        if width == 0 {
            return Some(1.0);
        }
        let overlap = overlap_upper.checked_sub(overlap_lower)?;
        Some(((overlap as f64) / (width as f64)).clamp(0.0, 1.0))
    }

    match (lower, upper, overlap_lower, overlap_upper) {
        (
            ScalarValue::Boolean(lower),
            ScalarValue::Boolean(upper),
            ScalarValue::Boolean(overlap_lower),
            ScalarValue::Boolean(overlap_upper),
        ) => discrete(
            i128::from(*lower),
            i128::from(*upper),
            i128::from(*overlap_lower),
            i128::from(*overlap_upper),
        ),
        (
            ScalarValue::Int32(lower),
            ScalarValue::Int32(upper),
            ScalarValue::Int32(overlap_lower),
            ScalarValue::Int32(overlap_upper),
        ) => discrete(
            i128::from(*lower),
            i128::from(*upper),
            i128::from(*overlap_lower),
            i128::from(*overlap_upper),
        ),
        (
            ScalarValue::Int64(lower),
            ScalarValue::Int64(upper),
            ScalarValue::Int64(overlap_lower),
            ScalarValue::Int64(overlap_upper),
        ) => discrete(
            i128::from(*lower),
            i128::from(*upper),
            i128::from(*overlap_lower),
            i128::from(*overlap_upper),
        ),
        (
            ScalarValue::Date32(lower),
            ScalarValue::Date32(upper),
            ScalarValue::Date32(overlap_lower),
            ScalarValue::Date32(overlap_upper),
        ) => discrete(
            i128::from(*lower),
            i128::from(*upper),
            i128::from(*overlap_lower),
            i128::from(*overlap_upper),
        ),
        (
            ScalarValue::Float64(lower),
            ScalarValue::Float64(upper),
            ScalarValue::Float64(overlap_lower),
            ScalarValue::Float64(overlap_upper),
        ) => continuous(*lower, *upper, *overlap_lower, *overlap_upper),
        (
            ScalarValue::Decimal128 {
                value: lower,
                precision: lower_precision,
                scale: lower_scale,
            },
            ScalarValue::Decimal128 {
                value: upper,
                precision: upper_precision,
                scale: upper_scale,
            },
            ScalarValue::Decimal128 {
                value: overlap_lower,
                precision: overlap_lower_precision,
                scale: overlap_lower_scale,
            },
            ScalarValue::Decimal128 {
                value: overlap_upper,
                precision: overlap_upper_precision,
                scale: overlap_upper_scale,
            },
        ) if lower_precision == upper_precision
            && lower_precision == overlap_lower_precision
            && lower_precision == overlap_upper_precision
            && lower_scale == upper_scale
            && lower_scale == overlap_lower_scale
            && lower_scale == overlap_upper_scale =>
        {
            continuous_i128(*lower, *upper, *overlap_lower, *overlap_upper)
        }
        _ => None,
    }
}

fn join_column_non_null_fraction(
    column: Column,
    left: &CardinalityProfile,
    right: &CardinalityProfile,
) -> f64 {
    let profile_and_rows = left
        .columns
        .get(&column)
        .map(|profile| (profile, left.rows.value))
        .or_else(|| {
            right
                .columns
                .get(&column)
                .map(|profile| (profile, right.rows.value))
        });
    let Some((profile, rows)) = profile_and_rows else {
        return 1.0;
    };
    if rows <= 0.0 {
        0.0
    } else {
        (profile.frequency.value / rows).clamp(0.0, 1.0)
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
        ExprData::Like { .. } => Estimate::derived(config.like_selectivity, Some(0.0), Some(1.0)),
        _ => Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0)),
    }
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
    if let Some((column, literal, normalized_op)) = column_literal(op, left, right, ctx)
        && let Some((profile, population_rows)) = left_profile
            .columns
            .get(&column)
            .map(|profile| (profile, left_profile.rows.value))
            .or_else(|| {
                right_profile
                    .columns
                    .get(&column)
                    .map(|profile| (profile, right_profile.rows.value))
            })
    {
        if matches!(literal, ScalarValue::Null(_)) {
            return Estimate::exact(0.0);
        }
        let non_null_fraction = if population_rows <= 0.0 {
            0.0
        } else {
            (profile.frequency.value / population_rows).clamp(0.0, 1.0)
        };
        let equality_selectivity = non_null_fraction / profile.distinct.value.max(1.0);
        return match normalized_op {
            BinaryOp::Eq => Estimate::derived(equality_selectivity, Some(0.0), Some(1.0)),
            BinaryOp::NotEq => Estimate::derived(
                (non_null_fraction - equality_selectivity).max(0.0),
                Some(0.0),
                Some(1.0),
            ),
            BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                let conditional = range_selectivity(profile, literal, normalized_op, config).value;
                Estimate::derived(conditional * non_null_fraction, Some(0.0), Some(1.0))
            }
            _ => Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0)),
        };
    }
    if let Some((left_col, right_col)) = column_equality_from_parts(op, left, right, ctx) {
        let profile_and_rows = |column| {
            left_profile
                .columns
                .get(&column)
                .map(|profile| (profile, left_profile.rows.value))
                .or_else(|| {
                    right_profile
                        .columns
                        .get(&column)
                        .map(|profile| (profile, right_profile.rows.value))
                })
        };
        let Some((left_column, left_rows)) = profile_and_rows(left_col) else {
            return Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0));
        };
        let Some((right_column, right_rows)) = profile_and_rows(right_col) else {
            return Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0));
        };
        let left_non_null = if left_rows <= 0.0 {
            0.0
        } else {
            (left_column.frequency.value / left_rows).clamp(0.0, 1.0)
        };
        if left_col == right_col {
            let value = if op == BinaryOp::IsNotDistinctFrom {
                1.0
            } else {
                left_non_null
            };
            return Estimate::derived(value, Some(0.0), Some(1.0));
        }
        let right_non_null = if right_rows <= 0.0 {
            0.0
        } else {
            (right_column.frequency.value / right_rows).clamp(0.0, 1.0)
        };
        let equal_non_null = left_non_null * right_non_null
            / left_column
                .distinct
                .value
                .max(right_column.distinct.value)
                .max(1.0);
        let null_match = if op == BinaryOp::IsNotDistinctFrom {
            (1.0 - left_non_null) * (1.0 - right_non_null)
        } else {
            0.0
        };
        return Estimate::derived(
            (equal_non_null + null_match).clamp(0.0, 1.0),
            Some(0.0),
            Some(1.0),
        );
    }
    Estimate::derived(config.default_predicate_selectivity, Some(0.0), Some(1.0))
}

fn value_restricted_filter_column(
    predicate: Expr,
    ctx: &QueryContext,
) -> Option<(Column, BinaryOp)> {
    let ExprData::Binary { op, left, right } = predicate.get(ctx) else {
        return None;
    };
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
    .then(|| {
        column_literal(*op, *left, *right, ctx)
            .map(|(column, _, oriented_op)| (column, oriented_op))
    })
    .flatten()
}

fn tighten_filter_columns(profile: &mut CardinalityProfile, predicate: Expr, ctx: &QueryContext) {
    let ExprData::Binary { op, left, right } = predicate.get(ctx) else {
        return;
    };
    let Some((column, literal, oriented_op)) = column_literal(*op, *left, *right, ctx) else {
        return;
    };
    let rows = profile.rows.clone();
    let Some(column_profile) = profile.columns.get_mut(&column) else {
        return;
    };
    match oriented_op {
        BinaryOp::Eq => {
            column_profile.lower_bound = Some(literal.clone());
            column_profile.upper_bound = Some(literal.clone());
            let distinct = if rows.value > 0.0 && !matches!(literal, ScalarValue::Null(_)) {
                1.0_f64.min(rows.value)
            } else {
                0.0
            };
            column_profile.distinct = Estimate::derived(distinct, Some(0.0), Some(distinct));
            column_profile.frequency = rows;
        }
        BinaryOp::Lt | BinaryOp::LtEq => {
            let tightened = if oriented_op == BinaryOp::Lt {
                strict_scalar_predecessor(literal).unwrap_or_else(|| literal.clone())
            } else {
                literal.clone()
            };
            if column_profile.upper_bound.as_ref().is_none_or(|current| {
                crate::catalog::scalar_order(&tightened, current).is_some_and(|order| order.is_lt())
            }) {
                column_profile.upper_bound = Some(tightened);
            }
        }
        BinaryOp::Gt | BinaryOp::GtEq => {
            let tightened = if oriented_op == BinaryOp::Gt {
                strict_scalar_successor(literal).unwrap_or_else(|| literal.clone())
            } else {
                literal.clone()
            };
            if column_profile.lower_bound.as_ref().is_none_or(|current| {
                crate::catalog::scalar_order(&tightened, current).is_some_and(|order| order.is_gt())
            }) {
                column_profile.lower_bound = Some(tightened);
            }
        }
        _ => {}
    }
}

fn strict_scalar_predecessor(value: &ScalarValue) -> Option<ScalarValue> {
    match value {
        ScalarValue::Boolean(true) => Some(ScalarValue::Boolean(false)),
        ScalarValue::Int32(value) => value.checked_sub(1).map(ScalarValue::Int32),
        ScalarValue::Int64(value) => value.checked_sub(1).map(ScalarValue::Int64),
        ScalarValue::Date32(value) => value.checked_sub(1).map(ScalarValue::Date32),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => value.checked_sub(1).map(|value| ScalarValue::Decimal128 {
            value,
            precision: *precision,
            scale: *scale,
        }),
        // TODO(statistics): Preserve bound inclusivity explicitly for continuous and non-discrete
        // ordered types instead of approximating a strict bound with the literal itself.
        _ => None,
    }
}

fn strict_scalar_successor(value: &ScalarValue) -> Option<ScalarValue> {
    match value {
        ScalarValue::Boolean(false) => Some(ScalarValue::Boolean(true)),
        ScalarValue::Int32(value) => value.checked_add(1).map(ScalarValue::Int32),
        ScalarValue::Int64(value) => value.checked_add(1).map(ScalarValue::Int64),
        ScalarValue::Date32(value) => value.checked_add(1).map(ScalarValue::Date32),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => value.checked_add(1).map(|value| ScalarValue::Decimal128 {
            value,
            precision: *precision,
            scale: *scale,
        }),
        // TODO(statistics): See `strict_scalar_predecessor`.
        _ => None,
    }
}

fn range_selectivity(
    profile: &ColumnProfile,
    literal: &ScalarValue,
    op: BinaryOp,
    config: &CardinalityEstimationConfig,
) -> Estimate {
    let Some(min) = profile.lower_bound.as_ref().and_then(scalar_to_f64) else {
        return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
    };
    let Some(max) = profile.upper_bound.as_ref().and_then(scalar_to_f64) else {
        return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
    };
    let Some(value) = scalar_to_f64(literal) else {
        return Estimate::derived(config.range_fallback_selectivity, Some(0.0), Some(1.0));
    };
    // Assume values are uniformly distributed over the collected [min, max]
    // range. Missing or non-numeric bounds use the generic range fallback above.
    let width = (max - min).abs().max(1.0);
    let selected = match op {
        BinaryOp::Lt | BinaryOp::LtEq => ((value - min) / width).clamp(0.0, 1.0),
        BinaryOp::Gt | BinaryOp::GtEq => ((max - value) / width).clamp(0.0, 1.0),
        _ => config.range_fallback_selectivity,
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

fn column_literal(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    ctx: &QueryContext,
) -> Option<(Column, &ScalarValue, BinaryOp)> {
    match (left.get(ctx), right.get(ctx)) {
        (ExprData::ColumnRef(column), ExprData::Literal(value)) => Some((*column, value, op)),
        (ExprData::Literal(value), ExprData::ColumnRef(column)) => {
            let reversed = match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                _ => op,
            };
            Some((*column, value, reversed))
        }
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
    if !matches!(op, BinaryOp::Eq | BinaryOp::IsNotDistinctFrom) {
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
    null_safe: bool,
    chosen_ndv: Estimate,
    left_ndv: Estimate,
    right_ndv: Estimate,
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
                .min_by_value(right_distinct)
                .min_by_value(distinct);
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
        ForeignKey, Join, JoinType, Limit, Map, MemoryCatalog, OperatorData, Output, Projection,
        ScalarValue, Scan, Selection, TableFunction, TableFunctionDef, TableRef, TableStatistics,
        UniqueKey,
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
    fn null_safe_equality_does_not_prove_columns_non_null() {
        let mut ctx = QueryContext::new();
        let left_column = ColumnData::new("left", DataType::Int64).add(&mut ctx);
        let right_column = ColumnData::new("right", DataType::Int64).add(&mut ctx);
        let left_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("left_t"),
            columns: vec![left_column],
        })
        .add(&mut ctx);
        let right_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("right_t"),
            columns: vec![right_column],
        })
        .add(&mut ctx);
        let left_ref = ExprData::ColumnRef(left_column).add(&mut ctx);
        let right_ref = ExprData::ColumnRef(right_column).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::IsNotDistinctFrom,
            left: left_ref,
            right: right_ref,
        }
        .add(&mut ctx);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: predicate,
            outer: left_scan,
            inner: right_scan,
        })
        .add(&mut ctx);

        let mut analyses = crate::test_analyses(&ctx);
        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
            vec![(left_column, true), (right_column, true)]
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

        let output = combine_join_columns(
            &left,
            &right,
            Estimate::exact(50.0),
            None,
            Vec::new(),
            false,
            false,
        );

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
        assert_eq!(estimate.equivalence_classes[0].distinct.value, 50.0);
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
                // Equal outputs cannot exceed the smallest inherited equality domain.
                distinct: Estimate::exact(40.0),
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
                    distinct: Estimate::exact(30.0),
                },
                ColumnEquivalenceClass {
                    columns: BTreeSet::from([c, d]),
                    distinct: Estimate::exact(40.0),
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
            (JoinType::FullOuter, Vec::new(), both_columns.clone(), 100.0),
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
                both_columns.clone(),
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
            assert_eq!(output.rows.lower, Some(expected_minimum), "{join_type:?}");
            assert_eq!(output.rows.upper, Some(expected_minimum), "{join_type:?}");
            let lower = output.rows.lower.unwrap_or_default();
            let upper = output.rows.upper.unwrap_or_default();

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
        assert_eq!(downstream_selectivity.value, 0.00001);
        assert_eq!(downstream.rows.value, 0.1);
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
    fn cardinality_estimation_keeps_computed_expressions_opaque() {
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

        assert_eq!(profile.columns[&shifted].lower_bound, None);
        assert_eq!(profile.columns[&shifted].upper_bound, None);
        assert_eq!(profile.columns[&shifted].distinct.value, 10.0);
        assert_eq!(
            profile.columns[&shifted].distinct.source,
            EstimateSource::Derived
        );
        assert!(profile.columns[&shifted].sketches.is_none());
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
    fn aggregation_counts_equivalent_grouping_keys_once() {
        let mut ctx = QueryContext::new();
        let first = ColumnData::new("first", DataType::Int64).add(&mut ctx);
        let alias = ColumnData::new("alias", DataType::Int64).add(&mut ctx);
        let count = ColumnData::new("count", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("pairs"),
            columns: vec![first],
        })
        .add(&mut ctx);
        let first_ref = ExprData::ColumnRef(first).add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(alias, first_ref)],
            input: scan,
        })
        .add(&mut ctx);
        let alias_ref = ExprData::ColumnRef(alias).add(&mut ctx);
        let aggregation = OperatorData::Aggregation(Aggregation {
            keys: vec![first_ref, alias_ref],
            aggregates: vec![(count, AggregateExpr::CountStar)],
            input: map,
        })
        .add(&mut ctx);

        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("pairs"),
                Arc::new(Schema::new(vec![Field::new(
                    "first",
                    DataType::Int64,
                    false,
                )])),
                None,
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("pairs"),
                table_stats_for_column("first", 1_000, 100),
            )
            .unwrap();
        let mut analyses = AnalysisContext::new(catalog);

        let profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, aggregation)
            .unwrap();
        assert_eq!(profile.rows.value, 100.0);
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
        for marker_nullable in [false, true] {
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

            assert_eq!(profile.columns[&marker].distinct.upper, Some(2.0));
            if marker_nullable {
                assert!(profile.columns[&marker].frequency.lower == Some(0.0));
            } else {
                assert_eq!(profile.columns[&marker].frequency, profile.rows);
            }
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
            value: None,
            sketches: None,
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
        let left_values = (1..=1_000).map(Some).collect::<Vec<_>>();
        let right_values = (1..=100).map(Some).collect::<Vec<_>>();
        catalog
            .set_table_statistics(
                TableRef::bare("left_t"),
                table_stats_from_i64_values("left_key", &left_values),
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("right_t"),
                table_stats_from_i64_values("right_key", &right_values),
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
    fn semi_join_coverage_is_invariant_to_right_duplicates() {
        fn estimate(right_values: &[Option<i64>]) -> f64 {
            let mut ctx = QueryContext::new();
            let left_key = ColumnData::new("key", DataType::Int64).add(&mut ctx);
            let right_key = ColumnData::new("key", DataType::Int64).add(&mut ctx);
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

            let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
            let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
            for table in ["left_t", "right_t"] {
                catalog
                    .create_table(TableRef::bare(table), key_schema.clone(), None)
                    .unwrap();
            }
            let left_values = (1..=1_000).map(Some).collect::<Vec<_>>();
            catalog
                .set_table_statistics(
                    TableRef::bare("left_t"),
                    table_stats_from_i64_values("key", &left_values),
                )
                .unwrap();
            catalog
                .set_table_statistics(
                    TableRef::bare("right_t"),
                    table_stats_from_i64_values("key", right_values),
                )
                .unwrap();
            AnalysisContext::new(catalog)
                .get::<CardinalityEstimationV1>(&ctx, semi)
                .unwrap()
                .rows
                .value
        }

        assert_eq!(estimate(&[Some(1)]), 1.0);
        assert_eq!(estimate(&vec![Some(1); 1_000]), 1.0);
    }

    #[test]
    fn ordered_range_overlap_controls_join_domain_intersection() {
        let mut ctx = QueryContext::new();
        let left_column = ColumnData::new("left", DataType::Int64).add(&mut ctx);
        let right_column = ColumnData::new("right", DataType::Int64).add(&mut ctx);
        let profile_from_values = |column, values: Vec<Option<i64>>| {
            let statistics = table_stats_from_i64_values("key", &values);
            let rows = Estimate::catalog(values.len() as f64);
            CardinalityProfile::new(
                rows.clone(),
                [(
                    column,
                    column_profile_from_stats(&statistics.column_statistics["key"], &rows, None),
                )]
                .into_iter()
                .collect(),
            )
        };
        let left = profile_from_values(
            left_column,
            (1..=100).map(Some).collect::<Vec<Option<i64>>>(),
        );
        let overlapping = profile_from_values(
            right_column,
            (51..=150).map(Some).collect::<Vec<Option<i64>>>(),
        );
        let disjoint = profile_from_values(
            right_column,
            (101..=200).map(Some).collect::<Vec<Option<i64>>>(),
        );

        let equality_edge = |null_safe, distinct| EqualityEdge {
            left: left_column,
            right: right_column,
            null_safe,
            chosen_ndv: Estimate::exact(distinct),
            left_ndv: Estimate::exact(distinct),
            right_ndv: Estimate::exact(distinct),
        };
        let overlap =
            equality_edge_estimate(&equality_edge(false, 100.0), &left, &overlapping, None);
        assert!((overlap.left_match_probability - 0.5).abs() < f64::EPSILON);
        assert!((overlap.pair_selectivity - 0.005).abs() < f64::EPSILON);

        let disjoint = equality_edge_estimate(&equality_edge(false, 100.0), &left, &disjoint, None);
        assert!(disjoint.domains_disjoint);
        assert_eq!(disjoint.pair_selectivity, 0.0);
        assert_eq!(disjoint.left_match_probability, 0.0);

        let nullable_left = profile_from_values(left_column, vec![None, Some(1)]);
        let nullable_right = profile_from_values(right_column, vec![None, Some(100)]);
        let null_safe = equality_edge_estimate(
            &equality_edge(true, 1.0),
            &nullable_left,
            &nullable_right,
            None,
        );
        assert!(!null_safe.domains_disjoint);
        assert_eq!(null_safe.pair_selectivity, 0.25);
        assert_eq!(null_safe.left_match_probability, 0.5);
    }

    #[test]
    fn filtered_ndv_distinguishes_row_thinning_from_domain_restriction() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("key", DataType::Int64).add(&mut ctx);
        let values = (0..100)
            .flat_map(|value| std::iter::repeat_n(Some(value), 10))
            .collect::<Vec<_>>();
        let statistics = table_stats_from_i64_values("key", &values);
        let rows = Estimate::catalog(values.len() as f64);
        let input = CardinalityProfile::new(
            rows.clone(),
            [(
                column,
                column_profile_from_stats(&statistics.column_statistics["key"], &rows, None),
            )]
            .into_iter()
            .collect(),
        );

        let thinned = scale_profile(input.clone(), 0.1, None);
        let expected_occupancy = 100.0 * (1.0 - 0.9_f64.powf(10.0));
        assert!((thinned.columns[&column].distinct.value - expected_occupancy).abs() < 1e-9);

        let domain_restricted = scale_profile(input, 0.1, Some((column, BinaryOp::Lt)));
        assert!((domain_restricted.columns[&column].distinct.value - 10.0).abs() < 1e-9);
        assert_eq!(domain_restricted.columns[&column].frequency.value, 100.0);
    }

    #[test]
    fn semi_and_anti_profiles_propagate_equality_key_domains() {
        let mut ctx = QueryContext::new();
        let left_key = ColumnData::new("left_key", DataType::Int64).add(&mut ctx);
        let right_key = ColumnData::new("right_key", DataType::Int64).add(&mut ctx);
        let predicate = equality_expr(&mut ctx, left_key, right_key);
        let left = CardinalityProfile::new(
            Estimate::exact(1_000.0),
            [(left_key, test_column_profile(1_000.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(right_key, test_column_profile(100.0, 10.0))]
                .into_iter()
                .collect(),
        );

        let semi =
            join_profile_from_conjuncts(&left, &right, JoinType::LeftSemi, &[predicate], &ctx);
        assert_eq!(semi.rows.value, 100.0);
        assert_eq!(semi.columns[&left_key].frequency.value, 100.0);
        assert_eq!(semi.columns[&left_key].distinct.value, 10.0);

        let anti =
            join_profile_from_conjuncts(&left, &right, JoinType::LeftAnti, &[predicate], &ctx);
        assert_eq!(anti.rows.value, 900.0);
        assert_eq!(anti.columns[&left_key].frequency.value, 900.0);
        assert_eq!(anti.columns[&left_key].distinct.value, 90.0);

        let mut nullable_left_key = test_column_profile(100.0, 10.0);
        nullable_left_key.frequency = Estimate::exact(50.0);
        let nullable_left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(left_key, nullable_left_key)].into_iter().collect(),
        );
        let covering_right = CardinalityProfile::new(
            Estimate::exact(10.0),
            [(right_key, test_column_profile(10.0, 10.0))]
                .into_iter()
                .collect(),
        );
        let nullable_anti = join_profile_from_conjuncts(
            &nullable_left,
            &covering_right,
            JoinType::LeftAnti,
            &[predicate],
            &ctx,
        );
        assert_eq!(nullable_anti.rows.value, 50.0);
        assert_eq!(nullable_anti.columns[&left_key].frequency.value, 0.0);
        assert_eq!(nullable_anti.columns[&left_key].distinct.value, 0.0);
    }

    #[test]
    fn filter_tightening_preserves_profile_invariants_and_strict_bounds() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("key", DataType::Int64).add(&mut ctx);
        let column_ref = ExprData::ColumnRef(column).add(&mut ctx);
        let fifty = ExprData::Literal(ScalarValue::Int64(50)).add(&mut ctx);
        let strict = ExprData::Binary {
            op: BinaryOp::Lt,
            left: column_ref,
            right: fifty,
        }
        .add(&mut ctx);
        let mut ranged = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(
                column,
                ColumnProfile {
                    lower_bound: Some(ScalarValue::Int64(1)),
                    upper_bound: Some(ScalarValue::Int64(100)),
                    frequency: Estimate::exact(100.0),
                    distinct: Estimate::exact(100.0),
                    value: None,
                    sketches: None,
                },
            )]
            .into_iter()
            .collect(),
        );
        tighten_filter_columns(&mut ranged, strict, &ctx);
        assert_eq!(
            ranged.columns[&column].upper_bound,
            Some(ScalarValue::Int64(49))
        );

        let one = ExprData::Literal(ScalarValue::Int64(1)).add(&mut ctx);
        let equality = ExprData::Binary {
            op: BinaryOp::Eq,
            left: column_ref,
            right: one,
        }
        .add(&mut ctx);
        let empty = CardinalityProfile::new(
            Estimate::exact(0.0),
            [(column, test_column_profile(0.0, 0.0))]
                .into_iter()
                .collect(),
        );
        let mut empty = scale_profile(empty, 0.0, Some((column, BinaryOp::Eq)));
        tighten_filter_columns(&mut empty, equality, &ctx);
        assert_eq!(empty.columns[&column].frequency.value, 0.0);
        assert_eq!(empty.columns[&column].distinct.value, 0.0);

        let fractional = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(column, test_column_profile(100.0, 100.0))]
                .into_iter()
                .collect(),
        );
        let mut fractional = scale_profile(fractional, 0.005, Some((column, BinaryOp::Eq)));
        tighten_filter_columns(&mut fractional, equality, &ctx);
        assert_eq!(fractional.rows.value, 0.5);
        assert_eq!(fractional.columns[&column].distinct.value, 0.5);
    }

    #[test]
    fn catalog_keys_enable_only_population_safe_foreign_key_coverage() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let key_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for table in ["child", "parent"] {
            catalog
                .create_table(TableRef::bare(table), key_schema.clone(), None)
                .unwrap();
        }
        let parent_values = (1..=100).map(Some).collect::<Vec<_>>();
        let child_values = (1..=100).cycle().take(1_000).map(Some).collect::<Vec<_>>();
        let mut parent_statistics = table_stats_from_i64_values("id", &parent_values);
        parent_statistics.constraints.unique_keys =
            vec![UniqueKey::try_new(vec!["id".to_string()]).unwrap()]
                .try_into()
                .unwrap();
        let mut child_statistics = table_stats_from_i64_values("id", &child_values);
        child_statistics.constraints.foreign_keys = vec![
            ForeignKey::try_new(
                vec!["id".to_string()],
                TableRef::bare("parent"),
                vec!["id".to_string()],
            )
            .unwrap(),
        ]
        .try_into()
        .unwrap();
        catalog
            .set_table_statistics(TableRef::bare("parent"), parent_statistics)
            .unwrap();
        catalog
            .set_table_statistics(TableRef::bare("child"), child_statistics)
            .unwrap();

        let mut ctx = QueryContext::new();
        let child_id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let parent_id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let child = OperatorData::Scan(Scan {
            table: TableRef::bare("child"),
            columns: vec![child_id],
        })
        .add(&mut ctx);
        let parent = OperatorData::Scan(Scan {
            table: TableRef::bare("parent"),
            columns: vec![parent_id],
        })
        .add(&mut ctx);
        let unfiltered_on = equality_expr(&mut ctx, child_id, parent_id);
        let unfiltered_semi = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on: unfiltered_on,
            outer: child,
            inner: parent,
        })
        .add(&mut ctx);

        let parent_ref = ExprData::ColumnRef(parent_id).add(&mut ctx);
        let fifty = ExprData::Literal(ScalarValue::Int64(50)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::LtEq,
            left: parent_ref,
            right: fifty,
        }
        .add(&mut ctx);
        let filtered_parent = OperatorData::Selection(Selection {
            predicate,
            input: parent,
        })
        .add(&mut ctx);
        let filtered_on = equality_expr(&mut ctx, child_id, parent_id);
        let filtered_semi = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on: filtered_on,
            outer: child,
            inner: filtered_parent,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new(catalog);
        let unfiltered = analyses
            .get::<CardinalityEstimationV1>(&ctx, unfiltered_semi)
            .unwrap();
        assert_eq!(unfiltered.rows.value, 1_000.0);

        let filtered = analyses
            .get::<CardinalityEstimationV1>(&ctx, filtered_semi)
            .unwrap();
        assert!(filtered.rows.value > 490.0 && filtered.rows.value < 510.0);
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
    fn accumulated_facts_do_not_charge_repeated_equalities_twice() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("t"),
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                None,
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("t"),
                table_stats_for_column("value", 100, 10),
            )
            .unwrap();

        let (mut ctx, scan) = single_column_scan();
        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            panic!("single_column_scan returns a scan");
        };
        let column = scan_data.columns[0];
        let column_ref = ExprData::ColumnRef(column).add(&mut ctx);
        let literal = ExprData::Literal(ScalarValue::Int64(7)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: column_ref,
            right: literal,
        }
        .add(&mut ctx);
        let first = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let repeated = OperatorData::Selection(Selection {
            predicate,
            input: first,
        })
        .add(&mut ctx);
        ctx.set_root(repeated);

        let mut analyses = AnalysisContext::new(catalog);
        let first_profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, first)
            .unwrap();
        let repeated_profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, repeated)
            .unwrap();
        assert_eq!(first_profile.rows.value, 10.0);
        assert_eq!(repeated_profile.rows.value, first_profile.rows.value);
    }

    #[test]
    fn accumulated_facts_do_not_charge_repeated_residual_predicates_twice() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("t"),
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
                None,
            )
            .unwrap();
        catalog
            .set_table_statistics(
                TableRef::bare("t"),
                table_stats_for_column("value", 100, 10),
            )
            .unwrap();

        let (mut ctx, scan) = single_column_scan();
        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            panic!("single_column_scan returns a scan");
        };
        let column_ref = ExprData::ColumnRef(scan_data.columns[0]).add(&mut ctx);
        let literal = ExprData::Literal(ScalarValue::Int64(7)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::NotEq,
            left: column_ref,
            right: literal,
        }
        .add(&mut ctx);
        let first = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let repeated = OperatorData::Selection(Selection {
            predicate,
            input: first,
        })
        .add(&mut ctx);

        let mut analyses = AnalysisContext::new(catalog);
        let first_rows = analyses
            .get::<CardinalityEstimationV1>(&ctx, first)
            .unwrap()
            .rows
            .value;
        let repeated_rows = analyses
            .get::<CardinalityEstimationV1>(&ctx, repeated)
            .unwrap()
            .rows
            .value;
        assert_eq!(first_rows, 90.0);
        assert_eq!(repeated_rows, first_rows);
    }

    #[test]
    fn comparison_selectivity_respects_orientation_nulls_and_non_null_frequency() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let mut column_profile = test_column_profile(100.0, 10.0);
        column_profile.lower_bound = Some(ScalarValue::Int64(0));
        column_profile.upper_bound = Some(ScalarValue::Int64(100));
        column_profile.frequency = Estimate::exact(80.0);
        let profile = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(column, column_profile)].into_iter().collect(),
        );
        let column_ref = ExprData::ColumnRef(column).add(&mut ctx);
        let twenty_five = ExprData::Literal(ScalarValue::Int64(25)).add(&mut ctx);
        let null = ExprData::Literal(ScalarValue::Null(DataType::Int64)).add(&mut ctx);
        let config = CardinalityEstimationConfig::default();

        let reversed_range = binary_selectivity(
            &profile,
            &profile,
            BinaryOp::Lt,
            twenty_five,
            column_ref,
            &ctx,
            &config,
        );
        assert!((reversed_range.value - 0.6).abs() < f64::EPSILON);
        let equality = binary_selectivity(
            &profile,
            &profile,
            BinaryOp::Eq,
            column_ref,
            twenty_five,
            &ctx,
            &config,
        );
        assert_eq!(equality.value, 0.08);
        for op in [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt] {
            assert_eq!(
                binary_selectivity(&profile, &profile, op, column_ref, null, &ctx, &config,).value,
                0.0,
            );
        }
    }

    #[test]
    fn column_equality_accounts_for_nulls_and_marks_matched_keys_non_null() {
        let mut ctx = QueryContext::new();
        let left_column = ColumnData::new("left", DataType::Int64).add(&mut ctx);
        let right_column = ColumnData::new("right", DataType::Int64).add(&mut ctx);
        let mut left_column_profile = test_column_profile(100.0, 10.0);
        left_column_profile.frequency = Estimate::exact(80.0);
        let mut right_column_profile = test_column_profile(100.0, 10.0);
        right_column_profile.frequency = Estimate::exact(50.0);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(left_column, left_column_profile)].into_iter().collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(right_column, right_column_profile)].into_iter().collect(),
        );
        let left_ref = ExprData::ColumnRef(left_column).add(&mut ctx);
        let right_ref = ExprData::ColumnRef(right_column).add(&mut ctx);
        let equality = ExprData::Binary {
            op: BinaryOp::Eq,
            left: left_ref,
            right: right_ref,
        }
        .add(&mut ctx);

        let joined = join_profile_from_predicate(&left, &right, JoinType::Inner, equality, &ctx);
        assert!((joined.rows.value - 400.0).abs() < f64::EPSILON);
        assert_eq!(joined.columns[&left_column].frequency.value, 400.0);
        assert_eq!(joined.columns[&right_column].frequency.value, 400.0);

        let self_equality = binary_selectivity(
            &left,
            &left,
            BinaryOp::Eq,
            left_ref,
            left_ref,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        assert_eq!(self_equality.value, 0.8);
        let null_safe_self_equality = binary_selectivity(
            &left,
            &left,
            BinaryOp::IsNotDistinctFrom,
            left_ref,
            left_ref,
            &ctx,
            &CardinalityEstimationConfig::default(),
        );
        assert_eq!(null_safe_self_equality.value, 1.0);
    }

    #[test]
    fn mixed_null_safe_and_ordinary_join_equality_rejects_nulls() {
        let mut ctx = QueryContext::new();
        let left_column = ColumnData::new("left", DataType::Int64).add(&mut ctx);
        let right_column = ColumnData::new("right", DataType::Int64).add(&mut ctx);
        let mut left_column_profile = test_column_profile(100.0, 10.0);
        left_column_profile.frequency = Estimate::exact(50.0);
        let mut right_column_profile = test_column_profile(100.0, 10.0);
        right_column_profile.frequency = Estimate::exact(50.0);
        let left = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(left_column, left_column_profile)].into_iter().collect(),
        );
        let right = CardinalityProfile::new(
            Estimate::exact(100.0),
            [(right_column, right_column_profile)].into_iter().collect(),
        );
        let left_ref = ExprData::ColumnRef(left_column).add(&mut ctx);
        let right_ref = ExprData::ColumnRef(right_column).add(&mut ctx);
        let null_safe = ExprData::Binary {
            op: BinaryOp::IsNotDistinctFrom,
            left: left_ref,
            right: right_ref,
        }
        .add(&mut ctx);
        let ordinary = ExprData::Binary {
            op: BinaryOp::Eq,
            left: left_ref,
            right: right_ref,
        }
        .add(&mut ctx);
        let conjunction = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![null_safe, ordinary],
        }
        .add(&mut ctx);

        let joined = join_profile_from_predicate(&left, &right, JoinType::Inner, conjunction, &ctx);
        // Ordinary equality makes the null-safe edge redundant only after excluding NULLs.
        assert_eq!(joined.rows.value, 250.0);
        assert_eq!(joined.columns[&left_column].frequency.value, 250.0);
        assert_eq!(joined.columns[&right_column].frequency.value, 250.0);
    }

    #[test]
    fn limit_applies_offset_before_fetch() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let values = [1_i64, 2, 3]
            .into_iter()
            .map(|value| vec![ExprData::Literal(ScalarValue::Int64(value)).add(&mut ctx)])
            .collect();
        let input = OperatorData::ConstScan(crate::ConstScan {
            columns: vec![column],
            rows: values,
        })
        .add(&mut ctx);
        let offset_only = OperatorData::Limit(Limit {
            offset: 2,
            fetch: None,
            input,
        })
        .add(&mut ctx);
        let beyond_end = OperatorData::Limit(Limit {
            offset: 5,
            fetch: None,
            input,
        })
        .add(&mut ctx);
        let offset_then_fetch = OperatorData::Limit(Limit {
            offset: 2,
            fetch: Some(2),
            input,
        })
        .add(&mut ctx);

        let mut analyses = crate::test_analyses(&ctx);
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, offset_only)
                .unwrap()
                .rows
                .value,
            1.0,
        );
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, beyond_end)
                .unwrap()
                .rows
                .value,
            0.0,
        );
        assert_eq!(
            analyses
                .get::<CardinalityEstimationV1>(&ctx, offset_then_fetch)
                .unwrap()
                .rows
                .value,
            1.0,
        );
    }

    #[test]
    fn full_outer_join_on_false_includes_both_unmatched_inputs() {
        let mut ctx = QueryContext::new();
        let predicate = ExprData::Literal(ScalarValue::Boolean(false)).add(&mut ctx);
        let left = CardinalityProfile::new(Estimate::exact(100.0), BTreeMap::new());
        let right = CardinalityProfile::new(Estimate::exact(10.0), BTreeMap::new());
        let full = join_profile_from_predicate(&left, &right, JoinType::FullOuter, predicate, &ctx);
        assert_eq!(full.rows.value, 110.0);
        assert_eq!(full.rows.lower, Some(110.0));
    }

    #[test]
    fn semi_and_anti_join_respect_an_empty_right_input() {
        let mut ctx = QueryContext::new();
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let left = CardinalityProfile::new(Estimate::exact(100.0), BTreeMap::new());
        let right = CardinalityProfile::new(Estimate::exact(0.0), BTreeMap::new());
        let semi = join_profile_from_predicate(&left, &right, JoinType::LeftSemi, predicate, &ctx);
        let anti = join_profile_from_predicate(&left, &right, JoinType::LeftAnti, predicate, &ctx);
        assert_eq!(semi.rows.value, 0.0);
        assert_eq!(anti.rows.value, 100.0);
    }

    #[test]
    fn scalar_aggregate_drops_input_contradiction_facts() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("value", DataType::Int64).add(&mut ctx);
        let count = ColumnData::new("count", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![column],
        })
        .add(&mut ctx);
        let false_predicate = ExprData::Literal(ScalarValue::Boolean(false)).add(&mut ctx);
        let empty = OperatorData::Selection(Selection {
            predicate: false_predicate,
            input: scan,
        })
        .add(&mut ctx);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: Vec::new(),
            aggregates: vec![(count, AggregateExpr::CountStar)],
            input: empty,
        })
        .add(&mut ctx);
        ctx.set_root(aggregate);

        let mut analyses = crate::test_analyses(&ctx);
        let profile = analyses
            .get::<CardinalityEstimationV1>(&ctx, aggregate)
            .unwrap();
        assert_eq!(profile.rows.value, 1.0);
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

    fn table_stats_from_i64_values(column: &str, values: &[Option<i64>]) -> TableStatistics {
        let observed = values.iter().flatten().copied().collect::<Vec<_>>();
        let distinct = observed.iter().copied().collect::<BTreeSet<_>>().len();
        TableStatistics {
            row_count: Some(values.len()),
            size_bytes: None,
            column_statistics: [(
                column.to_string(),
                ColumnStatistics {
                    lower_bound: observed.iter().min().copied().map(ScalarValue::Int64),
                    upper_bound: observed.iter().max().copied().map(ScalarValue::Int64),
                    frequency: Some(observed.len()),
                    distinct: Some(distinct),
                    distribution: None,
                },
            )]
            .into_iter()
            .collect(),
            constraints: Default::default(),
        }
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
            value: None,
            sketches: None,
        }
    }

    #[test]
    fn hll_supplies_scan_ndv_when_catalog_ndv_is_absent() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        catalog
            .create_table(TableRef::bare("t"), schema, None)
            .unwrap();
        let mut statistics = table_stats_for_column("key", 100, 100);
        statistics
            .column_statistics
            .get_mut("key")
            .unwrap()
            .distinct = None;
        catalog
            .set_table_statistics(TableRef::bare("t"), statistics)
            .unwrap();

        let mut ctx = QueryContext::new();
        let key = ColumnData::with_qualifier("key", DataType::Int64, "t").add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![key],
        })
        .add(&mut ctx);
        ctx.set_root(scan);
        let mut sketches = ColumnSketches::new(100).hll();
        for value in 0..100 {
            sketches.observe(&ScalarValue::Int64(value));
        }
        let mut analyses = AnalysisContext::new(catalog);
        analyses
            .set_column_sketches(TableRef::bare("t"), "key", sketches)
            .unwrap();
        let profile = analyses.get::<CardinalityEstimationV1>(&ctx, scan).unwrap();
        assert_eq!(
            profile.columns[&key].distinct.source,
            EstimateSource::Sketch
        );
        assert!((profile.columns[&key].distinct.value - 100.0).abs() < 20.0);
    }
}

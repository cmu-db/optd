use std::any::{Any, TypeId, type_name};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::{
    AggregateExpr, AggregateFunction, Catalog, Column, Expr, ExprData, JoinType, Operator,
    OperatorData, QueryContext, Scan,
};

/// Result type used by query analyses.
pub type AnalysisResult<T> = Result<T, AnalysisError>;

/// Error produced while running query analyses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisError {
    /// The current IR cannot expose an expression aggregation key as an output column.
    UnsupportedAggregationKey { operator: Operator, expr: Expr },
    /// The operator graph contained a cycle for the requested analysis.
    RecursiveOperatorAnalysis {
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
            Self::RecursiveOperatorAnalysis { analysis, operator } => write!(
                f,
                "recursive operator dependency for {analysis} at {operator:?}"
            ),
            Self::AnalysisTypeMismatch(analysis) => {
                write!(f, "registered analysis had the wrong type for {analysis}")
            }
        }
    }
}

impl std::error::Error for AnalysisError {}

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
pub trait OperatorAnalysis: Analysis {
    /// The value computed for one operator.
    type Output: Clone + 'static;

    /// Returns this analysis' operator-value state.
    fn state(&self) -> &OperatorAnalysisState<Self::Output>;

    /// Computes the value for `operator`.
    fn compute(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output>;

    /// Returns the value for `operator`, computing and caching it if needed.
    fn get(
        &self,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        operator: Operator,
    ) -> AnalysisResult<Self::Output>
    where
        Self: Sized,
    {
        let state = self.state();

        if let Some(value) = state.values.borrow().get(&operator) {
            return Ok(value.clone());
        }

        if !state.in_progress.borrow_mut().insert(operator) {
            return Err(AnalysisError::RecursiveOperatorAnalysis {
                analysis: type_name::<Self>(),
                operator,
            });
        }

        let result = self.compute(ctx, analyses, operator);
        state.in_progress.borrow_mut().remove(&operator);

        let value = result?;
        state.values.borrow_mut().insert(operator, value.clone());
        Ok(value)
    }

    /// Clears the operator-value cache.
    fn clear_operator_cache(&self) {
        self.state().clear();
    }
}

/// Cache state for an operator analysis.
#[derive(Default)]
pub struct OperatorAnalysisState<T> {
    values: RefCell<HashMap<Operator, T>>,
    in_progress: RefCell<HashSet<Operator>>,
}

impl<T> OperatorAnalysisState<T> {
    /// Clears cached values and in-progress markers.
    pub fn clear(&self) {
        self.values.borrow_mut().clear();
        self.in_progress.borrow_mut().clear();
    }
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
    pub fn get<A>(&mut self, ctx: &QueryContext, operator: Operator) -> AnalysisResult<A::Output>
    where
        A: OperatorAnalysis + Default + 'static,
    {
        let analysis = self.analysis::<A>();
        let analysis = typed_analysis::<A>(&analysis)?;
        analysis.get(ctx, self, operator)
    }

    fn analysis<A>(&mut self) -> Rc<dyn Analysis>
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
    collect_expr_used_columns(ctx, expr, &mut columns)?;
    Ok(columns)
}

fn collect_expr_used_columns(
    ctx: &QueryContext,
    expr: Expr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match expr.get(ctx) {
        ExprData::Literal(_) => {}
        ExprData::ColumnRef(column) => push_unique_column(columns, *column),
        ExprData::Unary { expr, .. } => collect_expr_used_columns(ctx, *expr, columns)?,
        ExprData::Binary { left, right, .. } => {
            collect_expr_used_columns(ctx, *left, columns)?;
            collect_expr_used_columns(ctx, *right, columns)?;
        }
        ExprData::Nary { exprs, .. } => {
            for expr in exprs {
                collect_expr_used_columns(ctx, *expr, columns)?;
            }
        }
        ExprData::Cast { expr, .. } => collect_expr_used_columns(ctx, *expr, columns)?,
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                collect_expr_used_columns(ctx, *when, columns)?;
                collect_expr_used_columns(ctx, *then, columns)?;
            }
            if let Some(else_expr) = else_expr {
                collect_expr_used_columns(ctx, *else_expr, columns)?;
            }
        }
        ExprData::ScalarFunction { args, .. } => {
            for arg in args {
                collect_expr_used_columns(ctx, *arg, columns)?;
            }
        }
    }

    Ok(())
}

fn collect_aggregate_expr_used_columns(
    ctx: &QueryContext,
    aggregate: &AggregateExpr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match aggregate {
        AggregateExpr::CountStar => Ok(()),
        AggregateExpr::Func { arg, .. } => collect_expr_used_columns(ctx, *arg, columns),
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
            JoinType::Mark(column) => vec![column],
            _ => Vec::new(),
        },
        OperatorData::Selection(_)
        | OperatorData::CrossProduct(_)
        | OperatorData::Sort(_)
        | OperatorData::Limit(_)
        | OperatorData::Projection(_)
        | OperatorData::Output(_) => Vec::new(),
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
        OperatorData::Scan(_) | OperatorData::CrossProduct(_) | OperatorData::Limit(_) => {}
        OperatorData::Sort(data) => {
            for key in &data.keys {
                collect_expr_used_columns(ctx, key.expr, &mut columns)?;
            }
        }
        OperatorData::Selection(data) => {
            collect_expr_used_columns(ctx, data.predicate, &mut columns)?;
        }
        OperatorData::Map(data) => {
            for (_, expr) in &data.computations {
                collect_expr_used_columns(ctx, *expr, &mut columns)?;
            }
        }
        OperatorData::TableFunction(data) => {
            for arg in &data.args {
                collect_expr_used_columns(ctx, *arg, &mut columns)?;
            }
        }
        OperatorData::Join(data) => {
            collect_expr_used_columns(ctx, data.on, &mut columns)?;
        }
        OperatorData::Aggregation(data) => {
            for key in &data.keys {
                collect_expr_used_columns(ctx, *key, &mut columns)?;
            }
            for (_, aggregate) in &data.aggregates {
                collect_aggregate_expr_used_columns(ctx, aggregate, &mut columns)?;
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
        OperatorData::Scan(_) | OperatorData::TableFunction(_) => {}
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

    for column in analyses.get::<UsedColumns>(ctx, operator)? {
        if !input_columns.contains(&column) {
            push_unique_column(&mut columns, column);
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
        ExprData::Binary { left, right, .. } => {
            Ok(expr_nullability(ctx, input_nullability, *left)?
                || expr_nullability(ctx, input_nullability, *right)?)
        }
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
        ExprData::ScalarFunction { .. } => Ok(true),
    }
}

fn collect_non_null_columns_from_predicate(
    ctx: &QueryContext,
    expr: Expr,
    columns: &mut Vec<Column>,
) -> AnalysisResult<()> {
    match expr.get(ctx) {
        ExprData::Literal(_)
        | ExprData::ColumnRef(_)
        | ExprData::Cast { .. }
        | ExprData::CaseWhen { .. }
        | ExprData::ScalarFunction { .. } => {}
        ExprData::Unary { op, expr } => match op {
            crate::UnaryOp::IsNotNull => collect_expr_used_columns(ctx, *expr, columns)?,
            crate::UnaryOp::Not => {
                if let ExprData::Unary {
                    op: crate::UnaryOp::IsNull,
                    expr,
                } = expr.get(ctx)
                {
                    collect_expr_used_columns(ctx, *expr, columns)?;
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
                collect_expr_used_columns(ctx, *left, columns)?;
                collect_expr_used_columns(ctx, *right, columns)?;
            }
            crate::BinaryOp::Add
            | crate::BinaryOp::Subtract
            | crate::BinaryOp::Multiply
            | crate::BinaryOp::Divide => {}
        },
        ExprData::Nary { op, exprs } => match op {
            crate::NaryOp::And => {
                for expr in exprs {
                    collect_non_null_columns_from_predicate(ctx, *expr, columns)?;
                }
            }
            crate::NaryOp::Or => {
                let mut iter = exprs.iter();
                let Some(first) = iter.next() else {
                    return Ok(());
                };

                let mut intersection = non_null_columns_from_predicate(ctx, *first)?;
                for expr in iter {
                    let branch = non_null_columns_from_predicate(ctx, *expr)?;
                    intersection.retain(|column| branch.contains(column));
                }

                extend_unique_columns(columns, intersection);
            }
        },
    }

    Ok(())
}

fn non_null_columns_from_predicate(ctx: &QueryContext, expr: Expr) -> AnalysisResult<Vec<Column>> {
    let mut columns = Vec::new();
    collect_non_null_columns_from_predicate(ctx, expr, &mut columns)?;
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
        OperatorData::TableFunction(_) => Ok(analyses
            .get::<CreatedColumns>(ctx, operator)?
            .into_iter()
            .map(|column| (column, true))
            .collect()),
        OperatorData::Selection(data) => {
            let mut columns = analyses.get::<ColumnNullability>(ctx, data.input)?;
            for column in non_null_columns_from_predicate(ctx, data.predicate)? {
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

            if let JoinType::Mark(column) = data.join_type {
                push_unique_nullability(&mut columns, column, false);
            }

            if matches!(data.join_type, JoinType::Inner) {
                for column in non_null_columns_from_predicate(ctx, data.on)? {
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

/// Analysis that records columns introduced directly by an operator.
#[derive(Default)]
pub struct CreatedColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl OperatorAnalysis for CreatedColumns {
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
        self.clear_operator_cache();
    }
}

/// Analysis that records columns referenced directly by an operator.
#[derive(Default)]
pub struct UsedColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl OperatorAnalysis for UsedColumns {
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
        let operator_data = operator.get(ctx);
        directly_used_columns(ctx, analyses, operator_data)
    }
}

impl Analysis for UsedColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_operator_cache();
    }
}

/// Analysis that records directly-used columns not available from operator inputs.
#[derive(Default)]
pub struct FreeColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl OperatorAnalysis for FreeColumns {
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
        self.clear_operator_cache();
    }
}

/// Analysis that records whether each output column may contain null values.
#[derive(Default)]
pub struct ColumnNullability {
    state: OperatorAnalysisState<Vec<(Column, bool)>>,
}

impl OperatorAnalysis for ColumnNullability {
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
        self.clear_operator_cache();
    }
}

/// Analysis that records columns available in an operator's output.
#[derive(Default)]
pub struct AvailableColumns {
    state: OperatorAnalysisState<Vec<Column>>,
}

impl OperatorAnalysis for AvailableColumns {
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
        match operator.get(ctx) {
            OperatorData::Scan(_) | OperatorData::TableFunction(_) => {
                analyses.get::<CreatedColumns>(ctx, operator)
            }
            OperatorData::Selection(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Map(data) => {
                let mut columns = analyses.get::<AvailableColumns>(ctx, data.input)?;
                columns.extend(analyses.get::<CreatedColumns>(ctx, operator)?);
                Ok(columns)
            }
            OperatorData::Join(data) => {
                let mut columns = analyses.get::<AvailableColumns>(ctx, data.outer)?;
                if !matches!(data.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                    columns.extend(analyses.get::<AvailableColumns>(ctx, data.inner)?);
                }
                columns.extend(analyses.get::<CreatedColumns>(ctx, operator)?);
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
                                operator,
                                expr: *expr,
                            });
                        }
                    }
                }
                columns.extend(analyses.get::<CreatedColumns>(ctx, operator)?);
                Ok(columns)
            }
            OperatorData::Projection(data) => {
                analyses.get::<AvailableColumns>(ctx, data.input)?;
                Ok(data.columns.clone())
            }
            OperatorData::Sort(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Limit(data) => analyses.get::<AvailableColumns>(ctx, data.input),
            OperatorData::Output(data) => analyses.get::<AvailableColumns>(ctx, data.input),
        }
    }
}

impl Analysis for AvailableColumns {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clear(&self) {
        self.clear_operator_cache();
    }
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
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let is_adult = ctx.add_column(ColumnData::new("is_adult", DataType::Boolean));

        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        }));

        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let adult_age = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(18)));
        let adult_expr = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: scan,
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![id, is_adult],
            input: map,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));

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
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let order_user_id = ctx.add_column(ColumnData::new("order_user_id", DataType::Int64));
        let order_total = ctx.add_column(ColumnData::new("order_total", DataType::Float64));
        let is_adult = ctx.add_column(ColumnData::new("is_adult", DataType::Boolean));

        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id, age],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id, order_total],
        }));

        let left = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        });
        let join = ctx.add_operator(OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        }));

        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let adult_age = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(18)));
        let adult_expr = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: join,
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![user_id, order_total, is_adult],
            input: map,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));

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
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let order_user_id = ctx.add_column(ColumnData::new("order_user_id", DataType::Int64));
        let order_total = ctx.add_column(ColumnData::new("order_total", DataType::Float64));
        let is_adult = ctx.add_column(ColumnData::new("is_adult", DataType::Boolean));

        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id, age],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id, order_total],
        }));

        let left = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        });
        let join = ctx.add_operator(OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        }));

        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let adult_age = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(18)));
        let adult_expr = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(is_adult, adult_expr)],
            input: join,
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![user_id, order_total, is_adult],
            input: map,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));

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
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let missing = ctx.add_column(ColumnData::new("missing", DataType::Int64));
        let computed = ctx.add_column(ColumnData::new("computed", DataType::Int64));

        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        }));

        let missing_ref = ctx.add_expr(ExprData::ColumnRef(missing));
        let selection = ctx.add_operator(OperatorData::Selection(Selection {
            predicate: missing_ref,
            input: scan,
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![id, missing],
            input: scan,
        }));
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(computed, missing_ref)],
            input: scan,
        }));
        let table_function = ctx.add_operator(OperatorData::TableFunction(TableFunction {
            function: TableFunctionDef::extension("read_from_column_path"),
            args: vec![missing_ref],
            columns: vec![id],
        }));

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
    fn column_nullability_tracks_output_columns() {
        let mut ctx = QueryContext::new();
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let is_age_null = ctx.add_column(ColumnData::new("is_age_null", DataType::Boolean));
        let age_plus_one = ctx.add_column(ColumnData::new("age_plus_one", DataType::Int32));

        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        }));
        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let is_null = ctx.add_expr(ExprData::Unary {
            op: crate::UnaryOp::IsNull,
            expr: age_ref,
        });
        let one = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(1)));
        let age_plus_one_expr = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Add,
            left: age_ref,
            right: one,
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(is_age_null, is_null), (age_plus_one, age_plus_one_expr)],
            input: scan,
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![id, is_age_null, age_plus_one],
            input: map,
        }));

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
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));

        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        }));
        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let predicate = ctx.add_expr(ExprData::Unary {
            op: crate::UnaryOp::IsNotNull,
            expr: age_ref,
        });
        let selection = ctx.add_operator(OperatorData::Selection(Selection {
            predicate,
            input: scan,
        }));

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, selection).unwrap(),
            vec![(id, true), (age, false)]
        );
    }

    #[test]
    fn column_nullability_uses_inner_join_null_rejection() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let order_user_id = ctx.add_column(ColumnData::new("order_user_id", DataType::Int64));

        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id],
        }));
        let left = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        });
        let join = ctx.add_operator(OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        }));

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
            vec![(user_id, false), (order_user_id, false)]
        );
    }

    #[test]
    fn column_nullability_tracks_outer_join_null_extension() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let order_user_id = ctx.add_column(ColumnData::new("order_user_id", DataType::Int64));

        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id],
        }));
        let left = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        });
        let join = ctx.add_operator(OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on,
            outer: users,
            inner: orders,
        }));

        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<ColumnNullability>(&ctx, join).unwrap(),
            vec![(user_id, true), (order_user_id, true)]
        );
    }

    #[test]
    fn expr_used_columns_deduplicates_by_first_use() {
        let mut ctx = QueryContext::new();
        let a = ctx.add_column(ColumnData::new("a", DataType::Int64));
        let b = ctx.add_column(ColumnData::new("b", DataType::Int64));

        let a_left = ctx.add_expr(ExprData::ColumnRef(a));
        let b_ref = ctx.add_expr(ExprData::ColumnRef(b));
        let a_right = ctx.add_expr(ExprData::ColumnRef(a));
        let expr = ctx.add_expr(ExprData::Nary {
            op: crate::NaryOp::And,
            exprs: vec![a_left, b_ref, a_right],
        });

        assert_eq!(expr_used_columns(&ctx, expr).unwrap(), vec![a, b]);
    }

    #[test]
    fn available_columns_rejects_expression_aggregation_keys() {
        let mut ctx = QueryContext::new();
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let count = ctx.add_column(ColumnData::new("count", DataType::Int64));

        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![age],
        }));
        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let one = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(1)));
        let key = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Add,
            left: age_ref,
            right: one,
        });
        let aggregation = ctx.add_operator(OperatorData::Aggregation(Aggregation {
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
        }));

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
        let first = ctx.add_column(ColumnData::new("first", DataType::Int64));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![first],
        }));
        let mut analyses = ctx.analyze();

        assert_eq!(
            analyses.get::<CreatedColumns>(&ctx, scan).unwrap(),
            vec![first]
        );

        let second = ctx.add_column(ColumnData::new("second", DataType::Int64));
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
}

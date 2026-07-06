use arrow_schema::DataType;
use std::any::{TypeId, type_name};
use std::cell::RefCell;
use std::sync::Arc;

pub mod analysis;
pub mod catalog;
pub mod cost;
mod display;
pub mod hypergraph;
pub mod optimize;
pub mod substrait;
pub mod tpch;

pub use analysis::{
    Analysis, AnalysisContext, AnalysisError, AnalysisResult, Analyzable, AvailableColumns,
    CardinalityEstimationV1, CardinalityProfile, ColumnNullability, ColumnProfile, CreatedColumns,
    Estimate, EstimateSource, FreeColumns, JoinInputProfiles, ParentIndex, ParentsOf, UsedColumns,
    expr_used_columns,
};
pub use catalog::{
    Catalog, CatalogError, CatalogResult, ColumnStatistics, MemoryCatalog, ResolvedTableRef,
    TableId, TableMetadata, TableRef, TableStatistics,
};
pub use cost::{CostModel, DefaultCostModel, JoinCostClass, join_work_cost};
pub use display::{
    BoxDrawingRenderer, BoxRendererConfig, BoxRendererTheme, ColorMode, DisplayField, DisplayInput,
    DisplayNode, DisplayNodeRecord, DisplayPlan, DisplayProperties, DisplayValue,
    OptimizerVisualizerNode, OptimizerVisualizerPass, OptimizerVisualizerValue,
};
pub use hypergraph::{
    Hyperedge, HyperedgeJoinType, HypergraphNode, HypergraphOf, JoinGroupOf, NodeId, NodeSet,
    QueryHypergraph, build_hypergraph, nodeset_iter, nodeset_min, nodeset_singleton,
};
pub use optimize::{
    Direction, ExprSimplify, HolisticUnnesting, JoinOrdering, JoinTreeNormalize,
    MarkJoinToSemiJoin, OperatorRewrite, OperatorRewriteAdaptor, OptimizeError, OptimizeResult,
    Pass, PassManager, PassProfile, PassResult, PassTrace, PredicatePushdown,
    ProjectionElimination, QueryPass, Rewrite, RewriteMap, SubqueryToJoin, Unnesting,
};

/// An opaque reference to a relational operator in a [`QueryContext`].
///
/// The corresponding payload is [`OperatorData`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Operator(usize);

impl Operator {
    /// Returns the operator payload from `ctx`.
    pub fn get(self, ctx: &QueryContext) -> &OperatorData {
        &ctx.operators[self.0]
    }

    /// Returns the mutable operator payload from `ctx`.
    pub fn get_mut(self, ctx: &mut QueryContext) -> &mut OperatorData {
        &mut ctx.operators[self.0]
    }
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}", self.0)
    }
}

/// A relational operator referenced by an [`Operator`] handle.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorData {
    /// Reads a table and exposes the listed columns.
    Scan(Scan),
    /// Filters the input using a boolean predicate expression.
    Selection(Selection),
    /// Applies a function to each row of the input, producing new columns.
    Map(Map),
    /// Reads rows produced by a table-valued function.
    TableFunction(TableFunction),
    /// Joins two inputs using the provided join type and condition.
    Join(Join),
    /// Produces the Cartesian product of the two inputs.
    CrossProduct(CrossProduct),
    /// Orders rows by one or more sort keys.
    Sort(Sort),
    /// Skips and/or takes rows from its input.
    Limit(Limit),
    /// Groups rows by key expressions and computes one or more aggregate columns.
    Aggregation(Aggregation),
    /// Keeps or reorders the listed columns from the input.
    Projection(Projection),
    /// Marks the final query output.
    Output(Output),
    /// Renames the output of its input under a new qualifier.
    /// Corresponds to SQL `(subquery) AS alias` or `table AS alias`.
    Rename(Rename),
    /// Produces constant rows from literal expressions.
    ConstScan(ConstScan),
}

impl OperatorData {
    /// Appends this operator to `ctx` and returns its handle.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, ctx: &mut QueryContext) -> Operator {
        QueryContext::add_operator(ctx, self)
    }

    /// Rebuilds this operator after applying `f` to each relational input.
    ///
    /// This centralizes the exhaustive input dispatch so generic tree rewrites
    /// do not need to duplicate a `match` over every operator variant.
    pub fn map_inputs(self, mut f: impl FnMut(Operator) -> Operator) -> Self {
        self.try_map_inputs::<std::convert::Infallible>(|input| Ok(f(input)))
            .expect("infallible input mapping cannot fail")
    }

    /// Rebuilds this operator after applying a fallible mapper to each input.
    pub fn try_map_inputs<E>(
        self,
        mut f: impl FnMut(Operator) -> Result<Operator, E>,
    ) -> Result<Self, E> {
        Ok(match self {
            OperatorData::Selection(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Selection(operator)
            }
            OperatorData::Map(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Map(operator)
            }
            OperatorData::Join(mut operator) => {
                operator.outer = f(operator.outer)?;
                operator.inner = f(operator.inner)?;
                OperatorData::Join(operator)
            }
            OperatorData::CrossProduct(mut operator) => {
                operator.outer = f(operator.outer)?;
                operator.inner = f(operator.inner)?;
                OperatorData::CrossProduct(operator)
            }
            OperatorData::Sort(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Sort(operator)
            }
            OperatorData::Limit(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Limit(operator)
            }
            OperatorData::Aggregation(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Aggregation(operator)
            }
            OperatorData::Projection(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Projection(operator)
            }
            OperatorData::Output(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Output(operator)
            }
            OperatorData::Rename(mut operator) => {
                operator.input = f(operator.input)?;
                OperatorData::Rename(operator)
            }
            OperatorData::Scan(operator) => OperatorData::Scan(operator),
            OperatorData::TableFunction(operator) => OperatorData::TableFunction(operator),
            OperatorData::ConstScan(operator) => OperatorData::ConstScan(operator),
        })
    }
}

/// Reads a base table and exposes its columns.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scan {
    pub table: TableRef,
    pub columns: Vec<Column>,
}

/// Produces literal rows with no input dependencies.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConstScan {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Expr>>,
}

/// Filters rows from its input.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Selection {
    pub predicate: Expr,
    pub input: Operator,
}

/// Computes new columns from input rows.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Map {
    pub computations: Vec<(Column, Expr)>,
    pub input: Operator,
}

/// Reads rows produced by a table-valued function.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableFunction {
    pub function: TableFunctionDef,
    pub args: Vec<Expr>,
    pub columns: Vec<Column>,
}

/// Joins two inputs using a join condition.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Join {
    pub join_type: JoinType,
    pub on: Expr,
    pub outer: Operator,
    pub inner: Operator,
}

/// Produces the Cartesian product of two inputs.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CrossProduct {
    pub outer: Operator,
    pub inner: Operator,
}

/// Orders rows by one or more expressions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sort {
    pub keys: Vec<SortKey>,
    pub input: Operator,
}

/// One expression and ordering policy used by [`Sort`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortKey {
    pub expr: Expr,
    pub direction: SortDirection,
    pub nulls: NullOrdering,
}

/// Sort direction for a [`SortKey`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Asc => f.write_str("Asc"),
            Self::Desc => f.write_str("Desc"),
        }
    }
}

/// Null ordering policy for a [`SortKey`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrdering {
    First,
    Last,
}

impl std::fmt::Display for NullOrdering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::First => f.write_str("NullsFirst"),
            Self::Last => f.write_str("NullsLast"),
        }
    }
}

/// Skips and/or takes rows from its input.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Limit {
    pub fetch: Option<usize>,
    pub offset: usize,
    pub input: Operator,
}

/// Groups rows and computes aggregate columns.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Aggregation {
    pub keys: Vec<Expr>,
    pub aggregates: Vec<(Column, AggregateExpr)>,
    pub input: Operator,
}

/// Keeps or reorders columns from its input.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Projection {
    pub columns: Vec<Column>,
    pub input: Operator,
}

/// Marks the final query output.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Output {
    pub input: Operator,
}

/// Renames the output of its input under a new qualifier.
///
/// `defs` maps each renamed column handle to the original column it replaces:
/// `(renamed, original)`. The `alias` is the new table qualifier.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Rename {
    pub alias: String,
    pub defs: Vec<(Column, Column)>,
    pub input: Operator,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    LeftSemi,
    LeftAnti,
    LeftOuter,
    RightOuter,
    FullOuter,
    Single,
    /// Left mark join. The marker is the SQL OR over all join predicate
    /// results for each left row: TRUE if any predicate is TRUE, FALSE if no
    /// predicate is TRUE and no UNKNOWN matters, and NULL if no predicate is
    /// TRUE but an UNKNOWN result remains observable.
    LeftMark {
        marker: Column,
        nullable: bool,
    },
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => f.write_str("Inner"),
            JoinType::LeftSemi => f.write_str("LeftSemi"),
            JoinType::LeftAnti => f.write_str("LeftAnti"),
            JoinType::LeftOuter => f.write_str("LeftOuter"),
            JoinType::RightOuter => f.write_str("RightOuter"),
            JoinType::FullOuter => f.write_str("FullOuter"),
            JoinType::Single => f.write_str("Single"),
            JoinType::LeftMark { .. } => write!(f, "LeftMark"),
        }
    }
}

/// An opaque reference to a column in a [`QueryContext`].
///
/// The corresponding payload is [`ColumnData`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Column(usize);

impl Column {
    /// Returns the column payload from `ctx`.
    pub fn get(self, ctx: &QueryContext) -> &ColumnData {
        &ctx.columns[self.0]
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Metadata for the column.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub name: String,
    pub ty: DataType,
    /// Optional table qualifier (e.g. `"customer"` for `customer.c_custkey`).
    pub qualifier: Option<String>,
}

impl ColumnData {
    /// Creates column metadata without a qualifier.
    pub fn new(name: impl Into<String>, ty: DataType) -> Self {
        Self {
            name: name.into(),
            ty,
            qualifier: None,
        }
    }

    /// Creates column metadata with a qualifier.
    pub fn with_qualifier(
        name: impl Into<String>,
        ty: DataType,
        qualifier: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            ty,
            qualifier: Some(qualifier.into()),
        }
    }

    /// Appends this column metadata to `ctx` and returns its handle.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, ctx: &mut QueryContext) -> Column {
        QueryContext::add_column(ctx, self)
    }
}

/// An opaque reference to a scalar expression in a [`QueryContext`].
///
/// The corresponding payload is [`ExprData`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Expr(usize);

impl Expr {
    /// Returns the expression payload from `ctx`.
    pub fn get(self, ctx: &QueryContext) -> &ExprData {
        &ctx.exprs[self.0]
    }

    /// Returns the mutable expression payload from `ctx`.
    pub fn get_mut(self, ctx: &mut QueryContext) -> &mut ExprData {
        &mut ctx.exprs[self.0]
    }
}

/// A scalar expression referenced by an [`Expr`] handle.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub enum ExprData {
    /// Constant scalar literal.
    Literal(ScalarValue),
    /// Reference to a column.
    ColumnRef(Column),
    /// Unary operation over one expression.
    Unary { op: UnaryOp, expr: Expr },
    /// Binary operation over two expressions.
    Binary {
        op: BinaryOp,
        left: Expr,
        right: Expr,
    },
    /// N-ary operation over a variable number of expressions.
    Nary { op: NaryOp, exprs: Vec<Expr> },
    /// Casts an expression to a target type.
    Cast { expr: Expr, ty: DataType },
    /// SQL-style conditional expression.
    CaseWhen {
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Expr>,
    },
    /// Scalar function call over zero or more argument expressions.
    ScalarFunction {
        function: ScalarFunction,
        args: Vec<Expr>,
    },
    /// SQL `EXISTS (subquery)` predicate. `negated` represents `NOT EXISTS`.
    Exists { subquery: Operator, negated: bool },
    /// SQL `[NOT] IN (subquery)` predicate.
    InSubquery {
        expr: Expr,
        subquery: Operator,
        negated: bool,
    },
    /// SQL scalar subquery expression.
    ScalarSubquery { subquery: Operator },
    /// SQL `[NOT] [I]LIKE pattern` predicate. `case_insensitive` = true for ILIKE.
    Like {
        negated: bool,
        expr: Expr,
        pattern: Expr,
        case_insensitive: bool,
    },
}

impl ExprData {
    /// Appends this expression to `ctx` and returns its handle.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, ctx: &mut QueryContext) -> Expr {
        QueryContext::add_expr(ctx, self)
    }
}

/// Unary expression operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    /// Boolean negation.
    Not,
    /// Tests whether an expression evaluates to null.
    IsNull,
    /// Tests whether an expression evaluates to non-null.
    IsNotNull,
    /// Numeric negation.
    Negate,
}

/// Binary expression operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    /// Equality comparison.
    Eq,
    /// Null-safe equality comparison (`IS NOT DISTINCT FROM`).
    IsNotDistinctFrom,
    /// Inequality comparison.
    NotEq,
    /// Less-than comparison.
    Lt,
    /// Less-than-or-equal comparison.
    LtEq,
    /// Greater-than comparison.
    Gt,
    /// Greater-than-or-equal comparison.
    GtEq,
    /// Numeric addition.
    Add,
    /// Numeric subtraction.
    Subtract,
    /// Numeric multiplication.
    Multiply,
    /// Numeric division.
    Divide,
}

/// N-ary logical expression operator.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NaryOp {
    /// Logical conjunction.
    And,
    /// Logical disjunction.
    Or,
}

/// Group-level expression used by [`OperatorData::Aggregation`].
///
/// Aggregate expressions consume the input rows for one group and produce one scalar value.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateExpr {
    /// Counts all rows in the group.
    CountStar,
    /// Applies an aggregate function to a scalar argument expression.
    Func {
        func: AggregateFunction,
        arg: Expr,
        distinct: bool,
    },
}

/// Aggregate function applied by [`AggregateExpr::Func`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Extension(String),
}

impl AggregateFunction {
    /// Creates an aggregate function identifier for a catalog or extension function.
    pub fn extension(name: impl Into<String>) -> Self {
        Self::Extension(name.into())
    }
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunction::Count => f.write_str("count"),
            AggregateFunction::Sum => f.write_str("sum"),
            AggregateFunction::Avg => f.write_str("avg"),
            AggregateFunction::Min => f.write_str("min"),
            AggregateFunction::Max => f.write_str("max"),
            AggregateFunction::Extension(name) => f.write_str(name),
        }
    }
}

/// Scalar function used by [`ExprData::ScalarFunction`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarFunction {
    Abs,
    Lower,
    Upper,
    Coalesce,
    Extension(String),
}

impl ScalarFunction {
    /// Creates a scalar function identifier for a catalog or extension function.
    pub fn extension(name: impl Into<String>) -> Self {
        Self::Extension(name.into())
    }
}

impl std::fmt::Display for ScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarFunction::Abs => f.write_str("abs"),
            ScalarFunction::Lower => f.write_str("lower"),
            ScalarFunction::Upper => f.write_str("upper"),
            ScalarFunction::Coalesce => f.write_str("coalesce"),
            ScalarFunction::Extension(name) => f.write_str(name),
        }
    }
}

/// Window function identifier for future window expressions/operators.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
    Extension(String),
}

impl WindowFunction {
    /// Creates a window function identifier for a catalog or extension function.
    pub fn extension(name: impl Into<String>) -> Self {
        Self::Extension(name.into())
    }
}

impl std::fmt::Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowFunction::RowNumber => f.write_str("row_number"),
            WindowFunction::Rank => f.write_str("rank"),
            WindowFunction::DenseRank => f.write_str("dense_rank"),
            WindowFunction::Lag => f.write_str("lag"),
            WindowFunction::Lead => f.write_str("lead"),
            WindowFunction::Extension(name) => f.write_str(name),
        }
    }
}

/// Table-valued function used by [`OperatorData::TableFunction`].
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFunctionDef {
    Values,
    ReadCsv,
    ReadParquet,
    Extension(String),
}

impl TableFunctionDef {
    /// Creates a table function identifier for a catalog or extension function.
    pub fn extension(name: impl Into<String>) -> Self {
        Self::Extension(name.into())
    }
}

impl std::fmt::Display for TableFunctionDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFunctionDef::Values => f.write_str("values"),
            TableFunctionDef::ReadCsv => f.write_str("read_csv"),
            TableFunctionDef::ReadParquet => f.write_str("read_parquet"),
            TableFunctionDef::Extension(name) => f.write_str(name),
        }
    }
}

/// Scalar value used by literal expressions.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// Null value with an explicit Arrow data type.
    Null(DataType),
    /// Boolean scalar.
    Boolean(bool),
    /// 32-bit signed integer scalar.
    Int32(i32),
    /// 64-bit signed integer scalar.
    Int64(i64),
    /// 64-bit floating-point scalar.
    Float64(f64),
    /// Decimal scalar with unscaled 128-bit value, precision, and scale.
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    /// Date scalar as days since the UNIX epoch.
    Date32(i32),
    /// UTF-8 string scalar.
    Utf8(String),
    /// Month-day-nanosecond interval scalar.
    IntervalMonthDayNano {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
    /// Day-time interval scalar.
    IntervalDayTime { days: i32, milliseconds: i32 },
}

impl ScalarValue {
    /// Returns the Arrow data type represented by this scalar value.
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null(ty) => ty.clone(),
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::IntervalMonthDayNano { .. } => {
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
            }
            ScalarValue::IntervalDayTime { .. } => {
                DataType::Interval(arrow_schema::IntervalUnit::DayTime)
            }
        }
    }
}

/// Owns the query IR graph and all node arenas.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Default)]
pub struct QueryContext {
    root: Option<Operator>,
    operators: Vec<OperatorData>,
    exprs: Vec<ExprData>,
    columns: Vec<ColumnData>,
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryContext")
            .field("operators", &self.operators)
            .field("exprs", &self.exprs)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// Owns a query and the analysis state used while optimizing it.
pub struct OptimizerContext {
    pub query: QueryContext,
    pub analyses: AnalysisContext,
    pub rewrites: optimize::RewriteMap,
    pub(crate) optimizer_run_id: u64,
}

impl OptimizerContext {
    /// Creates an optimizer context for `query`.
    pub fn new(query: QueryContext) -> Self {
        Self {
            query,
            analyses: AnalysisContext::new(),
            rewrites: optimize::RewriteMap::new(),
            optimizer_run_id: 0,
        }
    }

    /// Creates an optimizer context whose analyses can consult `catalog`.
    pub fn with_catalog(query: QueryContext, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            query,
            analyses: AnalysisContext::with_catalog(catalog),
            rewrites: optimize::RewriteMap::new(),
            optimizer_run_id: 0,
        }
    }

    /// Consumes this optimizer context and returns the optimized query.
    pub fn into_query(self) -> QueryContext {
        self.query
    }
}

impl QueryContext {
    /// Creates an empty query context.
    pub fn new() -> Self {
        Self {
            root: None,
            operators: Vec::new(),
            exprs: Vec::new(),
            columns: Vec::new(),
        }
    }

    /// Appends an operator and returns its handle.
    pub fn add_operator(&mut self, operator: OperatorData) -> Operator {
        let id = Operator(self.operators.len());
        self.operators.push(operator);
        id
    }

    /// Appends an expression and returns its handle.
    pub fn add_expr(&mut self, expr: ExprData) -> Expr {
        let id = Expr(self.exprs.len());
        self.exprs.push(expr);
        id
    }

    /// Appends column metadata and returns its handle.
    pub fn add_column(&mut self, column: ColumnData) -> Column {
        let id = Column(self.columns.len());
        self.columns.push(column);
        id
    }

    /// Appends a scan over already-created columns and returns its operator handle.
    pub fn add_scan(&mut self, table: TableRef, columns: Vec<Column>) -> Operator {
        self.add_operator(OperatorData::Scan(Scan { table, columns }))
    }

    /// Resolves `table` through `catalog`, creates query columns from its schema, and appends a scan.
    pub fn add_scan_from_catalog<C>(
        &mut self,
        catalog: &C,
        table: TableRef,
    ) -> CatalogResult<Operator>
    where
        C: Catalog + ?Sized,
    {
        let metadata = catalog.table_by_ref(&table)?;
        let columns = metadata
            .schema
            .fields()
            .iter()
            .map(|field| {
                self.add_column(ColumnData::new(
                    field.name().clone(),
                    field.data_type().clone(),
                ))
            })
            .collect();

        Ok(self.add_scan(TableRef::from(metadata.table), columns))
    }

    /// Updates the root operator of the query graph.
    pub fn set_root(&mut self, root: Operator) {
        self.root = Some(root);
    }

    /// Returns the root operator of the query graph, if one has been set.
    pub fn root(&self) -> Option<Operator> {
        self.root
    }

    /// Creates an empty analysis context for demand-driven query analysis.
    pub fn analyze(&self) -> AnalysisContext {
        AnalysisContext::new()
    }

    /// Returns the number of operators in the operator arena.
    pub fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// Returns the number of expressions in the expression arena.
    pub fn expr_count(&self) -> usize {
        self.exprs.len()
    }

    /// Returns the number of columns in the column arena.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns immutable operator data for a handle.
    pub fn operator(&self, operator: Operator) -> &OperatorData {
        operator.get(self)
    }

    /// Returns mutable operator data for a handle.
    pub fn operator_mut(&mut self, operator: Operator) -> &mut OperatorData {
        operator.get_mut(self)
    }

    /// Returns immutable expression data for a handle.
    pub fn expr(&self, expr: Expr) -> &ExprData {
        expr.get(self)
    }

    /// Returns mutable expression data for a handle.
    pub fn expr_mut(&mut self, expr: Expr) -> &mut ExprData {
        expr.get_mut(self)
    }

    /// Returns metadata for a column handle.
    pub fn column(&self, column: Column) -> &ColumnData {
        column.get(self)
    }

    /// Formats the reachable query plan using the default query formatter.
    pub fn pretty(&self) -> String {
        self.pretty_with_config(QueryFormatConfig::default())
    }

    /// Formats the reachable query plan using the provided query formatter config.
    pub fn pretty_with_config(&self, config: QueryFormatConfig) -> String {
        let node = QueryFormatter::with_config(self, config).format();
        BoxDrawingRenderer::default().render(&node)
    }

    /// Formats the reachable query plan as recursive JSON for inspecting the tree shape.
    #[cfg(feature = "serde")]
    pub fn pretty_json(&self) -> String {
        self.pretty_json_with_config(QueryFormatConfig::default())
    }

    /// Formats the reachable query plan as recursive JSON with the provided config.
    #[cfg(feature = "serde")]
    pub fn pretty_json_with_config(&self, config: QueryFormatConfig) -> String {
        let node = QueryFormatter::with_config(self, config).format();
        serde_json::to_string_pretty(&node).expect("display tree serialization should not fail")
    }

    /// Formats the reachable query plan as flat DFS post-order JSON for file diffs.
    #[cfg(feature = "serde")]
    pub fn pretty_flat(&self) -> String {
        self.pretty_flat_with_config(QueryFormatConfig::default())
    }

    /// Formats the reachable query plan as flat DFS post-order JSON with the provided config.
    #[cfg(feature = "serde")]
    pub fn pretty_flat_with_config(&self, config: QueryFormatConfig) -> String {
        let plan = QueryFormatter::with_config(self, config).format_plan();
        serde_json::to_string_pretty(&plan).expect("display plan serialization should not fail")
    }

    /// Formats the reachable query plan as an optd optimizer visualizer pass.
    #[cfg(feature = "serde")]
    pub fn optimizer_visualizer_pass(
        &self,
        pass_name: impl Into<String>,
    ) -> OptimizerVisualizerPass {
        let node = QueryFormatter::with_config(
            self,
            QueryFormatConfig::new().with_analysis::<CardinalityEstimationV1>(),
        )
        .format();
        OptimizerVisualizerPass::new(pass_name, OptimizerVisualizerNode::from_display_node(&node))
    }

    /// Formats the reachable query plan as optd optimizer visualizer JSON.
    #[cfg(feature = "serde")]
    pub fn optimizer_visualizer_json(&self, pass_name: impl Into<String>) -> String {
        serde_json::to_string_pretty(&vec![self.optimizer_visualizer_pass(pass_name)])
            .expect("optimizer visualizer serialization should not fail")
    }
}

/// Formats an optimizer trace as optd optimizer visualizer JSON.
#[cfg(feature = "serde")]
pub fn optimizer_visualizer_trace_json(initial: &QueryContext, traces: &[PassTrace]) -> String {
    let mut passes = Vec::with_capacity(traces.len() + 1);
    passes.push(initial.optimizer_visualizer_pass("Initial"));
    passes.extend(traces.iter().map(|trace| {
        let profile = &trace.profile;
        trace
            .query
            .optimizer_visualizer_pass(profile.pass)
            .with_profile(
                profile.iteration,
                profile.pass_index,
                match profile.result {
                    Some(PassResult::Changed) => "changed",
                    Some(PassResult::Unchanged) => "unchanged",
                    None => "error",
                },
                profile.duration_ms,
            )
    }));
    serde_json::to_string_pretty(&passes)
        .expect("optimizer visualizer trace serialization should not fail")
}

/// Configuration for query-specific formatting.
#[derive(Debug, Clone, Default)]
pub struct QueryFormatConfig {
    analysis_properties: Vec<AnalysisDisplayProperty>,
}

impl QueryFormatConfig {
    /// Creates a config that includes no optional analysis properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an analysis property to the formatted output.
    pub fn with_analysis<A>(mut self) -> Self
    where
        A: DisplayableOperatorAnalysis,
    {
        let property = AnalysisDisplayProperty::new::<A>();
        if !self
            .analysis_properties
            .iter()
            .any(|existing| existing.type_id == property.type_id)
        {
            self.analysis_properties.push(property);
        }
        self
    }

    /// Adds all currently supported analysis properties to the formatted output.
    pub fn with_all_analysis(self) -> Self {
        self.with_analysis::<CreatedColumns>()
            .with_analysis::<AvailableColumns>()
            .with_analysis::<UsedColumns>()
            .with_analysis::<FreeColumns>()
            .with_analysis::<ColumnNullability>()
    }

    /// Returns the display keys for analysis properties shown by this config.
    pub fn analysis_property_keys(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.analysis_properties.iter().map(|property| property.key)
    }
}

/// Operator analysis whose output can be shown as a display property.
pub trait DisplayableOperatorAnalysis: Analyzable + Default + 'static {
    /// Stable key used by pretty and JSON display output.
    const DISPLAY_KEY: &'static str;

    /// Formats this analysis' output as a display value.
    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue;
}

impl DisplayableOperatorAnalysis for CreatedColumns {
    const DISPLAY_KEY: &'static str = "analysis::created_columns";

    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        formatter.format_columns(output)
    }
}

impl DisplayableOperatorAnalysis for AvailableColumns {
    const DISPLAY_KEY: &'static str = "analysis::available_columns";

    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        formatter.format_columns(output)
    }
}

impl DisplayableOperatorAnalysis for CardinalityEstimationV1 {
    const DISPLAY_KEY: &'static str = "estimated_rows";

    fn format_output(_formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        DisplayValue::Atom(output.rows.value.to_string())
    }
}

impl DisplayableOperatorAnalysis for UsedColumns {
    const DISPLAY_KEY: &'static str = "analysis::used_columns";

    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        formatter.format_columns(output)
    }
}

impl DisplayableOperatorAnalysis for FreeColumns {
    const DISPLAY_KEY: &'static str = "analysis::free_columns";

    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        formatter.format_columns(output)
    }
}

impl DisplayableOperatorAnalysis for ColumnNullability {
    const DISPLAY_KEY: &'static str = "analysis::column_nullability";

    fn format_output(formatter: &QueryFormatter<'_>, output: Self::Value) -> DisplayValue {
        DisplayValue::List(
            output
                .into_iter()
                .map(|(column, nullable)| {
                    let state = if nullable { "nullable" } else { "not null" };
                    format!("{}: {state}", formatter.format_column_name(column))
                })
                .collect(),
        )
    }
}

#[derive(Clone)]
struct AnalysisDisplayProperty {
    type_id: TypeId,
    type_name: &'static str,
    key: &'static str,
    format: fn(&QueryFormatter<'_>, Operator) -> DisplayValue,
}

impl AnalysisDisplayProperty {
    fn new<A>() -> Self
    where
        A: DisplayableOperatorAnalysis,
    {
        Self {
            type_id: TypeId::of::<A>(),
            type_name: type_name::<A>(),
            key: A::DISPLAY_KEY,
            format: format_displayable_analysis::<A>,
        }
    }
}

impl std::fmt::Debug for AnalysisDisplayProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalysisDisplayProperty")
            .field("type_id", &self.type_id)
            .field("type_name", &self.type_name)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

/// Formats a [`QueryContext`] into a generic [`DisplayNode`] tree.
pub struct QueryFormatter<'a> {
    ctx: &'a QueryContext,
    config: QueryFormatConfig,
    analyses: RefCell<AnalysisContext>,
}

impl<'a> QueryFormatter<'a> {
    /// Creates a query formatter.
    pub fn new(ctx: &'a QueryContext) -> Self {
        Self::with_config(ctx, QueryFormatConfig::default())
    }

    /// Creates a query formatter with the provided config.
    pub fn with_config(ctx: &'a QueryContext, config: QueryFormatConfig) -> Self {
        Self {
            ctx,
            config,
            analyses: RefCell::new(AnalysisContext::new()),
        }
    }

    /// Formats the reachable query plan from the root operator into a display tree.
    pub fn format(&self) -> DisplayNode {
        if let Some(root) = self.ctx.root {
            self.format_operator(root)
        } else {
            DisplayNode::with_kind("empty_query", "EMPTY QUERY")
        }
    }

    /// Formats the reachable query plan as flat records in deterministic DFS post-order.
    pub fn format_plan(&self) -> DisplayPlan {
        let mut nodes = Vec::new();

        if let Some(root) = self.ctx.root {
            for operator in self.dfs_post_order(root) {
                nodes.push(self.format_operator_record(operator));
            }
        } else {
            nodes.push(DisplayNodeRecord {
                id: 0,
                kind: "empty_query".to_string(),
                title: "EMPTY QUERY".to_string(),
                fields: DisplayProperties::new(),
                inputs: DisplayProperties::new(),
                metadata: DisplayProperties::new(),
            });
        }

        DisplayPlan { nodes }
    }

    fn format_operator(&self, operator: Operator) -> DisplayNode {
        let operator_display = self.format_operator_display(operator);
        let mut node =
            DisplayNode::with_kind(operator_display.kind, operator_display.title).with_id(operator);

        for field in operator_display.fields {
            node.fields.push(field);
        }

        for input in operator_display.inputs {
            node = node.with_input(input.name, self.format_operator(input.target));
        }

        for input in self.operator_subquery_inputs(operator) {
            node = node.with_input(input.name, self.format_operator(input.target));
        }

        for (key, value) in self.format_analysis_properties(operator) {
            node.metadata.insert(key, value);
        }

        node
    }

    fn format_column_name(&self, column: Column) -> String {
        format!("{}({column})", self.ctx.column(column).name)
    }

    fn format_columns(&self, columns: Vec<Column>) -> DisplayValue {
        DisplayValue::List(
            columns
                .into_iter()
                .map(|column| self.format_column_name(column))
                .collect(),
        )
    }

    fn format_definition(&self, column: Column, value: impl Into<String>) -> String {
        format!("{} := {}", self.format_column_name(column), value.into())
    }

    fn format_sort_key(&self, key: &SortKey) -> String {
        format!(
            "{} {} {}",
            self.format_expr(key.expr),
            key.direction,
            key.nulls
        )
    }

    fn format_exprs(&self, exprs: &[Expr]) -> String {
        exprs
            .iter()
            .map(|expr| self.format_expr(*expr))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Formats a scalar expression as a string. Public for use by hypergraph pretty printing.
    pub fn format_expr_pub(&self, expr: Expr) -> String {
        self.format_expr(expr)
    }

    fn format_expr(&self, expr: Expr) -> String {
        match self.ctx.expr(expr) {
            ExprData::Literal(value) => self.format_scalar(value),
            ExprData::ColumnRef(column) => self.format_column_name(*column),
            ExprData::Unary { op, expr } => {
                format!("{}({})", self.format_unary_op(*op), self.format_expr(*expr))
            }
            ExprData::Binary { op, left, right } => {
                format!(
                    "({} {} {})",
                    self.format_expr(*left),
                    self.format_binary_op(*op),
                    self.format_expr(*right)
                )
            }
            ExprData::Nary { op, exprs } => {
                let separator = format!(" {} ", self.format_nary_op(*op));
                format!(
                    "({})",
                    exprs
                        .iter()
                        .map(|expr| self.format_expr(*expr))
                        .collect::<Vec<_>>()
                        .join(&separator)
                )
            }
            ExprData::ScalarFunction { function, args } => {
                format!("{}({})", function, self.format_exprs(args))
            }
            ExprData::Cast { expr, ty } => {
                format!("CAST({} AS {ty:?})", self.format_expr(*expr))
            }
            ExprData::CaseWhen {
                when_then,
                else_expr,
            } => {
                let mut parts = when_then
                    .iter()
                    .map(|(when, then)| {
                        format!(
                            "WHEN {} THEN {}",
                            self.format_expr(*when),
                            self.format_expr(*then)
                        )
                    })
                    .collect::<Vec<_>>();
                if let Some(else_expr) = else_expr {
                    parts.push(format!("ELSE {}", self.format_expr(*else_expr)));
                }
                format!("CASE {} END", parts.join(" "))
            }
            ExprData::Exists { subquery, negated } => {
                let prefix = if *negated { "NOT EXISTS" } else { "EXISTS" };
                format!("{prefix}({subquery})")
            }
            ExprData::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let op = if *negated { "NOT IN" } else { "IN" };
                format!("({} {op} {subquery})", self.format_expr(*expr))
            }
            ExprData::ScalarSubquery { subquery } => format!("SCALAR_SUBQUERY({subquery})"),
            ExprData::Like {
                negated,
                expr,
                pattern,
                case_insensitive,
            } => {
                let op = match (negated, case_insensitive) {
                    (false, false) => "LIKE",
                    (true, false) => "NOT LIKE",
                    (false, true) => "ILIKE",
                    (true, true) => "NOT ILIKE",
                };
                format!(
                    "({} {op} {})",
                    self.format_expr(*expr),
                    self.format_expr(*pattern)
                )
            }
        }
    }

    fn format_aggregate_expr(&self, aggregate: &AggregateExpr) -> String {
        match aggregate {
            AggregateExpr::CountStar => "count_star()".to_string(),
            AggregateExpr::Func {
                func,
                arg,
                distinct,
            } => {
                let distinct = if *distinct { "DISTINCT " } else { "" };
                format!("{}({}{})", func, distinct, self.format_expr(*arg))
            }
        }
    }

    fn format_scalar(&self, value: &ScalarValue) -> String {
        match value {
            ScalarValue::Null(ty) => format!("NULL::{ty:?}"),
            ScalarValue::Boolean(value) => value.to_string(),
            ScalarValue::Int32(value) => value.to_string(),
            ScalarValue::Int64(value) => value.to_string(),
            ScalarValue::Float64(value) => value.to_string(),
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => format!("{value}::Decimal128({precision}, {scale})"),
            ScalarValue::Date32(value) => format!("{value}::Date32"),
            ScalarValue::Utf8(value) => format!("{value:?}"),
            ScalarValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } => format!("INTERVAL({months}mo {days}d {nanoseconds}ns)"),
            ScalarValue::IntervalDayTime { days, milliseconds } => {
                format!("INTERVAL({days}d {milliseconds}ms)")
            }
        }
    }

    fn format_unary_op(&self, op: UnaryOp) -> &'static str {
        match op {
            UnaryOp::Not => "NOT",
            UnaryOp::IsNull => "IS_NULL",
            UnaryOp::IsNotNull => "IS_NOT_NULL",
            UnaryOp::Negate => "NEG",
        }
    }

    fn format_binary_op(&self, op: BinaryOp) -> &'static str {
        match op {
            BinaryOp::Eq => "=",
            BinaryOp::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
            BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Add => "+",
            BinaryOp::Subtract => "-",
            BinaryOp::Multiply => "*",
            BinaryOp::Divide => "/",
        }
    }

    fn format_nary_op(&self, op: NaryOp) -> &'static str {
        match op {
            NaryOp::And => "AND",
            NaryOp::Or => "OR",
        }
    }

    fn format_join_title(&self, join_type: &JoinType) -> String {
        format!("{} {join_type}Join", self.format_join_op(join_type))
    }

    fn format_join_op(&self, join_type: &JoinType) -> &'static str {
        match join_type {
            JoinType::Inner => "⋈",
            JoinType::LeftSemi => "⋉",
            JoinType::LeftAnti => "▷",
            JoinType::LeftOuter => "⟕",
            JoinType::RightOuter => "⟖",
            JoinType::FullOuter => "⟗",
            JoinType::Single => "⟕₁",
            JoinType::LeftMark { .. } => "⟕ᵐ",
        }
    }

    fn dfs_post_order(&self, root: Operator) -> Vec<Operator> {
        let mut visited = vec![false; self.ctx.operator_count()];
        let mut order = Vec::new();
        self.collect_dfs_post_order(root, &mut visited, &mut order);
        order
    }

    fn collect_dfs_post_order(
        &self,
        operator: Operator,
        visited: &mut [bool],
        order: &mut Vec<Operator>,
    ) {
        if visited[operator.0] {
            return;
        }

        visited[operator.0] = true;

        for input in self.operator_inputs(operator) {
            self.collect_dfs_post_order(input, visited, order);
        }

        order.push(operator);
    }

    fn operator_inputs(&self, operator: Operator) -> Vec<Operator> {
        let mut inputs = self.ctx.operator(operator).inputs();
        inputs.extend(
            self.operator_subquery_inputs(operator)
                .into_iter()
                .map(|input| input.target),
        );
        inputs
    }

    fn format_operator_display(&self, operator: Operator) -> OperatorDisplay {
        self.ctx.operator(operator).format(self)
    }

    fn format_operator_record(&self, operator: Operator) -> DisplayNodeRecord {
        let operator_display = self.format_operator_display(operator);
        let mut record = DisplayNodeRecord {
            id: operator.0,
            kind: operator_display.kind,
            title: operator_display.title,
            fields: DisplayProperties::new(),
            inputs: DisplayProperties::new(),
            metadata: DisplayProperties::new(),
        };

        for (order, field) in operator_display.fields.into_iter().enumerate() {
            record.fields.insert(order, field.key, field.value);
        }

        for (order, input) in operator_display.inputs.into_iter().enumerate() {
            insert_input(&mut record, order, input.name, input.target);
        }

        let offset = record.inputs.len();
        for (order, input) in self
            .operator_subquery_inputs(operator)
            .into_iter()
            .enumerate()
        {
            insert_input(&mut record, offset + order, input.name, input.target);
        }

        for (order, (key, value)) in self
            .format_analysis_properties(operator)
            .into_iter()
            .enumerate()
        {
            record.metadata.insert(order, key, value);
        }

        record
    }

    fn operator_subquery_inputs(&self, operator: Operator) -> Vec<OperatorDisplayInput> {
        let mut inputs = Vec::new();
        match self.ctx.operator(operator) {
            OperatorData::Scan(_) => {}
            OperatorData::Selection(data) => {
                self.collect_expr_subquery_inputs(data.predicate, &mut inputs);
            }
            OperatorData::Map(data) => {
                for (_, expr) in &data.computations {
                    self.collect_expr_subquery_inputs(*expr, &mut inputs);
                }
            }
            OperatorData::TableFunction(data) => {
                for expr in &data.args {
                    self.collect_expr_subquery_inputs(*expr, &mut inputs);
                }
            }
            OperatorData::Join(data) => {
                self.collect_expr_subquery_inputs(data.on, &mut inputs);
            }
            OperatorData::CrossProduct(_) => {}
            OperatorData::Sort(data) => {
                for key in &data.keys {
                    self.collect_expr_subquery_inputs(key.expr, &mut inputs);
                }
            }
            OperatorData::Limit(_) => {}
            OperatorData::Aggregation(data) => {
                for key in &data.keys {
                    self.collect_expr_subquery_inputs(*key, &mut inputs);
                }
                for (_, aggregate) in &data.aggregates {
                    self.collect_aggregate_subquery_inputs(aggregate, &mut inputs);
                }
            }
            OperatorData::Projection(_)
            | OperatorData::Output(_)
            | OperatorData::Rename(_)
            | OperatorData::ConstScan(_) => {}
        }
        inputs
    }

    fn collect_expr_subquery_inputs(&self, expr: Expr, inputs: &mut Vec<OperatorDisplayInput>) {
        match self.ctx.expr(expr) {
            ExprData::Literal(_) | ExprData::ColumnRef(_) => {}
            ExprData::Unary { expr, .. } | ExprData::Cast { expr, .. } => {
                self.collect_expr_subquery_inputs(*expr, inputs);
            }
            ExprData::Binary { left, right, .. } => {
                self.collect_expr_subquery_inputs(*left, inputs);
                self.collect_expr_subquery_inputs(*right, inputs);
            }
            ExprData::Nary { exprs, .. } | ExprData::ScalarFunction { args: exprs, .. } => {
                for expr in exprs {
                    self.collect_expr_subquery_inputs(*expr, inputs);
                }
            }
            ExprData::CaseWhen {
                when_then,
                else_expr,
            } => {
                for (when, then) in when_then {
                    self.collect_expr_subquery_inputs(*when, inputs);
                    self.collect_expr_subquery_inputs(*then, inputs);
                }
                if let Some(else_expr) = else_expr {
                    self.collect_expr_subquery_inputs(*else_expr, inputs);
                }
            }
            ExprData::Exists { subquery, negated } => {
                let name = if *negated {
                    "not exists subquery"
                } else {
                    "exists subquery"
                };
                inputs.push(OperatorDisplayInput {
                    name: name.to_string(),
                    target: *subquery,
                });
            }
            ExprData::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.collect_expr_subquery_inputs(*expr, inputs);
                let name = if *negated {
                    "not in subquery"
                } else {
                    "in subquery"
                };
                inputs.push(OperatorDisplayInput {
                    name: name.to_string(),
                    target: *subquery,
                });
            }
            ExprData::ScalarSubquery { subquery } => {
                inputs.push(OperatorDisplayInput {
                    name: "scalar subquery".to_string(),
                    target: *subquery,
                });
            }
            ExprData::Like { expr, pattern, .. } => {
                self.collect_expr_subquery_inputs(*expr, inputs);
                self.collect_expr_subquery_inputs(*pattern, inputs);
            }
        }
    }

    fn collect_aggregate_subquery_inputs(
        &self,
        aggregate: &AggregateExpr,
        inputs: &mut Vec<OperatorDisplayInput>,
    ) {
        match aggregate {
            AggregateExpr::CountStar => {}
            AggregateExpr::Func { arg, .. } => self.collect_expr_subquery_inputs(*arg, inputs),
        }
    }

    fn format_analysis_properties(&self, operator: Operator) -> Vec<(String, DisplayValue)> {
        self.config
            .analysis_properties
            .iter()
            .map(|property| (property.key.to_string(), (property.format)(self, operator)))
            .collect()
    }
}

fn format_displayable_analysis<A>(
    formatter: &QueryFormatter<'_>,
    operator: Operator,
) -> DisplayValue
where
    A: DisplayableOperatorAnalysis,
{
    match formatter
        .analyses
        .borrow_mut()
        .get::<A>(formatter.ctx, operator)
    {
        Ok(output) => A::format_output(formatter, output),
        Err(error) => DisplayValue::Atom(format!("error: {error}")),
    }
}

struct OperatorDisplay {
    kind: String,
    title: String,
    fields: Vec<DisplayField>,
    inputs: Vec<OperatorDisplayInput>,
}

struct OperatorDisplayInput {
    name: String,
    target: Operator,
}

trait OperatorDisplayFormat {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay;
}

pub trait Relation {
    fn inputs(&self) -> Vec<Operator> {
        Vec::new()
    }
}

impl Relation for OperatorData {
    fn inputs(&self) -> Vec<Operator> {
        match self {
            Self::Scan(operator) => operator.inputs(),
            Self::Selection(operator) => operator.inputs(),
            Self::Map(operator) => operator.inputs(),
            Self::TableFunction(operator) => operator.inputs(),
            Self::Join(operator) => operator.inputs(),
            Self::CrossProduct(operator) => operator.inputs(),
            Self::Sort(operator) => operator.inputs(),
            Self::Limit(operator) => operator.inputs(),
            Self::Aggregation(operator) => operator.inputs(),
            Self::Projection(operator) => operator.inputs(),
            Self::Output(operator) => operator.inputs(),
            Self::Rename(operator) => operator.inputs(),
            Self::ConstScan(operator) => operator.inputs(),
        }
    }
}

impl OperatorDisplayFormat for OperatorData {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        match self {
            Self::Scan(operator) => operator.format(formatter),
            Self::Selection(operator) => operator.format(formatter),
            Self::Map(operator) => operator.format(formatter),
            Self::TableFunction(operator) => operator.format(formatter),
            Self::Join(operator) => operator.format(formatter),
            Self::CrossProduct(operator) => operator.format(formatter),
            Self::Sort(operator) => operator.format(formatter),
            Self::Limit(operator) => operator.format(formatter),
            Self::Aggregation(operator) => operator.format(formatter),
            Self::Projection(operator) => operator.format(formatter),
            Self::Output(operator) => operator.format(formatter),
            Self::Rename(operator) => operator.format(formatter),
            Self::ConstScan(operator) => operator.format(formatter),
        }
    }
}

impl Relation for Scan {}

impl OperatorDisplayFormat for Scan {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "scan".to_string(),
            title: "⊞ Scan".to_string(),
            fields: vec![
                display_scalar_field("table_name", self.table.to_string()),
                display_list_field(
                    "columns",
                    self.columns
                        .iter()
                        .map(|column| formatter.format_column_name(*column)),
                ),
            ],
            inputs: Vec::new(),
        }
    }
}

impl Relation for ConstScan {}

impl OperatorDisplayFormat for ConstScan {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "const_scan".to_string(),
            title: "⊞ ConstScan".to_string(),
            fields: vec![
                display_list_field(
                    "columns",
                    self.columns
                        .iter()
                        .map(|column| formatter.format_column_name(*column)),
                ),
                display_scalar_field("rows", self.rows.len().to_string()),
            ],
            inputs: Vec::new(),
        }
    }
}

impl Relation for Selection {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Selection {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "selection".to_string(),
            title: "σ Selection".to_string(),
            fields: vec![display_scalar_field(
                "predicate",
                formatter.format_expr(self.predicate),
            )],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for Map {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Map {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "map".to_string(),
            title: "χ Map".to_string(),
            fields: vec![display_list_field(
                "computations",
                self.computations.iter().map(|(column, expr)| {
                    formatter.format_definition(*column, formatter.format_expr(*expr))
                }),
            )],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for TableFunction {}

impl OperatorDisplayFormat for TableFunction {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "table_function".to_string(),
            title: "⊞ TableFunction".to_string(),
            fields: vec![
                display_scalar_field("function", self.function.to_string()),
                display_list_field(
                    "args",
                    self.args.iter().map(|arg| formatter.format_expr(*arg)),
                ),
                display_list_field(
                    "columns",
                    self.columns
                        .iter()
                        .map(|column| formatter.format_column_name(*column)),
                ),
            ],
            inputs: Vec::new(),
        }
    }
}

impl Relation for Join {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.outer, self.inner]
    }
}

impl OperatorDisplayFormat for Join {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        let mut fields = vec![
            display_scalar_field("join_type", self.join_type.to_string()),
            display_scalar_field("condition", formatter.format_expr(self.on)),
        ];
        if let JoinType::LeftMark {
            marker: col,
            nullable,
        } = self.join_type
        {
            fields.push(display_scalar_field(
                "marker",
                formatter.format_column_name(col),
            ));
            fields.push(display_scalar_field(
                "marker_nullable",
                nullable.to_string(),
            ));
        }
        OperatorDisplay {
            kind: "join".to_string(),
            title: formatter.format_join_title(&self.join_type),
            fields,
            inputs: vec![
                OperatorDisplayInput {
                    name: "outer".to_string(),
                    target: self.outer,
                },
                OperatorDisplayInput {
                    name: "inner".to_string(),
                    target: self.inner,
                },
            ],
        }
    }
}

impl Relation for Sort {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Sort {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "sort".to_string(),
            title: "⇞ Sort".to_string(),
            fields: vec![display_list_field(
                "keys",
                self.keys.iter().map(|key| formatter.format_sort_key(key)),
            )],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for Limit {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Limit {
    fn format(&self, _formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        let fetch = self
            .fetch
            .map(|fetch| fetch.to_string())
            .unwrap_or_else(|| "all".to_string());
        OperatorDisplay {
            kind: "limit".to_string(),
            title: "τ Limit".to_string(),
            fields: vec![
                display_scalar_field("fetch", fetch),
                display_scalar_field("offset", self.offset.to_string()),
            ],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for CrossProduct {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.outer, self.inner]
    }
}

impl OperatorDisplayFormat for CrossProduct {
    fn format(&self, _formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "cross_product".to_string(),
            title: "× CrossProduct".to_string(),
            fields: Vec::new(),
            inputs: vec![
                OperatorDisplayInput {
                    name: "outer".to_string(),
                    target: self.outer,
                },
                OperatorDisplayInput {
                    name: "inner".to_string(),
                    target: self.inner,
                },
            ],
        }
    }
}

impl Relation for Aggregation {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Aggregation {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "aggregation".to_string(),
            title: "γ Aggregation".to_string(),
            fields: vec![
                display_scalar_field("keys", formatter.format_exprs(&self.keys)),
                display_list_field(
                    "aggregates",
                    self.aggregates.iter().map(|(column, aggregate)| {
                        formatter
                            .format_definition(*column, formatter.format_aggregate_expr(aggregate))
                    }),
                ),
            ],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for Projection {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Projection {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "projection".to_string(),
            title: "π Projection".to_string(),
            fields: vec![display_list_field(
                "columns",
                self.columns
                    .iter()
                    .map(|column| formatter.format_column_name(*column)),
            )],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for Output {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Output {
    fn format(&self, _formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "output".to_string(),
            title: "= Output".to_string(),
            fields: Vec::new(),
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

impl Relation for Rename {
    fn inputs(&self) -> Vec<Operator> {
        vec![self.input]
    }
}

impl OperatorDisplayFormat for Rename {
    fn format(&self, formatter: &QueryFormatter<'_>) -> OperatorDisplay {
        OperatorDisplay {
            kind: "rename".to_string(),
            title: format!("ρ Rename({})", self.alias),
            fields: vec![display_list_field(
                "defs",
                self.defs.iter().map(|(renamed, original)| {
                    format!(
                        "{} <- {}",
                        formatter.format_column_name(*renamed),
                        formatter.format_column_name(*original)
                    )
                }),
            )],
            inputs: vec![OperatorDisplayInput {
                name: "input".to_string(),
                target: self.input,
            }],
        }
    }
}

fn display_scalar_field(key: impl Into<String>, value: impl Into<String>) -> DisplayField {
    DisplayField {
        key: key.into(),
        value: DisplayValue::Atom(value.into()),
    }
}

fn display_list_field<I, S>(key: impl Into<String>, values: I) -> DisplayField
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    DisplayField {
        key: key.into(),
        value: DisplayValue::List(values.into_iter().map(Into::into).collect()),
    }
}

fn insert_input(
    record: &mut DisplayNodeRecord,
    order: usize,
    name: impl Into<String>,
    target: Operator,
) {
    record.inputs.insert(order, name, target.0);
}

impl std::fmt::Display for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pretty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn arena_payload_add_helpers_append_to_context() {
        let mut ctx = QueryContext::new();

        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let id_ref = ExprData::ColumnRef(id).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);

        assert_eq!(ctx.column(id).name, "id");
        assert_eq!(ctx.expr(id_ref), &ExprData::ColumnRef(id));
        let OperatorData::Scan(scan_data) = ctx.operator(scan) else {
            panic!("expected scan operator");
        };
        assert_eq!(scan_data.columns, vec![id]);
    }

    #[test]
    fn pretty_prints_query_plan() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let projection = OperatorData::Projection(Projection {
            columns: vec![id],
            input: selection,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
        ctx.set_root(output);

        assert_eq!(
            ctx.pretty(),
            "\
┌──────────────┐
│ = Output  @3 │
├──────────────┤
└──────────────┘
│ input
┌─────────────────┐
│ π Projection @2 │
├─────────────────┤
│ columns:        │
│   id(#0)        │
└─────────────────┘
│ input
┌────────────────────────────┐
│ σ Selection             @1 │
├────────────────────────────┤
│ predicate: (age(#1) >= 18) │
└────────────────────────────┘
│ input
┌───────────────────┐
│ ⊞ Scan         @0 │
├───────────────────┤
│ table_name: users │
│ columns:          │
│   id(#0)          │
│   age(#1)         │
└───────────────────┘
"
        );
    }

    #[test]
    #[cfg(feature = "serde")]
    fn pretty_json_serializes_recursive_display_tree() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        ctx.set_root(output);

        let json = ctx.pretty_json();
        let tree: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(tree["title"], "= Output");
        assert_eq!(tree["input"]["title"], "⊞ Scan");
        assert_eq!(tree["input"]["table_name"], "users");
        assert_eq!(tree["input"]["columns"][0], "id(#0)");
        assert!(tree.get("inputs").is_none());
        assert!(tree["input"].get("fields").is_none());

        let node = DisplayNode::new("Root").with_metadata("rows", "10");
        let node_json = serde_json::to_value(&node).unwrap();
        assert_eq!(node_json["rows"], "10");
        assert!(node_json.get("metadata").is_none());
    }

    #[test]
    #[cfg(feature = "serde")]
    fn optimizer_visualizer_json_uses_titles_for_nodes() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: selection }).add(&mut ctx);
        ctx.set_root(output);

        let json = ctx.optimizer_visualizer_json("0. Initial");
        let passes: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(passes[0]["passName"], "0. Initial");
        assert_eq!(passes[0]["root"]["op"], "output");
        assert_eq!(passes[0]["root"]["title"], "= Output");
        assert_eq!(passes[0]["root"]["estimated_rows"], "330");
        assert!(passes[0]["root"].get("cost").is_none());
        assert!(passes[0]["root"].get("rows").is_none());
        assert_eq!(passes[0]["root"]["children"][0]["op"], "selection");
        assert_eq!(passes[0]["root"]["children"][0]["title"], "σ Selection");
        assert!(passes[0].get("durationMs").is_none());
        assert_eq!(
            passes[0]["root"]["children"][0]["predicate"],
            "(age(#1) >= 18)"
        );
        assert_eq!(
            passes[0]["root"]["children"][0]["children"][0]["table"],
            "users"
        );
        assert_eq!(
            passes[0]["root"]["children"][0]["children"][0]["title"],
            "⊞ Scan"
        );
        assert_eq!(
            passes[0]["root"]["children"][0]["children"][0]["table_name"],
            "users"
        );
        assert_eq!(
            passes[0]["root"]["children"][0]["children"][0]["columns"][0],
            "id(#0)"
        );
    }

    #[test]
    #[cfg(feature = "serde")]
    fn optimizer_visualizer_trace_json_includes_pass_profile() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);
        ctx.set_root(scan);

        let trace = vec![PassTrace {
            profile: PassProfile {
                iteration: 2,
                pass_index: 3,
                pass: "PredicatePushdown",
                result: Some(PassResult::Changed),
                duration_ms: 1.25,
            },
            query: ctx.clone(),
        }];

        let json = optimizer_visualizer_trace_json(&ctx, &trace);
        let passes: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(passes[0]["passName"], "Initial");
        assert!(passes[0].get("durationMs").is_none());
        assert_eq!(passes[1]["passName"], "PredicatePushdown");
        assert_eq!(passes[1]["iteration"], 2);
        assert_eq!(passes[1]["passIndex"], 3);
        assert_eq!(passes[1]["result"], "changed");
        assert_eq!(passes[1]["durationMs"], 1.25);
    }

    #[test]
    fn add_scan_from_catalog_creates_columns_from_table_schema() {
        let catalog = MemoryCatalog::new("memory", "public");
        catalog
            .create_table(
                TableRef::bare("users"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                None,
            )
            .unwrap();

        let mut ctx = QueryContext::new();
        let scan = ctx
            .add_scan_from_catalog(&catalog, TableRef::bare("users"))
            .unwrap();

        let OperatorData::Scan(scan_data) = scan.get(&ctx) else {
            panic!("catalog scan should create a scan operator");
        };

        assert_eq!(scan_data.table, TableRef::full("memory", "public", "users"));
        assert_eq!(scan_data.columns.len(), 2);
        assert_eq!(ctx.column(scan_data.columns[0]).name, "id");
        assert_eq!(ctx.column(scan_data.columns[0]).ty, DataType::Int64);
        assert_eq!(ctx.column(scan_data.columns[1]).name, "name");
        assert_eq!(ctx.column(scan_data.columns[1]).ty, DataType::Utf8);
    }

    #[test]
    fn pretty_can_include_analysis_properties() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);
        ctx.set_root(scan);

        assert!(!ctx.pretty().contains("analysis::available_columns"));

        let pretty =
            ctx.pretty_with_config(QueryFormatConfig::new().with_analysis::<AvailableColumns>());

        assert!(pretty.contains("│ analysis::available_columns:"));
        assert!(pretty.contains("│   id(#0)"));
    }

    #[test]
    #[cfg(feature = "serde")]
    fn json_formats_can_include_analysis_properties() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        ctx.set_root(output);
        let config = QueryFormatConfig::new().with_analysis::<AvailableColumns>();

        let tree: serde_json::Value =
            serde_json::from_str(&ctx.pretty_json_with_config(config.clone())).unwrap();
        assert_eq!(tree["analysis::available_columns"][0], "id(#0)");
        assert_eq!(tree["input"]["analysis::available_columns"][0], "id(#0)");

        let plan: serde_json::Value =
            serde_json::from_str(&ctx.pretty_flat_with_config(config)).unwrap();
        let nodes = plan["nodes"].as_array().unwrap();
        assert_eq!(nodes[0]["analysis::available_columns"][0], "id(#0)");
        assert_eq!(nodes[1]["analysis::available_columns"][0], "id(#0)");
    }

    #[test]
    #[cfg(feature = "serde")]
    fn optimizer_visualizer_node_passes_through_metadata_rows() {
        let node = DisplayNode::with_kind("const_scan", "⊞ ConstScan")
            .with_field("rows", "7")
            .with_metadata("estimated_rows", "7");

        let visualizer = OptimizerVisualizerNode::from_display_node(&node);

        assert!(matches!(
            visualizer.properties.get("estimated_rows"),
            Some(OptimizerVisualizerValue::String(value)) if value == "7"
        ));
        assert!(matches!(
            visualizer.properties.get("rows"),
            Some(OptimizerVisualizerValue::String(value)) if value == "7"
        ));
    }

    #[test]
    fn set_root_does_not_append_operator() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        })
        .add(&mut ctx);

        ctx.set_root(scan);
        ctx.set_root(scan);

        assert_eq!(ctx.root(), Some(scan));
        assert_eq!(ctx.operator_count(), 1);
        assert_eq!(QueryFormatter::new(&ctx).format().title, "⊞ Scan");
    }

    #[test]
    fn pretty_prints_join_children_side_by_side() {
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
            outer: orders,
            inner: users,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: join }).add(&mut ctx);
        ctx.set_root(output);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ ⋈"));
        assert!(pretty.contains("│ outer"));
        assert!(pretty.contains("│ inner"));
        assert!(pretty.lines().any(|line| line.matches('┌').count() == 2));
    }

    #[test]
    fn pretty_prints_subquery_expression_inputs() {
        let mut ctx = QueryContext::new();
        let user_id = ColumnData::new("user_id", DataType::Int64).add(&mut ctx);
        let order_id = ColumnData::new("order_id", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_id],
        })
        .add(&mut ctx);
        let left = ExprData::ColumnRef(user_id).add(&mut ctx);
        let right = ExprData::ScalarSubquery { subquery: orders }.add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: users,
        })
        .add(&mut ctx);
        ctx.set_root(selection);

        let pretty = ctx.pretty();

        assert!(pretty.contains("SCALAR_SUBQUERY(@1)"));
        assert!(pretty.contains("│ scalar subquery"));
        assert!(
            pretty
                .lines()
                .any(|line| line.contains("⊞ Scan") && line.contains("@1"))
        );
    }

    #[test]
    #[cfg(feature = "serde")]
    fn flat_prints_operators_in_dfs_post_order() {
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
            outer: orders,
            inner: users,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: join }).add(&mut ctx);
        ctx.set_root(output);

        let flat = ctx.pretty_flat();
        let plan: serde_json::Value = serde_json::from_str(&flat).unwrap();
        let nodes = plan["nodes"].as_array().unwrap();

        assert_eq!(nodes[0]["id"], 1);
        assert_eq!(nodes[1]["id"], 0);
        assert_eq!(nodes[2]["id"], 2);
        assert_eq!(nodes[3]["id"], 3);
        assert_eq!(nodes[2]["kind"], "join");
        assert_eq!(nodes[2]["outer"], 1);
        assert_eq!(nodes[2]["inner"], 0);
        assert_eq!(nodes[3]["input"], 2);
    }

    #[test]
    fn pretty_prints_aggregation() {
        let mut ctx = QueryContext::new();
        let region = ColumnData::new("region", DataType::Utf8).add(&mut ctx);
        let amount = ColumnData::new("amount", DataType::Float64).add(&mut ctx);
        let total_amount = ColumnData::new("total_amount", DataType::Float64).add(&mut ctx);
        let order_count = ColumnData::new("order_count", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![region, amount],
        })
        .add(&mut ctx);
        let region_ref = ExprData::ColumnRef(region).add(&mut ctx);
        let amount_ref = ExprData::ColumnRef(amount).add(&mut ctx);
        let aggregation = OperatorData::Aggregation(Aggregation {
            keys: vec![region_ref],
            aggregates: vec![
                (
                    total_amount,
                    AggregateExpr::Func {
                        func: AggregateFunction::Sum,
                        arg: amount_ref,
                        distinct: false,
                    },
                ),
                (order_count, AggregateExpr::CountStar),
            ],
            input: scan,
        })
        .add(&mut ctx);
        ctx.set_root(aggregation);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ γ Aggregation"));
        assert!(pretty.contains("│ keys: region(#0)"));
        assert!(pretty.contains("│ aggregates:"));
        assert!(pretty.contains("total_amount(#2) :="));
        assert!(pretty.contains("sum(amount(#1))"));
        assert!(pretty.contains("order_count(#3) := count_star()"));
    }

    #[test]
    fn pretty_prints_sort_and_limit() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let sort = OperatorData::Sort(Sort {
            keys: vec![SortKey {
                expr: age_ref,
                direction: SortDirection::Desc,
                nulls: NullOrdering::Last,
            }],
            input: scan,
        })
        .add(&mut ctx);
        let limit = OperatorData::Limit(Limit {
            fetch: Some(10),
            offset: 5,
            input: sort,
        })
        .add(&mut ctx);
        ctx.set_root(limit);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ τ Limit"));
        assert!(pretty.contains("│ fetch: 10"));
        assert!(pretty.contains("│ offset: 5"));
        assert!(pretty.contains("│ ⇞ Sort"));
        assert!(pretty.contains("│   age(#1) Desc NullsLast"));

        let mut analyses = ctx.analyze();
        assert_eq!(analyses.get::<UsedColumns>(&ctx, sort).unwrap(), vec![age]);
        assert_eq!(
            analyses.get::<AvailableColumns>(&ctx, limit).unwrap(),
            vec![id, age]
        );
    }

    #[test]
    fn pretty_prints_extension_functions() {
        let mut ctx = QueryContext::new();
        let path = ExprData::Literal(ScalarValue::Utf8("orders.csv".to_string())).add(&mut ctx);
        let order_id = ColumnData::new("order_id", DataType::Int64).add(&mut ctx);
        let region = ColumnData::new("region", DataType::Utf8).add(&mut ctx);
        let normalized_region = ColumnData::new("normalized_region", DataType::Utf8).add(&mut ctx);
        let score = ColumnData::new("score", DataType::Float64).add(&mut ctx);
        let table = OperatorData::TableFunction(TableFunction {
            function: TableFunctionDef::extension("read_orders"),
            args: vec![path],
            columns: vec![order_id, region],
        })
        .add(&mut ctx);
        let region_ref = ExprData::ColumnRef(region).add(&mut ctx);
        let normalize_region = ExprData::ScalarFunction {
            function: ScalarFunction::extension("normalize_region"),
            args: vec![region_ref],
        }
        .add(&mut ctx);
        let map = OperatorData::Map(Map {
            computations: vec![(normalized_region, normalize_region)],
            input: table,
        })
        .add(&mut ctx);
        let key = ExprData::ColumnRef(normalized_region).add(&mut ctx);
        let order_id_ref = ExprData::ColumnRef(order_id).add(&mut ctx);
        let aggregation = OperatorData::Aggregation(Aggregation {
            keys: vec![key],
            aggregates: vec![(
                score,
                AggregateExpr::Func {
                    func: AggregateFunction::extension("approx_score"),
                    arg: order_id_ref,
                    distinct: true,
                },
            )],
            input: map,
        })
        .add(&mut ctx);
        ctx.set_root(aggregation);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ γ Aggregation"));
        assert!(pretty.contains("│ aggregates:"));
        assert!(pretty.contains("score(#3) :="));
        assert!(pretty.contains("approx_score(DISTINCT"));
        assert!(pretty.contains("│ computations:"));
        assert!(pretty.contains("normalize_region(region(#1))"));
        assert!(pretty.contains("│ ⊞ TableFunction"));
        assert!(pretty.contains("│ function: read_orders"));
    }

    #[test]
    fn formatter_wraps_box_details_at_configured_width() {
        let mut ctx = QueryContext::new();
        let first = ColumnData::new("first_really_long_column_name", DataType::Int64).add(&mut ctx);
        let second =
            ColumnData::new("second_really_long_column_name", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("wide_table"),
            columns: vec![first, second],
        })
        .add(&mut ctx);
        ctx.set_root(scan);

        let node = QueryFormatter::new(&ctx).format();
        let formatted = BoxDrawingRenderer::with_config(BoxRendererConfig {
            min_box_width: 8,
            max_box_width: 32,
            child_gap: 2,
            ..BoxRendererConfig::default()
        })
        .render(&node);

        assert!(formatted.contains("│ columns:"));
        assert!(formatted.contains("first_really_long_column"));
        assert!(formatted.contains("second_really_long_column"));
        assert!(!formatted.contains("Int64"));

        for line in formatted.lines() {
            assert!(line.chars().count() <= 32);
        }
    }

    #[test]
    fn renderer_wraps_indented_lists_at_tiny_width() {
        let node = DisplayNode::new("R").with_list_field("l", ["abcdef"]);
        let formatted = BoxDrawingRenderer::with_config(BoxRendererConfig {
            min_box_width: 1,
            max_box_width: 5,
            child_gap: 1,
            ..BoxRendererConfig::default()
        })
        .render(&node);

        for line in formatted.lines() {
            assert!(line.chars().count() <= 5, "{line}");
        }
    }

    #[test]
    fn box_renderer_works_with_generic_display_nodes() {
        let node = DisplayNode::new("Root")
            .with_field("field", "value")
            .with_metadata("metadata", "present")
            .with_input("left", DisplayNode::new("Left").with_field("rows", "10"))
            .with_input("right", DisplayNode::new("Right").with_field("rows", "20"));

        let rendered = BoxDrawingRenderer::default().render(&node);

        assert!(rendered.contains("│ Root"));
        assert!(rendered.contains("│ field: value"));
        assert!(rendered.contains("│ metadata: present"));
        let lines = rendered.lines().collect::<Vec<_>>();
        let field_index = lines
            .iter()
            .position(|line| line.contains("field: value"))
            .expect("field should render");
        let metadata_index = lines
            .iter()
            .position(|line| line.contains("metadata: present"))
            .expect("metadata should render");
        assert_eq!(metadata_index, field_index + 2);
        assert!(
            lines[field_index + 1]
                .chars()
                .all(|ch| matches!(ch, '│' | ' ' | '┄')),
            "{}",
            lines[field_index + 1]
        );
        assert!(rendered.contains("│ left"));
        assert!(rendered.contains("│ right"));
        assert!(rendered.contains("│ Left"));
        assert!(rendered.contains("│ Right"));
    }

    #[test]
    fn box_renderer_can_color_metadata() {
        let node = DisplayNode::new("Root").with_metadata("analysis::free_columns", "id(#0)");
        let rendered = BoxDrawingRenderer::with_config(
            BoxRendererConfig::default().with_color_mode(ColorMode::Always),
        )
        .render(&node);

        assert!(rendered.contains("\u{1b}["));
        assert!(rendered.contains("analysis::free_columns"));
        assert!(rendered.contains("id(#0)"));
    }

    #[test]
    fn operator_data_map_inputs_rewrites_unary_and_binary_inputs() {
        let mut ctx = QueryContext::new();
        let left_col = ColumnData::new("left_id", DataType::Int64).add(&mut ctx);
        let right_col = ColumnData::new("right_id", DataType::Int64).add(&mut ctx);
        let replacement_col = ColumnData::new("replacement_id", DataType::Int64).add(&mut ctx);
        let left = OperatorData::Scan(Scan {
            table: TableRef::bare("left_t"),
            columns: vec![left_col],
        })
        .add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("right_t"),
            columns: vec![right_col],
        })
        .add(&mut ctx);
        let replacement = OperatorData::Scan(Scan {
            table: TableRef::bare("replacement_t"),
            columns: vec![replacement_col],
        })
        .add(&mut ctx);
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);

        let selection = OperatorData::Selection(Selection {
            predicate,
            input: left,
        })
        .map_inputs(|input| if input == left { replacement } else { input });
        assert!(matches!(
            selection,
            OperatorData::Selection(Selection { input, .. }) if input == replacement
        ));

        let join = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: predicate,
            outer: left,
            inner: right,
        })
        .map_inputs(|input| if input == right { replacement } else { input });
        assert!(matches!(
            join,
            OperatorData::Join(Join { outer, inner, .. }) if outer == left && inner == replacement
        ));
    }

    #[test]
    fn operator_data_try_map_inputs_short_circuits_errors_and_leaves_leaf_operators() {
        let mut ctx = QueryContext::new();
        let col = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![col],
        })
        .add(&mut ctx);
        let predicate = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);

        let err = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .try_map_inputs(|_| Err::<Operator, _>("stop"))
        .unwrap_err();
        assert_eq!(err, "stop");

        let mut calls = 0;
        let leaf = OperatorData::Scan(Scan {
            table: TableRef::bare("leaf"),
            columns: vec![col],
        })
        .try_map_inputs::<()>(|input| {
            calls += 1;
            Ok(input)
        })
        .unwrap();
        assert!(matches!(leaf, OperatorData::Scan(_)));
        assert_eq!(calls, 0);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn query_context_round_trips_through_serde() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let age = ColumnData::new("age", DataType::Int32).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        })
        .add(&mut ctx);
        let age_ref = ExprData::ColumnRef(age).add(&mut ctx);
        let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        }
        .add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: selection }).add(&mut ctx);
        ctx.set_root(output);

        let serialized = serde_json::to_string(&ctx).unwrap();
        let deserialized: QueryContext = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.root(), ctx.root());
        assert_eq!(deserialized.operator_count(), ctx.operator_count());
        assert_eq!(deserialized.expr_count(), ctx.expr_count());
        assert_eq!(deserialized.column_count(), ctx.column_count());
        assert_eq!(deserialized.pretty(), ctx.pretty());
    }
}

use arrow_schema::DataType;

/// An opaque reference to a relational operator in a [`QueryContext`].
///
/// The corresponding payload is [`OperatorData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Operator(usize);

/// A relational operator.
#[derive(Debug, Clone)]
pub enum OperatorData {
    /// Reads a table and exposes the listed columns.
    Scan {
        table: String,
        columns: Vec<Column>,
    },
    /// Filters the input using a boolean predicate expression.
    Selection {
        predicate: Expr,
        input: Operator,
    },
    /// Applies a function to each row of the input, producing new columns.
    Map {
        computations: Vec<(Column, Expr)>,
        input: Operator,
    },
    /// Reads rows produced by a table-valued function.
    TableFunction {
        function: TableFunction,
        args: Vec<Expr>,
        columns: Vec<Column>,
    },
    Join {
        join_type: JoinType,
        on: Expr,
        outer: Operator,
        inner: Operator,
    },
    CrossProduct {
        outer: Operator,
        inner: Operator,
    },
    /// Groups rows by key expressions and computes one or more aggregate columns.
    Aggregation {
        keys: Vec<Expr>,
        aggregates: Vec<(Column, AggregateExpr)>,
        input: Operator,
    },
    /// Keeps or reorders the listed columns from the input.
    Projection {
        columns: Vec<Column>,
        input: Operator,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Single,
    Mark(Column),
}

impl std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => f.write_str("Inner"),
            JoinType::LeftOuter => f.write_str("LeftOuter"),
            JoinType::RightOuter => f.write_str("RightOuter"),
            JoinType::FullOuter => f.write_str("FullOuter"),
            JoinType::Single => f.write_str("Single"),
            JoinType::Mark(_) => f.write_str("Mark"),
        }
    }
}

/// An opaque reference to a column in a [`QueryContext`].
///
/// The corresponding payload is [`ColumnData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Column(usize);

/// Metadata for the column.
#[derive(Debug, Clone)]
pub struct ColumnData {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}

impl ColumnData {
    /// Creates column metadata.
    pub fn new(name: impl Into<String>, ty: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }
}

/// An opaque reference to a scalar expression in a [`QueryContext`].
///
/// The corresponding payload is [`ExprData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Expr(usize);

/// A scalar expression referenced by an [`Expr`] handle.
#[derive(Debug, Clone)]
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
    /// Scalar function call over zero or more argument expressions.
    ScalarFunction {
        function: ScalarFunction,
        args: Vec<Expr>,
    },
}

/// Unary expression operator.
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    /// Equality comparison.
    Eq,
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
#[derive(Debug, Clone, Copy)]
pub enum NaryOp {
    /// Logical conjunction.
    And,
    /// Logical disjunction.
    Or,
}

/// Group-level expression used by [`OperatorData::Aggregation`].
///
/// Aggregate expressions consume the input rows for one group and produce one scalar value.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableFunction {
    Values,
    ReadCsv,
    ReadParquet,
    Extension(String),
}

impl TableFunction {
    /// Creates a table function identifier for a catalog or extension function.
    pub fn extension(name: impl Into<String>) -> Self {
        Self::Extension(name.into())
    }
}

impl std::fmt::Display for TableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFunction::Values => f.write_str("values"),
            TableFunction::ReadCsv => f.write_str("read_csv"),
            TableFunction::ReadParquet => f.write_str("read_parquet"),
            TableFunction::Extension(name) => f.write_str(name),
        }
    }
}

/// Scalar value used by literal expressions.
#[derive(Debug, Clone)]
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
    /// UTF-8 string scalar.
    Utf8(String),
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
            ScalarValue::Utf8(_) => DataType::Utf8,
        }
    }
}

/// Owns the query IR graph and all node arenas.
#[derive(Clone)]
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

    /// Updates the root operator of the query graph.
    pub fn set_root(&mut self, root: Operator) {
        self.root = Some(root);
    }

    /// Returns the root operator of the query graph, if one has been set.
    pub fn root(&self) -> Option<Operator> {
        self.root
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
        &self.operators[operator.0]
    }

    /// Returns mutable operator data for a handle.
    pub fn operator_mut(&mut self, operator: Operator) -> &mut OperatorData {
        &mut self.operators[operator.0]
    }

    /// Returns immutable expression data for a handle.
    pub fn expr(&self, expr: Expr) -> &ExprData {
        &self.exprs[expr.0]
    }

    /// Returns mutable expression data for a handle.
    pub fn expr_mut(&mut self, expr: Expr) -> &mut ExprData {
        &mut self.exprs[expr.0]
    }

    /// Returns metadata for a column handle.
    pub fn column(&self, column: Column) -> &ColumnData {
        &self.columns[column.0]
    }

    /// Formats the reachable query plan using the default query formatter.
    pub fn pretty(&self) -> String {
        let node = QueryFormatter::new(self).format();
        BoxDrawingRenderer::default().render(&node)
    }
}

/// Generic display tree node that can be rendered independently of query plans.
#[derive(Debug, Clone)]
pub struct DisplayNode {
    pub title: String,
    pub fields: Vec<DisplayField>,
    pub inputs: Vec<DisplayInput>,
    pub metadata: Vec<DisplayField>,
}

impl DisplayNode {
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            fields: Vec::new(),
            inputs: Vec::new(),
            metadata: Vec::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.push(DisplayField {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn with_input(mut self, name: impl Into<String>, node: DisplayNode) -> Self {
        self.inputs.push(DisplayInput {
            name: name.into(),
            node: Box::new(node),
        });
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.push(DisplayField {
            key: key.into(),
            value: value.into(),
        });
        self
    }
}

/// Ordered key-value field for a [`DisplayNode`].
#[derive(Debug, Clone)]
pub struct DisplayField {
    pub key: String,
    pub value: String,
}

/// Named child input for a [`DisplayNode`].
#[derive(Debug, Clone)]
pub struct DisplayInput {
    pub name: String,
    pub node: Box<DisplayNode>,
}

/// Display settings for [`BoxDrawingRenderer`].
#[derive(Debug, Clone)]
pub struct QueryFormatConfig {
    pub min_box_width: usize,
    pub max_box_width: usize,
    pub child_gap: usize,
}

impl Default for QueryFormatConfig {
    fn default() -> Self {
        Self {
            min_box_width: 12,
            max_box_width: 40,
            child_gap: 4,
        }
    }
}

/// Formats a [`QueryContext`] into a generic [`DisplayNode`] tree.
pub struct QueryFormatter<'a> {
    ctx: &'a QueryContext,
}

impl<'a> QueryFormatter<'a> {
    /// Creates a query formatter.
    pub fn new(ctx: &'a QueryContext) -> Self {
        Self { ctx }
    }

    /// Formats the reachable query plan from the root operator into a display tree.
    pub fn format(&self) -> DisplayNode {
        if let Some(root) = self.ctx.root {
            self.format_operator(root)
        } else {
            DisplayNode::new("EMPTY QUERY")
        }
    }

    fn format_operator(&self, operator: Operator) -> DisplayNode {
        match self.ctx.operator(operator) {
            OperatorData::Scan { table, columns } => DisplayNode::new(format!("⊞ {table}"))
                .with_field("columns", self.format_columns(columns)),
            OperatorData::Selection { predicate, input } => DisplayNode::new("σ Filter")
                .with_field("predicate", self.format_expr(*predicate))
                .with_input("input", self.format_operator(*input)),
            OperatorData::Map {
                computations,
                input,
            } => {
                let mut node = DisplayNode::new("χ Map");

                for (column, expr) in computations {
                    node = node.with_field(self.format_column(*column), self.format_expr(*expr));
                }

                node.with_input("input", self.format_operator(*input))
            }
            OperatorData::TableFunction {
                function,
                args,
                columns,
            } => DisplayNode::new(format!("⊞ {function}"))
                .with_field("args", self.format_exprs(args))
                .with_field("columns", self.format_columns(columns)),
            OperatorData::Join {
                join_type,
                on,
                outer,
                inner,
            } => DisplayNode::new(self.format_join_title(join_type))
                .with_field("join_type", join_type.to_string())
                .with_field("condition", self.format_expr(*on))
                .with_input("outer", self.format_operator(*outer))
                .with_input("inner", self.format_operator(*inner)),
            OperatorData::CrossProduct { outer, inner } => DisplayNode::new("× CrossProduct")
                .with_input("outer", self.format_operator(*outer))
                .with_input("inner", self.format_operator(*inner)),
            OperatorData::Aggregation {
                keys,
                aggregates,
                input,
            } => {
                let mut node =
                    DisplayNode::new("γ Aggregation").with_field("keys", self.format_exprs(keys));

                for (column, aggregate) in aggregates {
                    node = node.with_field(
                        self.ctx.column(*column).name.clone(),
                        self.format_aggregate_expr(aggregate),
                    );
                }

                node.with_input("input", self.format_operator(*input))
            }
            OperatorData::Projection { columns, input } => DisplayNode::new("π Projection")
                .with_field("columns", self.format_columns(columns))
                .with_input("input", self.format_operator(*input)),
        }
    }

    fn format_columns(&self, columns: &[Column]) -> String {
        columns
            .iter()
            .map(|column| self.format_column(*column))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn format_column(&self, column: Column) -> String {
        let column = self.ctx.column(column);
        let nullability = if column.nullable {
            " nullable"
        } else {
            " not null"
        };

        format!("{}: {:?}{}", column.name, column.ty, nullability)
    }

    fn format_exprs(&self, exprs: &[Expr]) -> String {
        exprs
            .iter()
            .map(|expr| self.format_expr(*expr))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn format_expr(&self, expr: Expr) -> String {
        match self.ctx.expr(expr) {
            ExprData::Literal(value) => self.format_scalar(value),
            ExprData::ColumnRef(column) => self.ctx.column(*column).name.clone(),
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
            ScalarValue::Utf8(value) => format!("{value:?}"),
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
        format!("{} {join_type}", self.format_join_op(join_type))
    }

    fn format_join_op(&self, join_type: &JoinType) -> &'static str {
        match join_type {
            JoinType::Inner => "⋈",
            JoinType::LeftOuter => "⟕",
            JoinType::RightOuter => "⟖",
            JoinType::FullOuter => "⟗",
            JoinType::Single => "⋈₁",
            JoinType::Mark(_) => "⋈ᵐ",
        }
    }
}

/// Renders a generic [`DisplayNode`] tree using box drawing characters.
pub struct BoxDrawingRenderer {
    config: QueryFormatConfig,
}

impl Default for BoxDrawingRenderer {
    fn default() -> Self {
        Self {
            config: QueryFormatConfig::default(),
        }
    }
}

impl BoxDrawingRenderer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: QueryFormatConfig) -> Self {
        Self { config }
    }

    pub fn render(&self, node: &DisplayNode) -> String {
        self.render_node(node).to_string()
    }

    fn render_node(&self, node: &DisplayNode) -> RenderedBlock {
        let parent = self.render_box(&node.title, &self.render_details(node));

        match node.inputs.as_slice() {
            [] => parent,
            [input] => self.render_unary_node(parent, input),
            [outer, inner] => self.render_binary_node(parent, outer, inner),
            inputs => self.render_nary_node(parent, inputs),
        }
    }

    fn render_details(&self, node: &DisplayNode) -> Vec<String> {
        node.fields
            .iter()
            .chain(node.metadata.iter())
            .map(|field| format!("{}: {}", field.key, field.value))
            .collect()
    }

    fn render_unary_node(&self, parent: RenderedBlock, input: &DisplayInput) -> RenderedBlock {
        let child = self.render_node(&input.node);
        let width = parent.width.max(child.width);
        let mut lines = parent.lines;

        lines.push(format!("│ {}", input.name));
        lines.extend(child.lines);

        RenderedBlock { lines, width }
    }

    fn render_binary_node(
        &self,
        parent: RenderedBlock,
        outer: &DisplayInput,
        inner: &DisplayInput,
    ) -> RenderedBlock {
        let outer_block = self.render_node(&outer.node);
        let inner_block = self.render_node(&inner.node);
        let gap = self.config.child_gap;
        let left_width = parent.width.max(outer_block.width);
        let inner_start = left_width + gap;
        let branch_width = inner_start - parent.width;
        let width = left_width + gap + inner_block.width;
        let mut lines = parent.lines;

        if let Some(line) = lines.get_mut(1) {
            line.push_str(&"─".repeat(branch_width));
            line.push('┐');
        }
        for line in lines.iter_mut().skip(2) {
            line.push_str(&" ".repeat(branch_width));
            line.push('│');
        }

        lines.push(format!(
            "{:<left_width$}{}{}",
            format!("│ {}", outer.name),
            " ".repeat(gap),
            format!("│ {}", inner.name),
            left_width = left_width
        ));

        let height = outer_block.lines.len().max(inner_block.lines.len());
        for index in 0..height {
            let outer_line = outer_block
                .lines
                .get(index)
                .map(String::as_str)
                .unwrap_or("");
            let inner_line = inner_block
                .lines
                .get(index)
                .map(String::as_str)
                .unwrap_or("");

            lines.push(format!(
                "{outer_line:<left_width$}{}{inner_line}",
                " ".repeat(gap),
                left_width = left_width
            ));
        }

        RenderedBlock { lines, width }
    }

    fn render_nary_node(&self, parent: RenderedBlock, inputs: &[DisplayInput]) -> RenderedBlock {
        let mut width = parent.width;
        let mut lines = parent.lines;

        for input in inputs {
            let child = self.render_node(&input.node);
            width = width.max(child.width);
            lines.push(format!("│ {}", input.name));
            lines.extend(child.lines);
        }

        RenderedBlock { lines, width }
    }

    fn render_box(&self, title: &str, details: &[String]) -> RenderedBlock {
        let max_box_width = self.config.max_box_width.max(4);
        let max_content_width = max_box_width.saturating_sub(4).max(1);
        let wrapped_details = self.wrap_details(details, max_content_width);
        let content_width = wrapped_details
            .iter()
            .map(|detail| detail.chars().count())
            .chain(std::iter::once(title.chars().count()))
            .max()
            .unwrap_or(0)
            .max(self.config.min_box_width)
            .min(max_content_width);

        let mut lines = Vec::with_capacity(wrapped_details.len() + 4);
        lines.push(format!("┌{}┐", "─".repeat(content_width + 2)));
        lines.push(format!("│ {title:content_width$} │"));
        lines.push(format!("├{}┤", "─".repeat(content_width + 2)));

        for detail in wrapped_details {
            lines.push(format!("│ {detail:content_width$} │"));
        }

        lines.push(format!("└{}┘", "─".repeat(content_width + 2)));

        RenderedBlock {
            width: content_width + 4,
            lines,
        }
    }

    fn wrap_details(&self, details: &[String], max_width: usize) -> Vec<String> {
        details
            .iter()
            .flat_map(|detail| self.wrap_detail(detail, max_width))
            .collect()
    }

    fn wrap_detail(&self, detail: &str, max_width: usize) -> Vec<String> {
        if detail.chars().count() <= max_width {
            return vec![detail.to_string()];
        }

        let mut lines = Vec::new();
        let mut current = String::new();

        for word in detail.split_whitespace() {
            let separator_width = usize::from(!current.is_empty());
            let next_width = current.chars().count() + separator_width + word.chars().count();

            if !current.is_empty() && next_width > max_width {
                lines.push(current);
                current = String::new();
            }

            if word.chars().count() > max_width {
                if !current.is_empty() {
                    lines.push(current);
                    current = String::new();
                }

                lines.extend(self.wrap_long_word(word, max_width));
                continue;
            }

            if !current.is_empty() {
                current.push(' ');
            }

            current.push_str(word);
        }

        if !current.is_empty() {
            lines.push(current);
        }

        lines
    }

    fn wrap_long_word(&self, word: &str, max_width: usize) -> Vec<String> {
        let mut lines = Vec::new();
        let mut current = String::new();

        for ch in word.chars() {
            if current.chars().count() == max_width {
                lines.push(current);
                current = String::new();
            }

            current.push(ch);
        }

        if !current.is_empty() {
            lines.push(current);
        }

        lines
    }
}

struct RenderedBlock {
    lines: Vec<String>,
    width: usize,
}

impl std::fmt::Display for RenderedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for line in &self.lines {
            writeln!(f, "{line}")?;
        }

        Ok(())
    }
}

impl std::fmt::Display for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pretty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pretty_prints_query_plan() {
        let mut ctx = QueryContext::new();
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64, false));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32, true));
        let scan = ctx.add_operator(OperatorData::Scan {
            table: "users".to_string(),
            columns: vec![id, age],
        });
        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let adult_age = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(18)));
        let predicate = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: adult_age,
        });
        let selection = ctx.add_operator(OperatorData::Selection {
            predicate,
            input: scan,
        });
        let projection = ctx.add_operator(OperatorData::Projection {
            columns: vec![id],
            input: selection,
        });
        ctx.set_root(projection);

        assert_eq!(
            ctx.pretty(),
            "\
┌─────────────────────────────┐
│ π Projection                │
├─────────────────────────────┤
│ columns: id: Int64 not null │
└─────────────────────────────┘
│ input
┌────────────────────────┐
│ σ Filter               │
├────────────────────────┤
│ predicate: (age >= 18) │
└────────────────────────┘
│ input
┌───────────────────────────────────┐
│ ⊞ users                           │
├───────────────────────────────────┤
│ columns: id: Int64 not null, age: │
│ Int32 nullable                    │
└───────────────────────────────────┘
"
        );
    }

    #[test]
    fn pretty_prints_join_children_side_by_side() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64, false));
        let order_user_id =
            ctx.add_column(ColumnData::new("order_user_id", DataType::Int64, false));

        let users = ctx.add_operator(OperatorData::Scan {
            table: "users".to_string(),
            columns: vec![user_id],
        });
        let orders = ctx.add_operator(OperatorData::Scan {
            table: "orders".to_string(),
            columns: vec![order_user_id],
        });
        let left = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        });
        let join = ctx.add_operator(OperatorData::Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        });
        ctx.set_root(join);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ ⋈"));
        assert!(pretty.contains("│ outer"));
        assert!(pretty.contains("│ inner"));
        assert!(pretty.lines().any(|line| line.matches('┌').count() == 2));
    }

    #[test]
    fn pretty_prints_aggregation() {
        let mut ctx = QueryContext::new();
        let region = ctx.add_column(ColumnData::new("region", DataType::Utf8, false));
        let amount = ctx.add_column(ColumnData::new("amount", DataType::Float64, false));
        let total_amount = ctx.add_column(ColumnData::new("total_amount", DataType::Float64, true));
        let order_count = ctx.add_column(ColumnData::new("order_count", DataType::Int64, false));
        let scan = ctx.add_operator(OperatorData::Scan {
            table: "orders".to_string(),
            columns: vec![region, amount],
        });
        let region_ref = ctx.add_expr(ExprData::ColumnRef(region));
        let amount_ref = ctx.add_expr(ExprData::ColumnRef(amount));
        let aggregation = ctx.add_operator(OperatorData::Aggregation {
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
        });
        ctx.set_root(aggregation);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ γ Aggregation"));
        assert!(pretty.contains("│ keys: region"));
        assert!(pretty.contains("│ total_amount: sum(amount)"));
        assert!(pretty.contains("│ order_count: count(*)"));
    }

    #[test]
    fn pretty_prints_extension_functions() {
        let mut ctx = QueryContext::new();
        let path = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8(
            "orders.csv".to_string(),
        )));
        let order_id = ctx.add_column(ColumnData::new("order_id", DataType::Int64, false));
        let region = ctx.add_column(ColumnData::new("region", DataType::Utf8, false));
        let normalized_region =
            ctx.add_column(ColumnData::new("normalized_region", DataType::Utf8, true));
        let score = ctx.add_column(ColumnData::new("score", DataType::Float64, true));
        let table = ctx.add_operator(OperatorData::TableFunction {
            function: TableFunction::extension("read_orders"),
            args: vec![path],
            columns: vec![order_id, region],
        });
        let region_ref = ctx.add_expr(ExprData::ColumnRef(region));
        let normalize_region = ctx.add_expr(ExprData::ScalarFunction {
            function: ScalarFunction::extension("normalize_region"),
            args: vec![region_ref],
        });
        let map = ctx.add_operator(OperatorData::Map {
            computations: vec![(normalized_region, normalize_region)],
            input: table,
        });
        let key = ctx.add_expr(ExprData::ColumnRef(normalized_region));
        let order_id_ref = ctx.add_expr(ExprData::ColumnRef(order_id));
        let aggregation = ctx.add_operator(OperatorData::Aggregation {
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
        });
        ctx.set_root(aggregation);

        let pretty = ctx.pretty();

        assert!(pretty.contains("│ γ Aggregation"));
        assert!(pretty.contains("│ score: approx_score(DISTINCT"));
        assert!(pretty.contains("normalize_region(region)"));
        assert!(pretty.contains("│ ⊞ read_orders"));
    }

    #[test]
    fn formatter_wraps_box_details_at_configured_width() {
        let mut ctx = QueryContext::new();
        let first = ctx.add_column(ColumnData::new(
            "first_really_long_column_name",
            DataType::Int64,
            false,
        ));
        let second = ctx.add_column(ColumnData::new(
            "second_really_long_column_name",
            DataType::Int64,
            false,
        ));
        let scan = ctx.add_operator(OperatorData::Scan {
            table: "wide_table".to_string(),
            columns: vec![first, second],
        });
        ctx.set_root(scan);

        let node = QueryFormatter::new(&ctx).format();
        let formatted = BoxDrawingRenderer::with_config(QueryFormatConfig {
            min_box_width: 8,
            max_box_width: 32,
            child_gap: 2,
        })
        .render(&node);

        assert!(formatted.contains("│ columns:"));
        assert!(formatted.contains("first_really_long_column"));
        assert!(formatted.contains("second_really_long_column"));

        for line in formatted.lines() {
            assert!(line.chars().count() <= 32);
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
        assert!(rendered.contains("│ left"));
        assert!(rendered.contains("│ right"));
        assert!(rendered.contains("│ Left"));
        assert!(rendered.contains("│ Right"));
    }
}

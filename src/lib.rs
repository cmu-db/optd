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
    Scan { table: String, columns: Vec<Column> },
    /// Filters the input using a boolean predicate expression.
    Selection { input: Operator, predicate: Expr },
    /// Applies a function to each row of the input, producing new columns.
    Map {
        input: Operator,
        computations: Vec<(Column, Expr)>,
    },
    Join {
        outer: Operator,
        inner: Operator,
        on: Expr,
    },
    /// Keeps or reorders the listed columns from the input.
    Projection {
        input: Operator,
        columns: Vec<Column>,
    },
}

/// An opaque reference to a column in a [`QueryContext`].
///
/// The corresponding payload is [`ColumnData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Column(usize);

/// Metadata for the column.
#[derive(Debug, Clone)]
pub struct ColumnData {
    name: String,
    ty: DataType,
}

impl ColumnData {
    /// Creates column metadata.
    pub fn new(name: impl Into<String>, ty: DataType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }

    /// Returns the display name for this column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the Arrow data type for this column.
    pub fn data_type(&self) -> &DataType {
        &self.ty
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
}

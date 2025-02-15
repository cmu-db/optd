pub mod lir;
pub mod hir;

/// Binary operators with fixed precedence
#[derive(Debug, Clone)]
pub enum BinOp {
    Add,    // +
    Sub,    // -
    Mul,    // *
    Div,    // /
    Concat, // ++
    Eq,     // ==
    Neq,    // !=
    Gt,     // >
    Lt,     // <
    Ge,     // >=
    Le,     // <=
    And,    // &&
    Or,     // ||
    Range,  // ..
}

/// Unary operators
#[derive(Debug, Clone)]
pub enum UnaryOp {
    Neg, // -
    Not, // !
}

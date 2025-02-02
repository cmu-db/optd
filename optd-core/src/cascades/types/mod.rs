//! Defines the type system and expressions for OPTD-DSL.
//! This module contains only the type definitions without any execution logic.

pub mod interpreter;

/// All types supported by the OPTD-DSL.
#[derive(Clone, PartialEq, Debug)]
pub enum OptdType {
    /// Primitive Types
    Int64(i64),
    String(String),
    Bool(bool),

    /// Complex Types
    Enum(Vec<OptdType>),
    Array(Vec<OptdType>),
}

/// Expressions that can be evaluated on OptdTypes.
/// TODO(alexis): In the future, it would be nice to support user defined
/// functions on top of the basic expressions. This would enable the support
/// of custom defined checks on the metadata.
#[derive(Clone)]
pub enum OptdExpr {
    /// Control Flow
    IfThenElse {
        cond: Box<OptdExpr>,
        then: Box<OptdExpr>,
        otherwise: Box<OptdExpr>,
    },
    /// Reference to a bound value
    Ref(String),

    /// Direct value
    Value(OptdType),

    /// Comparisons
    Eq {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Lt {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Gt {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },

    /// Pattern Matching
    Match {
        expr: Box<OptdExpr>,
        cases: Vec<(Box<OptdExpr>, Box<OptdExpr>)>,
    },

    /// Array Operations
    ArrayLen(Box<OptdExpr>),
    ArrayGet {
        array: Box<OptdExpr>,
        index: Box<OptdExpr>,
    },
    ArrayConcat {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },

    /// Numeric Operations
    Add {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Sub {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Mul {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Div {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },

    /// Boolean Operations
    And {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Or {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    Not(Box<OptdExpr>),
}

//! Defines the type system and expressions for OPTD-DSL.
//!
//! This module contains the core type definitions that form the basis of OPTD's domain-specific
//! language. It defines both the primitive values that can be manipulated and the expressions
//! that operate on these values.

use serde::{Deserialize, Serialize};

/// All values supported by the OPTD-DSL.
///
/// These values form the basic building blocks for the type system and can be
/// used in expressions and pattern matching.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum OptdValue {
    /// 64-bit signed integer
    Int64(i64),
    /// UTF-8 encoded string
    String(String),
    /// Boolean value
    Bool(bool),
    // Complex Types: TODO(alexis). Enums, Optionals, Arrays, etc.
}

/// Expressions that can be evaluated on OptdValues.
///
/// This enum defines all possible expressions in the OPTD-DSL, including:
/// - Control flow expressions (if-then-else)
/// - Value references and literals
/// - Comparison operations
/// - Arithmetic operations
/// - Boolean operations
///
/// TODO(alexis): In the future, it would be nice to support user defined
/// functions on top of the basic expressions. This would enable the support
/// of custom defined checks on the optd values.
#[derive(Clone, Debug)]
pub enum OptdExpr {
    /// Conditional expression
    IfThenElse {
        /// The condition to evaluate
        cond: Box<OptdExpr>,
        /// Expression to evaluate if condition is true
        then: Box<OptdExpr>,
        /// Expression to evaluate if condition is false
        otherwise: Box<OptdExpr>,
    },
    /// Reference to a bound value by name
    Ref(String),
    /// Literal value
    Value(OptdValue),

    /// Equality comparison
    Eq {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Less than comparison
    Lt {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Greater than comparison
    Gt {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },

    /// Addition operation
    Add {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Subtraction operation
    Sub {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Multiplication operation
    Mul {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Division operation
    Div {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },

    /// Logical AND
    And {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Logical OR
    Or {
        left: Box<OptdExpr>,
        right: Box<OptdExpr>,
    },
    /// Logical NOT
    Not(Box<OptdExpr>),
}

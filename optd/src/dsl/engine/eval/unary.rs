//! This module provides implementation for unary operations on values.
//!
//! Unary operations transform a single value according to the operation type.
//! Currently supported operations include:
//! - Numeric negation for integers and floating-point numbers
//! - Logical NOT for boolean values

use crate::dsl::analyzer::hir::{CoreData, Literal, UnaryOp, Value};
use CoreData::*;
use Literal::*;
use UnaryOp::*;

/// Evaluates a unary operation on a value.
///
/// This function applies unary operations to values based on the operation type
/// and the value's type. It supports negation for numeric types and logical NOT
/// for boolean values.
///
/// # Parameters
/// * `op` - The unary operation to perform
/// * `expr` - The operand value to apply the operation to
///
/// # Returns
/// The result of applying the unary operation to the operand
///
/// # Panics
/// Panics when the operation is not defined for the given operand type
pub(crate) fn eval_unary_op(op: &UnaryOp, expr: Value) -> Value {
    match (op, &expr.data) {
        // Numeric negation for integers
        (Neg, Literal(Int64(x))) => Value::new(Literal(Int64(-x))),
        // Numeric negation for floating-point numbers
        (Neg, Literal(Float64(x))) => Value::new(Literal(Float64(-x))),
        // Logical NOT for boolean values
        (Not, Literal(Bool(x))) => Value::new(Literal(Bool(!x))),
        // Any other combination is invalid
        _ => panic!("Invalid unary operation or type mismatch"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create integer Value
    fn int(i: i64) -> Value {
        Value::new(Literal(Int64(i)))
    }

    // Helper function to create float Value
    fn float(f: f64) -> Value {
        Value::new(Literal(Float64(f)))
    }

    // Helper function to create boolean Value
    fn boolean(b: bool) -> Value {
        Value::new(Literal(Bool(b)))
    }

    #[test]
    fn test_integer_negation() {
        // Negating a positive integer
        if let Literal(Int64(result)) = eval_unary_op(&Neg, int(5)).data {
            assert_eq!(result, -5);
        } else {
            panic!("Expected Int64");
        }

        // Negating a negative integer
        if let Literal(Int64(result)) = eval_unary_op(&Neg, int(-7)).data {
            assert_eq!(result, 7);
        } else {
            panic!("Expected Int64");
        }

        // Negating zero
        if let Literal(Int64(result)) = eval_unary_op(&Neg, int(0)).data {
            assert_eq!(result, 0);
        } else {
            panic!("Expected Int64");
        }
    }

    #[test]
    fn test_float_negation() {
        use std::f64::consts::PI;

        // Negating a positive float
        if let Literal(Float64(result)) = eval_unary_op(&Neg, float(PI)).data {
            assert_eq!(result, -PI);
        } else {
            panic!("Expected Float64");
        }

        // Negating a negative float
        if let Literal(Float64(result)) = eval_unary_op(&Neg, float(-2.5)).data {
            assert_eq!(result, 2.5);
        } else {
            panic!("Expected Float64");
        }

        // Negating zero
        if let Literal(Float64(result)) = eval_unary_op(&Neg, float(0.0)).data {
            assert_eq!(result, -0.0);
            // Checking sign bit for -0.0
            assert!(result.to_bits() & 0x8000_0000_0000_0000 != 0);
        } else {
            panic!("Expected Float64");
        }
    }

    #[test]
    fn test_boolean_not() {
        // NOT true
        if let Literal(Bool(result)) = eval_unary_op(&Not, boolean(true)).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }

        // NOT false
        if let Literal(Bool(result)) = eval_unary_op(&Not, boolean(false)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }
    }
}

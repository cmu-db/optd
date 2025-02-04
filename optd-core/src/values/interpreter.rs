//! Interpreter for OPTD-DSL expressions.
//! Evaluates OptdExpr with given bindings to produce OptdValue values.

use super::{OptdExpr, OptdValue};

/// Evaluates an OptdExpr to an OptdValue value using provided bindings.
pub fn evaluate(expr: &OptdExpr, bindings: &[(String, OptdValue)]) -> OptdValue {
    match expr {
        OptdExpr::Value(val) => val.clone(),

        OptdExpr::Ref(name) => bindings
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
            .expect("Undefined reference"),

        OptdExpr::IfThenElse {
            cond,
            then,
            otherwise,
        } => match evaluate(cond, bindings) {
            OptdValue::Bool(true) => evaluate(then, bindings),
            OptdValue::Bool(false) => evaluate(otherwise, bindings),
            _ => panic!("IfThenElse condition must be boolean"),
        },

        OptdExpr::Eq { left, right } => {
            OptdValue::Bool(evaluate(left, bindings) == evaluate(right, bindings))
        }

        OptdExpr::Lt { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Bool(l < r),
                _ => panic!("Lt requires integer operands"),
            }
        }

        OptdExpr::Gt { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Bool(l > r),
                _ => panic!("Gt requires integer operands"),
            }
        }

        OptdExpr::Add { left, right } => {
            // TODO(alexis): overflow checks?
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l + r),
                _ => panic!("Add requires integer operands"),
            }
        }

        OptdExpr::Sub { left, right } => {
            // TODO(alexis): underflow checks?
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l - r),
                _ => panic!("Sub requires integer operands"),
            }
        }

        OptdExpr::Mul { left, right } => {
            // TODO(alexis): overflow checks?
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l * r),
                _ => panic!("Mul requires integer operands"),
            }
        }

        OptdExpr::Div { left, right } => {
            // TODO(alexis): div by 0 checks?
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Int64(l), OptdValue::Int64(r)) => {
                    if r == 0 {
                        panic!("Division by zero");
                    }
                    OptdValue::Int64(l / r)
                }
                _ => panic!("Div requires integer operands"),
            }
        }

        OptdExpr::And { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Bool(l), OptdValue::Bool(r)) => OptdValue::Bool(l && r),
                _ => panic!("And requires boolean operands"),
            }
        }

        OptdExpr::Or { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdValue::Bool(l), OptdValue::Bool(r)) => OptdValue::Bool(l || r),
                _ => panic!("Or requires boolean operands"),
            }
        }

        OptdExpr::Not(expr) => match evaluate(expr, bindings) {
            OptdValue::Bool(b) => OptdValue::Bool(!b),
            _ => panic!("Not requires boolean operand"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test bindings
    fn test_bindings() -> Vec<(String, OptdValue)> {
        vec![
            ("x".to_string(), OptdValue::Int64(5)),
            ("y".to_string(), OptdValue::Int64(3)),
            ("flag".to_string(), OptdValue::Bool(true)),
        ]
    }

    #[test]
    fn test_basic_values() {
        let bindings = test_bindings();
        assert_eq!(
            evaluate(&OptdExpr::Value(OptdValue::Int64(42)), &bindings),
            OptdValue::Int64(42)
        );
    }

    #[test]
    fn test_references() {
        let bindings = test_bindings();
        assert_eq!(
            evaluate(&OptdExpr::Ref("x".to_string()), &bindings),
            OptdValue::Int64(5)
        );
    }

    #[test]
    #[should_panic(expected = "Undefined reference")]
    fn test_undefined_reference() {
        let bindings = test_bindings();
        evaluate(&OptdExpr::Ref("undefined".to_string()), &bindings);
    }

    #[test]
    fn test_arithmetic() {
        let bindings = test_bindings();

        // Addition
        assert_eq!(
            evaluate(
                &OptdExpr::Add {
                    left: Box::new(OptdExpr::Ref("x".to_string())),
                    right: Box::new(OptdExpr::Ref("y".to_string()))
                },
                &bindings
            ),
            OptdValue::Int64(8)
        );

        // Multiplication
        assert_eq!(
            evaluate(
                &OptdExpr::Mul {
                    left: Box::new(OptdExpr::Ref("x".to_string())),
                    right: Box::new(OptdExpr::Ref("y".to_string()))
                },
                &bindings
            ),
            OptdValue::Int64(15)
        );
    }

    #[test]
    fn test_boolean_logic() {
        let bindings = test_bindings();

        // AND
        assert_eq!(
            evaluate(
                &OptdExpr::And {
                    left: Box::new(OptdExpr::Ref("flag".to_string())),
                    right: Box::new(OptdExpr::Value(OptdValue::Bool(true)))
                },
                &bindings
            ),
            OptdValue::Bool(true)
        );

        // NOT
        assert_eq!(
            evaluate(
                &OptdExpr::Not(Box::new(OptdExpr::Ref("flag".to_string()))),
                &bindings
            ),
            OptdValue::Bool(false)
        );
    }

    #[test]
    fn test_conditionals() {
        let bindings = test_bindings();

        let expr = OptdExpr::IfThenElse {
            cond: Box::new(OptdExpr::Gt {
                left: Box::new(OptdExpr::Ref("x".to_string())),
                right: Box::new(OptdExpr::Ref("y".to_string())),
            }),
            then: Box::new(OptdExpr::Value(OptdValue::Int64(1))),
            otherwise: Box::new(OptdExpr::Value(OptdValue::Int64(0))),
        };

        assert_eq!(evaluate(&expr, &bindings), OptdValue::Int64(1));
    }

    #[test]
    #[should_panic(expected = "Division by zero")]
    // TODO(alexis): probably we want to gracefully handle this case
    fn test_division_by_zero() {
        let bindings = test_bindings();
        evaluate(
            &OptdExpr::Div {
                left: Box::new(OptdExpr::Value(OptdValue::Int64(1))),
                right: Box::new(OptdExpr::Value(OptdValue::Int64(0))),
            },
            &bindings,
        );
    }
}

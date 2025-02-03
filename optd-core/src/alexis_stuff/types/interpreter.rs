//! Interpreter for OPTD-DSL expressions.
//! Evaluates OptdExpr with given bindings to produce OptdType values.

use super::{OptdExpr, OptdType};

/// Evaluates an OptdExpr to an OptdType value using provided bindings.
pub fn evaluate(expr: &OptdExpr, bindings: &[(String, OptdType)]) -> OptdType {
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
            OptdType::Bool(true) => evaluate(then, bindings),
            OptdType::Bool(false) => evaluate(otherwise, bindings),
            _ => panic!("IfThenElse condition must be boolean"),
        },

        OptdExpr::Eq { left, right } => {
            OptdType::Bool(evaluate(left, bindings) == evaluate(right, bindings))
        }

        OptdExpr::Lt { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => OptdType::Bool(l < r),
                _ => panic!("Lt requires integer operands"),
            }
        }

        OptdExpr::Gt { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => OptdType::Bool(l > r),
                _ => panic!("Gt requires integer operands"),
            }
        }

        OptdExpr::Match { expr, cases } => {
            let val = evaluate(expr, bindings);
            for (pattern, result) in cases {
                if evaluate(pattern, bindings) == val {
                    return evaluate(result, bindings);
                }
            }
            panic!("No matching pattern in Match expression")
        }

        OptdExpr::ArrayLen(arr) => match evaluate(arr, bindings) {
            OptdType::Array(elements) => OptdType::Int64(elements.len() as i64),
            _ => panic!("ArrayLen requires array input"),
        },

        OptdExpr::ArrayGet { array, index } => {
            match (evaluate(array, bindings), evaluate(index, bindings)) {
                (OptdType::Array(elements), OptdType::Int64(idx)) => elements
                    .get(idx as usize)
                    .expect("Array index out of bounds")
                    .clone(),
                _ => panic!("ArrayGet requires array and integer index"),
            }
        }

        OptdExpr::ArrayConcat { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Array(l), OptdType::Array(r)) => {
                    let mut result = l;
                    result.extend(r);
                    OptdType::Array(result)
                }
                _ => panic!("ArrayConcat requires array operands"),
            }
        }

        OptdExpr::Add { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => OptdType::Int64(l + r),
                _ => panic!("Add requires integer operands"),
            }
        }

        OptdExpr::Sub { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => OptdType::Int64(l - r),
                _ => panic!("Sub requires integer operands"),
            }
        }

        OptdExpr::Mul { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => OptdType::Int64(l * r),
                _ => panic!("Mul requires integer operands"),
            }
        }

        OptdExpr::Div { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Int64(l), OptdType::Int64(r)) => {
                    if r == 0 {
                        panic!("Division by zero");
                    }
                    OptdType::Int64(l / r)
                }
                _ => panic!("Div requires integer operands"),
            }
        }

        OptdExpr::And { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Bool(l), OptdType::Bool(r)) => OptdType::Bool(l && r),
                _ => panic!("And requires boolean operands"),
            }
        }

        OptdExpr::Or { left, right } => {
            match (evaluate(left, bindings), evaluate(right, bindings)) {
                (OptdType::Bool(l), OptdType::Bool(r)) => OptdType::Bool(l || r),
                _ => panic!("Or requires boolean operands"),
            }
        }

        OptdExpr::Not(expr) => match evaluate(expr, bindings) {
            OptdType::Bool(b) => OptdType::Bool(!b),
            _ => panic!("Not requires boolean operand"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test bindings
    fn test_bindings() -> Vec<(String, OptdType)> {
        vec![
            ("x".to_string(), OptdType::Int64(5)),
            ("y".to_string(), OptdType::Int64(3)),
            (
                "arr".to_string(),
                OptdType::Array(vec![
                    OptdType::Int64(1),
                    OptdType::Int64(2),
                    OptdType::Int64(3),
                ]),
            ),
            ("flag".to_string(), OptdType::Bool(true)),
        ]
    }

    #[test]
    fn test_basic_values() {
        let bindings = test_bindings();
        assert_eq!(
            evaluate(&OptdExpr::Value(OptdType::Int64(42)), &bindings),
            OptdType::Int64(42)
        );
    }

    #[test]
    fn test_references() {
        let bindings = test_bindings();
        assert_eq!(
            evaluate(&OptdExpr::Ref("x".to_string()), &bindings),
            OptdType::Int64(5)
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
            OptdType::Int64(8)
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
            OptdType::Int64(15)
        );
    }

    #[test]
    fn test_array_operations() {
        let bindings = test_bindings();

        // Array length
        assert_eq!(
            evaluate(
                &OptdExpr::ArrayLen(Box::new(OptdExpr::Ref("arr".to_string()))),
                &bindings
            ),
            OptdType::Int64(3)
        );

        // Array get
        assert_eq!(
            evaluate(
                &OptdExpr::ArrayGet {
                    array: Box::new(OptdExpr::Ref("arr".to_string())),
                    index: Box::new(OptdExpr::Value(OptdType::Int64(1)))
                },
                &bindings
            ),
            OptdType::Int64(2)
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
                    right: Box::new(OptdExpr::Value(OptdType::Bool(true)))
                },
                &bindings
            ),
            OptdType::Bool(true)
        );

        // NOT
        assert_eq!(
            evaluate(
                &OptdExpr::Not(Box::new(OptdExpr::Ref("flag".to_string()))),
                &bindings
            ),
            OptdType::Bool(false)
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
            then: Box::new(OptdExpr::Value(OptdType::Int64(1))),
            otherwise: Box::new(OptdExpr::Value(OptdType::Int64(0))),
        };

        assert_eq!(evaluate(&expr, &bindings), OptdType::Int64(1));
    }

    #[test]
    fn test_pattern_matching() {
        let bindings = test_bindings();

        let expr = OptdExpr::Match {
            expr: Box::new(OptdExpr::Ref("x".to_string())),
            cases: vec![
                (
                    Box::new(OptdExpr::Value(OptdType::Int64(5))),
                    Box::new(OptdExpr::Value(OptdType::Bool(true))),
                ),
                (
                    Box::new(OptdExpr::Value(OptdType::Int64(3))),
                    Box::new(OptdExpr::Value(OptdType::Bool(false))),
                ),
            ],
        };

        assert_eq!(evaluate(&expr, &bindings), OptdType::Bool(true));
    }

    #[test]
    #[should_panic(expected = "Division by zero")]
    fn test_division_by_zero() {
        let bindings = test_bindings();
        evaluate(
            &OptdExpr::Div {
                left: Box::new(OptdExpr::Value(OptdType::Int64(1))),
                right: Box::new(OptdExpr::Value(OptdType::Int64(0))),
            },
            &bindings,
        );
    }
}

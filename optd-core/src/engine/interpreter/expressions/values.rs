//! Interpreter for OPTD-DSL expressions.
//! Evaluates OptdExpr with given bindings to produce OptdValue values.

use std::collections::HashMap;

use crate::values::{OptdExpr, OptdValue};

/// Evaluates an OptdExpr to an OptdValue value using provided bindings.
impl OptdExpr {
    pub fn evaluate(&self, bindings: &HashMap<String, OptdValue>) -> OptdValue {
        match self {
            OptdExpr::Value(val) => val.clone(),

            OptdExpr::Ref(name) => bindings.get(name).cloned().unwrap_or_else(|| {
                panic!("Undefined reference: {}", name);
            }),

            OptdExpr::IfThenElse {
                cond,
                then,
                otherwise,
            } => match cond.evaluate(bindings) {
                OptdValue::Bool(true) => then.evaluate(bindings),
                OptdValue::Bool(false) => otherwise.evaluate(bindings),
                _ => panic!("IfThenElse condition must be boolean"),
            },

            OptdExpr::Eq { left, right } => {
                OptdValue::Bool(left.evaluate(bindings) == right.evaluate(bindings))
            }

            OptdExpr::Lt { left, right } => {
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Bool(l < r),
                    _ => panic!("Lt requires integer operands"),
                }
            }

            OptdExpr::Gt { left, right } => {
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Bool(l > r),
                    _ => panic!("Gt requires integer operands"),
                }
            }

            OptdExpr::Add { left, right } => {
                // TODO(alexis): overflow checks
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l + r),
                    _ => panic!("Add requires integer operands"),
                }
            }

            OptdExpr::Sub { left, right } => {
                // TODO(alexis): underflow checks
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l - r),
                    _ => panic!("Sub requires integer operands"),
                }
            }

            OptdExpr::Mul { left, right } => {
                // TODO(alexis): overflow checks
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Int64(l), OptdValue::Int64(r)) => OptdValue::Int64(l * r),
                    _ => panic!("Mul requires integer operands"),
                }
            }

            OptdExpr::Div { left, right } => {
                // TODO(alexis): div by 0 checks
                match (left.evaluate(bindings), right.evaluate(bindings)) {
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
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Bool(l), OptdValue::Bool(r)) => OptdValue::Bool(l && r),
                    _ => panic!("And requires boolean operands"),
                }
            }

            OptdExpr::Or { left, right } => {
                match (left.evaluate(bindings), right.evaluate(bindings)) {
                    (OptdValue::Bool(l), OptdValue::Bool(r)) => OptdValue::Bool(l || r),
                    _ => panic!("Or requires boolean operands"),
                }
            }

            OptdExpr::Not(expr) => match expr.evaluate(bindings) {
                OptdValue::Bool(b) => OptdValue::Bool(!b),
                _ => panic!("Not requires boolean operand"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create test bindings
    fn test_bindings() -> HashMap<String, OptdValue> {
        let mut map = HashMap::new();
        map.insert("x".to_string(), OptdValue::Int64(5));
        map.insert("y".to_string(), OptdValue::Int64(3));
        map.insert("flag".to_string(), OptdValue::Bool(true));
        map
    }

    #[test]
    fn test_basic_values() {
        let bindings = test_bindings();
        assert_eq!(
            OptdExpr::Value(OptdValue::Int64(42)).evaluate(&bindings),
            OptdValue::Int64(42)
        );
    }

    #[test]
    fn test_references() {
        let bindings = test_bindings();
        assert_eq!(
            OptdExpr::Ref("x".to_string()).evaluate(&bindings),
            OptdValue::Int64(5)
        );
    }

    #[test]
    fn test_arithmetic() {
        let bindings = test_bindings();

        // Addition
        assert_eq!(
            OptdExpr::Add {
                left: Box::new(OptdExpr::Ref("x".to_string())),
                right: Box::new(OptdExpr::Ref("y".to_string()))
            }
            .evaluate(&bindings),
            OptdValue::Int64(8)
        );

        // Multiplication
        assert_eq!(
            OptdExpr::Mul {
                left: Box::new(OptdExpr::Ref("x".to_string())),
                right: Box::new(OptdExpr::Ref("y".to_string()))
            }
            .evaluate(&bindings),
            OptdValue::Int64(15)
        );
    }

    #[test]
    fn test_boolean_logic() {
        let bindings = test_bindings();

        // AND
        assert_eq!(
            OptdExpr::And {
                left: Box::new(OptdExpr::Ref("flag".to_string())),
                right: Box::new(OptdExpr::Value(OptdValue::Bool(true)))
            }
            .evaluate(&bindings),
            OptdValue::Bool(true)
        );

        // NOT
        assert_eq!(
            OptdExpr::Not(Box::new(OptdExpr::Ref("flag".to_string()))).evaluate(&bindings),
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

        assert_eq!(expr.evaluate(&bindings), OptdValue::Int64(1));
    }
}

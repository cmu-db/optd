//! This module provides implementation for binary operations between values.
//!
//! Binary operations are fundamental for expression evaluation, allowing values to be combined and
//! compared in various ways. The module supports operations on different value types, including:
//!
//! - Arithmetic operations on numbers (add, subtract, multiply, divide).
//! - Comparison operations for various types (equality, less than).
//! - Logical operations on boolean values (AND, OR).
//! - Collection operations (concatenation, range creation).

use crate::analyzer::hir::{BinOp, CoreData, Literal, Value};

/// Evaluates a binary operation between two values.
///
/// This function performs binary operations on values based on their types and the operation
/// requested. It handles arithmetic, logical, comparison, and collection operations.
///
/// # Panics
///
/// Panics when the operation is not defined for the given operand types.
pub(crate) fn eval_binary_op(left: Value, op: &BinOp, right: Value) -> Value {
    use self::Literal::*;
    use BinOp::*;
    use CoreData::*;

    match (left.data, op, right.data) {
        // Handle operations between two literals.
        (Literal(l), op, Literal(r)) => match (l, op, r) {
            // Integer operations (arithmetic, comparison).
            (Int64(l), Add | Sub | Mul | Div | Eq | Lt, Int64(r)) => {
                Value::new(Literal(match op {
                    Add => Int64(l + r), // Integer addition
                    Sub => Int64(l - r), // Integer subtraction
                    Mul => Int64(l * r), // Integer multiplication
                    Div => Int64(l / r), // Integer division (panics on divide by zero)
                    Eq => Bool(l == r),  // Integer equality comparison
                    Lt => Bool(l < r),   // Integer less-than comparison
                    _ => unreachable!(), // This branch is unreachable due to pattern guard
                }))
            }

            // Integer range operation (creates an array of sequential integers).
            (Int64(l), Range, Int64(r)) => Value::new(Array(
                (l..=r).map(|n| Value::new(Literal(Int64(n)))).collect(),
            )),

            // Float operations (arithmetic, comparison).
            (Float64(l), op, Float64(r)) => Value::new(Literal(match op {
                Add => Float64(l + r),                  // Float addition
                Sub => Float64(l - r),                  // Float subtraction
                Mul => Float64(l * r),                  // Float multiplication
                Div => Float64(l / r),                  // Float division
                Lt => Bool(l < r),                      // Float less-than comparison
                _ => panic!("Invalid float operation"), // Other operations not supported
            })),

            // Boolean operations (logical, comparison).
            (Bool(l), op, Bool(r)) => Value::new(Literal(match op {
                And => Bool(l && r),                      // Logical AND
                Or => Bool(l || r),                       // Logical OR
                Eq => Bool(l == r),                       // Boolean equality comparison
                _ => panic!("Invalid boolean operation"), // Other operations not supported
            })),

            // String operations (comparison, concatenation).
            (String(l), op, String(r)) => Value::new(Literal(match op {
                Eq => Bool(l == r),                      // String equality comparison
                Concat => String(format!("{l}{r}")),     // String concatenation
                _ => panic!("Invalid string operation"), // Other operations not supported
            })),

            // Any other combination of literal types is not supported.
            expr => panic!("Invalid binary operation: {:?}", expr),
        },

        // Array concatenation (joins two arrays).
        (Array(l), Concat, Array(r)) => {
            let mut result = l.clone();
            result.extend(r.iter().cloned());
            Value::new(Array(result))
        }

        // Map concatenation (joins two maps).
        (Map(mut l), Concat, Map(r)) => {
            l.concat(r);
            Value::new(Map(l))
        }

        // Any other combination of value types or operations is not supported.
        expr => panic!("Invalid binary operation: {:?}", expr),
    }
}

#[cfg(test)]
mod tests {
    use crate::analyzer::hir::{BinOp, CoreData, Literal, Value};
    use BinOp::*;
    use CoreData::*;
    use Literal::*;

    use super::eval_binary_op;

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

    // Helper function to create string Value
    fn string(s: &str) -> Value {
        Value::new(Literal(String(s.to_string())))
    }

    #[test]
    fn test_integer_arithmetic() {
        // Addition
        if let Literal(Int64(result)) = eval_binary_op(int(5), &Add, int(7)).data {
            assert_eq!(result, 12);
        } else {
            panic!("Expected Int64");
        }

        // Subtraction
        if let Literal(Int64(result)) = eval_binary_op(int(10), &Sub, int(3)).data {
            assert_eq!(result, 7);
        } else {
            panic!("Expected Int64");
        }

        // Multiplication
        if let Literal(Int64(result)) = eval_binary_op(int(4), &Mul, int(5)).data {
            assert_eq!(result, 20);
        } else {
            panic!("Expected Int64");
        }

        // Division
        if let Literal(Int64(result)) = eval_binary_op(int(20), &Div, int(4)).data {
            assert_eq!(result, 5);
        } else {
            panic!("Expected Int64");
        }
    }

    #[test]
    fn test_integer_comparison() {
        // Equality - true case
        if let Literal(Bool(result)) = eval_binary_op(int(5), &Eq, int(5)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }

        // Equality - false case
        if let Literal(Bool(result)) = eval_binary_op(int(5), &Eq, int(7)).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }

        // Less than - true case
        if let Literal(Bool(result)) = eval_binary_op(int(5), &Lt, int(10)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }

        // Less than - false case
        if let Literal(Bool(result)) = eval_binary_op(int(10), &Lt, int(5)).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }
    }

    #[test]
    fn test_integer_range() {
        // Range operation
        if let Array(result) = eval_binary_op(int(1), &Range, int(5)).data {
            assert_eq!(result.len(), 5);

            // Check individual elements
            for (i, val) in result.iter().enumerate() {
                if let Literal(Int64(n)) = val.data {
                    assert_eq!(n, (i as i64) + 1);
                } else {
                    panic!("Expected Int64 in array");
                }
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_float_operations() {
        // Addition
        if let Literal(Float64(result)) = eval_binary_op(float(3.5), &Add, float(2.25)).data {
            assert_eq!(result, 5.75);
        } else {
            panic!("Expected Float64");
        }

        // Subtraction
        if let Literal(Float64(result)) = eval_binary_op(float(10.5), &Sub, float(3.25)).data {
            assert_eq!(result, 7.25);
        } else {
            panic!("Expected Float64");
        }

        // Multiplication
        if let Literal(Float64(result)) = eval_binary_op(float(4.0), &Mul, float(2.5)).data {
            assert_eq!(result, 10.0);
        } else {
            panic!("Expected Float64");
        }

        // Division
        if let Literal(Float64(result)) = eval_binary_op(float(10.0), &Div, float(2.5)).data {
            assert_eq!(result, 4.0);
        } else {
            panic!("Expected Float64");
        }

        // Less than
        if let Literal(Bool(result)) = eval_binary_op(float(3.5), &Lt, float(3.6)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }
    }

    #[test]
    fn test_boolean_operations() {
        // AND - true case
        if let Literal(Bool(result)) = eval_binary_op(boolean(true), &And, boolean(true)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }

        // AND - false case
        if let Literal(Bool(result)) = eval_binary_op(boolean(true), &And, boolean(false)).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }

        // OR - true case
        if let Literal(Bool(result)) = eval_binary_op(boolean(false), &Or, boolean(true)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }

        // OR - false case
        if let Literal(Bool(result)) = eval_binary_op(boolean(false), &Or, boolean(false)).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }

        // Equality
        if let Literal(Bool(result)) = eval_binary_op(boolean(true), &Eq, boolean(true)).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }
    }

    #[test]
    fn test_string_operations() {
        // Equality - true case
        if let Literal(Bool(result)) = eval_binary_op(string("hello"), &Eq, string("hello")).data {
            assert!(result);
        } else {
            panic!("Expected Bool");
        }

        // Equality - false case
        if let Literal(Bool(result)) = eval_binary_op(string("hello"), &Eq, string("world")).data {
            assert!(!result);
        } else {
            panic!("Expected Bool");
        }

        // Concatenation
        if let Literal(String(result)) =
            eval_binary_op(string("hello "), &Concat, string("world")).data
        {
            assert_eq!(result, "hello world");
        } else {
            panic!("Expected String");
        }
    }

    #[test]
    fn test_array_concatenation() {
        // Create two arrays
        let array1 = Value::new(Array(vec![int(1), int(2), int(3)]));
        let array2 = Value::new(Array(vec![int(4), int(5)]));

        // Concatenate arrays
        if let Array(result) = eval_binary_op(array1, &Concat, array2).data {
            assert_eq!(result.len(), 5);

            // Check the elements
            let expected = [1, 2, 3, 4, 5];
            for (i, val) in result.iter().enumerate() {
                if let Literal(Int64(n)) = val.data {
                    assert_eq!(n, expected[i] as i64);
                } else {
                    panic!("Expected Int64 in array");
                }
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_map_concatenation() {
        use crate::analyzer::map::Map;

        // Create two maps using Map::from_pairs
        let map1 = Value::new(Map(Map::from_pairs(vec![
            (string("a"), int(1)),
            (string("b"), int(2)),
        ])));
        let map2 = Value::new(Map(Map::from_pairs(vec![
            (string("c"), int(3)),
            (string("d"), int(4)),
        ])));

        // Concatenate maps
        if let Map(result) = eval_binary_op(map1, &Concat, map2).data {
            // Check each key-value pair is accessible
            if let Literal(Int64(v)) = result.get(&string("a")).data {
                assert_eq!(v, 1);
            } else {
                panic!("Expected Int64 for key 'a'");
            }

            if let Literal(Int64(v)) = result.get(&string("b")).data {
                assert_eq!(v, 2);
            } else {
                panic!("Expected Int64 for key 'b'");
            }

            if let Literal(Int64(v)) = result.get(&string("c")).data {
                assert_eq!(v, 3);
            } else {
                panic!("Expected Int64 for key 'c'");
            }

            if let Literal(Int64(v)) = result.get(&string("d")).data {
                assert_eq!(v, 4);
            } else {
                panic!("Expected Int64 for key 'd'");
            }

            // Check a non-existent key returns None
            if let None = result.get(&string("z")).data {
                // This is the expected behavior
            } else {
                panic!("Expected None for non-existent key");
            }
        } else {
            panic!("Expected Map");
        }
    }
}

use pest::iterators::Pair;
use std::collections::HashMap;

use super::{expr::parse_expr, parse_field_def, Rule};
use crate::dsl::ast::upper_layer::{Expr, Field, LogicalOp, Operator, OperatorKind, ScalarOp};

/// Parse an operator definition from a pest Pair
///
/// # Arguments
/// * `pair` - The pest Pair containing the operator definition
///
/// # Returns
/// * `Operator` - The parsed operator AST node
pub fn parse_operator_def(pair: Pair<'_, Rule>) -> Operator {
    let mut operator_type = None;
    let mut name = None;
    let mut fields = Vec::new();
    let mut derived_props = HashMap::new();

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::operator_type => {
                operator_type = Some(parse_operator_type(inner_pair));
            }
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::field_def_list => {
                fields = parse_field_def_list(inner_pair);
            }
            Rule::derive_props_block => {
                derived_props = parse_derive_props_block(inner_pair);
            }
            _ => unreachable!(
                "Unexpected operator definition rule: {:?}",
                inner_pair.as_rule()
            ),
        }
    }

    create_operator(operator_type.unwrap(), name.unwrap(), fields, derived_props)
}

/// Parse an operator type (Scalar or Logical)
fn parse_operator_type(pair: Pair<'_, Rule>) -> OperatorKind {
    match pair.as_str() {
        "Scalar" => OperatorKind::Scalar,
        "Logical" => OperatorKind::Logical,
        _ => unreachable!("Unexpected operator type: {}", pair.as_str()),
    }
}

/// Parse a list of field definitions
fn parse_field_def_list(pair: Pair<'_, Rule>) -> Vec<Field> {
    pair.into_inner().map(parse_field_def).collect()
}

/// Parse a derive properties block
fn parse_derive_props_block(pair: Pair<'_, Rule>) -> HashMap<String, Expr> {
    let mut props = HashMap::new();

    for prop_pair in pair.into_inner() {
        if prop_pair.as_rule() == Rule::prop_derivation {
            let (name, expr) = parse_prop_derivation(prop_pair);
            props.insert(name, expr);
        }
    }

    props
}

/// Parse a single property derivation
fn parse_prop_derivation(pair: Pair<'_, Rule>) -> (String, Expr) {
    let mut prop_name = None;
    let mut expr = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                prop_name = Some(inner_pair.as_str().to_string());
            }
            Rule::expr => {
                expr = Some(parse_expr(inner_pair));
            }
            _ => unreachable!(
                "Unexpected property derivation rule: {:?}",
                inner_pair.as_rule()
            ),
        }
    }

    (prop_name.unwrap(), expr.unwrap())
}

/// Create an operator based on its type and components
fn create_operator(
    operator_type: OperatorKind,
    name: String,
    fields: Vec<Field>,
    derived_props: HashMap<String, Expr>,
) -> Operator {
    match operator_type {
        OperatorKind::Scalar => Operator::Scalar(ScalarOp { name, fields }),
        OperatorKind::Logical => Operator::Logical(LogicalOp {
            name,
            fields,
            derived_props,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ast::upper_layer::{BinOp, Type};
    use crate::dsl::parser::DslParser;
    use pest::Parser;

    fn parse_operator_from_str(input: &str) -> Operator {
        let pair = DslParser::parse(Rule::operator_def, input)
            .unwrap()
            .next()
            .unwrap();
        parse_operator_def(pair)
    }

    #[test]
    fn test_parse_scalar_operator() {
        let input = r#"Scalar Add(
                x: Int64,
                y: Int64
            )
        "#;

        match parse_operator_from_str(input) {
            Operator::Scalar(op) => {
                assert_eq!(op.name, "Add");
                assert_eq!(op.fields.len(), 2);
                assert_eq!(op.fields[0].name, "x");
                assert!(matches!(op.fields[0].ty, Type::Int64));
                assert_eq!(op.fields[1].name, "y");
                assert!(matches!(op.fields[1].ty, Type::Int64));
            }
            _ => panic!("Expected scalar operator"),
        }
    }

    #[test]
    fn test_parse_logical_operator() {
        let input = r#"Logical And(
                left: Bool,
                right: Bool
            ) derive {
                result = left && right
            }
        "#;

        match parse_operator_from_str(input) {
            Operator::Logical(op) => {
                assert_eq!(op.name, "And");
                assert_eq!(op.fields.len(), 2);
                assert_eq!(op.fields[0].name, "left");
                assert!(matches!(op.fields[0].ty, Type::Bool));
                assert_eq!(op.fields[1].name, "right");
                assert!(matches!(op.fields[1].ty, Type::Bool));

                assert_eq!(op.derived_props.len(), 1);
                assert!(op.derived_props.contains_key("result"));

                match op.derived_props.get("result").unwrap() {
                    Expr::Binary(left, BinOp::And, right) => {
                        assert!(matches!(**left, Expr::Var(ref v) if v == "left"));
                        assert!(matches!(**right, Expr::Var(ref v) if v == "right"));
                    }
                    _ => panic!("Expected binary AND expression"),
                }
            }
            _ => panic!("Expected logical operator"),
        }
    }

    #[test]
    fn test_parse_operator_with_multiple_derived_props() {
        let input = r#"Logical Or(
                left: Bool,
                right: Bool
            ) derive {
                result = left || right,
                description = "Logical OR"
            }
        "#;

        match parse_operator_from_str(input) {
            Operator::Logical(op) => {
                assert_eq!(op.name, "Or");
                assert_eq!(op.derived_props.len(), 2);
                assert!(op.derived_props.contains_key("result"));
                assert!(op.derived_props.contains_key("description"));
            }
            _ => panic!("Expected logical operator"),
        }
    }

    #[test]
    fn test_parse_operator_without_fields() {
        let input = r#"Scalar True()
        "#;

        match parse_operator_from_str(input) {
            Operator::Scalar(op) => {
                assert_eq!(op.name, "True");
                assert_eq!(op.fields.len(), 0);
            }
            _ => panic!("Expected scalar operator"),
        }
    }

    #[test]
    fn test_parse_operator_with_array_field() {
        let input = r#"Scalar Sum(
                values: [Int64]
            )
        "#;

        match parse_operator_from_str(input) {
            Operator::Scalar(op) => {
                assert_eq!(op.name, "Sum");
                assert_eq!(op.fields.len(), 1);
                assert_eq!(op.fields[0].name, "values");
                match &op.fields[0].ty {
                    Type::Array(inner) => assert!(matches!(**inner, Type::Int64)),
                    _ => panic!("Expected array type"),
                }
            }
            _ => panic!("Expected scalar operator"),
        }
    }
}

use pest::iterators::Pair;

use crate::dsl::ast::upper_layer::{Function, OperatorKind, Type};

use super::expr::parse_expr;
use super::types::parse_type_expr;
use super::Rule;

/// Parse a function definition from a pest Pair
///
/// # Arguments
/// * `pair` - The pest Pair containing the function definition
///
/// # Returns
/// * `Function` - The parsed function AST node
pub fn parse_function_def(pair: Pair<'_, Rule>) -> Function {
    let mut name = None;
    let mut params = Vec::new();
    let mut return_type = None;
    let mut body = None;
    let mut rule_type = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::params => {
                params = parse_params(inner_pair);
            }
            Rule::type_expr => {
                return_type = Some(parse_type_expr(inner_pair));
            }
            Rule::expr => {
                body = Some(parse_expr(inner_pair));
            }
            Rule::rule_annot => {
                rule_type = Some(parse_rule_annotation(inner_pair));
            }
            _ => unreachable!(
                "Unexpected function definition rule: {:?}",
                inner_pair.as_rule()
            ),
        }
    }

    Function {
        name: name.unwrap(),
        params,
        return_type: return_type.unwrap(),
        body: body.unwrap(),
        rule_type,
    }
}

/// Parse function parameters
fn parse_params(pair: Pair<'_, Rule>) -> Vec<(String, Type)> {
    let mut params = Vec::new();

    for param_pair in pair.into_inner() {
        if param_pair.as_rule() == Rule::param {
            let (name, ty) = parse_param(param_pair);
            params.push((name, ty));
        }
    }

    params
}

/// Parse a single parameter
fn parse_param(pair: Pair<'_, Rule>) -> (String, Type) {
    let mut param_name = None;
    let mut param_type = None;

    for param_inner_pair in pair.into_inner() {
        match param_inner_pair.as_rule() {
            Rule::identifier => {
                param_name = Some(param_inner_pair.as_str().to_string());
            }
            Rule::type_expr => {
                param_type = Some(parse_type_expr(param_inner_pair));
            }
            _ => unreachable!(
                "Unexpected parameter rule: {:?}",
                param_inner_pair.as_rule()
            ),
        }
    }

    (param_name.unwrap(), param_type.unwrap())
}

/// Parse a rule annotation (@rule(Scalar) or @rule(Logical))
fn parse_rule_annotation(pair: Pair<'_, Rule>) -> OperatorKind {
    let annot_inner_pair = pair.into_inner().next().unwrap();
    match annot_inner_pair.as_str() {
        "Scalar" => OperatorKind::Scalar,
        "Logical" => OperatorKind::Logical,
        _ => unreachable!("Unexpected rule type: {}", annot_inner_pair.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::ast::upper_layer::{BinOp, Expr, Literal, Type};
    use crate::dsl::parser::{DslParser, Rule};
    use pest::Parser;

    fn parse_function_from_str(input: &str) -> Function {
        let pair = DslParser::parse(Rule::function_def, input)
            .unwrap()
            .next()
            .unwrap();
        parse_function_def(pair)
    }

    #[test]
    fn test_parse_simple_function() {
        let input = "def add(x: Int64, y: Int64): Int64 = x + y";

        let function = parse_function_from_str(input);
        assert_eq!(function.name, "add");
        assert_eq!(function.params.len(), 2);

        // Check parameters
        assert_eq!(function.params[0].0, "x");
        assert!(matches!(function.params[0].1, Type::Int64));
        assert_eq!(function.params[1].0, "y");
        assert!(matches!(function.params[1].1, Type::Int64));

        // Check return type
        assert!(matches!(function.return_type, Type::Int64));

        // Check body
        match function.body {
            Expr::Binary(left, BinOp::Add, right) => {
                assert!(matches!(*left, Expr::Var(v) if v == "x"));
                assert!(matches!(*right, Expr::Var(v) if v == "y"));
            }
            _ => panic!("Expected binary addition expression"),
        }

        assert!(function.rule_type.is_none());
    }

    #[test]
    fn test_parse_function_with_rule_annotation() {
        let input = r#"@rule(Scalar)
            def increment(x: Int64): Int64 = x + 1
        "#;

        let function = parse_function_from_str(input);
        assert_eq!(function.name, "increment");
        assert_eq!(function.params.len(), 1);
        assert!(matches!(function.rule_type, Some(OperatorKind::Scalar)));
    }

    #[test]
    fn test_parse_function_with_block_body() {
        let input = r#"def max(x: Int64, y: Int64): Int64 = {
                if x > y then x else y
            }
        "#;

        let function = parse_function_from_str(input);
        assert_eq!(function.name, "max");
        assert_eq!(function.params.len(), 2);

        match function.body {
            Expr::If(cond, then_branch, else_branch) => {
                match *cond {
                    Expr::Binary(left, BinOp::Gt, right) => {
                        assert!(matches!(*left, Expr::Var(v) if v == "x"));
                        assert!(matches!(*right, Expr::Var(v) if v == "y"));
                    }
                    _ => panic!("Expected comparison expression"),
                }
                assert!(matches!(*then_branch, Expr::Var(v) if v == "x"));
                assert!(matches!(*else_branch, Expr::Var(v) if v == "y"));
            }
            _ => panic!("Expected if expression"),
        }
    }

    #[test]
    fn test_parse_function_with_array_parameter() {
        let input = "def sum(values: [Int64]): Int64 = 0";

        let function = parse_function_from_str(input);
        assert_eq!(function.name, "sum");
        assert_eq!(function.params.len(), 1);

        match &function.params[0].1 {
            Type::Array(inner) => assert!(matches!(**inner, Type::Int64)),
            _ => panic!("Expected array type parameter"),
        }
    }

    #[test]
    fn test_parse_function_with_no_parameters() {
        let input = "def get_zero(): Int64 = 0";

        let function = parse_function_from_str(input);
        assert_eq!(function.name, "get_zero");
        assert_eq!(function.params.len(), 0);
        assert!(matches!(function.return_type, Type::Int64));
        match function.body {
            Expr::Literal(Literal::Int64(n)) => assert_eq!(n, 0),
            _ => panic!("Expected integer literal"),
        }
    }

    #[test]
    fn test_parse_function_with_complex_return_type() {
        let input = "def make_pair(x: Int64): (Int64, Int64) = (x, x)";

        let function = parse_function_from_str(input);
        match function.return_type {
            Type::Tuple(types) => {
                assert_eq!(types.len(), 2);
                assert!(matches!(types[0], Type::Int64));
                assert!(matches!(types[1], Type::Int64));
            }
            _ => panic!("Expected tuple return type"),
        }
    }
}

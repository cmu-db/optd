use pest::iterators::Pair;

use crate::dsl::ast::upper_layer::{OperatorKind, Type};

use super::Rule;

/// Parse a type expression from a pest Pair
///
/// # Arguments
/// * `pair` - The pest Pair containing the type expression
///
/// # Returns
/// * `Type` - The parsed type AST node
pub fn parse_type_expr(pair: Pair<'_, Rule>) -> Type {
    match pair.as_rule() {
        Rule::type_expr => parse_type_expr(pair.into_inner().next().unwrap()),
        Rule::base_type => parse_base_type(pair),
        Rule::array_type => parse_array_type(pair),
        Rule::map_type => parse_map_type(pair),
        Rule::tuple_type => parse_tuple_type(pair),
        Rule::function_type => parse_function_type(pair),
        Rule::operator_type => parse_operator_type(pair),
        _ => unreachable!("Unexpected type rule: {:?}", pair.as_rule()),
    }
}

/// Parse a base type (Int64, String, Bool, Float64)
fn parse_base_type(pair: Pair<'_, Rule>) -> Type {
    match pair.as_str() {
        "Int64" => Type::Int64,
        "String" => Type::String,
        "Bool" => Type::Bool,
        "Float64" => Type::Float64,
        _ => unreachable!("Unexpected base type: {}", pair.as_str()),
    }
}

/// Parse an array type (e.g., [Int64])
fn parse_array_type(pair: Pair<'_, Rule>) -> Type {
    let inner_type = pair.into_inner().next().unwrap();
    Type::Array(Box::new(parse_type_expr(inner_type)))
}

/// Parse a map type (e.g., Map[String, Int64])
fn parse_map_type(pair: Pair<'_, Rule>) -> Type {
    let mut inner_types = pair.into_inner();
    let key_type = parse_type_expr(inner_types.next().unwrap());
    let value_type = parse_type_expr(inner_types.next().unwrap());
    Type::Map(Box::new(key_type), Box::new(value_type))
}

/// Parse a tuple type (e.g., (Int64, String))
fn parse_tuple_type(pair: Pair<'_, Rule>) -> Type {
    let types = pair.into_inner().map(parse_type_expr).collect();
    Type::Tuple(types)
}

/// Parse a function type (e.g., (Int64) -> String)
fn parse_function_type(pair: Pair<'_, Rule>) -> Type {
    let mut inner_types = pair.into_inner();
    let input_type = parse_type_expr(inner_types.next().unwrap());
    let output_type = parse_type_expr(inner_types.next().unwrap());
    Type::Function(Box::new(input_type), Box::new(output_type))
}

/// Parse an operator type (Scalar or Logical)
fn parse_operator_type(pair: Pair<'_, Rule>) -> Type {
    match pair.as_str() {
        "Scalar" => Type::Operator(OperatorKind::Scalar),
        "Logical" => Type::Operator(OperatorKind::Logical),
        _ => unreachable!("Unexpected operator type: {}", pair.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::parser::DslParser;
    use pest::Parser;

    fn parse_type_from_str(input: &str) -> Type {
        let pair = DslParser::parse(Rule::type_expr, input)
            .unwrap()
            .next()
            .unwrap();
        parse_type_expr(pair)
    }

    #[test]
    fn test_parse_base_types() {
        assert!(matches!(parse_type_from_str("Int64"), Type::Int64));
        assert!(matches!(parse_type_from_str("String"), Type::String));
        assert!(matches!(parse_type_from_str("Bool"), Type::Bool));
        assert!(matches!(parse_type_from_str("Float64"), Type::Float64));
    }

    #[test]
    fn test_parse_array_type() {
        match parse_type_from_str("[Int64]") {
            Type::Array(inner) => assert!(matches!(*inner, Type::Int64)),
            _ => panic!("Expected array type"),
        }
    }

    #[test]
    fn test_parse_map_type() {
        match parse_type_from_str("Map[String, Int64]") {
            Type::Map(key, value) => {
                assert!(matches!(*key, Type::String));
                assert!(matches!(*value, Type::Int64));
            }
            _ => panic!("Expected map type"),
        }
    }

    #[test]
    fn test_parse_tuple_type() {
        match parse_type_from_str("(Int64, String, Bool)") {
            Type::Tuple(types) => {
                assert_eq!(types.len(), 3);
                assert!(matches!(types[0], Type::Int64));
                assert!(matches!(types[1], Type::String));
                assert!(matches!(types[2], Type::Bool));
            }
            _ => panic!("Expected tuple type"),
        }
    }

    #[test]
    fn test_parse_function_type() {
        match parse_type_from_str("(Int64) -> String") {
            Type::Function(input, output) => {
                assert!(matches!(*input, Type::Int64));
                assert!(matches!(*output, Type::String));
            }
            _ => panic!("Expected function type"),
        }
    }

    #[test]
    fn test_parse_operator_type() {
        match parse_type_from_str("Scalar") {
            Type::Operator(kind) => assert!(matches!(kind, OperatorKind::Scalar)),
            _ => panic!("Expected scalar operator type"),
        }

        match parse_type_from_str("Logical") {
            Type::Operator(kind) => assert!(matches!(kind, OperatorKind::Logical)),
            _ => panic!("Expected logical operator type"),
        }
    }

    #[test]
    fn test_parse_nested_types() {
        match parse_type_from_str("Map[String, [Int64]]") {
            Type::Map(key, value) => {
                assert!(matches!(*key, Type::String));
                match *value {
                    Type::Array(inner) => assert!(matches!(*inner, Type::Int64)),
                    _ => panic!("Expected array type"),
                }
            }
            _ => panic!("Expected map type"),
        }
    }
}

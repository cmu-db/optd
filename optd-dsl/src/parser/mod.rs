use pest::{iterators::Pair, Parser};
use pest_derive::Parser;

pub mod expr;
pub mod functions;
pub mod operators;
pub mod patterns;
pub mod types;

use functions::parse_function_def;
use operators::parse_operator_def;
use types::parse_type_expr;

use pest::error::Error;

use super::ast::{Field, File, Properties};

/// The main parser for the DSL, derived using pest
#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct DslParser;

/// Parses a complete DSL file into the AST
///
/// # Arguments
/// * `input` - The input string containing the DSL code
///
/// # Returns
/// * `Result<File, Error<Rule>>` - The parsed AST or a parsing error
pub fn parse_file(input: &str) -> Result<File, Box<Error<Rule>>> {
    let pairs = DslParser::parse(Rule::file, input)?
        .next()
        .unwrap()
        .into_inner();

    let mut file = File {
        properties: Properties { fields: Vec::new() },
        operators: Vec::new(),
        functions: Vec::new(),
    };

    for pair in pairs {
        match pair.as_rule() {
            Rule::props_block => {
                file.properties = parse_properties_block(pair);
            }
            Rule::operator_def => {
                file.operators.push(parse_operator_def(pair));
            }
            Rule::function_def => {
                file.functions.push(parse_function_def(pair));
            }
            _ => {}
        }
    }

    Ok(file)
}

/// Parses a properties block in the DSL
///
/// # Arguments
/// * `pair` - The pest Pair containing the properties block
///
/// # Returns
/// * `Properties` - The parsed properties structure
fn parse_properties_block(pair: Pair<Rule>) -> Properties {
    let mut properties = Properties { fields: Vec::new() };

    for field_pair in pair.into_inner() {
        if field_pair.as_rule() == Rule::field_def {
            properties.fields.push(parse_field_def(field_pair));
        }
    }

    properties
}

/// Parses a field definition
///
/// # Arguments
/// * `pair` - The pest Pair containing the field definition
///
/// # Returns
/// * `Field` - The parsed field structure
pub(crate) fn parse_field_def(pair: Pair<Rule>) -> Field {
    let mut name = None;
    let mut ty = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::type_expr => {
                ty = Some(parse_type_expr(inner_pair));
            }
            _ => {}
        }
    }

    Field {
        name: name.unwrap(),
        ty: ty.unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::Type;

    use super::*;
    use pest::Parser;

    fn parse_props_from_str(input: &str) -> Properties {
        let pair = DslParser::parse(Rule::props_block, input)
            .unwrap()
            .next()
            .unwrap();
        parse_properties_block(pair)
    }

    #[test]
    fn parse_example_files() {
        let input = include_str!("../programs/example.optd");
        parse_file(input).unwrap();

        let input = include_str!("../programs/working.optd");
        let out = parse_file(input).unwrap();
        println!("{:#?}", out);
    }

    #[test]
    fn test_parse_single_property() {
        let input = r#"Logical Props (
            name: String
        )"#;

        let props = parse_props_from_str(input);
        assert_eq!(props.fields.len(), 1);
        assert_eq!(props.fields[0].name, "name");
        assert!(matches!(props.fields[0].ty, Type::String));
    }

    #[test]
    fn test_parse_multiple_properties() {
        let input = r#"Logical Props(
            name: String,
            value: Int64
        )"#;

        let props = parse_props_from_str(input);
        assert_eq!(props.fields.len(), 2);
        assert_eq!(props.fields[0].name, "name");
        assert!(matches!(props.fields[0].ty, Type::String));
        assert_eq!(props.fields[1].name, "value");
        assert!(matches!(props.fields[1].ty, Type::Int64));
    }

    #[test]
    fn test_parse_array_property() {
        let input = r#"Logical Props (
            values: [Int64]
        )"#;

        let props = parse_props_from_str(input);
        assert_eq!(props.fields.len(), 1);
        assert_eq!(props.fields[0].name, "values");
        match &props.fields[0].ty {
            Type::Array(inner) => assert!(matches!(**inner, Type::Int64)),
            _ => panic!("Expected array type"),
        }
    }

    #[test]
    fn test_parse_complex_property_types() {
        let input = r#"Logical Props (
            pairs: Map[String, Int64],
            coords: (Int64, Int64)
        )"#;

        let props = parse_props_from_str(input);
        assert_eq!(props.fields.len(), 2);

        // Check Map type
        assert_eq!(props.fields[0].name, "pairs");
        match &props.fields[0].ty {
            Type::Map(key, value) => {
                assert!(matches!(**key, Type::String));
                assert!(matches!(**value, Type::Int64));
            }
            _ => panic!("Expected map type"),
        }

        // Check Tuple type
        assert_eq!(props.fields[1].name, "coords");
        match &props.fields[1].ty {
            Type::Tuple(types) => {
                assert_eq!(types.len(), 2);
                assert!(matches!(types[0], Type::Int64));
                assert!(matches!(types[1], Type::Int64));
            }
            _ => panic!("Expected tuple type"),
        }
    }

    #[test]
    #[should_panic]
    fn test_parse_invalid_property() {
        let input = r#"Logical Props (
            invalid
        )"#;

        parse_props_from_str(input);
    }
}

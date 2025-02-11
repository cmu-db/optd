use pest::iterators::Pair;

use super::{expr::parse_expr, Rule};
use crate::dsl::ast::upper_layer::{Literal, Pattern};

/// Parse a pattern from a pest Pair
///
/// # Arguments
/// * `pair` - The pest Pair containing the pattern
///
/// # Returns
/// * `Pattern` - The parsed pattern AST node
pub fn parse_pattern(pair: Pair<'_, Rule>) -> Pattern {
    match pair.as_rule() {
        Rule::pattern => {
            let mut pairs = pair.into_inner();
            let first = pairs.next().unwrap();

            if first.as_rule() == Rule::identifier {
                if let Some(at_pattern) = pairs.next() {
                    // This is a binding pattern (x @ pattern)
                    let name = first.as_str().to_string();
                    let pattern = parse_pattern(at_pattern);
                    Pattern::Bind(name, Box::new(pattern))
                } else {
                    // This is a simple variable pattern
                    Pattern::Var(first.as_str().to_string())
                }
            } else {
                parse_pattern(first)
            }
        }
        Rule::constructor_pattern => parse_constructor_pattern(pair),
        Rule::literal_pattern => parse_literal_pattern(pair),
        Rule::wildcard_pattern => Pattern::Wildcard,
        _ => unreachable!("Unexpected pattern rule: {:?}", pair.as_rule()),
    }
}

/// Parse a constructor pattern (e.g., Some(x), Node(left, right))
fn parse_constructor_pattern(pair: Pair<'_, Rule>) -> Pattern {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let mut subpatterns = Vec::new();

    if let Some(fields) = pairs.next() {
        for field in fields.into_inner() {
            if field.as_rule() == Rule::pattern_field {
                let mut field_pairs = field.into_inner();
                let first = field_pairs.next().unwrap();

                if first.as_rule() == Rule::identifier {
                    // Check for field binding (identifier : pattern)
                    if let Some(colon_pattern) = field_pairs.next() {
                        let field_name = first.as_str().to_string();
                        let pattern = parse_pattern(colon_pattern);
                        subpatterns.push(Pattern::Bind(field_name, Box::new(pattern)));
                    } else {
                        // Simple identifier pattern
                        subpatterns.push(Pattern::Var(first.as_str().to_string()));
                    }
                } else {
                    // Regular pattern
                    subpatterns.push(parse_pattern(first));
                }
            }
        }
    }

    Pattern::Constructor(name, subpatterns)
}

/// Parse a literal pattern (numbers, strings, booleans, arrays, tuples)
fn parse_literal_pattern(pair: Pair<'_, Rule>) -> Pattern {
    let literal = parse_literal(pair.into_inner().next().unwrap());
    Pattern::Literal(literal)
}

/// Parse a literal value
fn parse_literal(pair: Pair<'_, Rule>) -> Literal {
    match pair.as_rule() {
        Rule::number => Literal::Int64(pair.as_str().parse().unwrap()),
        Rule::string => Literal::String(pair.as_str().to_string()),
        Rule::boolean => Literal::Bool(pair.as_str().parse().unwrap()),
        Rule::array_literal => {
            let exprs = pair.into_inner().map(parse_expr).collect();
            Literal::Array(exprs)
        }
        Rule::tuple_literal => {
            let exprs = pair.into_inner().map(parse_expr).collect();
            Literal::Tuple(exprs)
        }
        _ => unreachable!("Unexpected literal rule: {:?}", pair.as_rule()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::parser::DslParser;
    use pest::Parser;

    fn parse_pattern_from_str(input: &str) -> Pattern {
        let pair = DslParser::parse(Rule::pattern, input)
            .unwrap()
            .next()
            .unwrap();
        parse_pattern(pair)
    }

    #[test]
    fn test_parse_constructor_pattern() {
        match parse_pattern_from_str("Some(x)") {
            Pattern::Constructor(name, patterns) => {
                assert_eq!(name, "Some");
                assert_eq!(patterns.len(), 1);
                match &patterns[0] {
                    Pattern::Var(var_name) => assert_eq!(var_name, "x"),
                    _ => panic!("Expected variable pattern inside constructor"),
                }
            }
            _ => panic!("Expected constructor pattern"),
        }
    }

    #[test]
    fn test_parse_literal_pattern() {
        match parse_pattern_from_str("42") {
            Pattern::Literal(Literal::Int64(n)) => assert_eq!(n, 42),
            _ => panic!("Expected integer literal pattern"),
        }

        match parse_pattern_from_str("\"hello\"") {
            Pattern::Literal(Literal::String(s)) => assert_eq!(s, "\"hello\""),
            _ => panic!("Expected string literal pattern"),
        }

        match parse_pattern_from_str("true") {
            Pattern::Literal(Literal::Bool(b)) => assert!(b),
            _ => panic!("Expected boolean literal pattern"),
        }
    }

    #[test]
    fn test_parse_wildcard_pattern() {
        assert!(matches!(parse_pattern_from_str("_"), Pattern::Wildcard));
    }

    #[test]
    fn test_parse_binding_pattern() {
        match parse_pattern_from_str("x @ Some(y)") {
            Pattern::Bind(name, pattern) => {
                assert_eq!(name, "x");
                match *pattern {
                    Pattern::Constructor(cname, patterns) => {
                        assert_eq!(cname, "Some");
                        assert_eq!(patterns.len(), 1);
                        match &patterns[0] {
                            Pattern::Var(var_name) => assert_eq!(var_name, "y"),
                            _ => panic!("Expected variable pattern inside constructor"),
                        }
                    }
                    _ => panic!("Expected constructor pattern after binding"),
                }
            }
            _ => panic!("Expected binding pattern"),
        }
    }

    #[test]
    fn test_parse_complex_constructor_pattern() {
        match parse_pattern_from_str("Node(_, right: Some(x))") {
            Pattern::Constructor(name, patterns) => {
                assert_eq!(name, "Node");
                assert_eq!(patterns.len(), 2);

                // Check left pattern
                match &patterns[0] {
                    Pattern::Wildcard => {}
                    _ => panic!("Expected wildcard pattern for left field"),
                }

                // Check right pattern
                match &patterns[1] {
                    Pattern::Bind(name, pattern) => {
                        assert_eq!(name, "right");
                        match **pattern {
                            Pattern::Constructor(ref cname, ref inner_patterns) => {
                                assert_eq!(cname, "Some");
                                assert_eq!(inner_patterns.len(), 1);
                                assert!(matches!(&inner_patterns[0], Pattern::Var(n) if n == "x"));
                            }
                            _ => panic!("Expected Some constructor in right pattern"),
                        }
                    }
                    _ => panic!("Expected binding pattern for right field"),
                }
            }
            _ => panic!("Expected complex constructor pattern"),
        }
    }
}

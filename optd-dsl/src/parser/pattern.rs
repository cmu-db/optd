use chumsky::{
    error::Simple,
    prelude::{choice, just, nested_delimiters, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::{Token, ALL_DELIMITERS},
    parser::ast::{Literal, Pattern},
};

pub fn pattern_parser() -> impl Parser<Token, Spanned<Pattern>, Error = Simple<Token, Span>> + Clone
{
    recursive(|pattern_parser| {
        let wildcard = just(Token::UnderScore)
            .map(|_| Pattern::Wildcard)
            .map_with_span(Spanned::new);

        let term_ident = select! { Token::TermIdent(name) => name }.map_with_span(Spanned::new);
        let type_ident = select! {
            Token::TypeIdent(name) => name,
        }
        .map_with_span(Spanned::new);

        let lit_int = select! { Token::Int64(n) => Literal::Int64(n) };
        let lit_float = select! { Token::Float64(f) => Literal::Float64(f) };
        let lit_string = select! { Token::String(s) => Literal::String(s) };
        let lit_bool = select! { Token::Bool(b) => Literal::Bool(b) };
        let lit_unit = just(Token::Unit).map(|_| Literal::Unit);

        let literal = choice((lit_int, lit_float, lit_string, lit_bool, lit_unit))
            .map(Pattern::Literal)
            .map_with_span(Spanned::new);

        let constructor_with_args = type_ident
            .then(
                pattern_parser
                    .clone()
                    .separated_by(just(Token::Comma))
                    .allow_trailing()
                    .map(Some)
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        ALL_DELIMITERS,
                        |_| None,
                    )),
            )
            .map(|(name, patterns)| match patterns {
                Some(patterns) => Pattern::Constructor(name, patterns),
                None => Pattern::Error,
            })
            .map_with_span(Spanned::new);

        let simple_constructor = type_ident
            .map(|name| Pattern::Constructor(name, vec![]))
            .map_with_span(Spanned::new);

        let binding_with_pattern = term_ident
            .then_ignore(just(Token::Colon))
            .then(pattern_parser.clone())
            .map(|(name, pattern)| Pattern::Bind(name, pattern))
            .map_with_span(Spanned::new);

        let simple_binding = term_ident
            .map(|name| {
                let wildcard = Spanned::new(Pattern::Wildcard, name.span.clone());
                Pattern::Bind(name, wildcard)
            })
            .map_with_span(Spanned::new);

        choice((
            wildcard,
            binding_with_pattern,
            constructor_with_args,
            literal,
            simple_constructor,
            simple_binding,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex::lex;
    use chumsky::{prelude::end, Stream};

    fn parse_pattern(input: &str) -> (Option<Spanned<Pattern>>, Vec<Simple<Token, Span>>) {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);

        if let Some(tokens) = tokens {
            pattern_parser()
                .then_ignore(end())
                .parse_recovery(Stream::from_iter(eoi, tokens.into_iter()))
        } else {
            (None, Vec::new())
        }
    }

    #[test]
    fn test_wildcard_pattern() {
        let (result, errors) = parse_pattern("_");

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(pattern) = result {
            match *pattern.value {
                Pattern::Wildcard => {}
                _ => panic!("Expected Wildcard pattern"),
            }
        }
    }

    #[test]
    fn test_binding_with_wildcard() {
        let (result, errors) = parse_pattern("x: _");

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(pattern) = result {
            match *pattern.value {
                Pattern::Bind(name, sub_pattern) => {
                    assert_eq!(*name.value, "x");
                    match *sub_pattern.value {
                        Pattern::Wildcard => {}
                        _ => panic!("Expected Wildcard sub-pattern"),
                    }
                }
                _ => panic!("Expected Bind pattern"),
            }
        }
    }

    #[test]
    fn test_simple_binding() {
        let (result, errors) = parse_pattern("variable");

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(pattern) = result {
            match *pattern.value {
                Pattern::Bind(name, sub_pattern) => {
                    assert_eq!(*name.value, "variable");
                    match *sub_pattern.value {
                        Pattern::Wildcard => {}
                        _ => panic!("Expected Wildcard sub-pattern"),
                    }
                }
                _ => panic!("Expected Bind pattern"),
            }
        }
    }

    #[test]
    fn test_constructor_pattern() {
        let (result, errors) = parse_pattern("Some(x)");

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(pattern) = result {
            match *pattern.value {
                Pattern::Constructor(name, patterns) => {
                    assert_eq!(*name.value, "Some");
                    assert_eq!(patterns.len(), 1);
                }
                _ => panic!("Expected Constructor pattern"),
            }
        }
    }

    #[test]
    fn test_literal_pattern() {
        // Int literal
        let (result, errors) = parse_pattern("42");
        assert!(
            result.is_some(),
            "Expected successful parse for int literal"
        );
        assert!(errors.is_empty(), "Expected no errors for int literal");

        if let Some(pattern) = result {
            match *pattern.value {
                Pattern::Literal(Literal::Int64(n)) => {
                    assert_eq!(n, 42);
                }
                _ => panic!("Expected Int64 literal pattern"),
            }
        }
    }

    #[test]
    fn test_complex_nested_patterns() {
        let pattern_str = "Expression(
            left: BinaryOp(
                left: Literal(value: 42),
                op: Plus,
                right: FunctionCall(
                    name: \"calculate\",
                    arg1: x,
                    arg2: Pair(first: 3.14, second: true)
                )
            ),
            right: Match(
                scrutinee: Variable(name: \"input\"),
                case1: Some(Value(value: _)),
                case2: None
            )
        )";

        let (result, errors) = parse_pattern(pattern_str);

        assert!(
            errors.is_empty(),
            "Expected no errors, but got: {:?}",
            errors
        );
        assert!(result.is_some(), "Expected successful parse");

        if let Some(pattern) = result {
            match &*pattern.value {
                Pattern::Constructor(name, args) => {
                    assert_eq!(*name.value, "Expression");
                    assert_eq!(args.len(), 2, "Expected 2 arguments to Expression");

                    match &*args[0].value {
                        Pattern::Bind(name, _) => {
                            assert_eq!(*name.value, "left");
                        }
                        _ => panic!("Expected binding for left argument"),
                    }

                    match &*args[1].value {
                        Pattern::Bind(name, _) => {
                            assert_eq!(*name.value, "right");
                        }
                        _ => panic!("Expected binding for right argument"),
                    }
                }
                _ => panic!("Expected Constructor pattern at top level"),
            }
        }
    }
}

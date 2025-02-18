use chumsky::{
    error::Simple,
    prelude::{choice, just, nested_delimiters},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::{Token, ALL_DELIMITERS},
    parser::r#type::type_parser,
};

use super::ast::{Field, Operator, PhysicalOp, ScalarOp};

fn field_parser() -> impl Parser<Token, Spanned<Field>, Error = Simple<Token, Span>> + Clone {
    select! { Token::TermIdent(name) => name }
        .map_with_span(Spanned::new)
        .then_ignore(just(Token::Colon))
        .then(type_parser())
        .map(|(name, ty)| Field { name, ty })
        .map_with_span(Spanned::new)
}

fn fields_list_parser(
) -> impl Parser<Token, Vec<Spanned<Field>>, Error = Simple<Token, Span>> + Clone {
    field_parser()
        .separated_by(just(Token::Comma))
        .allow_trailing()
        .map(Some)
        .delimited_by(just(Token::LParen), just(Token::RParen))
        .recover_with(nested_delimiters(
            Token::LParen,
            Token::RParen,
            ALL_DELIMITERS,
            |_| None,
        ))
        .map(|x| x.unwrap_or(vec![]))
}

pub fn operator_parser(
) -> impl Parser<Token, Spanned<Operator>, Error = Simple<Token, Span>> + Clone {
    let op_type = choice((just(Token::Scalar), just(Token::Physical)));

    let type_ident = select! { Token::TypeIdent(name) => name }.map_with_span(Spanned::new);

    op_type
        .then(type_ident)
        .then(fields_list_parser())
        .then_ignore(just(Token::Semi))
        .map(|((op_type, name), fields)| {
            let operator = match op_type {
                Token::Scalar => Operator::Scalar(ScalarOp { name, fields }),
                Token::Physical => Operator::Physical(PhysicalOp { name, fields }),
                _ => unreachable!(),
            };
            operator
        })
        .map_with_span(Spanned::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lexer::lex::lex, parser::ast::Type};
    use chumsky::Stream;

    fn parse_operator(input: &str) -> (Option<Spanned<Operator>>, Vec<Simple<Token, Span>>) {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);
        operator_parser().parse_recovery(Stream::from_iter(eoi, tokens.unwrap().into_iter()))
    }

    #[test]
    fn test_scalar_operator() {
        let input = "Scalar Add(left: I64, right: I64);";
        let (result, errors) = parse_operator(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Operator::Scalar(ScalarOp { fields, .. }) => {
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("Expected Scalar operator"),
        }
    }

    #[test]
    fn test_physical_operator() {
        let input = "Physical HashJoin(build: Physical, probe: Physical);";
        let (result, errors) = parse_operator(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Operator::Physical(PhysicalOp { fields, .. }) => {
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("Expected Physical operator"),
        }
    }

    #[test]
    fn test_invalid_type_recovery() {
        // With an invalid type (string instead of String)
        let input = "Physical HashJoin(build: Physical, probe: Map[string, String]);";
        let (result, errors) = parse_operator(input);
        println!("Result: {:?}", result);
        println!("Errors: {:?}", errors);

        // We should still get a result despite the errors
        assert!(result.is_some(), "Expected successful recovery");
        assert!(!errors.is_empty(), "Expected errors with invalid type");

        match &*result.unwrap().value {
            Operator::Physical(PhysicalOp { fields, .. }) => {
                assert_eq!(fields.len(), 2);
                let second_field = &fields[1];
                assert_eq!(*second_field.value.ty.value, Type::Error);
            }
            _ => panic!("Expected Physical operator"),
        }
    }
}

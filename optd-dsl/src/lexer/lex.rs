use std::collections::HashMap;

use chumsky::{
    error::Simple,
    prelude::{choice, end, just, none_of, skip_then_retry_until},
    text::{digits, ident, int, TextParser},
    Parser, Stream,
};
use ordered_float::OrderedFloat;

use crate::errors::{span::Span, reporter::Error};

use super::{errors::LexerError, tokens::Token};

/// Lexes a source string into a sequence of tokens with their positions.
/// Uses Chumsky for lexing and Ariadne for error reporting.
///
/// # Arguments
/// * `source` - The source code to lex
/// * `file_name` - Name of the source file, used for error reporting
///
/// # Returns
/// * `(Option<Vec<(Token, Span)>>, Vec<Error>)` - Any successfully lexed tokens and errors
pub fn lex(source: &str, file_name: &str) -> (Option<Vec<(Token, Span)>>, Vec<Error>) {
    let len = source.chars().count();
    let eoi = Span::new(file_name.into(), len..len);

    let (tokens, errors) = lexer().parse_recovery(Stream::from_iter(
        eoi,
        source
            .chars()
            .enumerate()
            .map(|(i, c)| (c, Span::new(file_name.into(), i..i + 1))),
    ));

    let errors = errors
        .into_iter()
        .map(|e| LexerError::new(source.into(), e).into())
        .collect();

    (tokens, errors)
}

/// Creates the lexer parser. Uses immediate error recovery to maximize the number of
/// tokens that can be successfully lexed even in the presence of errors.
fn lexer() -> impl Parser<char, Vec<(Token, Span)>, Error = Simple<char, Span>> {
    let keywords = HashMap::from([
        ("fn", Token::Fn),
        ("I64", Token::TInt64),
        ("F64", Token::TFloat64),
        ("String", Token::TString),
        ("Bool", Token::TBool),
        ("true", Token::Bool(true)),
        ("false", Token::Bool(false)),
        ("Unit", Token::TUnit),
        ("data", Token::Data),
        ("with", Token::With),
        ("as", Token::As),
        ("in", Token::In),
        ("let", Token::Let),
        ("match", Token::Match),
        ("if", Token::If),
        ("then", Token::Then),
        ("else", Token::Else),
        ("fail", Token::Fail),
    ]);

    let ident = ident().map(move |ident: String| {
        if let Some(keyword) = keywords.get(ident.as_str()) {
            keyword.clone()
        } else {
            match ident.chars().next() {
                Some('_') if ident.len() == 1 => Token::UnderScore,
                Some(c) if (!c.is_lowercase() && c.is_ascii_alphabetic()) => {
                    Token::TypeIdent(ident)
                }
                _ => Token::TermIdent(ident),
            }
        }
    });

    let int64 = int::<char, Simple<char, Span>>(10).try_map(|s, span| {
        s.parse::<i64>()
            .map(Token::Int64)
            .map_err(|e| Simple::custom(span, e.to_string()))
    });

    let float64 = choice((
        int::<char, Simple<char, Span>>(10)
            .then(just('.').ignore_then(digits(10)))
            .try_map(|(whole, frac), span| {
                let num_str = format!("{}.{}", whole, frac);
                num_str
                    .parse::<f64>()
                    .map(|num: f64| Token::Float64(OrderedFloat::from(num)))
                    .map_err(|e| Simple::custom(span, e.to_string()))
            }),
        int::<char, Simple<char, Span>>(10)
            .then_ignore(just('f'))
            .try_map(|num, span| {
                num.parse::<f64>()
                    .map(|num: f64| Token::Float64(OrderedFloat::from(num)))
                    .map_err(|e| Simple::custom(span, e.to_string()))
            }),
    ));

    let string = none_of('"')
        .repeated()
        .delimited_by(just('"'), just('"'))
        .collect::<String>()
        .map(Token::String);

    let op = choice((
        just("==").to(Token::EqEq),
        just("!=").to(Token::NotEq),
        just(">=").to(Token::GreaterEq),
        just("<=").to(Token::LessEq),
        just("->").to(Token::Arrow),
        just("&&").to(Token::And),
        just("||").to(Token::Or),
        just("++").to(Token::Concat),
        just("..").to(Token::Range),
        just("()").to(Token::Unit),
        just("+").to(Token::Plus),
        just("-").to(Token::Minus),
        just("*").to(Token::Mul),
        just("/").to(Token::Div),
        just("=").to(Token::Eq),
        just(">").to(Token::Greater),
        just("<").to(Token::Less),
        just("!").to(Token::Not),
    ));

    let delimiter = choice((
        just("(").to(Token::LParen),
        just(")").to(Token::RParen),
        just("{").to(Token::LBrace),
        just("}").to(Token::RBrace),
        just("[").to(Token::LBracket),
        just("]").to(Token::RBracket),
        just("|").to(Token::Vertical),
        just("\\").to(Token::Backward),
        just(",").to(Token::Comma),
        just(".").to(Token::Dot),
        just(":").to(Token::Colon),
    ));

    let comments = just("//")
        .then(none_of('\n').repeated())
        .padded()
        .ignored()
        .repeated();

    let token = choice((ident, float64, int64, string, op, delimiter))
        .map_with_span(|token, span| (token, span))
        .padded()
        .recover_with(skip_then_retry_until([]));

    token.padded_by(comments).repeated().then_ignore(end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword() {
        let (maybe_tokens, errors) = lex("if then else", "test.txt");
        assert!(errors.is_empty());
        let tokens = maybe_tokens.unwrap();
        assert!(tokens.contains(&(Token::If, Span::new("test.txt".into(), 0..2))));
        assert!(tokens.contains(&(Token::Then, Span::new("test.txt".into(), 3..7))));
        assert!(tokens.contains(&(Token::Else, Span::new("test.txt".into(), 8..12))));
    }

    #[test]
    fn test_integer_overflow() {
        let (_, errors) = lex("9223372036854775808", "test.txt");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_unterminated_string() {
        let (_, errors) = lex("\"unterminated", "test.txt");
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_valid_tokens() {
        let (maybe_tokens, errors) =
            lex("if (x == 42) { print(\"hello\") } // comment", "test.txt");
        assert!(errors.is_empty());
        let tokens = maybe_tokens.unwrap();
        assert!(tokens.contains(&(Token::If, Span::new("test.txt".into(), 0..2))));
        assert!(tokens.contains(&(Token::LParen, Span::new("test.txt".into(), 3..4))));
        assert!(tokens.contains(&(
            Token::TermIdent("x".to_string()),
            Span::new("test.txt".into(), 4..5)
        )));
        assert!(tokens.contains(&(Token::EqEq, Span::new("test.txt".into(), 6..8))));
        assert!(tokens.contains(&(Token::Int64(42), Span::new("test.txt".into(), 9..11))));
        assert!(tokens.contains(&(Token::RParen, Span::new("test.txt".into(), 11..12))));
        assert!(tokens.contains(&(Token::LBrace, Span::new("test.txt".into(), 13..14))));
        assert!(tokens.contains(&(
            Token::TermIdent("print".to_string()),
            Span::new("test.txt".into(), 15..20)
        )));
        assert!(tokens.contains(&(
            Token::String("hello".to_string()),
            Span::new("test.txt".into(), 21..28)
        )));
        assert!(tokens.contains(&(Token::RBrace, Span::new("test.txt".into(), 30..31))));
    }

    #[test]
    fn test_recovery_after_error() {
        let (maybe_tokens, errors) = lex("1010 let x = 99999999++&; let y = 42; &_&&", "test.txt");

        assert!(!errors.is_empty());
        if let Some(tokens) = maybe_tokens {
            assert!(tokens.contains(&(Token::Int64(42), Span::new("test.txt".into(), 34..36))));
        } else {
            panic!("Expected some tokens even after errors");
        }
    }

    #[test]
    fn test_underscore_token() {
        let (maybe_tokens, errors) = lex("_ _var Under_score", "test.txt");
        assert!(errors.is_empty());
        let tokens = maybe_tokens.unwrap();

        assert!(tokens.contains(&(Token::UnderScore, Span::new("test.txt".into(), 0..1))));
        assert!(tokens.contains(&(
            Token::TermIdent("_var".to_string()),
            Span::new("test.txt".into(), 2..6)
        )),);
        assert!(tokens.contains(&(
            Token::TypeIdent("Under_score".to_string()),
            Span::new("test.txt".into(), 7..18)
        )));
    }
}

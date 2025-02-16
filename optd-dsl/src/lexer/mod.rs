use ariadne::{Color, Label, Report, ReportKind, Source};
use chumsky::{
    error::Simple,
    prelude::{choice, end, just, none_of, skip_then_retry_until},
    text::{digits, ident, int, TextParser},
    Parser,
};
use std::{collections::HashMap, ops::Range};

pub mod tokens;
use tokens::Token;

/// A token with its span in the source code
pub type TokenSpan = (Token, Range<usize>);
/// Lexer-specific error type
pub type LexerError = Simple<char>;

/// Lexes a source string into a sequence of tokens with their positions.
/// Uses Chumsky for lexing and Ariadne for error reporting.
///
/// # Arguments
/// * `source` - The source code to lex
/// * `file_name` - Name of the source file, used for error reporting
///
/// # Returns
/// * `Ok(Vec<TokenSpan>)` - Successfully lexed tokens with their spans
/// * `Err(Vec<LexerError>)` - Lexing errors, which are also reported via Ariadne
pub fn lex(source: &str, file_name: &str) -> Result<Vec<TokenSpan>, Vec<LexerError>> {
    let result = lexer().parse(source);

    match result {
        Ok(tokens) => Ok(tokens),
        Err(errors) => {
            /*for error in errors.clone() {
                let reason = match &error.reason() {
                    chumsky::error::SimpleReason::Custom(msg) => msg.clone(),
                    _ => error.to_string(),
                };

                Report::build(ReportKind::Error, (file_name, error.span()))
                    .with_message("Lexer error")
                    .with_label(
                        Label::new((file_name, error.span()))
                            .with_message(&reason)
                            .with_color(Color::Red),
                    )
                    .finish()
                    .eprint((file_name, Source::from(source)))
                    .unwrap();
            }*/
            Err(errors)
        }
    }
}

/// Creates the lexer parser. Uses immediate error recovery to maximize the number of
/// tokens that can be successfully lexed even in the presence of errors.
fn lexer() -> impl Parser<char, Vec<TokenSpan>, Error = LexerError> {
    let keywords = HashMap::from([
        ("Scalar", Token::Scalar),
        ("Logical", Token::Logical),
        ("Physical", Token::Physical),
        ("Props", Token::Properties),
        ("val", Token::Val),
        ("match", Token::Match),
        ("case", Token::Case),
        ("if", Token::If),
        ("then", Token::Then),
        ("else", Token::Else),
        ("true", Token::Bool(true)),
        ("false", Token::Bool(false)),
        ("derive", Token::Derive),
    ]);

    let ident = ident().map(move |ident: String| {
        keywords
            .get(&ident as &str)
            .cloned()
            .unwrap_or(Token::Identifier(ident))
    });

    let int64 = int::<char, LexerError>(10).try_map(|s, span| {
        s.parse::<i64>()
            .map(Token::Int64)
            .map_err(|e| Simple::custom(span, e.to_string()))
    });

    let float64 = choice((
        int::<char, LexerError>(10)
            .then(just('.').ignore_then(digits(10)))
            .try_map(|(whole, frac), span| {
                let num_str = format!("{}.{}", whole, frac);
                num_str
                    .parse::<f64>()
                    .map(Token::Float64)
                    .map_err(|e| Simple::custom(span, e.to_string()))
            }),
        int::<char, LexerError>(10)
            .then_ignore(just('f'))
            .try_map(|num, span| {
                num.parse::<f64>()
                    .map(Token::Float64)
                    .map_err(|e| Simple::custom(span, e.to_string()))
            }),
    ));

    let string = none_of('"')
        .repeated()
        .delimited_by(just('"'), just('"'))
        .collect::<String>()
        .map(Token::String)
        .labelled("string");

    let op = choice((
        just("==").to(Token::EqEq),
        just("!=").to(Token::NotEq),
        just(">=").to(Token::GreaterEq),
        just("<=").to(Token::LessEq),
        just("=>").to(Token::Arrow),
        just("&&").to(Token::And),
        just("||").to(Token::Or),
        just("++").to(Token::Concat),
        just("..").to(Token::Range),
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
        just(",").to(Token::Comma),
        just(".").to(Token::Dot),
        just(";").to(Token::Semi),
        just(":").to(Token::Colon),
        just("@").to(Token::At),
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

    fn test_lex(input: &str) -> Result<Vec<TokenSpan>, Vec<LexerError>> {
        lex(input, "test.txt")
    }

    #[test]
    fn test_keyword() {
        let result = test_lex("if then else");
        assert!(result.is_ok());
        let tokens = result.unwrap();
        assert!(tokens.contains(&(Token::If, 0..2)));
        assert!(tokens.contains(&(Token::Then, 3..7)));
        assert!(tokens.contains(&(Token::Else, 8..12)));
    }

    #[test]
    fn test_integer_overflow() {
        let result = test_lex("9223372036854775808");
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_string() {
        assert!(test_lex("\"unterminated").is_err());
    }

    #[test]
    fn test_valid_tokens() {
        let result = test_lex("if (x == 42) { print(\"hello\"); } // comment").unwrap();
        assert!(result.contains(&(Token::If, 0..2)));
        assert!(result.contains(&(Token::EqEq, 6..8)));
        assert!(result.contains(&(Token::Int64(42), 9..11)));
        assert!(result.contains(&(Token::String("hello".to_string()), 21..28)));
    }

    #[test]
    fn test_recovery_after_error() {
        let input = "1010 let x = 99999999++&; let y = 42; &&";
        let (maybe_tokens, errors) = lexer().parse_recovery(input);

        assert!(!errors.is_empty());
        if let Some(tokens) = maybe_tokens {
            assert!(tokens.contains(&(Token::Int64(42), 34..36)));
        } else {
            panic!("Expected some tokens even with errors");
        }
    }
}

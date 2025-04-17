use chumsky::{
    Parser,
    error::Simple,
    prelude::{just, nested_delimiters},
    select,
};

use crate::{
    lexer::tokens::{ALL_DELIMITERS, Token},
    utils::span::{Span, Spanned},
};

use super::{ast::Field, types::type_parser};

fn field_parser() -> impl Parser<Token, Spanned<Field>, Error = Simple<Token, Span>> + Clone {
    select! { Token::TermIdent(name) => name }
        .map_with_span(Spanned::new)
        .then_ignore(just(Token::Colon))
        .then(type_parser())
        .map(|(name, ty)| Field { name, ty })
        .map_with_span(Spanned::new)
}

pub(super) fn fields_list_parser()
-> impl Parser<Token, Vec<Spanned<Field>>, Error = Simple<Token, Span>> + Clone {
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
        .map(|x| x.unwrap_or_default())
}

pub(super) fn delimited_parser<T, R, F>(
    parser: impl Parser<Token, T, Error = Simple<Token, Span>> + Clone,
    open: Token,
    close: Token,
    f: F,
) -> impl Parser<Token, R, Error = Simple<Token, Span>> + Clone
where
    F: Fn(Option<T>) -> R + Clone + 'static,
{
    parser
        .map(Some)
        .delimited_by(just(open.clone()), just(close.clone()))
        .recover_with(nested_delimiters(open, close, ALL_DELIMITERS, |_| None))
        .map(f)
}

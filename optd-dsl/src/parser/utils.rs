use chumsky::{
    error::Simple,
    prelude::{just, nested_delimiters},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::{Token, ALL_DELIMITERS},
};

use super::{ast::Field, r#type::type_parser};

fn field_parser() -> impl Parser<Token, Spanned<Field>, Error = Simple<Token, Span>> + Clone {
    select! { Token::TermIdent(name) => name }
        .map_with_span(Spanned::new)
        .then_ignore(just(Token::Colon))
        .then(type_parser())
        .map(|(name, ty)| Field { name, ty })
        .map_with_span(Spanned::new)
}

pub(super) fn fields_list_parser(
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

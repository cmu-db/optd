use chumsky::{
    error::Simple,
    prelude::{choice, just, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::Token,
};

use super::{
    ast::{Field, Function, Identifier, Module, Properties, Type},
    types::type_parser,
};

pub fn ident_parser() -> impl Parser<Token, Spanned<Identifier>, Error = Simple<Token, Span>> + Clone
{
    select! {
        Token::TermIdent(ident) => ident,
    }
    .map_with_span(Spanned::new)
}

pub fn field_parser() -> impl Parser<Token, Spanned<Field>, Error = Simple<Token, Span>> + Clone {
    ident_parser()
        .then_ignore(just(Token::Colon))
        .then(type_parser())
        .map_with_span(|(name, ty), span| Spanned::new(Field { name, ty }, span))
}

/*pub fn properties_parser(
    typ: Token,
) -> impl Parser<Token, Properties, Error = Simple<Token>> + Clone {
    just(Token::And).map(|_| Properties { props: todo!() })
}*/

/* pub fn type_parser() -> impl Parser<Token, TypeVariant, Error = Simple<Token, Span>> + Clone {
    just(Token::Type)
        .ignore_then(type_ident_parser())
        .then(
            just(Token::LParen)
                .ignore_then(properties_parser())
                .then_ignore(just(Token::RParen))
                .or(just(Token::Horizontal).map(|_| Properties { props: vec![] })),
        )
        .map(|(name, props)| TypeVariant { name, props })
}

pub fn operator_parser() -> impl Parser<Token, Operator, Error = Simple<Token>> + Clone {
    just(Token::And).map(|_| {
        Operator::Scalar(ScalarOp {
            name: todo!(),
            fields: todo!(),
        })
    })
}*/

/*pub fn function_parser() -> impl Parser<Token, Function, Error = Simple<Token>> + Clone {
    just(Token::And).map(|_| Function {
        name: todo!(),
        params: todo!(),
        return_type: todo!(),
        body: todo!(),
        annotation: todo!(),
    })
}

pub fn module_parser() -> impl Parser<Token, Module, Error = Simple<Token>> + Clone {
    just(Token::And).map(|_| Module {
        logical_props: todo!(),
        physical_props: todo!(),
        types: todo!(),
        operators: todo!(),
        functions: todo!(),
    })
}
*/

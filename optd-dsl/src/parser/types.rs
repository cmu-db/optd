use chumsky::{
    error::Simple,
    prelude::{choice, just, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::Token,
};

use super::ast::Type;

pub fn type_parser() -> impl Parser<Token, Spanned<Type>, Error = Simple<Token, Span>> + Clone {
    recursive(|type_parser| {
        // Atomic types (everything except closures)
        let atom = {
            let base_type = select! {
                Token::TInt64 => Type::Int64,
                Token::TString => Type::String,
                Token::TBool => Type::Bool,
                Token::TFloat64 => Type::Float64,
                Token::Unit => Type::Unit,
                Token::Scalar => Type::Scalar,
                Token::Logical => Type::Logical,
                Token::Physical => Type::Physical,
            }
            .map_with_span(Spanned::new);

            let array_type = type_parser
                .clone()
                .delimited_by(just(Token::LBracket), just(Token::RBracket))
                .map_with_span(|elem_type, span| Spanned::new(Type::Array(elem_type), span));

            let map_type = just(Token::Map)
                .ignore_then(
                    type_parser
                        .clone()
                        .then_ignore(just(Token::Comma))
                        .then(type_parser.clone())
                        .delimited_by(just(Token::LBracket), just(Token::RBracket)),
                )
                .map_with_span(|(key_type, val_type), span| {
                    Spanned::new(Type::Map(key_type, val_type), span)
                });

            let tuple_type = type_parser
                .clone()
                .separated_by(just(Token::Comma))
                .allow_trailing()
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .map_with_span(|types, span| Spanned::new(Type::Tuple(types), span));

            let custom_type = select! {
                Token::TypeIdent(name) => name,
            }
            .map_with_span(|name, span: Span| {
                Spanned::new(Type::Custom(Spanned::new(name, span.clone()), vec![]), span)
            });

            // Group atomic types with parentheses for precedence
            let atom_with_parens = just(Token::LParen)
                .ignore_then(type_parser.clone())
                .then_ignore(just(Token::RParen));

            choice((
                atom_with_parens,
                map_type,
                array_type,
                tuple_type,
                custom_type,
                base_type,
            ))
        };

        // A type is either an atom or a closure (right associative)
        atom.clone()
            .then(
                just(Token::Arrow)
                    .ignore_then(type_parser)
                    .map_with_span(|ret, span| (ret, span))
                    .or_not(),
            )
            .map_with_span(|(param_type, maybe_return), span| match maybe_return {
                Some((return_type, _)) => {
                    Spanned::new(Type::Closure(param_type, return_type), span)
                }
                None => param_type,
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex::lex;
    use chumsky::{prelude::end, Stream};

    fn parse_type(input: &str) -> Result<Spanned<Type>, Vec<Simple<Token, Span>>> {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);
        type_parser()
            .then_ignore(end())
            .parse(Stream::from_iter(eoi, tokens.unwrap().into_iter()))
    }

    #[test]
    fn test_base_types() {
        let result = parse_type("I64").unwrap();
        assert!(matches!(*result.value, Type::Int64));

        let result = parse_type("String").unwrap();
        assert!(matches!(*result.value, Type::String));
    }

    #[test]
    fn test_array_types() {
        // Simple array
        let result = parse_type("[I64]").unwrap();
        assert!(matches!(*result.value, Type::Array(t) if matches!(*t.value, Type::Int64)));

        // Nested arrays
        let result = parse_type("[[String]]").unwrap();
        assert!(matches!(*result.value,
            Type::Array(t) if matches!(*t.value.clone(),
                Type::Array(inner) if matches!(*inner.value, Type::String)
            )
        ));
    }

    #[test]
    fn test_map_types() {
        let result = parse_type("Map[String, I64]").unwrap();
        assert!(matches!(*result.value,
            Type::Map(k, v)
            if matches!(*k.value, Type::String)
            && matches!(*v.value, Type::Int64)
        ));
    }

    #[test]
    fn test_tuple_types() {
        let result = parse_type("(I64, String)").unwrap();
        assert!(matches!(*result.value,
            Type::Tuple(t) if t.len() == 2
            && matches!(*t[0].value, Type::Int64)
            && matches!(*t[1].value, Type::String)
        ));
    }

    #[test]
    fn test_closure_types() {
        let result = parse_type("(I64) => String").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::String)
        ));

        let result = parse_type("I64 => String").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::String)
        ));

        // Is right-associative
        let result = parse_type("I64 => String => String").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::Closure(_, _))
            && matches!(*ret.value.clone(),
                Type::Closure(param, ret)
                if matches!(*param.value, Type::String)
                && matches!(*ret.value, Type::String))
        ));
    }

    #[test]
    fn test_complex_type() {
        // Test mix of Map, Array, Tuple, and Closure
        let insane_type = "Map[String, [((I64, [Map[String, Physical]]) => [(Logical, SomeType, (Bool => [Scalar]))])]]";
        let result = parse_type(insane_type).unwrap();

        let map_type = result.value;
        assert!(matches!(*map_type, Type::Map(_, _)));

        if let Type::Map(key_type, val_type) = *map_type {
            assert!(matches!(*key_type.value, Type::String));
            assert!(matches!(*val_type.value, Type::Array(_)));
            if let Type::Array(closure_type) = &*val_type.value {
                assert!(matches!(*closure_type.value, Type::Closure(_, _)));
                if let Type::Closure(param_type, return_type) = &*closure_type.value {
                    assert!(matches!(*param_type.value, Type::Tuple(_)));
                    if let Type::Tuple(param_tuple) = &*param_type.value {
                        assert_eq!(param_tuple.len(), 2);
                        assert!(matches!(*param_tuple[0].value, Type::Int64));
                        assert!(matches!(*param_tuple[1].value, Type::Array(_)));
                        if let Type::Array(map_array) = &*param_tuple[1].value {
                            assert!(matches!(*map_array.value, Type::Map(_, _)));
                            if let Type::Map(map_key, map_val) = &*map_array.value {
                                assert!(matches!(*map_key.value, Type::String));
                                assert!(matches!(*map_val.value, Type::Physical));
                            }
                        }
                    }

                    assert!(matches!(*return_type.value, Type::Array(_)));
                    if let Type::Array(ret_tuple) = &*return_type.value {
                        assert!(matches!(*ret_tuple.value, Type::Tuple(_)));
                        if let Type::Tuple(elements) = &*ret_tuple.value {
                            assert_eq!(elements.len(), 3);
                            assert!(matches!(*elements[0].value, Type::Logical));
                            assert!(
                                matches!(*elements[1].value.clone(), Type::Custom(name, _) if *name.value == "SomeType")
                            );
                            assert!(matches!(*elements[2].value, Type::Closure(_, _)));
                            if let Type::Closure(bool_param, scalar_arr) = &*elements[2].value {
                                assert!(matches!(*bool_param.value, Type::Bool));
                                assert!(matches!(*scalar_arr.value, Type::Array(_)));
                                if let Type::Array(scalar) = &*scalar_arr.value {
                                    assert!(matches!(*scalar.value, Type::Scalar));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Test an even more complex nested type
        let even_more_insane =
            "Map[String, Map[I64, [(Logical => Map[String, [((Bool, [Scalar]) => Physical)]])]]]";
        assert!(parse_type(even_more_insane).is_ok());
    }
}

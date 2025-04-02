use super::{ast::Type, utils::delimited_parser};
use crate::{
    lexer::tokens::Token,
    utils::span::{Span, Spanned},
};
use chumsky::{
    Parser,
    error::Simple,
    prelude::{choice, just, recursive},
    select,
};

/// Creates a parser for type expressions.
///
/// This parser supports:
/// - Primitive types: I64, String, Bool, Float64, Unit
/// - Array types: \[T\]
/// - Tuple types: (T1, T2, ...)
/// - Map types: {K : V}
/// - Function types: T1 -> T2
/// - User-defined types: TypeName
/// - Optional types: T?
/// - Starred types: T*
/// - Dollared types: T$
///
/// The parser follows standard precedence rules and supports
/// arbitrary nesting of type expressions.
pub fn type_parser() -> impl Parser<Token, Spanned<Type>, Error = Simple<Token, Span>> + Clone {
    recursive(|type_parser| {
        let atom = {
            let base_type = select! {
                Token::TInt64 => Type::Int64,
                Token::TString => Type::String,
                Token::TBool => Type::Bool,
                Token::TFloat64 => Type::Float64,
                Token::TUnit => Type::Unit,
            }
            .map_with_span(Spanned::new);

            let array_type = delimited_parser(
                type_parser.clone(),
                Token::LBracket,
                Token::RBracket,
                |inner_type| inner_type.map(Type::Array).unwrap_or(Type::Error),
            )
            .map_with_span(Spanned::new);

            let tuple_type = delimited_parser(
                choice((
                    type_parser
                        .clone()
                        .separated_by(just(Token::Comma))
                        .allow_trailing()
                        .at_least(2),
                    type_parser
                        .clone()
                        .then_ignore(just(Token::Comma))
                        .map(|t| vec![t]),
                )),
                Token::LParen,
                Token::RParen,
                |types_opt| types_opt.map(Type::Tuple).unwrap_or(Type::Error),
            )
            .map_with_span(Spanned::new);

            let map_type = delimited_parser(
                type_parser
                    .clone()
                    .then_ignore(just(Token::Colon))
                    .then(type_parser.clone()),
                Token::LBrace,
                Token::RBrace,
                |pair_opt| match pair_opt {
                    Some((key_type, val_type)) => Type::Map(key_type, val_type),
                    None => Type::Error,
                },
            )
            .map_with_span(Spanned::new);

            let data_type = select! { Token::TypeIdent(name) => Type::Identifier(name) }
                .map_with_span(Spanned::new);

            // Note: cannot apply delimiter recovery, as its recovery
            // would block further successful parses (e.g. tuples).
            let atom_with_parens = just(Token::LParen)
                .ignore_then(type_parser.clone())
                .then_ignore(just(Token::RParen));

            choice((
                atom_with_parens,
                data_type,
                map_type,
                array_type,
                tuple_type,
                base_type,
            ))
        };

        // Process function types
        let function_type = atom
            .then(
                just(Token::SmallArrow)
                    .ignore_then(type_parser)
                    .map_with_span(|ret, span| (ret, span))
                    .or_not(),
            )
            .map_with_span(|(param_type, maybe_return), span| match maybe_return {
                Some((return_type, _)) => {
                    Spanned::new(Type::Closure(param_type, return_type), span)
                }
                None => param_type,
            });

        // Process optional, starred, and dollared types
        function_type
            .then(
                choice((just(Token::Question), just(Token::Mul), just(Token::Dollar)))
                    .repeated()
                    .at_least(0)
                    .collect::<Vec<_>>(),
            )
            .map_with_span(|(base_type, modifiers), span| {
                let mut result = base_type;
                for modifier in modifiers {
                    result = match modifier {
                        Token::Question => Spanned::new(Type::Questioned(result), span.clone()),
                        Token::Mul => Spanned::new(Type::Starred(result), span.clone()),
                        Token::Dollar => Spanned::new(Type::Dollared(result), span.clone()),
                        _ => unreachable!("Invalid type modifier"),
                    };
                }
                result
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex::lex;
    use chumsky::{Stream, prelude::end};

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
        let result = parse_type("{String : I64}").unwrap();
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
    fn test_singleton_tuple() {
        let result = parse_type("(I64,)").unwrap();
        assert!(matches!(*result.value,
            Type::Tuple(t) if t.len() == 1
            && matches!(*t[0].value, Type::Int64)
        ));

        let result = parse_type("(I64)").unwrap();
        assert!(matches!(*result.value, Type::Int64));

        let result = parse_type("[(I64,)]").unwrap();
        if let Type::Array(array_t) = &*result.value {
            if let Type::Tuple(inner) = &*array_t.value {
                assert_eq!(inner.len(), 1);
                assert!(matches!(*inner[0].value, Type::Int64));
            } else {
                panic!("Expected Tuple type, got {:?}", array_t.value);
            }
        } else {
            panic!("Expected Array type, got {:?}", result.value);
        }
    }

    #[test]
    fn test_closure_types() {
        let result = parse_type("(I64) -> String").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::String)
        ));

        let result = parse_type("I64 -> String").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::String)
        ));

        // Is right-associative
        let result = parse_type("I64 -> String -> String").unwrap();
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
    fn test_optional_types() {
        // Basic optional types
        let result = parse_type("I64?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.value, Type::Int64)
        ));

        let result = parse_type("String?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.value, Type::String)
        ));

        // Nested optional types
        let result = parse_type("I64??").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.clone().value,
                Type::Questioned(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        // Complex types with optional
        let result = parse_type("[I64]?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.clone().value,
                Type::Array(arr_inner) if matches!(*arr_inner.value, Type::Int64)
            )
        ));

        let result = parse_type("(I64, String)?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.value, Type::Tuple(_))
        ));

        // Function return type is optional
        let result = parse_type("I64 -> String?").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::Questioned(_))
            && matches!(*ret.value.clone(),
                Type::Questioned(inner) if matches!(*inner.value, Type::String))
        ));

        // Entire function type is optional
        let result = parse_type("(I64 -> String)?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.value, Type::Closure(_, _))
        ));

        // Optional map type
        let result = parse_type("{String : I64}?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.value, Type::Map(_, _))
        ));
    }

    #[test]
    fn test_starred_types() {
        // Basic starred types
        let result = parse_type("I64*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.value, Type::Int64)
        ));

        let result = parse_type("String*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.value, Type::String)
        ));

        // Nested starred types
        let result = parse_type("I64**").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.clone().value,
                Type::Starred(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        // Complex types with starred
        let result = parse_type("[I64]*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.clone().value,
                Type::Array(arr_inner) if matches!(*arr_inner.value, Type::Int64)
            )
        ));

        let result = parse_type("(I64, String)*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.value, Type::Tuple(_))
        ));

        // Function return type is starred
        let result = parse_type("I64 -> String*").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::Starred(_))
            && matches!(*ret.value.clone(),
                Type::Starred(inner) if matches!(*inner.value, Type::String))
        ));

        // Entire function type is starred
        let result = parse_type("(I64 -> String)*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.value, Type::Closure(_, _))
        ));
    }

    #[test]
    fn test_dollared_types() {
        // Basic dollared types
        let result = parse_type("I64$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.value, Type::Int64)
        ));

        let result = parse_type("String$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.value, Type::String)
        ));

        // Nested dollared types
        let result = parse_type("I64$$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.clone().value,
                Type::Dollared(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        // Complex types with dollared
        let result = parse_type("[I64]$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.clone().value,
                Type::Array(arr_inner) if matches!(*arr_inner.value, Type::Int64)
            )
        ));

        let result = parse_type("(I64, String)$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.value, Type::Tuple(_))
        ));

        // Function return type is dollared
        let result = parse_type("I64 -> String$").unwrap();
        assert!(matches!(*result.value,
            Type::Closure(param, ret)
            if matches!(*param.value, Type::Int64)
            && matches!(*ret.value, Type::Dollared(_))
            && matches!(*ret.value.clone(),
                Type::Dollared(inner) if matches!(*inner.value, Type::String))
        ));

        // Entire function type is dollared
        let result = parse_type("(I64 -> String)$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.value, Type::Closure(_, _))
        ));
    }

    #[test]
    fn test_mixed_modifiers() {
        // Test combination of modifiers
        let result = parse_type("I64?*").unwrap();
        assert!(matches!(*result.value,
            Type::Starred(inner) if matches!(*inner.clone().value,
                Type::Questioned(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        let result = parse_type("I64*$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(inner) if matches!(*inner.clone().value,
                Type::Starred(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        let result = parse_type("I64$?").unwrap();
        assert!(matches!(*result.value,
            Type::Questioned(inner) if matches!(*inner.clone().value,
                Type::Dollared(inner_inner) if matches!(*inner_inner.value, Type::Int64)
            )
        ));

        // Complex mixed type
        let result = parse_type("(I64 -> String?)*$").unwrap();
        assert!(matches!(*result.value,
            Type::Dollared(outer) if matches!(*outer.clone().value,
                Type::Starred(inner) if matches!(*inner.value, Type::Closure(_, _))
            )
        ));
    }

    #[test]
    fn test_complex_type() {
        // Test mix of Map, Array, Tuple, and Closure
        let insane_type = "{String : [((I64, [{String : Physical}]) -> [(AdtType, LogicalProps, (Bool -> [Scalar]))])]}";
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
                                assert!(matches!(*map_val.value, Type::Identifier(_)));
                            }
                        }
                    }

                    assert!(matches!(*return_type.value, Type::Array(_)));
                    if let Type::Array(ret_tuple) = &*return_type.value {
                        assert!(matches!(*ret_tuple.value, Type::Tuple(_)));
                        if let Type::Tuple(elements) = &*ret_tuple.value {
                            assert_eq!(elements.len(), 3);
                            assert!(matches!(*elements[0].value, Type::Identifier(_)));
                            assert!(matches!(*elements[1].value, Type::Identifier(_)));
                            assert!(matches!(*elements[2].value, Type::Closure(_, _)));
                            if let Type::Closure(bool_param, scalar_arr) = &*elements[2].value {
                                assert!(matches!(*bool_param.value, Type::Bool));
                                assert!(matches!(*scalar_arr.value, Type::Array(_)));
                                if let Type::Array(scalar) = &*scalar_arr.value {
                                    assert!(matches!(*scalar.value, Type::Identifier(_)));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Test an even more insane nested type with optionals, starred, and dollared
        let complex_type = "{String*? : {I64$ : [(Logical -> {String : [((Bool?, [Scalar]$) -> Physical?*)]?}$)]}}*$?";
        assert!(parse_type(complex_type).is_ok());
    }
}

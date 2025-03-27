use chumsky::{
    Parser,
    error::Simple,
    prelude::{choice, just, recursive},
    select,
};

use crate::{
    lexer::tokens::Token,
    parser::ast::Adt,
    utils::span::{Span, Spanned},
};

use super::utils::fields_list_parser;

pub fn adt_parser() -> impl Parser<Token, Spanned<Adt>, Error = Simple<Token, Span>> + Clone {
    just(Token::Data).ignore_then(recursive(|inner_adt_parser| {
        let type_ident = select! { Token::TypeIdent(name) => name }.map_with_span(Spanned::new);

        let product_parser = type_ident
            .then(fields_list_parser().or_not())
            .map(|(name, fields)| Adt::Product {
                name,
                fields: fields.unwrap_or_else(Vec::new),
            })
            .map_with_span(Spanned::new);

        let with_sum_parser = type_ident
            .then_ignore(just(Token::Eq))
            .then(
                just(Token::Vertical)
                    .ignore_then(inner_adt_parser.clone())
                    .repeated()
                    .then(just(Token::Backward).ignore_then(inner_adt_parser.clone())),
            )
            .map(|(name, (variants, end_variant))| Adt::Sum {
                name,
                variants: variants
                    .into_iter()
                    .chain(std::iter::once(end_variant))
                    .collect(),
            })
            .map_with_span(Spanned::new);

        choice((with_sum_parser, product_parser))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{lexer::lex::lex, parser::ast::Type};
    use chumsky::{Stream, prelude::end};

    fn parse_adt(input: &str) -> (Option<Spanned<Adt>>, Vec<Simple<Token, Span>>) {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);
        adt_parser()
            .then_ignore(end())
            .parse_recovery(Stream::from_iter(eoi, tokens.unwrap().into_iter()))
    }

    #[test]
    fn test_struct_adt() {
        let input = "data Cool(bla: I64)";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Product { fields, .. } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(*fields[0].value.name.value, "bla");
                assert!(matches!(*fields[0].value.ty.value, Type::Int64));
            }
            _ => panic!("Expected Product ADT"),
        }
    }

    #[test]
    fn test_empty_struct_adt() {
        let input = "data Empty";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Product { fields, .. } => {
                assert_eq!(fields.len(), 0);
            }
            _ => panic!("Expected Product ADT"),
        }
    }

    #[test]
    fn test_enum_adt() {
        let input = "data JoinType =
                         | Inner 
                         \\ Outer";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Sum { variants, .. } => {
                assert_eq!(variants.len(), 2);

                // First variant should be Inner
                match &*variants[0].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Inner");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected Inner to be a Product variant"),
                }

                // Second variant should be Outer
                match &*variants[1].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Outer");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected Outer to be a Product variant"),
                }
            }
            _ => panic!("Expected Sum ADT"),
        }
    }

    #[test]
    fn test_enum_with_struct_variants() {
        let input = "data Shape =
                         | Circle(center: Point, radius: F64)
                         | Rectangle(topLeft: Point, width: F64, height: F64)
                         \\ Triangle(p1: Point, p2: Point, p3: Point)";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Sum { variants, .. } => {
                assert_eq!(variants.len(), 3);

                match &*variants[0].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Circle");
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Circle to be a Product variant"),
                }

                match &*variants[1].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Rectangle");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected Rectangle to be a Product variant"),
                }

                match &*variants[2].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Triangle");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected Triangle to be a Product variant"),
                }
            }
            _ => panic!("Expected Sum ADT"),
        }
    }

    #[test]
    fn test_nested_enum() {
        let input = "data Expression =
                         | Literal =
                             | IntLiteral(value: I64)
                             | BoolLiteral(value: Bool)
                             \\ StringLiteral(value: String)
                         | BinaryOp(left: Expression, op: Operator, right: Expression)
                         \\ Call(func: String, args: [Expression])";

        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Sum { variants, .. } => {
                assert_eq!(variants.len(), 3);

                // Check Literal and its nested enum
                match &*variants[0].value {
                    Adt::Sum { name, variants } => {
                        assert_eq!(*name.value, "Literal");
                        assert_eq!(variants.len(), 3);

                        // Check IntLiteral
                        match &*variants[0].value {
                            Adt::Product { name, fields } => {
                                assert_eq!(*name.value, "IntLiteral");
                                assert_eq!(fields.len(), 1);
                            }
                            _ => panic!("Expected IntLiteral to be a Product variant"),
                        }
                    }
                    _ => panic!("Expected Literal to be an Sum variant"),
                }

                // Check BinaryOp
                match &*variants[1].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "BinaryOp");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected BinaryOp to be a Product variant"),
                }

                // Check Call
                match &*variants[2].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "Call");
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Call to be a Product variant"),
                }
            }
            _ => panic!("Expected Sum ADT"),
        }
    }

    #[test]
    fn test_double_nested_enum() {
        let input = "data Menu =
                         | File =
                             | New =
                                 | Document
                                 | Project
                                 \\ Template
                             | Open
                             \\ Save
                         | Edit =
                             | Cut
                             | Copy
                             \\ Paste
                         \\ View";

        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Sum { name, variants } => {
                assert_eq!(*name.value, "Menu");
                assert_eq!(variants.len(), 3);

                // Check File and its nested variants
                match &*variants[0].value {
                    Adt::Sum { name, variants } => {
                        assert_eq!(*name.value, "File");
                        assert_eq!(variants.len(), 3);

                        // Check New and its nested variants
                        match &*variants[0].value {
                            Adt::Sum { name, variants } => {
                                assert_eq!(*name.value, "New");
                                assert_eq!(variants.len(), 3);

                                match &*variants[0].value {
                                    Adt::Product { name, fields } => {
                                        assert_eq!(*name.value, "Document");
                                        assert_eq!(fields.len(), 0);
                                    }
                                    _ => panic!("Expected Document to be a Product variant"),
                                }
                            }
                            _ => panic!("Expected New to be an Sum variant"),
                        }
                    }
                    _ => panic!("Expected File to be an Sum variant"),
                }

                // Check last variant
                match &*variants[2].value {
                    Adt::Product { name, fields } => {
                        assert_eq!(*name.value, "View");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected View to be a Product variant"),
                }
            }
            _ => panic!("Expected Sum ADT"),
        }
    }
}

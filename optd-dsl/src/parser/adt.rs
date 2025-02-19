use chumsky::{
    error::Simple,
    prelude::{choice, just, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::Token,
    parser::ast::Adt,
};

use super::utils::fields_list_parser;

pub fn adt_parser() -> impl Parser<Token, Spanned<Adt>, Error = Simple<Token, Span>> + Clone {
    just(Token::Data).ignore_then(recursive(|inner_adt_parser| {
        let type_ident = select! { Token::TypeIdent(name) => name }.map_with_span(Spanned::new);
        
        // Parse a product: TypeName(fields...)
        let product_parser = type_ident
            .then(fields_list_parser().or_not())
            .map(|(name, fields)| Adt::Struct {
                name,
                fields: fields.unwrap_or_else(Vec::new),
            })
            .map_with_span(Spanned::new);

        // Parse a product with the 'with' keyword: TypeName with | V1 | V2 \ V3
        let with_sum_parser = type_ident
            .then_ignore(just(Token::With))
            .then(
                just(Token::Vertical)
                    .ignore_then(inner_adt_parser.clone())
                    .repeated()
                    .then(just(Token::Backward).ignore_then(inner_adt_parser.clone())),
            )
            .map(|(name, (variants, end_variant))| Adt::Enum {
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
    use chumsky::{prelude::end, Stream};

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
            Adt::Struct { fields, .. } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(*fields[0].value.name.value, "bla");
                assert!(matches!(*fields[0].value.ty.value, Type::Int64));
            }
            _ => panic!("Expected Struct ADT"),
        }
    }

    #[test]
    fn test_empty_struct_adt() {
        let input = "data Empty";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Struct { fields, .. } => {
                assert_eq!(fields.len(), 0);
            }
            _ => panic!("Expected Struct ADT"),
        }
    }

    #[test]
    fn test_enum_adt() {
        let input = "data JoinType with
                         | Inner 
                         \\ Outer";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Enum { variants, .. } => {
                assert_eq!(variants.len(), 2);

                // First variant should be Inner
                match &*variants[0].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Inner");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected Inner to be a Struct variant"),
                }

                // Second variant should be Outer
                match &*variants[1].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Outer");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected Outer to be a Struct variant"),
                }
            }
            _ => panic!("Expected Enum ADT"),
        }
    }

    #[test]
    fn test_enum_with_struct_variants() {
        let input = "data Shape with
                         | Circle(center: Point, radius: F64)
                         | Rectangle(topLeft: Point, width: F64, height: F64)
                         \\ Triangle(p1: Point, p2: Point, p3: Point)";
        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Enum { variants, .. } => {
                assert_eq!(variants.len(), 3);

                match &*variants[0].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Circle");
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Circle to be a Struct variant"),
                }

                match &*variants[1].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Rectangle");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected Rectangle to be a Struct variant"),
                }

                match &*variants[2].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Triangle");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected Triangle to be a Struct variant"),
                }
            }
            _ => panic!("Expected Enum ADT"),
        }
    }

    #[test]
    fn test_nested_enum() {
        let input = "data Expression with
                         | Literal with
                             | IntLiteral(value: I64)
                             | BoolLiteral(value: Bool)
                             \\ StringLiteral(value: String)
                         | BinaryOp(left: Expression, op: Operator, right: Expression)
                         \\ Call(func: String, args: [Expression])";

        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Enum { variants, .. } => {
                assert_eq!(variants.len(), 3);

                // Check Literal and its nested enum
                match &*variants[0].value {
                    Adt::Enum { name, variants } => {
                        assert_eq!(*name.value, "Literal");
                        assert_eq!(variants.len(), 3);

                        // Check IntLiteral
                        match &*variants[0].value {
                            Adt::Struct { name, fields } => {
                                assert_eq!(*name.value, "IntLiteral");
                                assert_eq!(fields.len(), 1);
                            }
                            _ => panic!("Expected IntLiteral to be a Struct variant"),
                        }
                    }
                    _ => panic!("Expected Literal to be an Enum variant"),
                }

                // Check BinaryOp
                match &*variants[1].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "BinaryOp");
                        assert_eq!(fields.len(), 3);
                    }
                    _ => panic!("Expected BinaryOp to be a Struct variant"),
                }

                // Check Call
                match &*variants[2].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "Call");
                        assert_eq!(fields.len(), 2);
                    }
                    _ => panic!("Expected Call to be a Struct variant"),
                }
            }
            _ => panic!("Expected Enum ADT"),
        }
    }

    #[test]
    fn test_double_nested_enum() {
        let input = "data Menu with
                         | File with
                             | New with
                                 | Document
                                 | Project
                                 \\ Template
                             | Open
                             \\ Save
                         | Edit with
                             | Cut
                             | Copy
                             \\ Paste
                         \\ View";

        let (result, errors) = parse_adt(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        match &*result.unwrap().value {
            Adt::Enum { name, variants } => {
                assert_eq!(*name.value, "Menu");
                assert_eq!(variants.len(), 3);

                // Check File and its nested variants
                match &*variants[0].value {
                    Adt::Enum { name, variants } => {
                        assert_eq!(*name.value, "File");
                        assert_eq!(variants.len(), 3);

                        // Check New and its nested variants
                        match &*variants[0].value {
                            Adt::Enum { name, variants } => {
                                assert_eq!(*name.value, "New");
                                assert_eq!(variants.len(), 3);

                                match &*variants[0].value {
                                    Adt::Struct { name, fields } => {
                                        assert_eq!(*name.value, "Document");
                                        assert_eq!(fields.len(), 0);
                                    }
                                    _ => panic!("Expected Document to be a Struct variant"),
                                }
                            }
                            _ => panic!("Expected New to be an Enum variant"),
                        }
                    }
                    _ => panic!("Expected File to be an Enum variant"),
                }

                // Check last variant
                match &*variants[2].value {
                    Adt::Struct { name, fields } => {
                        assert_eq!(*name.value, "View");
                        assert_eq!(fields.len(), 0);
                    }
                    _ => panic!("Expected View to be a Struct variant"),
                }
            }
            _ => panic!("Expected Enum ADT"),
        }
    }
}

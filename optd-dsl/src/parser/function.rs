use chumsky::{
    error::Simple,
    prelude::{choice, just},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::Token,
    parser::ast::Function,
};

use super::{
    ast::{Field, Type},
    expr::expr_parser,
    r#type::type_parser,
    utils::{delimited_parser, fields_list_parser},
};

/// Parser for function definitions, supporting both:
/// - Regular functions: `[annotations] fn (receiver): name(params): ReturnType = body`
/// - Extern functions: `[annotations] fn (receiver): name(params): ReturnType` (no "=" and body)
///
/// Where:
/// - `[annotations]`: Optional list of annotations like `[rule, rust]`
/// - `(receiver)`: Optional receiver for method-style functions like `(self: Person)`
/// - `name`: Function name
/// - `(params)`: Optional parameter list, can be empty `()` or omitted entirely
/// - `ReturnType`: Optional for regular functions, mandatory for extern functions
/// - `body`: Expression representing function implementation (only for regular functions)
///
/// Extern functions are identified by the absence of "=" and body, and must have a return type.
/// They can be used to represent external implementations like Rust UDFs.
pub fn function_parser(
) -> impl Parser<Token, Spanned<Function>, Error = Simple<Token, Span>> + Clone {
    let annotations = delimited_parser(
        select! { Token::TermIdent(name) => name }
            .map_with_span(Spanned::new)
            .separated_by(just(Token::Comma))
            .allow_trailing(),
        Token::LBracket,
        Token::RBracket,
        |annot_opt| annot_opt.unwrap_or_default(),
    )
    .or_not()
    .map(|opt| opt.unwrap_or_default());

    let ident_parser = select! { Token::TermIdent(name) => name }.map_with_span(Spanned::new);

    let receiver = delimited_parser(
        ident_parser
            .then_ignore(just(Token::Colon))
            .then(type_parser())
            .map(|(name, ty)| Field { name, ty })
            .map_with_span(Spanned::new),
        Token::LParen,
        Token::RParen,
        |field_opt| field_opt,
    )
    .or_not()
    .map(|opt| opt.unwrap_or_default());

    let params = choice((just(Token::Unit).map(|_| vec![]), fields_list_parser())).or_not();

    // Mandatory return type parser for extern functions
    let mandatory_return_type = just(Token::Colon).ignore_then(type_parser());

    // Optional return type parser for regular functions
    let optional_return_type = mandatory_return_type.clone().or_not();

    // Common function prefix parser (annotations, fn, receiver, name, params)
    let function_prefix = annotations
        .then_ignore(just(Token::Fn))
        .then(receiver)
        .then(ident_parser)
        .then(params);

    // Regular function with body
    let regular_function = function_prefix
        .clone()
        .then(optional_return_type)
        .then_ignore(just(Token::Eq))
        .then(expr_parser())
        .map(
            |(((((annotations, receiver), name), params), return_type_opt), body)| {
                let return_type = match return_type_opt {
                    Some(rt) => rt,
                    None => Spanned::new(Type::Unknown, name.span.clone()),
                };

                Function {
                    name,
                    receiver,
                    params,
                    return_type,
                    body: Some(body),
                    annotations,
                }
            },
        );

    // Extern function without body but with mandatory return type
    let extern_function = function_prefix
        .then(mandatory_return_type) // Return type is mandatory for extern functions
        .map(|((((annotations, receiver), name), params), return_type)| {
            Function {
                name,
                receiver,
                params,
                return_type,
                body: None, // No body for extern functions
                annotations,
            }
        });

    // Try the regular function parser first, as it's more specific
    // If that fails, try the extern function parser
    regular_function
        .or(extern_function)
        .map_with_span(Spanned::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        errors::span::Span,
        lexer::lex::lex,
        parser::ast::{Expr, Type},
    };
    use chumsky::{prelude::end, Stream};

    fn parse_function(input: &str) -> (Option<Spanned<Function>>, Vec<Simple<Token, Span>>) {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);
        function_parser()
            .then_ignore(end())
            .parse_recovery(Stream::from_iter(eoi, tokens.unwrap().into_iter()))
    }

    #[test]
    fn test_simple_function() {
        let input = "fn add(a: I64, b: I64): I64 = a + b";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "add");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);
            assert_eq!(*params[0].value.name.value, "a");
            assert!(matches!(*params[0].value.ty.value, Type::Int64));
            assert_eq!(*params[1].value.name.value, "b");
            assert!(matches!(*params[1].value.ty.value, Type::Int64));
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_extern_function() {
        let input = "fn add(a: I64, b: I64): I64"; // No "=" and body
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "add");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);
            assert_eq!(*params[0].value.name.value, "a");
            assert!(matches!(*params[0].value.ty.value, Type::Int64));
            assert_eq!(*params[1].value.name.value, "b");
            assert!(matches!(*params[1].value.ty.value, Type::Int64));
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_none()); // Extern function has no body
        }
    }

    #[test]
    fn test_extern_function_with_annotations() {
        let input = "[rust, ffi] fn native_add(a: I64, b: I64): I64";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "native_add");
            assert_eq!(func.value.annotations.len(), 2);
            assert_eq!(*func.value.annotations[0].value, "rust");
            assert_eq!(*func.value.annotations[1].value, "ffi");
            assert!(func.value.params.is_some());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_none()); // Extern function has no body
        }
    }

    #[test]
    fn test_extern_function_with_receiver() {
        let input = "fn (self: Person) native_greet(name: String): String";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "native_greet");

            // Check receiver
            assert!(func.value.receiver.is_some());
            if let Some(receiver) = &func.value.receiver {
                assert_eq!(*receiver.value.name.value, "self");
                assert!(matches!(&*receiver.value.ty.value, Type::Adt(name) if name == "Person"));
            }

            // Check params
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "name");

            // Extern functions must have a return type
            assert!(matches!(*func.value.return_type.value, Type::String));
            assert!(func.value.body.is_none()); // Extern function has no body
        }
    }

    #[test]
    fn test_function_with_receiver() {
        let input = "fn (self: Person) greet(name: String) = \"Hello, \" ++ name";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "greet");

            // Check receiver
            assert!(func.value.receiver.is_some());
            if let Some(receiver) = &func.value.receiver {
                assert_eq!(*receiver.value.name.value, "self");
                assert!(matches!(&*receiver.value.ty.value, Type::Adt(name) if name == "Person"));
            }

            // Check params
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "name");
            assert!(matches!(*params[0].value.ty.value, Type::String));

            // Default return type should be Unknown since none was specified
            assert!(matches!(*func.value.return_type.value, Type::Unknown));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_function_with_empty_annotations() {
        let input = "[] fn noAnnotations(): I64 = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "noAnnotations");
            assert!(func.value.annotations.is_empty());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_function_with_no_params() {
        let input = "fn getCurrentTime: I64 = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "getCurrentTime");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_none());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_function_with_empty_params() {
        let input = "fn getCurrentTime(): I64 = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "getCurrentTime");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            assert!(func.value.params.unwrap().is_empty());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_extern_function_with_empty_params() {
        let input = "fn getNativeTime(): I64";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "getNativeTime");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            assert!(func.value.params.unwrap().is_empty());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_none()); // Extern function has no body
        }
    }

    #[test]
    fn test_function_with_unit_params() {
        let input = "fn getCurrentTime(): I64 = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "getCurrentTime");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            assert!(func.value.params.unwrap().is_empty());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_function_with_annotations() {
        let input = "[public, async] fn fetchData(url: String): {String: I64} = {\"status\": 200}";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "fetchData");
            assert_eq!(func.value.annotations.len(), 2);
            assert_eq!(*func.value.annotations[0].value, "public");
            assert_eq!(*func.value.annotations[1].value, "async");

            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "url");
            assert!(matches!(*params[0].value.ty.value, Type::String));

            // Check return type is a map
            assert!(matches!(*func.value.return_type.value, Type::Map(_, _)));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }

    #[test]
    fn test_function_complex_body() {
        let input =
            "fn calculate(x: I64): I64 = let result = x * 2 in if result > 10 then result else 0";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "calculate");

            // Check for let expression in body
            if let Some(body) = &func.value.body {
                if let Expr::Let(_, _, _) = *body.value {
                    // Successfully parsed complex body
                } else {
                    panic!("Expected Let expression in function body");
                }
            } else {
                panic!("Expected body to be present");
            }
        }
    }

    #[test]
    fn test_function_with_complex_return_type() {
        let input = "fn process(dat: [I64]): (I64) => {String: [Bool]} = (x) => {\"result\": [true, false]}";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "process");

            // Check param
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(*params[0].value.name.value, "dat");
            assert!(matches!(*params[0].value.ty.value, Type::Array(_)));

            // Check complex return type
            if let Type::Closure(param_type, return_type) = &*func.value.return_type.value {
                assert!(matches!(*param_type.value, Type::Int64));

                if let Type::Map(key_type, val_type) = &*return_type.value {
                    assert!(matches!(*key_type.value, Type::String));
                    assert!(matches!(*val_type.value, Type::Array(_)));
                } else {
                    panic!("Expected Map type in return type");
                }
            } else {
                panic!("Expected Closure type in return type");
            }

            // Check body is a closure
            if let Some(body) = &func.value.body {
                assert!(matches!(*body.value, Expr::Closure(_, _)));
            } else {
                panic!("Expected body to be present");
            }
        }
    }

    #[test]
    fn test_extern_function_with_complex_return_type() {
        let input = "fn nativeProcess(dat: [I64]): (I64) => {String: [Bool]}";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "nativeProcess");

            // Check param
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "dat");
            assert!(matches!(*params[0].value.ty.value, Type::Array(_)));

            // Check complex return type
            if let Type::Closure(param_type, return_type) = &*func.value.return_type.value {
                assert!(matches!(*param_type.value, Type::Int64));

                if let Type::Map(key_type, val_type) = &*return_type.value {
                    assert!(matches!(*key_type.value, Type::String));
                    assert!(matches!(*val_type.value, Type::Array(_)));
                } else {
                    panic!("Expected Map type in return type");
                }
            } else {
                panic!("Expected Closure type in return type");
            }

            // Extern function has no body
            assert!(func.value.body.is_none());
        }
    }

    #[test]
    fn test_missing_return_type_inherits_span() {
        let input = "fn noReturnType = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "noReturnType");
            assert!(func.value.receiver.is_none());
            assert!(func.value.params.is_none());

            // The return type span should match the function name span
            assert_eq!(func.value.return_type.span, func.value.name.span);
            assert!(matches!(*func.value.return_type.value, Type::Unknown));
            assert!(func.value.body.is_some()); // Regular function has a body
        }
    }
}

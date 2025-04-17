use super::{
    ast::{Field, Type},
    expr::expr_parser,
    types::type_parser,
    utils::{delimited_parser, fields_list_parser},
};
use crate::{
    lexer::tokens::Token,
    parser::ast::Function,
    utils::span::{Span, Spanned},
};
use chumsky::{
    Parser,
    error::Simple,
    prelude::{choice, just},
    select,
};

/// Parser for function definitions in the DSL.
///
/// # Function Syntax
///
/// The function parser supports two main forms with optional generic type parameters:
///
/// 1. Regular functions with implementation:
///    ```ignore
///    [annotations] fn <TypeParam1, TypeParam2> (receiver): name(params): ReturnType = body
///    ```
///
/// 2. Extern functions (declarations without implementation):
///    ```ignore
///    [annotations] fn <TypeParam1, TypeParam2> (receiver): name(params): ReturnType
///    ```
///
/// # Components
///
/// * `[annotations]`: Optional list of annotations like `[rule, rust]`
///   - Used to mark special properties or behaviors
///   - Example: `[rust]` might indicate the function is implemented in Rust
///
/// * `<TypeParams>`: Optional generic type parameters
///   - Enclosed in angle brackets
///   - Used for generic functions
///   - Example: `<T, U>` for type parameters T and U
///
/// * `(receiver)`: Optional receiver parameter for method-style functions
///   - Similar to `self` in Rust or `this` in other languages
///   - Example: `(self: Person)` for methods on a Person type
///
/// * `name`: Function identifier
///   - Must be a valid term identifier in the language
///
/// * `(params)`: Parameter list
///   - Can be empty `()`, omitted entirely, or contain typed parameters
///   - Example: `(x: I64, y: String)`
///
/// * `ReturnType`: Type annotation for the function's return value
///   - Optional for regular functions (defaults to `Unknown` type)
///   - Required for extern functions
///   - Example: `I64` or `{String: [Bool]}`
///
/// * `body`: Expression implementing the function
///   - Only present in regular functions
///   - Can be any valid expression in the language
///
/// # Examples
///
/// Regular function with parameters:
/// ```ignore
/// fn add(a: I64, b: I64): I64 = a + b
/// ```
///
/// Generic function:
/// ```ignore
/// fn <T> identity(x: T): T = x
/// ```
///
/// Extern function with generic parameters:
/// ```ignore
/// [rust] fn <K, V> native_map_get(map: {K: V}, key: K): V?
/// ```
///
/// # Error Recovery
///
/// The parser includes error recovery for common syntax mistakes, including:
/// - Missing or malformed annotations
/// - Incorrect parameter lists
/// - Missing return types for extern functions
/// - Malformed generic type parameter lists
///
/// This helps provide better error messages and allows parsing to continue
/// even when parts of a function definition contain errors.
pub fn function_parser()
-> impl Parser<Token, Spanned<Function>, Error = Simple<Token, Span>> + Clone {
    // Parse optional function annotations like [rule, rust]
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

    // Parse identifiers (function names, parameter names, type parameter names)
    let ident_parser = select! { Token::TermIdent(name) => name }.map_with_span(Spanned::new);
    let type_ident_parser = select! { Token::TypeIdent(name) => name }.map_with_span(Spanned::new);

    // Parse optional generic type parameters like <A, B, C>
    let type_params = delimited_parser(
        type_ident_parser
            .separated_by(just(Token::Comma))
            .allow_trailing(),
        Token::Less,
        Token::Greater,
        |params_opt| params_opt.unwrap_or_default(),
    )
    .or_not()
    .map(|opt| opt.unwrap_or_default());

    // Parse optional receiver parameter like (self: Type)
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

    // Parse parameter list - either empty () or (param1: Type, param2: Type, ...)
    // Also handles missing parameters
    let params = choice((just(Token::Unit).map(|_| vec![]), fields_list_parser())).or_not();

    // Mandatory return type parser for extern functions (: ReturnType)
    let mandatory_return_type = just(Token::Colon).ignore_then(type_parser());

    // Optional return type parser for regular functions
    let optional_return_type = mandatory_return_type.clone().or_not();

    // Common function prefix parser - captures elements shared between regular and extern funcs
    // [annotations] fn <type_params> (receiver): name(params)
    let function_prefix = annotations
        .then_ignore(just(Token::Fn))
        .then(type_params)
        .then(receiver)
        .then(ident_parser)
        .then(params);

    // Regular function with body
    // [annotations] fn <type_params> (receiver): name(params): ReturnType = body
    let regular_function = function_prefix
        .clone()
        .then(optional_return_type)
        .then_ignore(just(Token::Eq))
        .then(expr_parser())
        .map(
            |(
                (((((annotations, type_params), receiver), name), params), return_type_opt),
                body,
            )| {
                // Default to Unknown type if no return type specified
                let return_type = match return_type_opt {
                    Some(rt) => rt,
                    None => Spanned::new(Type::Unknown, name.span.clone()),
                };

                Function {
                    name,
                    type_params,
                    receiver,
                    params,
                    return_type,
                    body: Some(body),
                    annotations,
                }
            },
        );

    // Extern function without body but with mandatory return type
    // [annotations] fn <type_params> (receiver): name(params): ReturnType
    let extern_function = function_prefix
        .then(mandatory_return_type) // Return type is mandatory for extern functions
        .map(
            |(((((annotations, type_params), receiver), name), params), return_type)| {
                Function {
                    name,
                    type_params,
                    receiver,
                    params,
                    return_type,
                    body: None, // No body for extern functions
                    annotations,
                }
            },
        );

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
        lexer::lex::lex,
        parser::ast::{Expr, Type},
    };
    use chumsky::{Stream, prelude::end};

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
            assert!(func.value.type_params.is_empty());
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
    fn test_generic_function() {
        let input = "fn <T> identity(x: T): T = x";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "identity");

            // Check type parameters
            assert_eq!(func.value.type_params.len(), 1);
            assert_eq!(*func.value.type_params[0].value, "T");

            // Check parameter
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "x");
            assert!(
                matches!(*params[0].clone().value.ty.value, Type::Identifier(name) if name == "T")
            );

            // Check return type
            assert!(matches!(*func.value.return_type.value, Type::Identifier(name) if name == "T"));

            // Check body
            assert!(func.value.body.is_some());
        }
    }

    #[test]
    fn test_generic_function_multiple_type_params() {
        let input = "fn <K, V> mapGet(map: {K: V}, key: K): V? = map.get(key)";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "mapGet");

            // Check type parameters
            assert_eq!(func.value.type_params.len(), 2);
            assert_eq!(*func.value.type_params[0].value, "K");
            assert_eq!(*func.value.type_params[1].value, "V");

            // Check parameters
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);

            // Check first parameter (map: {K: V})
            assert_eq!(*params[0].value.name.value, "map");
            if let Type::Map(key_ty, val_ty) = &*params[0].value.ty.value {
                assert!(matches!(*key_ty.clone().value, Type::Identifier(name) if name == "K"));
                assert!(matches!(*val_ty.clone().value, Type::Identifier(name) if name == "V"));
            } else {
                panic!("Expected Map type for first parameter");
            }

            // Check second parameter (key: K)
            assert_eq!(*params[1].value.name.value, "key");
            assert!(
                matches!(*params[1].clone().value.ty.value, Type::Identifier(name) if name == "K")
            );

            // Check return type (V?)
            if let Type::Questioned(inner) = &*func.value.return_type.value {
                assert!(matches!(*inner.clone().value, Type::Identifier(name) if name == "V"));
            } else {
                panic!("Expected Optional return type");
            }
        }
    }

    #[test]
    fn test_generic_extern_function() {
        let input = "fn <A, B, C> externalFunc(a: A, b: B): C";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "externalFunc");

            // Check type parameters
            assert_eq!(func.value.type_params.len(), 3);
            assert_eq!(*func.value.type_params[0].value, "A");
            assert_eq!(*func.value.type_params[1].value, "B");
            assert_eq!(*func.value.type_params[2].value, "C");

            // Check parameters
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 2);
            assert_eq!(*params[0].value.name.value, "a");
            assert!(
                matches!(*params[0].clone().value.ty.value, Type::Identifier(name) if name == "A")
            );
            assert_eq!(*params[1].value.name.value, "b");
            assert!(
                matches!(*params[1].clone().value.ty.value, Type::Identifier(name) if name == "B")
            );

            // Check return type
            assert!(matches!(*func.value.return_type.value, Type::Identifier(name) if name == "C"));

            // Check body is None
            assert!(func.value.body.is_none());
        }
    }

    #[test]
    fn test_function_with_empty_type_params() {
        let input = "fn <> empty(): I64 = 42";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "empty");
            assert!(func.value.type_params.is_empty());
            assert!(matches!(*func.value.return_type.value, Type::Int64));
            assert!(func.value.body.is_some());
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
            assert!(func.value.body.is_none()); // Extern function has no body.
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

            // Check receiver.
            assert!(func.value.receiver.is_some());
            if let Some(receiver) = &func.value.receiver {
                assert_eq!(*receiver.value.name.value, "self");
                assert!(
                    matches!(&*receiver.value.ty.value, Type::Identifier(name) if name == "Person")
                );
            }

            // Check params.
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "name");

            // Extern functions must have a return type.
            assert!(matches!(*func.value.return_type.value, Type::String));
            assert!(func.value.body.is_none()); // Extern function has no body.
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

            // Check receiver.
            assert!(func.value.receiver.is_some());
            if let Some(receiver) = &func.value.receiver {
                assert_eq!(*receiver.value.name.value, "self");
                assert!(
                    matches!(&*receiver.value.ty.value, Type::Identifier(name) if name == "Person")
                );
            }

            // Check params.
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "name");
            assert!(matches!(*params[0].value.ty.value, Type::String));

            // Default return type should be Unknown since none was specified.
            assert!(matches!(*func.value.return_type.value, Type::Unknown));
            assert!(func.value.body.is_some()); // Regular function has a body.
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
            assert!(func.value.body.is_some()); // Regular function has a body.
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
            assert!(func.value.body.is_some()); // Regular function has a body.
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
            assert!(func.value.body.is_some()); // Regular function has a body.
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
            assert!(func.value.body.is_none()); // Extern function has no body.
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
            assert!(func.value.body.is_some()); // Regular function has a body.
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

            // Check return type is a map.
            assert!(matches!(*func.value.return_type.value, Type::Map(_, _)));
            assert!(func.value.body.is_some()); // Regular function has a body.
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

            // Check for let expression in body.
            if let Some(body) = &func.value.body {
                if let Expr::Let(_, _, _) = *body.value {
                    // Successfully parsed complex body.
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
        let input = "fn process(dat: [I64]): (I64) -> {String: [Bool]} = (x) -> {\"result\": [true, false]}";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "process");

            // Check param.
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(*params[0].value.name.value, "dat");
            assert!(matches!(*params[0].value.ty.value, Type::Array(_)));

            // Check complex return type.
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

            // Check body is a closure.
            if let Some(body) = &func.value.body {
                assert!(matches!(*body.value, Expr::Closure(_, _)));
            } else {
                panic!("Expected body to be present");
            }
        }
    }

    #[test]
    fn test_extern_function_with_complex_return_type() {
        let input = "fn nativeProcess(dat: [I64]): I64 -> {String: [Bool]}";
        let (result, errors) = parse_function(input);

        assert!(result.is_some(), "Expected successful parse");
        assert!(errors.is_empty(), "Expected no errors");

        if let Some(func) = result {
            assert_eq!(*func.value.name.value, "nativeProcess");

            // Check param.
            assert!(func.value.params.is_some());
            let params = func.value.params.as_ref().unwrap();
            assert_eq!(params.len(), 1);
            assert_eq!(*params[0].value.name.value, "dat");
            assert!(matches!(*params[0].value.ty.value, Type::Array(_)));

            // Check complex return type.
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

            // Extern function has no body.
            assert!(func.value.body.is_none());
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
            assert!(func.value.type_params.is_empty());
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
}

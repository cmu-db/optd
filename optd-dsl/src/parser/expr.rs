use chumsky::{
    error::Simple,
    prelude::{choice, just, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::Token,
    parser::ast::{BinOp, Expr, Literal, MatchArm, UnaryOp},
};

use super::{
    ast::{Field, PostfixOp, Type},
    pattern::pattern_parser,
    r#type::type_parser,
    utils::delimited_parser,
};

fn binary_op(
    term: impl Parser<Token, Spanned<Expr>, Error = Simple<Token, Span>> + Clone,
    op: impl Parser<Token, BinOp, Error = Simple<Token, Span>> + Clone,
) -> impl Parser<Token, Spanned<Expr>, Error = Simple<Token, Span>> + Clone {
    term.clone()
        .then(op.then(term.clone()).repeated())
        .map_with_span(|(left, rights), span| {
            let mut result = left;
            for (bin_op, right) in rights {
                result = Spanned::new(Expr::Binary(result, bin_op, right), span.clone());
            }
            result
        })
}

pub fn expr_parser() -> impl Parser<Token, Spanned<Expr>, Error = Simple<Token, Span>> + Clone {
    recursive(|expr| {
        let literal = {
            let lit_int = select! { Token::Int64(n) => Literal::Int64(n) };
            let lit_float = select! { Token::Float64(f) => Literal::Float64(f) };
            let lit_string = select! { Token::String(s) => Literal::String(s) };
            let lit_bool = select! { Token::Bool(b) => Literal::Bool(b) };
            let lit_unit = just(Token::Unit).map(|_| Literal::Unit);

            choice((lit_int, lit_float, lit_string, lit_bool, lit_unit))
                .map(Expr::Literal)
                .map_with_span(Spanned::new)
                .boxed()
        };

        let reference =
            select! { Token::TermIdent(name) => Expr::Ref(name) }.map_with_span(Spanned::new);

        let array = delimited_parser(
            expr.clone()
                .separated_by(just(Token::Comma))
                .allow_trailing(),
            Token::LBracket,
            Token::RBracket,
            |items| items.map(Expr::Array).unwrap_or(Expr::Error),
        )
        .map_with_span(Spanned::new)
        .boxed();

        let tuple = delimited_parser(
            choice((
                expr.clone()
                    .separated_by(just(Token::Comma))
                    .allow_trailing()
                    .at_least(2),
                expr.clone()
                    .then_ignore(just(Token::Comma))
                    .map(|e| vec![e]),
            )),
            Token::LParen,
            Token::RParen,
            |exprs_opt| exprs_opt.map(Expr::Tuple).unwrap_or(Expr::Error),
        )
        .map_with_span(Spanned::new)
        .boxed();

        let map = delimited_parser(
            expr.clone()
                .then_ignore(just(Token::Colon))
                .then(expr.clone())
                .separated_by(just(Token::Comma))
                .allow_trailing(),
            Token::LBrace,
            Token::RBrace,
            |items| match items {
                Some(items) => Expr::Map(items.into_iter().collect()),
                None => Expr::Error,
            },
        )
        .map_with_span(Spanned::new)
        .boxed();

        let fail = just(Token::Fail)
            .ignore_then(delimited_parser(
                expr.clone(),
                Token::LParen,
                Token::RParen,
                |expr_opt| expr_opt.map(Expr::Fail).unwrap_or(Expr::Error),
            ))
            .map_with_span(Spanned::new)
            .boxed();

        let constructor = select! { Token::TypeIdent(name) => name }
            .then(
                delimited_parser(
                    expr.clone()
                        .separated_by(just(Token::Comma))
                        .allow_trailing(),
                    Token::LParen,
                    Token::RParen,
                    |args| args,
                )
                .map_with_span(Spanned::new)
                .or_not(),
            )
            .map(|(name, args)| match args {
                Some(args) => match *args.value {
                    Some(args) => Expr::Constructor(name, args),
                    None => Expr::Error,
                },
                None => Expr::Constructor(name, vec![]),
            })
            .map_with_span(Spanned::new)
            .boxed();

        let closure = {
            let param_parser = select! { Token::TermIdent(name) => name }
                .map_with_span(Spanned::new)
                .then(just(Token::Colon).ignore_then(type_parser()).or_not())
                .map(|(name, type_opt)| {
                    let ty = type_opt.unwrap_or(Spanned::new(Type::Unknown, name.span.clone()));
                    Field { name, ty }
                })
                .map_with_span(Spanned::new);

            let param_list = delimited_parser(
                param_parser
                    .clone()
                    .separated_by(just(Token::Comma))
                    .allow_trailing(),
                Token::LParen,
                Token::RParen,
                |params| params.unwrap_or_default(),
            );

            let empty_params = just(Token::Unit).map(|_| vec![]);
            let single_param = param_parser.map(|field| vec![field]);

            choice((param_list, empty_params, single_param))
                .then_ignore(just(Token::BigArrow))
                .then(expr.clone())
                .map(|(params, body)| Expr::Closure(params, body))
                .map_with_span(Spanned::new)
                .boxed()
        };

        // Note: cannot apply delimiter recoveries, as they would
        // block further successful parses (e.g. tuples & maps).
        let paren_expr = just(Token::LParen)
            .ignore_then(expr.clone())
            .then_ignore(just(Token::RParen))
            .boxed();

        let brace_expr = just(Token::LBrace)
            .ignore_then(expr.clone())
            .then_ignore(just(Token::RBrace))
            .boxed();

        let atom = choice((
            closure,
            brace_expr,
            paren_expr,
            literal,
            reference,
            array,
            tuple,
            map,
            fail,
            constructor,
        ));

        let postfix = atom
            .clone()
            .then(
                choice((
                    delimited_parser(
                        expr.clone()
                            .separated_by(just(Token::Comma))
                            .allow_trailing(),
                        Token::LParen,
                        Token::RParen,
                        |args| args.unwrap_or_default(),
                    )
                    .map(PostfixOp::Call),
                    just(Token::Unit).map(|_| PostfixOp::Call(vec![])),
                    just(Token::Dot)
                        .ignore_then(select! { Token::TermIdent(name) => name })
                        .map(PostfixOp::Member),
                    just(Token::SmallArrow)
                        .ignore_then(select! { Token::TermIdent(name) => name })
                        .map(PostfixOp::Compose),
                ))
                .map_with_span(|op, span| (op, span))
                .repeated(),
            )
            .map(|(initial, ops)| {
                ops.into_iter().fold(initial, |acc, (op, span)| {
                    Spanned::new(Expr::Postfix(acc, op), span)
                })
            })
            .boxed();

        let unary = choice((
            just(Token::Minus).map(|_| UnaryOp::Neg),
            just(Token::Not).map(|_| UnaryOp::Not),
        ))
        .then(postfix.clone())
        .map(|(op, expr)| Expr::Unary(op, expr))
        .map_with_span(Spanned::new)
        .or(postfix)
        .boxed();

        let product = binary_op(
            unary.clone(),
            choice((
                just(Token::Mul).map(|_| BinOp::Mul),
                just(Token::Div).map(|_| BinOp::Div),
            )),
        )
        .boxed();

        let sum = binary_op(
            product.clone(),
            choice((
                just(Token::Plus).map(|_| BinOp::Add),
                just(Token::Minus).map(|_| BinOp::Sub),
                just(Token::Concat).map(|_| BinOp::Concat),
            )),
        )
        .boxed();

        let range = binary_op(
            sum.clone(),
            choice((just(Token::Range).map(|_| BinOp::Range),)),
        )
        .boxed();

        let comparison = binary_op(
            range.clone(),
            choice((
                just(Token::EqEq).map(|_| BinOp::Eq),
                just(Token::NotEq).map(|_| BinOp::Neq),
                just(Token::Greater).map(|_| BinOp::Gt),
                just(Token::Less).map(|_| BinOp::Lt),
                just(Token::GreaterEq).map(|_| BinOp::Ge),
                just(Token::LessEq).map(|_| BinOp::Le),
            )),
        )
        .boxed();

        let and = binary_op(
            comparison.clone(),
            choice((just(Token::And).map(|_| BinOp::And),)),
        )
        .boxed();

        let or = binary_op(and.clone(), choice((just(Token::Or).map(|_| BinOp::Or),))).boxed();

        let let_binding = select! { Token::TermIdent(name) => name }
            .map_with_span(Spanned::new)
            .then(just(Token::Colon).ignore_then(type_parser()).or_not())
            .then_ignore(just(Token::Eq))
            .then(expr.clone())
            .map(|((name, type_opt), value)| {
                let field = {
                    let ty = type_opt.unwrap_or(Spanned::new(Type::Unknown, name.span.clone()));
                    Spanned::new(
                        Field {
                            name: name.clone(),
                            ty,
                        },
                        name.span.clone(),
                    )
                };
                (field, value)
            });

        let let_expr = just(Token::Let)
            .ignore_then(
                let_binding
                    .separated_by(just(Token::Comma))
                    .allow_trailing(),
            )
            .then_ignore(just(Token::In))
            .then(expr.clone())
            .map_with_span(|(bindings, body), span| {
                bindings
                    .into_iter()
                    .rev()
                    .fold(body, |acc_body, (field, value)| {
                        Spanned::new(Expr::Let(field, value, acc_body), span.clone())
                    })
            })
            .boxed();

        let match_arm = pattern_parser()
            .then_ignore(just(Token::BigArrow))
            .then(expr.clone())
            .map(|(pattern, expr)| MatchArm { pattern, expr })
            .map_with_span(Spanned::new);

        let match_expr = just(Token::Match)
            .ignore_then(expr.clone())
            .then(
                just(Token::Vertical)
                    .ignore_then(match_arm.clone())
                    .repeated()
                    .then(just(Token::Backward).ignore_then(match_arm)),
            )
            .map(|(scrutinee, (arms, last_arm))| {
                let mut all_arms = arms;
                all_arms.push(last_arm);
                Expr::PatternMatch(scrutinee, all_arms)
            })
            .map_with_span(Spanned::new)
            .boxed();

        let if_expr = just(Token::If)
            .ignore_then(expr.clone())
            .then_ignore(just(Token::Then))
            .then(expr.clone())
            .then_ignore(just(Token::Else))
            .then(expr.clone())
            .map(|((condition, then_branch), else_branch)| {
                Expr::IfThenElse(condition, then_branch, else_branch)
            })
            .map_with_span(Spanned::new)
            .boxed();

        choice((let_expr, match_expr, if_expr, or)).labelled("expression")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        errors::span::Span,
        lexer::lex::lex,
        parser::ast::{Expr, Literal, Pattern},
    };
    use chumsky::{prelude::end, Stream};
    use ordered_float::OrderedFloat;

    fn parse_expr(input: &str) -> (Option<Spanned<Expr>>, Vec<Simple<Token, Span>>) {
        let (tokens, _) = lex(input, "test.txt");
        let len = input.chars().count();
        let eoi = Span::new("test.txt".into(), len..len);

        expr_parser()
            .then_ignore(end())
            .parse_recovery(Stream::from_iter(eoi, tokens.unwrap().into_iter()))
    }

    fn assert_expr_eq(actual: &Expr, expected: &Expr) {
        match (actual, expected) {
            (Expr::Literal(a), Expr::Literal(b)) => assert_eq!(a, b),
            (Expr::Ref(a), Expr::Ref(b)) => assert_eq!(a, b),
            (Expr::Binary(a_left, a_op, a_right), Expr::Binary(e_left, e_op, e_right)) => {
                assert_eq!(a_op, e_op);
                assert_expr_eq(&a_left.value, &e_left.value);
                assert_expr_eq(&a_right.value, &e_right.value);
            }
            (Expr::Unary(a_op, a_expr), Expr::Unary(e_op, e_expr)) => {
                assert_eq!(a_op, e_op);
                assert_expr_eq(&a_expr.value, &e_expr.value);
            }
            (Expr::Array(a_items), Expr::Array(e_items)) => {
                assert_eq!(a_items.len(), e_items.len());
                for (a, e) in a_items.iter().zip(e_items.iter()) {
                    assert_expr_eq(&a.value, &e.value);
                }
            }
            (Expr::Tuple(a_items), Expr::Tuple(e_items)) => {
                assert_eq!(a_items.len(), e_items.len());
                for (a, e) in a_items.iter().zip(e_items.iter()) {
                    assert_expr_eq(&a.value, &e.value);
                }
            }
            (Expr::Postfix(a_expr, a_op), Expr::Postfix(e_expr, e_op)) => {
                assert_expr_eq(&a_expr.value, &e_expr.value);
                match (a_op, e_op) {
                    (PostfixOp::Call(a_args), PostfixOp::Call(e_args)) => {
                        assert_eq!(a_args.len(), e_args.len());
                        for (a, e) in a_args.iter().zip(e_args.iter()) {
                            assert_expr_eq(&a.value, &e.value);
                        }
                    }
                    (PostfixOp::Member(a_member), PostfixOp::Member(e_member)) => {
                        assert_eq!(a_member, e_member);
                    }
                    (PostfixOp::Compose(a_id), PostfixOp::Compose(e_id)) => {
                        assert_eq!(a_id, e_id);
                    }
                    _ => assert!(
                        false,
                        "Postfix operation mismatch: expected {:?}, got {:?}",
                        e_op, a_op
                    ),
                }
            }
            _ => assert!(
                false,
                "Expression mismatch: expected {:?}, got {:?}",
                expected, actual
            ),
        }
    }

    #[test]
    fn test_literals() {
        // Integers
        let (result, errors) = parse_expr("42");
        assert!(result.is_some(), "Expected successful parse for integer");
        assert!(errors.is_empty(), "Expected no errors for integer");
        assert_expr_eq(&result.unwrap().value, &Expr::Literal(Literal::Int64(42)));

        // Floats
        let (result, errors) = parse_expr("3.14");
        assert!(result.is_some(), "Expected successful parse for float");
        assert!(errors.is_empty(), "Expected no errors for float");
        assert_expr_eq(
            &result.unwrap().value,
            &Expr::Literal(Literal::Float64(OrderedFloat(3.14))),
        );

        // Strings
        let (result, errors) = parse_expr("\"hello\"");
        assert!(result.is_some(), "Expected successful parse for string");
        assert!(errors.is_empty(), "Expected no errors for string");
        assert_expr_eq(
            &result.unwrap().value,
            &Expr::Literal(Literal::String("hello".to_string())),
        );

        // Booleans
        let (result, errors) = parse_expr("true");
        assert!(result.is_some(), "Expected successful parse for boolean");
        assert!(errors.is_empty(), "Expected no errors for boolean");
        assert_expr_eq(&result.unwrap().value, &Expr::Literal(Literal::Bool(true)));

        let (result, errors) = parse_expr("false");
        assert!(result.is_some(), "Expected successful parse for boolean");
        assert!(errors.is_empty(), "Expected no errors for boolean");
        assert_expr_eq(&result.unwrap().value, &Expr::Literal(Literal::Bool(false)));

        // Unit
        let (result, errors) = parse_expr("()");
        assert!(result.is_some(), "Expected successful parse for unit");
        assert!(errors.is_empty(), "Expected no errors for unit");
        assert_expr_eq(&result.unwrap().value, &Expr::Literal(Literal::Unit));
    }

    #[test]
    fn test_references() {
        let (result, errors) = parse_expr("variable");
        assert!(result.is_some(), "Expected successful parse for reference");
        assert!(errors.is_empty(), "Expected no errors for reference");
        assert_expr_eq(&result.unwrap().value, &Expr::Ref("variable".to_string()));
    }

    #[test]
    fn test_arrays() {
        // Empty array
        let (result, errors) = parse_expr("[]");
        assert!(
            result.is_some(),
            "Expected successful parse for empty array"
        );
        assert!(errors.is_empty(), "Expected no errors for empty array");
        if let Some(expr) = result {
            assert!(matches!(*expr.value, Expr::Array(items) if items.is_empty()));
        }

        // Single element array
        let (result, errors) = parse_expr("[42]");
        assert!(
            result.is_some(),
            "Expected successful parse for single element array"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for single element array"
        );
        if let Some(expr) = result {
            if let Expr::Array(elements) = &*expr.value {
                assert_eq!(elements.len(), 1);
                assert_expr_eq(&elements[0].value, &Expr::Literal(Literal::Int64(42)));
            } else {
                panic!("Expected Array expression");
            }
        }

        // Multiple element array
        let (result, errors) = parse_expr("[1, 2, 3]");
        assert!(
            result.is_some(),
            "Expected successful parse for multiple element array"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for multiple element array"
        );
        if let Some(expr) = result {
            if let Expr::Array(elements) = &*expr.value {
                assert_eq!(elements.len(), 3);
                assert_expr_eq(&elements[0].value, &Expr::Literal(Literal::Int64(1)));
                assert_expr_eq(&elements[1].value, &Expr::Literal(Literal::Int64(2)));
                assert_expr_eq(&elements[2].value, &Expr::Literal(Literal::Int64(3)));
            } else {
                panic!("Expected Array expression");
            }
        }

        // Array with trailing comma
        let (result, errors) = parse_expr("[1, 2, 3,]");
        assert!(
            result.is_some(),
            "Expected successful parse for array with trailing comma"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for array with trailing comma"
        );
        if let Some(expr) = result {
            if let Expr::Array(elements) = &*expr.value {
                assert_eq!(elements.len(), 3);
            } else {
                panic!("Expected Array expression");
            }
        }
    }

    #[test]
    fn test_tuples() {
        // Empty tuple is just the unit value
        let (result, errors) = parse_expr("()");
        assert!(
            result.is_some(),
            "Expected successful parse for empty tuple"
        );
        assert!(errors.is_empty(), "Expected no errors for empty tuple");
        assert_expr_eq(&result.unwrap().value, &Expr::Literal(Literal::Unit));

        // Single element tuple
        let (result, errors) = parse_expr("(42,)");
        assert!(
            result.is_some(),
            "Expected successful parse for single element tuple"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for single element tuple"
        );
        if let Some(expr) = result {
            if let Expr::Tuple(elements) = &*expr.value {
                assert_eq!(elements.len(), 1);
                assert_expr_eq(&elements[0].value, &Expr::Literal(Literal::Int64(42)));
            } else {
                panic!("Expected Tuple expression");
            }
        }

        // Multiple element tuple
        let (result, errors) = parse_expr("(1, 2, 3)");
        assert!(
            result.is_some(),
            "Expected successful parse for multiple element tuple"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for multiple element tuple"
        );
        if let Some(expr) = result {
            if let Expr::Tuple(elements) = &*expr.value {
                assert_eq!(elements.len(), 3);
                assert_expr_eq(&elements[0].value, &Expr::Literal(Literal::Int64(1)));
                assert_expr_eq(&elements[1].value, &Expr::Literal(Literal::Int64(2)));
                assert_expr_eq(&elements[2].value, &Expr::Literal(Literal::Int64(3)));
            } else {
                panic!("Expected Tuple expression");
            }
        }

        // Tuple with trailing comma
        let (result, errors) = parse_expr("(1, 2, 3,)");
        assert!(
            result.is_some(),
            "Expected successful parse for tuple with trailing comma"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for tuple with trailing comma"
        );
        if let Some(expr) = result {
            if let Expr::Tuple(elements) = &*expr.value {
                assert_eq!(elements.len(), 3);
            } else {
                panic!("Expected Tuple expression");
            }
        }
    }

    #[test]
    fn test_maps() {
        // Empty map
        let (result, errors) = parse_expr("{}");
        assert!(result.is_some(), "Expected successful parse for empty map");
        assert!(errors.is_empty(), "Expected no errors for empty map");
        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert!(entries.is_empty());
            } else {
                panic!("Expected Map expression, got {:?}", expr.value);
            }
        }

        // Single entry map
        let (result, errors) = parse_expr("{\"key\": 42}");
        assert!(
            result.is_some(),
            "Expected successful parse for single entry map"
        );
        assert!(errors.is_empty(), "Expected no errors for single entry map");
        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 1);
                assert_expr_eq(
                    &entries[0].0.value,
                    &Expr::Literal(Literal::String("key".to_string())),
                );
                assert_expr_eq(&entries[0].1.value, &Expr::Literal(Literal::Int64(42)));
            } else {
                panic!("Expected Map expression");
            }
        }

        // Multiple entry map
        let (result, errors) = parse_expr("{\"a\": 1, \"b\": 2, \"c\": 3}");
        assert!(
            result.is_some(),
            "Expected successful parse for multiple entry map"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for multiple entry map"
        );
        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 3);
                assert_expr_eq(
                    &entries[0].0.value,
                    &Expr::Literal(Literal::String("a".to_string())),
                );
                assert_expr_eq(&entries[0].1.value, &Expr::Literal(Literal::Int64(1)));
                assert_expr_eq(
                    &entries[1].0.value,
                    &Expr::Literal(Literal::String("b".to_string())),
                );
                assert_expr_eq(&entries[1].1.value, &Expr::Literal(Literal::Int64(2)));
                assert_expr_eq(
                    &entries[2].0.value,
                    &Expr::Literal(Literal::String("c".to_string())),
                );
                assert_expr_eq(&entries[2].1.value, &Expr::Literal(Literal::Int64(3)));
            } else {
                panic!("Expected Map expression");
            }
        }

        // Map with trailing comma
        let (result, errors) = parse_expr("{\"a\": 1, \"b\": 2,}");
        assert!(
            result.is_some(),
            "Expected successful parse for map with trailing comma"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for map with trailing comma"
        );
        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 2);
            } else {
                panic!("Expected Map expression");
            }
        }

        // Complex map with expressions as keys and values
        let (result, errors) = parse_expr("{x + 1: y * 2, \"key\": z.field}");
        assert!(
            result.is_some(),
            "Expected successful parse for complex map"
        );
        assert!(errors.is_empty(), "Expected no errors for complex map");
        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 2);

                // First entry: x + 1: y * 2
                if let Expr::Binary(add_left, BinOp::Add, add_right) = &*entries[0].0.value {
                    assert_expr_eq(&add_left.value, &Expr::Ref("x".to_string()));
                    assert_expr_eq(&add_right.value, &Expr::Literal(Literal::Int64(1)));
                } else {
                    panic!("Expected binary addition in first key");
                }

                if let Expr::Binary(mul_left, BinOp::Mul, mul_right) = &*entries[0].1.value {
                    assert_expr_eq(&mul_left.value, &Expr::Ref("y".to_string()));
                    assert_expr_eq(&mul_right.value, &Expr::Literal(Literal::Int64(2)));
                } else {
                    panic!("Expected binary multiplication in first value");
                }

                // Second entry: "key": z.field
                assert_expr_eq(
                    &entries[1].0.value,
                    &Expr::Literal(Literal::String("key".to_string())),
                );

                if let Expr::Postfix(expr, PostfixOp::Member(member)) = &*entries[1].1.value {
                    assert_expr_eq(&expr.value, &Expr::Ref("z".to_string()));
                    assert_eq!(member, "field");
                } else {
                    panic!("Expected member access in second value");
                }
            } else {
                panic!("Expected Map expression");
            }
        }
    }

    #[test]
    fn test_complex_binary_operations() {
        // Test complex binary operation precedence and associativity
        let (result, errors) = parse_expr("1 + 2 * 3 - 4 / 2 == 5 && true || x > 10");

        assert!(
            result.is_some(),
            "Expected successful parse for complex binary operations"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for complex binary operations"
        );

        if let Some(expr) = result {
            // Expect: ((1 + (2 * 3)) - (4 / 2) == 5) && true || (x > 10)
            if let Expr::Binary(left, BinOp::Or, right) = &*expr.value {
                // Check left side: ((1 + (2 * 3)) - (4 / 2) == 5) && true
                if let Expr::Binary(and_left, BinOp::And, and_right) = &*left.value {
                    // Check equality condition
                    if let Expr::Binary(eq_left, BinOp::Eq, eq_right) = &*and_left.value {
                        // Check subtraction
                        if let Expr::Binary(sub_left, BinOp::Sub, sub_right) = &*eq_left.value {
                            // Check addition
                            if let Expr::Binary(add_left, BinOp::Add, add_right) = &*sub_left.value
                            {
                                assert_expr_eq(&add_left.value, &Expr::Literal(Literal::Int64(1)));

                                // Check multiplication
                                if let Expr::Binary(mul_left, BinOp::Mul, mul_right) =
                                    &*add_right.value
                                {
                                    assert_expr_eq(
                                        &mul_left.value,
                                        &Expr::Literal(Literal::Int64(2)),
                                    );
                                    assert_expr_eq(
                                        &mul_right.value,
                                        &Expr::Literal(Literal::Int64(3)),
                                    );
                                } else {
                                    panic!("Expected multiplication in add_right");
                                }
                            } else {
                                panic!("Expected addition in sub_left");
                            }

                            // Check division
                            if let Expr::Binary(div_left, BinOp::Div, div_right) = &*sub_right.value
                            {
                                assert_expr_eq(&div_left.value, &Expr::Literal(Literal::Int64(4)));
                                assert_expr_eq(&div_right.value, &Expr::Literal(Literal::Int64(2)));
                            } else {
                                panic!("Expected division in sub_right");
                            }
                        } else {
                            panic!("Expected subtraction in eq_left");
                        }

                        assert_expr_eq(&eq_right.value, &Expr::Literal(Literal::Int64(5)));
                    } else {
                        panic!("Expected equality comparison in and_left");
                    }

                    assert_expr_eq(&and_right.value, &Expr::Literal(Literal::Bool(true)));
                } else {
                    panic!("Expected AND operation in left side of OR");
                }

                // Check right side: x > 10
                if let Expr::Binary(gt_left, BinOp::Gt, gt_right) = &*right.value {
                    assert_expr_eq(&gt_left.value, &Expr::Ref("x".to_string()));
                    assert_expr_eq(&gt_right.value, &Expr::Literal(Literal::Int64(10)));
                } else {
                    panic!("Expected greater-than comparison in right side of OR");
                }
            } else {
                panic!("Expected OR operation at top level");
            }
        }
    }

    #[test]
    fn test_nested_expressions() {
        // Test deeply nested expressions with different expression types
        let (result, errors) = parse_expr(
            "if ({[1 + 2, 3]: 5}.size > 0) then { let x = 42 in x } else fail(\"Empty map\")",
        );

        assert!(
            result.is_some(),
            "Expected successful parse for nested expressions"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for nested expressions"
        );

        if let Some(expr) = result {
            // Test if-then-else structure
            if let Expr::IfThenElse(condition, then_branch, else_branch) = &*expr.value {
                // Test condition with member access and binary operation
                if let Expr::Binary(left, BinOp::Gt, right) = &*condition.value {
                    // Test member access on map expression
                    if let Expr::Postfix(expr, PostfixOp::Member(member)) = &*left.value {
                        assert_eq!(member, "size");

                        // Check that expression is a Map
                        if let Expr::Map(entries) = &*expr.value {
                            assert_eq!(entries.len(), 1, "Expected one map entry");

                            // Check the key is an array [1 + 2, 3]
                            let (key, value) = &entries[0];
                            if let Expr::Array(items) = &*key.value {
                                assert_eq!(items.len(), 2);

                                // Test binary addition in first array element
                                if let Expr::Binary(add_left, BinOp::Add, add_right) =
                                    &*items[0].value
                                {
                                    assert_expr_eq(
                                        &add_left.value,
                                        &Expr::Literal(Literal::Int64(1)),
                                    );
                                    assert_expr_eq(
                                        &add_right.value,
                                        &Expr::Literal(Literal::Int64(2)),
                                    );
                                } else {
                                    panic!("Expected addition in first array element");
                                }

                                assert_expr_eq(&items[1].value, &Expr::Literal(Literal::Int64(3)));
                            } else {
                                panic!("Expected array as map key");
                            }

                            // Check the value is 5
                            assert_expr_eq(&value.value, &Expr::Literal(Literal::Int64(5)));
                        } else {
                            panic!("Expected Map expression");
                        }
                    } else {
                        panic!("Expected member access in condition");
                    }

                    assert_expr_eq(&right.value, &Expr::Literal(Literal::Int64(0)));
                } else {
                    panic!("Expected binary comparison in condition");
                }

                // Test then branch with let expression
                if let Expr::Let(name, value, body) = &*then_branch.value {
                    assert_eq!(*name.value.name.value, "x");
                    assert_expr_eq(&value.value, &Expr::Literal(Literal::Int64(42)));
                    assert_expr_eq(&body.value, &Expr::Ref("x".to_string()));
                } else {
                    panic!("Expected let expression in then branch");
                }

                // Test else branch with fail expression
                if let Expr::Fail(message) = &*else_branch.value {
                    if let Expr::Literal(Literal::String(s)) = &*message.value {
                        assert_eq!(s, "Empty map");
                    } else {
                        panic!("Expected string literal in fail message");
                    }
                } else {
                    panic!("Expected fail expression in else branch");
                }
            } else {
                panic!("Expected if-then-else expression");
            }
        }
    }

    #[test]
    fn test_nested_closures_and_calls() {
        // Test nested closures with function calls
        let (result, errors) =
            parse_expr("(x: I64) => (y: F64) => func(x + 1, y * 2.5, z => z && true)");

        assert!(
            result.is_some(),
            "Expected successful parse for nested closures"
        );
        assert!(errors.is_empty(), "Expected no errors for nested closures");

        if let Some(expr) = result {
            // Outer closure
            if let Expr::Closure(params1, body1) = &*expr.value {
                assert_eq!(params1.len(), 1);
                assert_eq!(*params1[0].value.name.value, "x");
                if let Type::Int64 = *params1[0].value.ty.value {
                    // Expected Int64 type
                } else {
                    panic!("Expected Int64 type for first parameter");
                }

                // Inner closure
                if let Expr::Closure(params2, body2) = &*body1.value {
                    assert_eq!(params2.len(), 1);
                    assert_eq!(*params2[0].value.name.value, "y");
                    if let Type::Float64 = *params2[0].value.ty.value {
                        // Expected Float64 type
                    } else {
                        panic!("Expected Float64 type for second parameter");
                    }

                    // Function call
                    if let Expr::Postfix(func, PostfixOp::Call(args)) = &*body2.value {
                        assert_expr_eq(&func.value, &Expr::Ref("func".to_string()));
                        assert_eq!(args.len(), 3);

                        // First argument: x + 1
                        if let Expr::Binary(left, BinOp::Add, right) = &*args[0].value {
                            assert_expr_eq(&left.value, &Expr::Ref("x".to_string()));
                            assert_expr_eq(&right.value, &Expr::Literal(Literal::Int64(1)));
                        } else {
                            panic!("Expected addition in first argument");
                        }

                        // Second argument: y * 2.5
                        if let Expr::Binary(left, BinOp::Mul, right) = &*args[1].value {
                            assert_expr_eq(&left.value, &Expr::Ref("y".to_string()));
                            if let Expr::Literal(Literal::Float64(f)) = &*right.value {
                                assert_eq!(f.into_inner(), 2.5);
                            } else {
                                panic!("Expected float literal in second argument");
                            }
                        } else {
                            panic!("Expected multiplication in second argument");
                        }

                        // Third argument: (z) -> z && true
                        if let Expr::Closure(params3, body3) = &*args[2].value {
                            assert_eq!(params3.len(), 1);
                            assert_eq!(*params3[0].value.name.value, "z");

                            if let Expr::Binary(left, BinOp::And, right) = &*body3.value {
                                assert_expr_eq(&left.value, &Expr::Ref("z".to_string()));
                                assert_expr_eq(&right.value, &Expr::Literal(Literal::Bool(true)));
                            } else {
                                panic!("Expected logical AND in innermost closure");
                            }
                        } else {
                            panic!("Expected closure in third argument");
                        }
                    } else {
                        panic!("Expected function call in inner closure body");
                    }
                } else {
                    panic!("Expected inner closure in outer closure body");
                }
            } else {
                panic!("Expected outer closure");
            }
        }
    }

    #[test]
    fn test_map_constructor_and_collections() {
        // Test map constructor and complex collections
        let (result, errors) = parse_expr("{\"key1\": (1, true, [1..5]), someVar: {x: y}}");

        assert!(
            result.is_some(),
            "Expected successful parse for map constructor"
        );
        assert!(errors.is_empty(), "Expected no errors for map constructor");

        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 2, "Expected two map entries");

                // First entry: "key1": (1, true, [1..5])
                if let Expr::Literal(Literal::String(key)) = &*entries[0].0.value {
                    assert_eq!(key, "key1");
                } else {
                    panic!("Expected string literal as first key");
                }

                if let Expr::Tuple(elements) = &*entries[0].1.value {
                    assert_eq!(elements.len(), 3);
                    assert_expr_eq(&elements[0].value, &Expr::Literal(Literal::Int64(1)));
                    assert_expr_eq(&elements[1].value, &Expr::Literal(Literal::Bool(true)));

                    // Range inside array
                    if let Expr::Array(items) = &*elements[2].value {
                        assert_eq!(items.len(), 1);
                        if let Expr::Binary(left, BinOp::Range, right) = &*items[0].value {
                            assert_expr_eq(&left.value, &Expr::Literal(Literal::Int64(1)));
                            assert_expr_eq(&right.value, &Expr::Literal(Literal::Int64(5)));
                        } else {
                            panic!("Expected range in array");
                        }
                    } else {
                        panic!("Expected array in tuple");
                    }
                } else {
                    panic!("Expected tuple as first value");
                }

                // Second entry: someVar: {x: y}
                assert_expr_eq(&entries[1].0.value, &Expr::Ref("someVar".to_string()));

                if let Expr::Map(nested_entries) = &*entries[1].1.value {
                    assert_eq!(nested_entries.len(), 1);
                    assert_expr_eq(&nested_entries[0].0.value, &Expr::Ref("x".to_string()));
                    assert_expr_eq(&nested_entries[0].1.value, &Expr::Ref("y".to_string()));
                } else {
                    panic!("Expected nested map as second value");
                }
            } else {
                panic!("Expected map constructor");
            }
        }
    }

    #[test]
    fn test_let_expressions() {
        // Basic let expression
        let (result, errors) = parse_expr("let x = 42 in x + 10");
        assert!(
            result.is_some(),
            "Expected successful parse for basic let expression"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for basic let expression"
        );

        if let Some(expr) = result {
            if let Expr::Let(field, value, body) = &*expr.value {
                assert_eq!(*field.value.name.value, "x");
                assert!(matches!(*field.value.ty.value, Type::Unknown));
                assert_expr_eq(&value.value, &Expr::Literal(Literal::Int64(42)));

                if let Expr::Binary(left, BinOp::Add, right) = &*body.value {
                    assert_expr_eq(&left.value, &Expr::Ref("x".to_string()));
                    assert_expr_eq(&right.value, &Expr::Literal(Literal::Int64(10)));
                } else {
                    panic!("Expected binary addition in let body");
                }
            } else {
                panic!("Expected let expression");
            }
        }

        // Let with type annotation
        let (result, errors) = parse_expr("let x: I64 = 42 in x + 10");
        assert!(
            result.is_some(),
            "Expected successful parse for let with type annotation"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for let with type annotation"
        );

        if let Some(expr) = result {
            if let Expr::Let(field, value, _) = &*expr.value {
                assert_eq!(*field.value.name.value, "x");
                assert!(matches!(*field.value.ty.value, Type::Int64));
                assert_expr_eq(&value.value, &Expr::Literal(Literal::Int64(42)));
            } else {
                panic!("Expected let expression with type annotation");
            }
        }

        // Chained let expressions
        let (result, errors) = parse_expr("let x = 5, y: I64 = x * 2, z = \"hello\" in x + y");
        assert!(
            result.is_some(),
            "Expected successful parse for chained let bindings"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for chained let bindings"
        );

        if let Some(expr) = result {
            // The outermost let should be for x (first in the chain)
            if let Expr::Let(field_x, val_x, body_x) = &*expr.value {
                assert_eq!(*field_x.value.name.value, "x");
                assert!(matches!(*field_x.value.ty.value, Type::Unknown));
                assert_expr_eq(&val_x.value, &Expr::Literal(Literal::Int64(5)));

                // Next level should be y with explicit I64 type
                if let Expr::Let(field_y, val_y, body_y) = &*body_x.value {
                    assert_eq!(*field_y.value.name.value, "y");
                    assert!(matches!(*field_y.value.ty.value, Type::Int64));

                    if let Expr::Binary(mul_left, BinOp::Mul, mul_right) = &*val_y.value {
                        assert_expr_eq(&mul_left.value, &Expr::Ref("x".to_string()));
                        assert_expr_eq(&mul_right.value, &Expr::Literal(Literal::Int64(2)));
                    } else {
                        panic!("Expected multiplication in y's value");
                    }

                    // Innermost let should be z
                    if let Expr::Let(field_z, val_z, body_z) = &*body_y.value {
                        assert_eq!(*field_z.value.name.value, "z");
                        assert!(matches!(*field_z.value.ty.value, Type::Unknown));
                        assert_expr_eq(
                            &val_z.value,
                            &Expr::Literal(Literal::String("hello".to_string())),
                        );

                        // The final body should be x + y
                        if let Expr::Binary(add_left, BinOp::Add, add_right) = &*body_z.value {
                            assert_expr_eq(&add_left.value, &Expr::Ref("x".to_string()));
                            assert_expr_eq(&add_right.value, &Expr::Ref("y".to_string()));
                        } else {
                            panic!("Expected addition in final body");
                        }
                    } else {
                        panic!("Expected innermost let for z");
                    }
                } else {
                    panic!("Expected middle let for y");
                }
            } else {
                panic!("Expected outermost let for x");
            }
        }

        // Traditional nested let expressions should still work
        let (result, errors) = parse_expr("let x = 5 in let y = x * 2 in x + y");
        assert!(
            result.is_some(),
            "Expected successful parse for traditional nested let expressions"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for traditional nested let expressions"
        );

        if let Some(expr) = result {
            if let Expr::Let(outer_field, outer_value, outer_body) = &*expr.value {
                assert_eq!(*outer_field.value.name.value, "x");
                assert_expr_eq(&outer_value.value, &Expr::Literal(Literal::Int64(5)));

                if let Expr::Let(inner_field, inner_value, inner_body) = &*outer_body.value {
                    assert_eq!(*inner_field.value.name.value, "y");

                    if let Expr::Binary(mul_left, BinOp::Mul, mul_right) = &*inner_value.value {
                        assert_expr_eq(&mul_left.value, &Expr::Ref("x".to_string()));
                        assert_expr_eq(&mul_right.value, &Expr::Literal(Literal::Int64(2)));
                    } else {
                        panic!("Expected multiplication in inner let value");
                    }

                    if let Expr::Binary(add_left, BinOp::Add, add_right) = &*inner_body.value {
                        assert_expr_eq(&add_left.value, &Expr::Ref("x".to_string()));
                        assert_expr_eq(&add_right.value, &Expr::Ref("y".to_string()));
                    } else {
                        panic!("Expected addition in inner let body");
                    }
                } else {
                    panic!("Expected inner let expression");
                }
            } else {
                panic!("Expected outer let expression");
            }
        }

        // Let with complex body
        let (result, errors) = parse_expr("let f: I64 => I64 = (x) => x * x in f(10)");
        assert!(
            result.is_some(),
            "Expected successful parse for let with complex body and type annotation"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for let with complex body and type annotation"
        );

        if let Some(expr) = result {
            if let Expr::Let(field, value, body) = &*expr.value {
                assert_eq!(*field.value.name.value, "f");
                assert!(matches!(*field.value.ty.value, Type::Closure(_, _)));

                // Check that value is a closure
                if let Expr::Closure(params, closure_body) = &*value.value {
                    assert_eq!(params.len(), 1);
                    assert_eq!(*params[0].value.name.value, "x");

                    if let Expr::Binary(mul_left, BinOp::Mul, mul_right) = &*closure_body.value {
                        assert_expr_eq(&mul_left.value, &Expr::Ref("x".to_string()));
                        assert_expr_eq(&mul_right.value, &Expr::Ref("x".to_string()));
                    } else {
                        panic!("Expected multiplication in closure body");
                    }
                } else {
                    panic!("Expected closure in let value");
                }

                // Check that body is a function call
                if let Expr::Postfix(func, PostfixOp::Call(args)) = &*body.value {
                    assert_expr_eq(&func.value, &Expr::Ref("f".to_string()));
                    assert_eq!(args.len(), 1);
                    assert_expr_eq(&args[0].value, &Expr::Literal(Literal::Int64(10)));
                } else {
                    panic!("Expected function call in let body");
                }
            } else {
                panic!("Expected let expression");
            }
        }
    }

    #[test]
    fn test_chained_postfix_operations() {
        // Test method call
        let (result, errors) = parse_expr("obj.method(a)(a)(a)");

        assert!(
            result.is_some(),
            "Expected successful parse for method call"
        );
        assert!(errors.is_empty(), "Expected no errors for method call");

        if let Some(expr) = result {
            if let Expr::Postfix(inner1, PostfixOp::Call(args1)) = &*expr.value {
                assert_eq!(args1.len(), 1);
                if let Expr::Postfix(inner2, PostfixOp::Call(args2)) = &*inner1.value {
                    assert_eq!(args2.len(), 1);
                    if let Expr::Postfix(inner3, PostfixOp::Call(args3)) = &*inner2.value {
                        assert_eq!(args3.len(), 1);
                        if let Expr::Postfix(obj, PostfixOp::Member(method)) = &*inner3.value {
                            assert_expr_eq(&obj.value, &Expr::Ref("obj".to_string()));
                            assert_eq!(method, "method");
                        } else {
                            panic!("Expected member access at base");
                        }
                    } else {
                        panic!("Expected third call");
                    }
                } else {
                    panic!("Expected second call");
                }
            } else {
                panic!("Expected first call");
            }
        }

        // Test function call followed by field access
        let (result, errors) = parse_expr("func().field");
        assert!(
            result.is_some(),
            "Expected successful parse for func().field"
        );
        assert!(errors.is_empty(), "Expected no errors for func().field");

        if let Some(expr) = result {
            if let Expr::Postfix(inner, PostfixOp::Member(member)) = &*expr.value {
                assert_eq!(member, "field");
                if let Expr::Postfix(func, PostfixOp::Call(args)) = &*inner.value {
                    assert_eq!(args.len(), 0);
                    assert_expr_eq(&func.value, &Expr::Ref("func".to_string()));
                } else {
                    panic!("Expected call in function call");
                }
            } else {
                panic!("Expected member access");
            }
        }

        // Test complex chain of operations
        let (result, errors) = parse_expr("obj.method().field.other_method(arg)");
        assert!(
            result.is_some(),
            "Expected successful parse for complex chain"
        );
        assert!(errors.is_empty(), "Expected no errors for complex chain");

        if let Some(expr) = result {
            if let Expr::Postfix(inner1, PostfixOp::Call(args)) = &*expr.value {
                assert_eq!(args.len(), 1);
                assert_expr_eq(&args[0].value, &Expr::Ref("arg".to_string()));

                if let Expr::Postfix(inner2, PostfixOp::Member(member)) = &*inner1.value {
                    assert_eq!(member, "other_method");

                    if let Expr::Postfix(inner3, PostfixOp::Member(field)) = &*inner2.value {
                        assert_eq!(field, "field");

                        if let Expr::Postfix(inner4, PostfixOp::Call(method_args)) = &*inner3.value
                        {
                            assert_eq!(method_args.len(), 0);

                            if let Expr::Postfix(obj, PostfixOp::Member(method)) = &*inner4.value {
                                assert_expr_eq(&obj.value, &Expr::Ref("obj".to_string()));
                                assert_eq!(method, "method");
                            } else {
                                panic!("Expected member access for initial method");
                            }
                        } else {
                            panic!("Expected call for first method");
                        }
                    } else {
                        panic!("Expected member access for field");
                    }
                } else {
                    panic!("Expected member access for final method");
                }
            } else {
                panic!("Expected call at top level");
            }
        }

        // Test compose operator
        let (result, errors) = parse_expr("map(dat) -> filter");

        assert!(
            result.is_some(),
            "Expected successful parse for compose operator"
        );
        assert!(errors.is_empty(), "Expected no errors for compose operator");

        if let Some(expr) = result {
            if let Expr::Postfix(inner, PostfixOp::Compose(name)) = &*expr.value {
                assert_eq!(name, "filter");

                if let Expr::Postfix(func, PostfixOp::Call(args)) = &*inner.value {
                    assert_expr_eq(&func.value, &Expr::Ref("map".to_string()));
                    assert_eq!(args.len(), 1);
                    assert_expr_eq(&args[0].value, &Expr::Ref("dat".to_string()));
                } else {
                    panic!("Expected function call before compose operator");
                }
            } else {
                panic!("Expected compose operator");
            }
        }

        // Test chained compose operators
        let (result, errors) = parse_expr("transform(input) -> map -> filter -> reduce");
        assert!(
            result.is_some(),
            "Expected successful parse for chained compose operators"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for chained compose operators"
        );

        if let Some(expr) = result {
            if let Expr::Postfix(inner1, PostfixOp::Compose(name1)) = &*expr.value {
                assert_eq!(name1, "reduce");

                if let Expr::Postfix(inner2, PostfixOp::Compose(name2)) = &*inner1.value {
                    assert_eq!(name2, "filter");

                    if let Expr::Postfix(inner3, PostfixOp::Compose(name3)) = &*inner2.value {
                        assert_eq!(name3, "map");

                        if let Expr::Postfix(func, PostfixOp::Call(args)) = &*inner3.value {
                            assert_expr_eq(&func.value, &Expr::Ref("transform".to_string()));
                            assert_eq!(args.len(), 1);
                            assert_expr_eq(&args[0].value, &Expr::Ref("input".to_string()));
                        } else {
                            panic!("Expected function call at the beginning of chain");
                        }
                    } else {
                        panic!("Expected first compose operation in chain");
                    }
                } else {
                    panic!("Expected second compose operation in chain");
                }
            } else {
                panic!("Expected third compose operation in chain");
            }
        }
    }

    #[test]
    fn test_match_expressions() {
        // Simple match expression
        let (result, errors) =
            parse_expr("match x | 1 => \"one\" | 2 => \"two\" \\ _ => \"other\"");
        assert!(
            result.is_some(),
            "Expected successful parse for simple match expression"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for simple match expression"
        );

        if let Some(expr) = result {
            if let Expr::PatternMatch(scrutinee, arms) = &*expr.value {
                assert_expr_eq(&scrutinee.value, &Expr::Ref("x".to_string()));
                assert_eq!(arms.len(), 3);

                // First arm: 1 -> "one"
                if let Pattern::Literal(Literal::Int64(n)) = *arms[0].value.pattern.value {
                    assert_eq!(n, 1);
                } else {
                    panic!("Expected integer literal pattern in first arm");
                }
                assert_expr_eq(
                    &arms[0].value.expr.value,
                    &Expr::Literal(Literal::String("one".to_string())),
                );

                // Second arm: 2 -> "two"
                if let Pattern::Literal(Literal::Int64(n)) = *arms[1].value.pattern.value {
                    assert_eq!(n, 2);
                } else {
                    panic!("Expected integer literal pattern in second arm");
                }
                assert_expr_eq(
                    &arms[1].value.expr.value,
                    &Expr::Literal(Literal::String("two".to_string())),
                );

                // Third arm: _ -> "other"
                if let Pattern::Wildcard = *arms[2].value.pattern.value {
                    // Expected wildcard pattern
                } else {
                    panic!("Expected wildcard pattern in third arm");
                }
                assert_expr_eq(
                    &arms[2].value.expr.value,
                    &Expr::Literal(Literal::String("other".to_string())),
                );
            } else {
                panic!("Expected match expression");
            }
        }

        // Match with complex patterns and expressions
        let (result, errors) = parse_expr(
            "match point | Point(x, y) => \"first quadrant\" \
                         | Circle(r) => \"circle\" \
                         | Rectangle(b: Stuff(_), h) => \"rectangle\" \
                         \\ _ => \"unknown shape\"",
        );
        assert!(
            result.is_some(),
            "Expected successful parse for match with complex patterns"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for match with complex patterns"
        );

        if let Some(expr) = result {
            if let Expr::PatternMatch(scrutinee, arms) = &*expr.value {
                assert_expr_eq(&scrutinee.value, &Expr::Ref("point".to_string()));
                assert_eq!(arms.len(), 4);

                // First arm: Point(x, y) -> "first quadrant"
                if let Pattern::Constructor(ref name, ref fields) = *arms[0].value.pattern.value {
                    assert_eq!(*name.value, "Point");
                    assert_eq!(fields.len(), 2);

                    // Check that x and y are captured as variables
                    if let Pattern::Bind(ref var_name, _) = *fields[0].value {
                        assert_eq!(*var_name.value, "x");
                    } else {
                        panic!("Expected binding pattern for x");
                    }

                    if let Pattern::Bind(ref var_name, _) = *fields[1].value {
                        assert_eq!(*var_name.value, "y");
                    } else {
                        panic!("Expected binding pattern for y");
                    }
                } else {
                    panic!("Expected constructor pattern in first arm");
                }

                // Second arm: Circle(r) -> "circle"
                if let Pattern::Constructor(ref name, ref fields) = *arms[1].value.pattern.value {
                    assert_eq!(*name.value, "Circle");
                    assert_eq!(fields.len(), 1);

                    if let Pattern::Bind(ref var_name, _) = *fields[0].value {
                        assert_eq!(*var_name.value, "r");
                    } else {
                        panic!("Expected binding pattern for r");
                    }
                } else {
                    panic!("Expected constructor pattern in second arm");
                }

                // Third arm: Rectangle(w, h) -> "rectangle"
                if let Pattern::Constructor(ref name, ref fields) = *arms[2].value.pattern.value {
                    assert_eq!(*name.value, "Rectangle");
                    assert_eq!(fields.len(), 2);
                } else {
                    panic!("Expected constructor pattern in third arm");
                }

                // Last arm should be a wildcard
                if let Pattern::Wildcard = *arms[3].value.pattern.value {
                    // Expected wildcard pattern
                } else {
                    panic!("Expected wildcard pattern in last arm");
                }
            } else {
                panic!("Expected match expression");
            }
        }
    }

    #[test]
    fn test_crazy_composite_expression() {
        // This test creates an insanely complex nested expression with multiple features
        let crazy_expr = "let create_calculator = (operation) => match operation \
                          | \"add\" => (x, y) => x + y \
                          | \"subtract\" => (x, y) => x - y \
                          | \"multiply\" => (x, y) => x * y \
                          | \"divide\" => (x, y) => if y == 0 then fail(\"Division by zero\") else x / y \
                          \\ _ => (x, y) => -1, \
                            calc = create_calculator(\"multiply\"), \
                            result = calc({\"key\": 6}.key, 7), \
                          in if result > 40 \
                             then { \
                               let final_result = (result, [1..5], {\"message\": \"High value\"}) in \
                               DataResult(final_result).output \
                             } \
                             else result";

        let (result, errors) = parse_expr(crazy_expr);

        assert!(
            result.is_some(),
            "Expected successful parse for crazy composite expression"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for crazy composite expression"
        );

        // Just verify the outer structure because testing everything would be excessive
        if let Some(expr) = result {
            if let Expr::Let(field1, _, body1) = &*expr.value {
                assert_eq!(*field1.value.name.value, "create_calculator");

                if let Expr::Let(field2, _, body2) = &*body1.value {
                    assert_eq!(*field2.value.name.value, "calc");

                    if let Expr::Let(field3, _, body3) = &*body2.value {
                        assert_eq!(*field3.value.name.value, "result");

                        if let Expr::IfThenElse(_, _, _) = &*body3.value {
                            // Successfully parsed the entire structure
                        } else {
                            panic!("Expected if-then-else as innermost body");
                        }
                    } else {
                        panic!("Expected third let expression");
                    }
                } else {
                    panic!("Expected second let expression");
                }
            } else {
                panic!("Expected first let expression");
            }
        }

        // Test the chained version of the let expressions
        let chained_crazy_expr = "let \
                                  create_calculator = (operation) => match operation \
                                  | \"add\" => (x, y) => x + y \
                                  | \"subtract\" => (x, y) => x - y \
                                  | \"multiply\" => (x, y) => x * y \
                                  | \"divide\" => (x, y) => if y == 0 then fail(\"Division by zero\") else x / y \
                                  \\ _ => (x, y) => -1, \
                                  calc = create_calculator(\"multiply\"), \
                                  result: I64 = calc({\"key\": 6}.key, 7) \
                                  in if result > 40 \
                                     then { \
                                       let final_result = (result, [1..5], {\"message\": \"High value\"}) in \
                                       DataResult(final_result).output \
                                     } \
                                     else result";

        let (result, errors) = parse_expr(chained_crazy_expr);

        assert!(
            result.is_some(),
            "Expected successful parse for chained crazy composite expression"
        );
        assert!(
            errors.is_empty(),
            "Expected no errors for chained crazy composite expression"
        );

        // Check that the chained version has the same structure
        if let Some(expr) = result {
            if let Expr::Let(field1, _, body1) = &*expr.value {
                assert_eq!(*field1.value.name.value, "create_calculator");

                if let Expr::Let(field2, _, body2) = &*body1.value {
                    assert_eq!(*field2.value.name.value, "calc");

                    if let Expr::Let(field3, _, body3) = &*body2.value {
                        assert_eq!(*field3.value.name.value, "result");
                        assert!(matches!(*field3.value.ty.value, Type::Int64));

                        if let Expr::IfThenElse(_, _, _) = &*body3.value {
                            // Successfully parsed the entire structure
                        } else {
                            panic!("Expected if-then-else as innermost body");
                        }
                    } else {
                        panic!("Expected result let expression");
                    }
                } else {
                    panic!("Expected calc let expression");
                }
            } else {
                panic!("Expected create_calculator let expression");
            }
        }
    }
}

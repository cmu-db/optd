use chumsky::{
    error::Simple,
    prelude::{choice, just, nested_delimiters, recursive},
    select, Parser,
};

use crate::{
    errors::span::{Span, Spanned},
    lexer::tokens::{Token, ALL_DELIMITERS},
    parser::ast::{BinOp, Expr, Literal, MatchArm, UnaryOp},
};

use super::{
    ast::{Field, Type},
    pattern::pattern_parser,
    r#type::type_parser,
};

// Generic helper for delimited parsing with error recovery (TODO: move to utils)
fn delimited_parser<T, R, F>(
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
        .map(move |x| f(x))
}

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
            expr.clone()
                .separated_by(just(Token::Comma))
                .allow_trailing(),
            Token::LParen,
            Token::RParen,
            |items| items.map(Expr::Tuple).unwrap_or(Expr::Error),
        )
        .map_with_span(Spanned::new)
        .boxed();

        let map = just(Token::Map)
            .ignore_then(delimited_parser(
                expr.clone()
                    .then_ignore(just(Token::Arrow))
                    .then(expr.clone())
                    .separated_by(just(Token::Comma))
                    .allow_trailing(),
                Token::LBracket,
                Token::RBracket,
                |items| match items {
                    Some(items) => Expr::Map(items.into_iter().collect()),
                    None => Expr::Error,
                },
            ))
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
            let param_list = delimited_parser(
                select! { Token::TermIdent(name) => name }
                    .map_with_span(Spanned::new)
                    .then(just(Token::Colon).ignore_then(type_parser()).or_not())
                    .map(|(name, type_opt)| {
                        let ty = type_opt.unwrap_or(Spanned::new(Type::Unknown, name.span.clone()));
                        Field { name, ty }
                    })
                    .map_with_span(Spanned::new)
                    .separated_by(just(Token::Comma))
                    .allow_trailing(),
                Token::LParen,
                Token::RParen,
                |params| params.unwrap_or_default(),
            );

            let empty_params = just(Token::Unit).map(|_| vec![]);

            choice((param_list, empty_params))
                .then_ignore(just(Token::Arrow))
                .then(expr.clone())
                .map(|(params, body)| Expr::Closure(params, body))
                .map_with_span(Spanned::new)
                .boxed()
        };

        let paren_expr =
            delimited_parser(expr.clone(), Token::LParen, Token::RParen, |inner_expr| {
                inner_expr.map(|e| *e.value).unwrap_or(Expr::Error)
            })
            .map_with_span(Spanned::new)
            .boxed();

        let brace_expr =
            delimited_parser(expr.clone(), Token::LBrace, Token::RBrace, |inner_expr| {
                inner_expr.map(|e| *e.value).unwrap_or(Expr::Error)
            })
            .map_with_span(Spanned::new)
            .boxed();

        let atom = choice((
            closure,
            literal,
            reference,
            array,
            tuple,
            map,
            fail,
            constructor,
            paren_expr,
            brace_expr,
        ));

        let call_expr = atom
            .clone()
            .then(delimited_parser(
                expr.clone()
                    .separated_by(just(Token::Comma))
                    .allow_trailing(),
                Token::LParen,
                Token::RParen,
                |args| args.unwrap_or_default(),
            ))
            .map_with_span(|(func, args), span| Spanned::new(Expr::Call(func, args), span))
            .boxed();

        let member_access_expr = atom
            .clone()
            .then(just(Token::Dot).ignore_then(select! { Token::TermIdent(name) => name }))
            .map_with_span(|(expr, member), span| {
                Spanned::new(Expr::MemberAccess(expr, member), span)
            })
            .boxed();

        let postfix = choice((call_expr, member_access_expr, atom));

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

        let let_expr = just(Token::Let)
            .ignore_then(select! { Token::TermIdent(name) => name })
            .then_ignore(just(Token::Eq))
            .then(expr.clone())
            .then_ignore(just(Token::In))
            .then(expr.clone())
            .map(|((name, value), body)| Expr::Let(name, value, body))
            .map_with_span(Spanned::new)
            .boxed();

        let match_arm = pattern_parser()
            .then_ignore(just(Token::Arrow))
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
            (Expr::Call(a_func, a_args), Expr::Call(e_func, e_args)) => {
                assert_expr_eq(&a_func.value, &e_func.value);
                assert_eq!(a_args.len(), e_args.len());
                for (a, e) in a_args.iter().zip(e_args.iter()) {
                    assert_expr_eq(&a.value, &e.value);
                }
            }
            (Expr::MemberAccess(a_expr, a_member), Expr::MemberAccess(e_expr, e_member)) => {
                assert_expr_eq(&a_expr.value, &e_expr.value);
                assert_eq!(a_member, e_member);
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
            "if (Map[[1 + 2, 3]->5].size > 0) then { let x = 42 in x } else fail(\"Empty map\")",
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
                    if let Expr::MemberAccess(expr, member) = &*left.value {
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

                    assert_expr_eq(&right.value, &Expr::Literal(Literal::Int64(0))); // TODO: paranthesis problem & tuple, should enforce at least one , (then test); same for match (must be tested). end with giga test
                } else {
                    panic!("Expected binary comparison in condition");
                }

                // Test then branch with let expression
                if let Expr::Let(name, value, body) = &*then_branch.value {
                    assert_eq!(name, "x");
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
            parse_expr("(x: I64) -> (y: F64) -> func(x + 1, y * 2.5, (z) -> z && true)");

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
                    if let Expr::Call(func, args) = &*body2.value {
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
        let (result, errors) =
            parse_expr("Map[\"key1\" -> (1, true, [1..5]), someVar -> Map[x -> y]]");

        assert!(
            result.is_some(),
            "Expected successful parse for map constructor"
        );
        assert!(errors.is_empty(), "Expected no errors for map constructor");

        if let Some(expr) = result {
            if let Expr::Map(entries) = &*expr.value {
                assert_eq!(entries.len(), 2, "Expected two map entries");

                // First entry: "key1" -> (1, true, [1..5])
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

                // Second entry: someVar -> map[x -> y]
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
}

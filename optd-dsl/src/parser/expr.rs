use pest::iterators::Pair;

use crate::irs::{hir::{Expr, Literal, MatchArm}, BinOp, UnaryOp};

use super::{patterns::parse_pattern, Rule};

/// Parse a complete expression from a pest Pair
///
/// # Arguments
/// * `pair` - The pest Pair containing the expression
///
/// # Returns
/// * `Expr` - The parsed expression AST node
pub fn parse_expr(pair: Pair<'_, Rule>) -> Expr {
    match pair.as_rule() {
        Rule::expr => parse_expr(pair.into_inner().next().unwrap()),
        Rule::closure => parse_closure(pair),
        Rule::logical_or
        | Rule::logical_and
        | Rule::comparison
        | Rule::concatenation
        | Rule::additive
        | Rule::range
        | Rule::multiplicative => parse_binary_operation(pair),
        Rule::postfix => parse_postfix(pair),
        Rule::prefix => parse_prefix(pair),
        Rule::match_expr => parse_match_expr(pair),
        Rule::if_expr => parse_if_expr(pair),
        Rule::val_expr => parse_val_expr(pair),
        Rule::array_literal => parse_array_literal(pair),
        Rule::tuple_literal => parse_tuple_literal(pair),
        Rule::constructor_expr => parse_constructor(pair),
        Rule::number => parse_number(pair),
        Rule::string => parse_string(pair),
        Rule::boolean => parse_boolean(pair),
        Rule::identifier => Expr::Var(pair.as_str().to_string()),
        Rule::fail_expr => parse_fail_expr(pair),
        _ => unreachable!("Unexpected expression rule: {:?}", pair.as_rule()),
    }
}

/// Parse a closure expression (e.g., "(x, y) => x + y")
fn parse_closure(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let params_pair = pairs.next().unwrap();

    let params = if params_pair.as_rule() == Rule::identifier {
        vec![params_pair.as_str().to_string()]
    } else {
        params_pair
            .into_inner()
            .map(|p| p.as_str().to_string())
            .collect()
    };

    let body = parse_expr(pairs.next().unwrap());
    Expr::Closure(params, Box::new(body))
}

/// Parse a binary operation with proper operator precedence
fn parse_binary_operation(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let mut expr = parse_expr(pairs.next().unwrap());

    while let Some(op_pair) = pairs.next() {
        let op = parse_binary_operator(op_pair);
        let rhs = parse_expr(pairs.next().unwrap());
        expr = Expr::Binary(Box::new(expr), op, Box::new(rhs));
    }

    expr
}

/// Parse a postfix expression (function calls, member access, array indexing)
fn parse_postfix(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let mut expr = parse_expr(pairs.next().unwrap());

    for postfix_pair in pairs {
        match postfix_pair.as_rule() {
            Rule::call => {
                let args = postfix_pair.into_inner().map(parse_expr).collect();
                expr = Expr::Call(Box::new(expr), args);
            }
            Rule::member_access => {
                let member = postfix_pair.as_str().trim_start_matches('.').to_string();
                expr = Expr::Member(Box::new(expr), member);
            }
            Rule::array_index => {
                let index = parse_expr(postfix_pair.into_inner().next().unwrap());
                expr = Expr::ArrayIndex(Box::new(expr), Box::new(index));
            }
            Rule::member_call => {
                let mut pairs = postfix_pair.into_inner();
                let member = pairs
                    .next()
                    .unwrap()
                    .as_str()
                    .trim_start_matches('.')
                    .to_string();
                let args = pairs.map(parse_expr).collect();
                expr = Expr::MemberCall(Box::new(expr), member, args);
            }
            _ => unreachable!("Unexpected postfix rule: {:?}", postfix_pair.as_rule()),
        }
    }
    expr
}

/// Parse a prefix expression (unary operators)
fn parse_prefix(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let first = pairs.next().unwrap();

    if first.as_str() == "!" || first.as_str() == "-" {
        let op = match first.as_str() {
            "-" => UnaryOp::Neg,
            "!" => UnaryOp::Not,
            _ => unreachable!("Unexpected prefix operator: {}", first.as_str()),
        };
        let expr = parse_expr(pairs.next().unwrap());
        Expr::Unary(op, Box::new(expr))
    } else {
        parse_expr(first)
    }
}

/// Parse a match expression
fn parse_match_expr(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let expr = parse_expr(pairs.next().unwrap());
    let mut arms = Vec::new();

    for arm_pair in pairs {
        if arm_pair.as_rule() == Rule::match_arm {
            arms.push(parse_match_arm(arm_pair));
        }
    }

    Expr::Match(Box::new(expr), arms)
}

/// Parse a match arm within a match expression
fn parse_match_arm(pair: Pair<'_, Rule>) -> MatchArm {
    let mut pairs = pair.into_inner();
    let pattern = parse_pattern(pairs.next().unwrap());
    let expr = parse_expr(pairs.next().unwrap());
    MatchArm { pattern, expr }
}

/// Parse an if expression
fn parse_if_expr(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let condition = parse_expr(pairs.next().unwrap());
    let then_branch = parse_expr(pairs.next().unwrap());
    let else_branch = parse_expr(pairs.next().unwrap());

    Expr::If(
        Box::new(condition),
        Box::new(then_branch),
        Box::new(else_branch),
    )
}

/// Parse a val expression (local binding)
fn parse_val_expr(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let value = parse_expr(pairs.next().unwrap());
    let body = parse_expr(pairs.next().unwrap());

    Expr::Val(name, Box::new(value), Box::new(body))
}

/// Parse an array literal
fn parse_array_literal(pair: Pair<'_, Rule>) -> Expr {
    let exprs = pair.into_inner().map(parse_expr).collect();
    Expr::Literal(Literal::Array(exprs))
}

/// Parse a tuple literal
fn parse_tuple_literal(pair: Pair<'_, Rule>) -> Expr {
    let exprs = pair.into_inner().map(parse_expr).collect();
    Expr::Literal(Literal::Tuple(exprs))
}

/// Parse a constructor expression
fn parse_constructor(pair: Pair<'_, Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let name = pairs.next().unwrap().as_str().to_string();
    let args = pairs.map(parse_expr).collect();

    // Check if first character is lowercase
    // TODO(alexis): small hack until I rewrite the grammar using Chumsky
    if name.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
        Expr::Call(Box::new(Expr::Var(name)), args)
    } else {
        Expr::Constructor(name, args)
    }
}

/// Parse a numeric literal
fn parse_number(pair: Pair<'_, Rule>) -> Expr {
    let num = pair.as_str().parse().unwrap();
    Expr::Literal(Literal::Int64(num))
}

/// Parse a string literal
fn parse_string(pair: Pair<'_, Rule>) -> Expr {
    let s = pair.as_str().to_string();
    Expr::Literal(Literal::String(s))
}

/// Parse a boolean literal
fn parse_boolean(pair: Pair<'_, Rule>) -> Expr {
    let b = pair.as_str().parse().unwrap();
    Expr::Literal(Literal::Bool(b))
}

/// Parse a fail expression
fn parse_fail_expr(pair: Pair<'_, Rule>) -> Expr {
    let msg = pair.into_inner().next().unwrap().as_str().to_string();
    Expr::Fail(msg)
}

/// Parse a binary operator
fn parse_binary_operator(pair: Pair<'_, Rule>) -> BinOp {
    let op_str = match pair.as_rule() {
        Rule::add_op
        | Rule::mult_op
        | Rule::or_op
        | Rule::and_op
        | Rule::compare_op
        | Rule::concat_op
        | Rule::range_op => pair.as_str(),
        _ => pair.as_str(),
    };

    match op_str {
        "+" => BinOp::Add,
        "-" => BinOp::Sub,
        "*" => BinOp::Mul,
        "/" => BinOp::Div,
        "++" => BinOp::Concat,
        "==" => BinOp::Eq,
        "!=" => BinOp::Neq,
        ">" => BinOp::Gt,
        "<" => BinOp::Lt,
        ">=" => BinOp::Ge,
        "<=" => BinOp::Le,
        "&&" => BinOp::And,
        "||" => BinOp::Or,
        ".." => BinOp::Range,
        _ => unreachable!("Unexpected binary operator: {}", op_str),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::DslParser;
    use pest::Parser;

    fn parse_expr_from_str(input: &str) -> Expr {
        let pair = DslParser::parse(Rule::expr, input).unwrap().next().unwrap();
        parse_expr(pair)
    }

    #[test]
    fn test_parse_binary_operations() {
        let expr = parse_expr_from_str("1 + 2 * 3");
        match expr {
            Expr::Binary(left, BinOp::Add, right) => {
                assert!(matches!(*left, Expr::Literal(Literal::Int64(1))));
                match *right {
                    Expr::Binary(l, BinOp::Mul, r) => {
                        assert!(matches!(*l, Expr::Literal(Literal::Int64(2))));
                        assert!(matches!(*r, Expr::Literal(Literal::Int64(3))));
                    }
                    _ => panic!("Expected multiplication"),
                }
            }
            _ => panic!("Expected addition"),
        }
    }

    #[test]
    fn test_parse_if_expression() {
        let expr = parse_expr_from_str("if x > 0 then 1 else 2");
        match expr {
            Expr::If(cond, then_branch, else_branch) => {
                match *cond {
                    Expr::Binary(left, BinOp::Gt, right) => {
                        assert!(matches!(*left, Expr::Var(v) if v == "x"));
                        assert!(matches!(*right, Expr::Literal(Literal::Int64(0))));
                    }
                    _ => panic!("Expected comparison"),
                }
                assert!(matches!(*then_branch, Expr::Literal(Literal::Int64(1))));
                assert!(matches!(*else_branch, Expr::Literal(Literal::Int64(2))));
            }
            _ => panic!("Expected if expression"),
        }
    }

    #[test]
    fn test_parse_closure() {
        let expr = parse_expr_from_str("(x, y) => x + y");
        match expr {
            Expr::Closure(params, body) => {
                assert_eq!(params, vec!["x", "y"]);
                match *body {
                    Expr::Binary(left, BinOp::Add, right) => {
                        assert!(matches!(*left, Expr::Var(v) if v == "x"));
                        assert!(matches!(*right, Expr::Var(v) if v == "y"));
                    }
                    _ => panic!("Expected addition in closure body"),
                }
            }
            _ => panic!("Expected closure"),
        }
    }
}

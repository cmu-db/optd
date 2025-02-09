use pest::{iterators::Pair, Parser};
use pest_derive::Parser;
use std::collections::HashMap;

pub mod ast;

#[derive(Parser)]
#[grammar = "dsl/parser/grammar.pest"]
pub struct DslParser;

use ast::*;
use pest::error::Error;

pub fn parse_file(input: &str) -> Result<File, Error<Rule>> {
    let pairs = DslParser::parse(Rule::file, input)?
        .next()
        .unwrap()
        .into_inner();

    let mut file = File {
        properties: Properties { fields: Vec::new() },
        operators: Vec::new(),
        functions: Vec::new(),
    };

    for pair in pairs {
        match pair.as_rule() {
            Rule::props_block => {
                file.properties = parse_properties_block(pair);
            }
            Rule::operator_def => {
                file.operators.push(parse_operator_def(pair));
            }
            Rule::function_def => {
                file.functions.push(parse_function_def(pair));
            }
            _ => {}
        }
    }

    Ok(file)
}

fn parse_properties_block(pair: Pair<Rule>) -> Properties {
    let mut properties = Properties { fields: Vec::new() };

    for field_pair in pair.into_inner() {
        if field_pair.as_rule() == Rule::field_def {
            properties.fields.push(parse_field_def(field_pair));
        }
    }

    properties
}

fn parse_operator_def(pair: Pair<Rule>) -> Operator {
    let mut operator_type = None;
    let mut name = None;
    let mut fields = Vec::new();
    let mut derived_props = HashMap::new();

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::operator_type => {
                operator_type = Some(match inner_pair.as_str() {
                    "Scalar" => OperatorKind::Scalar,
                    "Logical" => OperatorKind::Logical,
                    _ => unreachable!(),
                });
            }
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::field_def_list => {
                for field_pair in inner_pair.into_inner() {
                    fields.push(parse_field_def(field_pair));
                }
            }
            Rule::derive_props_block => {
                for prop_pair in inner_pair.into_inner() {
                    if prop_pair.as_rule() == Rule::prop_derivation {
                        let mut prop_name = None;
                        let mut expr = None;

                        for prop_inner_pair in prop_pair.into_inner() {
                            match prop_inner_pair.as_rule() {
                                Rule::identifier => {
                                    prop_name = Some(prop_inner_pair.as_str().to_string());
                                }
                                Rule::expr => {
                                    expr = Some(parse_expr(prop_inner_pair));
                                }
                                _ => unreachable!(),
                            }
                        }

                        if let (Some(name), Some(expr)) = (prop_name, expr) {
                            derived_props.insert(name, expr);
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    match operator_type {
        Some(OperatorKind::Scalar) => Operator::Scalar(ScalarOp {
            name: name.unwrap(),
            fields,
        }),
        Some(OperatorKind::Logical) => Operator::Logical(LogicalOp {
            name: name.unwrap(),
            fields,
            derived_props,
        }),
        _ => unreachable!(),
    }
}

fn parse_function_def(pair: Pair<Rule>) -> Function {
    let mut name = None;
    let mut params = Vec::new();
    let mut return_type = None;
    let mut body = None;
    let mut rule_type = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::params => {
                for param_pair in inner_pair.into_inner() {
                    if param_pair.as_rule() == Rule::param {
                        let mut param_name = None;
                        let mut param_type = None;

                        for param_inner_pair in param_pair.into_inner() {
                            match param_inner_pair.as_rule() {
                                Rule::identifier => {
                                    param_name = Some(param_inner_pair.as_str().to_string());
                                }
                                Rule::type_expr => {
                                    param_type = Some(parse_type_expr(param_inner_pair));
                                }
                                _ => {}
                            }
                        }

                        if let (Some(name), Some(ty)) = (param_name, param_type) {
                            params.push((name, ty));
                        }
                    }
                }
            }
            Rule::type_expr => {
                return_type = Some(parse_type_expr(inner_pair));
            }
            Rule::expr => {
                body = Some(parse_expr(inner_pair));
            }
            Rule::rule_annot => {
                for annot_inner_pair in inner_pair.into_inner() {
                    if annot_inner_pair.as_rule() == Rule::operator_type {
                        rule_type = Some(match annot_inner_pair.as_str() {
                            "scalar" => OperatorKind::Scalar,
                            "logical" => OperatorKind::Logical,
                            _ => unreachable!(),
                        });
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    Function {
        name: name.unwrap(),
        params,
        return_type: return_type.unwrap(),
        body: body.unwrap(),
        rule_type,
    }
}

fn parse_field_def(pair: Pair<Rule>) -> Field {
    let mut name = None;
    let mut ty = None;
    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::identifier => {
                name = Some(inner_pair.as_str().to_string());
            }
            Rule::type_expr => {
                ty = Some(parse_type_expr(inner_pair));
            }
            _ => {}
        }
    }

    Field {
        name: name.unwrap(),
        ty: ty.unwrap(),
    }
}

fn parse_type_expr(pair: Pair<Rule>) -> Type {
    match pair.as_rule() {
        Rule::base_type => match pair.as_str() {
            "Int64" => Type::Int64,
            "String" => Type::String,
            "Bool" => Type::Bool,
            "Float64" => Type::Float64,
            _ => unreachable!(),
        },
        Rule::array_type => {
            let inner_type = pair.into_inner().next().unwrap();
            Type::Array(Box::new(parse_type_expr(inner_type)))
        }
        Rule::map_type => {
            let mut inner_types = pair.into_inner();
            let key_type = parse_type_expr(inner_types.next().unwrap());
            let value_type = parse_type_expr(inner_types.next().unwrap());
            Type::Map(Box::new(key_type), Box::new(value_type))
        }
        Rule::tuple_type => {
            let mut types = Vec::new();
            for inner_pair in pair.into_inner() {
                types.push(parse_type_expr(inner_pair));
            }
            Type::Tuple(types)
        }
        Rule::function_type => {
            let mut inner_types = pair.into_inner();
            let input_type = parse_type_expr(inner_types.next().unwrap());
            let output_type = parse_type_expr(inner_types.next().unwrap());
            Type::Function(Box::new(input_type), Box::new(output_type))
        }
        Rule::operator_type => match pair.as_str() {
            "Scalar" => Type::Operator(OperatorKind::Scalar),
            "Logical" => Type::Operator(OperatorKind::Logical),
            _ => unreachable!(),
        },
        Rule::type_expr => parse_type_expr(pair.into_inner().next().unwrap()),
        _ => unreachable!(),
    }
}

fn parse_operator(pair: Pair<Rule>) -> Expr {
    let mut pairs = pair.clone().into_inner();
    let mut expr = parse_expr(pairs.next().unwrap());

    while let Some(op_pair) = pairs.next() {
        let op = parse_bin_op(op_pair);
        let rhs = parse_expr(pairs.next().unwrap());
        expr = Expr::Binary(Box::new(expr), op, Box::new(rhs));
    }

    expr
}

fn parse_postfix(pair: Pair<Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let mut expr = parse_expr(pairs.next().unwrap());
    for postfix_pair in pairs {
        match postfix_pair.as_rule() {
            Rule::call => {
                let mut args = Vec::new();
                for arg_pair in postfix_pair.into_inner() {
                    args.push(parse_expr(arg_pair));
                }
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
                let mut args = Vec::new();
                for arg_pair in pairs {
                    args.push(parse_expr(arg_pair));
                }
                expr = Expr::MemberCall(Box::new(expr), member, args);
            }
            _ => unreachable!(),
        }
    }
    expr
}

fn parse_prefix(pair: Pair<Rule>) -> Expr {
    let mut pairs = pair.into_inner();
    let first = pairs.next().unwrap();

    if first.as_str() == "!" || first.as_str() == "-" {
        let op = match first.as_str() {
            "-" => UnaryOp::Neg,
            "!" => UnaryOp::Not,
            _ => unreachable!(),
        };
        let expr = parse_expr(pairs.next().unwrap());
        Expr::Unary(op, Box::new(expr))
    } else {
        parse_expr(first)
    }
}

fn parse_expr(pair: Pair<Rule>) -> Expr {
    match pair.as_rule() {
        Rule::closure => {
            let mut pairs = pair.into_inner();
            let params = pairs
                .next()
                .unwrap()
                .as_str()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
            let body = parse_expr(pairs.next().unwrap());
            Expr::Closure(params, Box::new(body))
        }
        Rule::logical_or
        | Rule::logical_and
        | Rule::comparison
        | Rule::concatenation
        | Rule::additive
        | Rule::range
        | Rule::multiplicative => parse_operator(pair),
        Rule::postfix => parse_postfix(pair),
        Rule::prefix => parse_prefix(pair),
        Rule::match_expr => {
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
        Rule::if_expr => {
            let mut pairs = pair.into_inner();
            let cond = parse_expr(pairs.next().unwrap());
            let then_branch = parse_expr(pairs.next().unwrap());
            let else_branch = parse_expr(pairs.next().unwrap());
            Expr::If(Box::new(cond), Box::new(then_branch), Box::new(else_branch))
        }
        Rule::val_expr => {
            let mut pairs = pair.into_inner();
            let name = pairs.next().unwrap().as_str().to_string();
            let value = parse_expr(pairs.next().unwrap());
            let body = parse_expr(pairs.next().unwrap());
            Expr::Val(name, Box::new(value), Box::new(body))
        }
        Rule::array_literal => {
            let mut exprs = Vec::new();
            for inner_pair in pair.into_inner() {
                exprs.push(parse_expr(inner_pair));
            }
            Expr::Array(exprs)
        }
        Rule::tuple_literal => {
            let mut exprs = Vec::new();
            for inner_pair in pair.into_inner() {
                exprs.push(parse_expr(inner_pair));
            }
            Expr::Tuple(exprs)
        }
        Rule::constructor_expr => {
            let mut pairs = pair.into_inner();
            let name = pairs.next().unwrap().as_str().to_string();
            let mut args = Vec::new();
            for arg_pair in pairs {
                args.push(parse_expr(arg_pair));
            }
            Expr::Constructor(name, args)
        }
        Rule::number => {
            let num = pair.as_str().parse().unwrap();
            Expr::Literal(Literal::Int64(num))
        }
        Rule::string => {
            let s = pair.as_str().to_string();
            Expr::Literal(Literal::String(s))
        }
        Rule::boolean => {
            let b = pair.as_str().parse().unwrap();
            Expr::Literal(Literal::Bool(b))
        }
        Rule::identifier => {
            let name = pair.as_str().to_string();
            Expr::Var(name)
        }
        Rule::fail_expr => {
            let msg = pair.into_inner().next().unwrap().as_str().to_string();
            Expr::Fail(msg)
        }
        Rule::expr => parse_expr(pair.into_inner().next().unwrap()),
        _ => unreachable!(),
    }
}

fn parse_match_arm(pair: Pair<Rule>) -> MatchArm {
    let mut pairs = pair.into_inner();
    let pattern = parse_pattern(pairs.next().unwrap());
    let expr = parse_expr(pairs.next().unwrap());
    MatchArm { pattern, expr }
}

fn parse_pattern(pair: Pair<Rule>) -> Pattern {
    match pair.as_rule() {
        Rule::pattern => {
            let mut pairs = pair.into_inner();
            let first = pairs.next().unwrap();
            if first.as_rule() == Rule::identifier {
                if let Some(at_pattern) = pairs.next() {
                    let name = first.as_str().to_string();
                    let pattern = parse_pattern(at_pattern);
                    Pattern::Bind(name, Box::new(pattern))
                } else {
                    Pattern::Var(first.as_str().to_string())
                }
            } else {
                parse_pattern(first)
            }
        }
        Rule::constructor_pattern => {
            let mut pairs = pair.into_inner();
            let name = pairs.next().unwrap().as_str().to_string();
            let mut subpatterns = Vec::new();

            if let Some(fields) = pairs.next() {
                for field in fields.into_inner() {
                    if field.as_rule() == Rule::pattern_field {
                        let mut field_pairs = field.into_inner();
                        let first = field_pairs.next().unwrap();

                        if first.as_rule() == Rule::identifier {
                            // Check if we have a field binding (identifier : pattern)
                            if let Some(colon_pattern) = field_pairs.next() {
                                let field_name = first.as_str().to_string();
                                let pattern = parse_pattern(colon_pattern);
                                subpatterns.push(Pattern::Bind(field_name, Box::new(pattern)));
                            } else {
                                // Just an identifier
                                subpatterns.push(Pattern::Var(first.as_str().to_string()));
                            }
                        } else {
                            // Regular pattern
                            subpatterns.push(parse_pattern(first));
                        }
                    }
                }
            }

            Pattern::Constructor(name, subpatterns)
        }
        Rule::literal_pattern => {
            let lit = parse_literal(pair.into_inner().next().unwrap());
            Pattern::Literal(lit)
        }
        Rule::wildcard_pattern => Pattern::Wildcard,
        _ => unreachable!(),
    }
}

fn parse_literal(pair: Pair<Rule>) -> Literal {
    match pair.as_rule() {
        Rule::number => Literal::Int64(pair.as_str().parse().unwrap()),
        Rule::string => Literal::String(pair.as_str().to_string()),
        Rule::boolean => Literal::Bool(pair.as_str().parse().unwrap()),
        Rule::array_literal => {
            let mut exprs = Vec::new();
            for inner_pair in pair.into_inner() {
                exprs.push(parse_expr(inner_pair));
            }
            Literal::Array(exprs)
        }
        Rule::tuple_literal => {
            let mut exprs = Vec::new();
            for inner_pair in pair.into_inner() {
                exprs.push(parse_expr(inner_pair));
            }
            Literal::Tuple(exprs)
        }
        _ => unreachable!(),
    }
}

fn parse_bin_op(pair: Pair<Rule>) -> BinOp {
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
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_parse_operator() {
        let input = fs::read_to_string("/home/alexis/optd/optd-core/src/dsl/parser/test.optd")
            .expect("Failed to read file");

        let file = parse_file(&input).unwrap();
        assert!(file.properties.fields.len() == 1);
        println!("{:#?}", file);
    }
}

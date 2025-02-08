pub mod ast;
use std::collections::HashMap;

use ast::*;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/parser/grammar.pest"]
pub struct DslParser;

#[cfg(test)]
mod tests {
    use std::fs;

    use pest::Parser;

    use super::*;

    #[test]
    fn test_parse_operator() {
        let input = fs::read_to_string("/home/alexis/optd/optd-core/src/dsl/parser/test.optd")
            .expect("Failed to read file");

        let pairs = DslParser::parse(Rule::file, &input)
            .map_err(|e| e.to_string())
            .unwrap();
        print!("{:?}", pairs);

        // assert_eq!(file.operators.len(), 1);
        // Add more assertions...
    }
}

/*pub fn parse_file(input: &str) -> Result<File, String> {
    let pairs = DslParser::parse(Rule::file, input).map_err(|e| e.to_string())?;

    let mut file = File {
        operators: Vec::new(),
        functions: Vec::new(),
    };

    for pair in pairs {
        match pair.as_rule() {
            Rule::operator_def => {
                let operator = parse_operator(pair)?;
                file.operators.push(operator);
            }
            Rule::function_def => {
                let function = parse_function(pair)?;
                file.functions.push(function);
            }
            _ => {}
        }
    }

    Ok(file)
}

fn parse_operator(pair: Pair<Rule>) -> Result<Operator, String> {
    let mut inner_pairs = pair.into_inner();
    let operator_type = inner_pairs.next().unwrap().as_str();
    let name = inner_pairs.next().unwrap().as_str().to_string();

    let fields = parse_field_def_list(inner_pairs.next().unwrap())?;

    match operator_type {
        "scalar" => Ok(Operator::Scalar(ScalarOp { name, fields })),
        "logical" => {
            let derived_props = parse_derive_props_block(inner_pairs.next().unwrap())?;
            Ok(Operator::Logical(LogicalOp {
                name,
                fields,
                derived_props,
            }))
        }
        _ => Err("Unknown operator type".to_string()),
    }
}

fn parse_field_def_list(pair: Pair<Rule>) -> Result<Vec<Field>, String> {
    let mut fields = Vec::new();
    for field_pair in pair.into_inner() {
        let mut field_pairs = field_pair.into_inner();
        let name = field_pairs.next().unwrap().as_str().to_string();
        let ty = parse_type_expr(field_pairs.next().unwrap())?;
        fields.push(Field { name, ty });
    }
    Ok(fields)
}

fn parse_type_expr(pair: Pair<Rule>) -> Result<Type, String> {
    match pair.as_rule() {
        Rule::base_type => match pair.as_str() {
            "i64" => Ok(Type::Int64),
            "String" => Ok(Type::String),
            "Bool" => Ok(Type::Bool),
            "f64" => Ok(Type::Float64),
            _ => Err("Unknown base type".to_string()),
        },
        Rule::array_type => {
            let inner_type = parse_type_expr(pair.into_inner().next().unwrap())?;
            Ok(Type::Array(Box::new(inner_type)))
        }
        Rule::map_type => {
            let mut inner_pairs = pair.into_inner();
            let key_type = parse_type_expr(inner_pairs.next().unwrap())?;
            let value_type = parse_type_expr(inner_pairs.next().unwrap())?;
            Ok(Type::Map(Box::new(key_type), Box::new(value_type)))
        }
        Rule::tuple_type => {
            let mut types = Vec::new();
            for type_pair in pair.into_inner() {
                types.push(parse_type_expr(type_pair)?);
            }
            Ok(Type::Tuple(types))
        }
        Rule::function_type => {
            let mut inner_pairs = pair.into_inner();
            let input_type = parse_type_expr(inner_pairs.next().unwrap())?;
            let output_type = parse_type_expr(inner_pairs.next().unwrap())?;
            Ok(Type::Function(Box::new(input_type), Box::new(output_type)))
        }
        _ => Err("Unknown type expression".to_string()),
    }
}

fn parse_derive_props_block(pair: Pair<Rule>) -> Result<HashMap<String, Expr>, String> {
    let mut derived_props = HashMap::new();
    for prop_pair in pair.into_inner() {
        let mut prop_pairs = prop_pair.into_inner();
        let name = prop_pairs.next().unwrap().as_str().to_string();
        let expr = parse_expr(prop_pairs.next().unwrap())?;
        derived_props.insert(name, expr);
    }
    Ok(derived_props)
}

fn parse_function(pair: Pair<Rule>) -> Result<Function, String> {
    let mut inner_pairs = pair.into_inner();
    let name = inner_pairs.next().unwrap().as_str().to_string();
    let params = parse_params(inner_pairs.next().unwrap())?;
    let return_type = parse_type_expr(inner_pairs.next().unwrap())?;
    let body = parse_block(inner_pairs.next().unwrap())?;

    Ok(Function {
        name,
        params,
        return_type,
        body,
        is_rule: false,
        is_operator: false,
    })
}

fn parse_params(pair: Pair<Rule>) -> Result<Vec<(String, Type)>, String> {
    let mut params = Vec::new();
    for param_pair in pair.into_inner() {
        let mut param_pairs = param_pair.into_inner();
        let name = param_pairs.next().unwrap().as_str().to_string();
        let ty = parse_type_expr(param_pairs.next().unwrap())?;
        params.push((name, ty));
    }
    Ok(params)
}

fn parse_block(pair: Pair<Rule>) -> Result<Block, String> {
    let mut exprs = Vec::new();
    for expr_pair in pair.into_inner() {
        exprs.push(parse_expr(expr_pair)?);
    }
    Ok(Block { exprs })
}

fn parse_expr(pair: Pair<Rule>) -> Result<Expr, String> {
    match pair.as_rule() {
        Rule::match_expr => {
            let mut inner_pairs = pair.into_inner();
            let expr = parse_expr(inner_pairs.next().unwrap())?;
            let arms = parse_match_arms(inner_pairs.next().unwrap())?;
            Ok(Expr::Match(Box::new(expr), arms))
        }
        Rule::if_expr => {
            let mut inner_pairs = pair.into_inner();
            let cond = parse_expr(inner_pairs.next().unwrap())?;
            let then_block = parse_block(inner_pairs.next().unwrap())?;
            let else_block = if inner_pairs.peek().is_some() {
                Some(Box::new(parse_block(inner_pairs.next().unwrap())?))
            } else {
                None
            };
            Ok(Expr::If(Box::new(cond), Box::new(then_block), else_block))
        }
        Rule::val_expr => {
            let mut inner_pairs = pair.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let value = parse_expr(inner_pairs.next().unwrap())?;
            let body = parse_expr(inner_pairs.next().unwrap())?;
            Ok(Expr::Val(name, Box::new(value), Box::new(body)))
        }
        Rule::array_literal => {
            let mut exprs = Vec::new();
            for expr_pair in pair.into_inner() {
                exprs.push(parse_expr(expr_pair)?);
            }
            Ok(Expr::Array(exprs))
        }
        Rule::map_literal => {
            let mut entries = Vec::new();
            for entry_pair in pair.into_inner() {
                let mut entry_pairs = entry_pair.into_inner();
                let key = parse_expr(entry_pairs.next().unwrap())?;
                let value = parse_expr(entry_pairs.next().unwrap())?;
                entries.push((key, value));
            }
            Ok(Expr::Map(entries))
        }
        Rule::range_expr => {
            let mut inner_pairs = pair.into_inner();
            let start = parse_expr(inner_pairs.next().unwrap())?;
            let end = parse_expr(inner_pairs.next().unwrap())?;
            Ok(Expr::Range(Box::new(start), Box::new(end)))
        }
        Rule::binary_expr => {
            let mut inner_pairs = pair.into_inner();
            let left = parse_expr(inner_pairs.next().unwrap())?;
            let op = parse_bin_op(inner_pairs.next().unwrap())?;
            let right = parse_expr(inner_pairs.next().unwrap())?;
            Ok(Expr::Binary(Box::new(left), op, Box::new(right)))
        }
        Rule::call_expr => {
            let mut inner_pairs = pair.into_inner();
            let callee = parse_expr(inner_pairs.next().unwrap())?;
            let args = parse_expr_list(inner_pairs.next().unwrap())?;
            Ok(Expr::Call(Box::new(callee), args))
        }
        Rule::member_access => {
            let mut inner_pairs = pair.into_inner();
            let object = parse_expr(inner_pairs.next().unwrap())?;
            let member = inner_pairs.next().unwrap().as_str().to_string();
            Ok(Expr::Member(Box::new(object), member))
        }
        Rule::member_call => {
            let mut inner_pairs = pair.into_inner();
            let object = parse_expr(inner_pairs.next().unwrap())?;
            let member = inner_pairs.next().unwrap().as_str().to_string();
            let args = parse_expr_list(inner_pairs.next().unwrap())?;
            Ok(Expr::MemberCall(Box::new(object), member, args))
        }
        Rule::var_expr => {
            let name = pair.as_str().to_string();
            Ok(Expr::Var(name))
        }
        Rule::literal => {
            let literal = parse_literal(pair.into_inner().next().unwrap())?;
            Ok(Expr::Literal(literal))
        }
        Rule::constructor_expr => {
            let mut inner_pairs = pair.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let fields = parse_constructor_fields(inner_pairs.next().unwrap())?;
            Ok(Expr::Constructor(name, fields))
        }
        Rule::fail_expr => {
            let message = pair.into_inner().next().unwrap().as_str().to_string();
            Ok(Expr::Fail(message))
        }
        Rule::closure => {
            let mut inner_pairs = pair.into_inner();
            let params = parse_closure_params(inner_pairs.next().unwrap())?;
            let body = parse_expr(inner_pairs.next().unwrap())?;
            Ok(Expr::Closure(params, Box::new(body)))
        }
        _ => Err("Unknown expression".to_string()),
    }
}

fn parse_match_arms(pair: Pair<Rule>) -> Result<Vec<(Pattern, Block)>, String> {
    let mut arms = Vec::new();
    for arm_pair in pair.into_inner() {
        let mut arm_pairs = arm_pair.into_inner();
        let pattern = parse_pattern(arm_pairs.next().unwrap())?;
        let block = parse_block(arm_pairs.next().unwrap())?;
        arms.push((pattern, block));
    }
    Ok(arms)
}

fn parse_pattern(pair: Pair<Rule>) -> Result<Pattern, String> {
    match pair.as_rule() {
        Rule::bind_pattern => {
            let mut inner_pairs = pair.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let pattern = parse_pattern(inner_pairs.next().unwrap())?;
            Ok(Pattern::Bind(name, Box::new(pattern)))
        }
        Rule::constructor_pattern => {
            let mut inner_pairs = pair.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let fields = parse_constructor_pattern_fields(inner_pairs.next().unwrap())?;
            Ok(Pattern::Constructor(name, fields))
        }
        Rule::enum_pattern => {
            let mut inner_pairs = pair.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let pattern = if inner_pairs.peek().is_some() {
                Some(Box::new(parse_pattern(inner_pairs.next().unwrap())?))
            } else {
                None
            };
            Ok(Pattern::Enum(name, pattern))
        }
        Rule::literal_pattern => {
            let literal = parse_literal(pair.into_inner().next().unwrap())?;
            Ok(Pattern::Literal(literal))
        }
        Rule::wildcard_pattern => Ok(Pattern::Wildcard),
        Rule::var_pattern => {
            let name = pair.as_str().to_string();
            Ok(Pattern::Var(name))
        }
        _ => Err("Unknown pattern".to_string()),
    }
}

fn parse_constructor_pattern_fields(pair: Pair<Rule>) -> Result<Vec<(String, Pattern)>, String> {
    let mut fields = Vec::new();
    for field_pair in pair.into_inner() {
        let mut field_pairs = field_pair.into_inner();
        let name = field_pairs.next().unwrap().as_str().to_string();
        let pattern = parse_pattern(field_pairs.next().unwrap())?;
        fields.push((name, pattern));
    }
    Ok(fields)
}

fn parse_literal(pair: Pair<Rule>) -> Result<Literal, String> {
    match pair.as_rule() {
        Rule::number => {
            let num = pair.as_str().parse::<i64>().map_err(|e| e.to_string())?;
            Ok(Literal::Number(num))
        }
        Rule::string => {
            let s = pair.as_str().to_string();
            Ok(Literal::StringLit(s))
        }
        Rule::boolean => {
            let b = pair.as_str().parse::<bool>().map_err(|e| e.to_string())?;
            Ok(Literal::Boolean(b))
        }
        _ => Err("Unknown literal".to_string()),
    }
}

fn parse_bin_op(pair: Pair<Rule>) -> Result<BinOp, String> {
    match pair.as_str() {
        "+" => Ok(BinOp::Add),
        "-" => Ok(BinOp::Sub),
        "*" => Ok(BinOp::Mul),
        "/" => Ok(BinOp::Div),
        "++" => Ok(BinOp::Concat),
        "==" => Ok(BinOp::Eq),
        "!=" => Ok(BinOp::Neq),
        ">" => Ok(BinOp::Gt),
        "<" => Ok(BinOp::Lt),
        ">=" => Ok(BinOp::Ge),
        "<=" => Ok(BinOp::Le),
        "&&" => Ok(BinOp::And),
        "||" => Ok(BinOp::Or),
        _ => Err("Unknown binary operator".to_string()),
    }
}

fn parse_expr_list(pair: Pair<Rule>) -> Result<Vec<Expr>, String> {
    let mut exprs = Vec::new();
    for expr_pair in pair.into_inner() {
        exprs.push(parse_expr(expr_pair)?);
    }
    Ok(exprs)
}

fn parse_constructor_fields(pair: Pair<Rule>) -> Result<Vec<(String, Expr)>, String> {
    let mut fields = Vec::new();
    for field_pair in pair.into_inner() {
        let mut field_pairs = field_pair.into_inner();
        let name = field_pairs.next().unwrap().as_str().to_string();
        let expr = parse_expr(field_pairs.next().unwrap())?;
        fields.push((name, expr));
    }
    Ok(fields)
}

fn parse_closure_params(pair: Pair<Rule>) -> Result<Vec<String>, String> {
    let mut params = Vec::new();
    for param_pair in pair.into_inner() {
        params.push(param_pair.as_str().to_string());
    }
    Ok(params)
}
*/
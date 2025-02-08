use std::collections::HashMap;

use crate::dsl::ast::*;
use pest::iterators::{Pair, Pairs};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "dsl/grammar.pest"]
pub struct FluxParser;

pub fn parse_file(input: &str) -> Result<File, String> {
    let pairs = FluxParser::parse(Rule::file, input).map_err(|e| e.to_string())?;

    parse_file_contents(pairs)
}

fn parse_file_contents(pairs: Pairs<Rule>) -> Result<File, String> {
    let mut operators = Vec::new();
    let mut functions = Vec::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::operator_def => operators.push(parse_operator(pair)?),
            Rule::function_def => functions.push(parse_function(pair)?),
            Rule::EOI => break,
            _ => return Err(format!("Unexpected rule in file: {:?}", pair.as_rule())),
        }
    }

    Ok(File {
        operators,
        functions,
    })
}

fn parse_operator(pair: Pair<Rule>) -> Result<Operator, String> {
    let mut inner = pair.into_inner();
    let annot = inner.next().unwrap();
    let is_scalar = annot.as_str().contains("scalar");

    let name = inner.next().unwrap().as_str().to_string();
    let mut fields = Vec::new();
    let mut derived_props = HashMap::new();

    for pair in inner {
        match pair.as_rule() {
            Rule::field_def => {
                let field = parse_field(pair)?;
                fields.push(field);
            }
            Rule::derive_props_block => {
                if !is_scalar {
                    derived_props = parse_derive_props(pair)?;
                }
            }
            _ => {}
        }
    }

    Ok(if is_scalar {
        Operator::Scalar(ScalarOp { name, fields })
    } else {
        Operator::Logical(LogicalOp {
            name,
            fields,
            derived_props,
        })
    })
}

fn parse_field(pair: Pair<Rule>) -> Result<Field, String> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let ty = parse_type(inner.next().unwrap())?;
    Ok(Field { name, ty })
}

fn parse_type(pair: Pair<Rule>) -> Result<Type, String> {
    match pair.as_rule() {
        Rule::base_type => match pair.as_str() {
            "i64" => Ok(Type::I64),
            "String" => Ok(Type::String),
            "Bool" => Ok(Type::Bool),
            "Float64" => Ok(Type::Float64),
            _ => Err(format!("Unknown base type: {}", pair.as_str())),
        },
        Rule::array_type => {
            let inner = pair.into_inner().next().unwrap();
            Ok(Type::Array(Box::new(parse_type(inner)?)))
        }
        Rule::map_type => {
            let mut inner = pair.into_inner();
            let key_type = parse_type(inner.next().unwrap())?;
            let val_type = parse_type(inner.next().unwrap())?;
            Ok(Type::Map(Box::new(key_type), Box::new(val_type)))
        }
        Rule::tuple_type => {
            let types = pair
                .into_inner()
                .map(|p| parse_type(p))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Type::Tuple(types))
        }
        _ => Err(format!("Unexpected type rule: {:?}", pair.as_rule())),
    }
}

fn parse_function(pair: Pair<Rule>) -> Result<Function, String> {
    let mut inner = pair.clone().into_inner();
    let is_rule = matches!(pair.as_rule(), Rule::rule_def);

    let name = inner.next().unwrap().as_str().to_string();
    let params = parse_params(inner.next().unwrap())?;
    let return_type = parse_type(inner.next().unwrap())?;
    let body = parse_block(inner.next().unwrap())?;

    Ok(Function {
        name,
        params,
        return_type,
        body,
        is_rule,
    })
}

fn parse_expr(pair: Pair<Rule>) -> Result<Expr, String> {
    match pair.as_rule() {
        Rule::expr => {
            let mut inner = pair.into_inner();
            let first = parse_expr(inner.next().unwrap())?;

            // Handle infix operators
            let mut result = first;
            while let Some(op) = inner.next() {
                let right = parse_expr(inner.next().unwrap())?;
                let bin_op = match op.as_rule() {
                    Rule::add => BinOp::Add,
                    Rule::subtract => BinOp::Sub,
                    Rule::multiply => BinOp::Mul,
                    Rule::divide => BinOp::Div,
                    Rule::concat => BinOp::Concat,
                    Rule::map_op => BinOp::Map,
                    Rule::filter_op => BinOp::Filter,
                    Rule::range_op => BinOp::Range,
                    _ => return Err(format!("Unknown operator: {:?}", op.as_rule())),
                };
                result = Expr::Binary(Box::new(result), bin_op, Box::new(right));
            }
            Ok(result)
        }
        Rule::primary => {
            let inner = pair.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::match_expr => parse_match_expr(inner),
                Rule::if_expr => parse_if_expr(inner),
                Rule::val_expr => parse_val_expr(inner),
                Rule::array_literal => parse_array_literal(inner),
                Rule::map_literal => parse_map_literal(inner),
                Rule::term => parse_term(inner),
                _ => Err(format!(
                    "Unexpected primary expression: {:?}",
                    inner.as_rule()
                )),
            }
        }
        _ => Err(format!("Unexpected expression rule: {:?}", pair.as_rule())),
    }
}

fn parse_params(pair: Pair<Rule>) -> Result<Vec<(String, Type)>, String> {
    pair.into_inner()
        .map(|p| {
            let mut inner = p.into_inner();
            let name = inner.next().unwrap().as_str().to_string();
            let ty = parse_type(inner.next().unwrap())?;
            Ok((name, ty))
        })
        .collect()
}

fn parse_pattern(pair: Pair<Rule>) -> Result<Pattern, String> {
    match pair.as_rule() {
        Rule::pattern => {
            let mut inner = pair.into_inner();
            let first = inner.next().unwrap();
            match first.as_rule() {
                Rule::identifier => {
                    let name = first.as_str().to_string();
                    let constructor = parse_pattern(inner.next().unwrap())?;
                    Ok(Pattern::Bind(name, Box::new(constructor)))
                }
                Rule::constructor_pattern => parse_constructor_pattern(first),
                _ if first.as_str() == "_" => Ok(Pattern::Wildcard),
                _ => Err(format!("Unexpected pattern type: {:?}", first.as_rule())),
            }
        }
        _ => Err(format!("Expected pattern, got {:?}", pair.as_rule())),
    }
}

fn parse_constructor_pattern(pair: Pair<Rule>) -> Result<Pattern, String> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let fields = inner
        .map(|p| {
            let mut field = p.into_inner();
            let name = field.next().unwrap().as_str().to_string();
            let pattern = parse_pattern(field.next().unwrap())?;
            Ok((name, pattern))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Pattern::Constructor(name, fields))
}

fn parse_block(pair: Pair<Rule>) -> Result<Block, String> {
    let exprs = pair
        .into_inner()
        .map(parse_expr)
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Block { exprs })
}

fn parse_match_expr(pair: Pair<Rule>) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let expr = parse_expr(inner.next().unwrap())?;
    let arms = inner
        .map(|p| {
            let mut arm = p.into_inner();
            let pattern = parse_pattern(arm.next().unwrap())?;
            let block = parse_block(arm.next().unwrap())?;
            Ok((pattern, block))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Expr::Match(Box::new(expr), arms))
}

fn parse_if_expr(pair: Pair<Rule>) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let cond = parse_expr(inner.next().unwrap())?;
    let then_block = parse_block(inner.next().unwrap())?;
    let else_block = inner.next().map(parse_block).transpose()?.map(Box::new);
    Ok(Expr::If(Box::new(cond), Box::new(then_block), else_block))
}

fn parse_val_expr(pair: Pair<Rule>) -> Result<Expr, String> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let expr = parse_expr(inner.next().unwrap())?;
    Ok(Expr::Val(name, Box::new(expr)))
}

fn parse_array_literal(pair: Pair<Rule>) -> Result<Expr, String> {
    let exprs = pair
        .into_inner()
        .map(parse_expr)
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Expr::Array(exprs))
}

fn parse_map_literal(pair: Pair<Rule>) -> Result<Expr, String> {
    let pairs = pair
        .into_inner()
        .map(|p| {
            let mut inner = p.into_inner();
            let key = parse_expr(inner.next().unwrap())?;
            let value = parse_expr(inner.next().unwrap())?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Expr::Map(pairs))
}

fn parse_term(pair: Pair<Rule>) -> Result<Expr, String> {
    match pair.as_rule() {
        Rule::number => Ok(Expr::Number(pair.as_str().parse().unwrap())),
        Rule::string => Ok(Expr::StringLit(pair.as_str().to_string())),
        Rule::identifier => Ok(Expr::Var(pair.as_str().to_string())),
        Rule::constructor_expr => {
            let mut inner = pair.into_inner();
            let name = inner.next().unwrap().as_str().to_string();
            let fields = inner
                .map(|p| {
                    let mut field = p.into_inner();
                    let name = field.next().unwrap().as_str().to_string();
                    let expr = parse_expr(field.next().unwrap())?;
                    Ok((name, expr))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Expr::Constructor(name, fields))
        }
        Rule::fail_expr => {
            let msg = pair.into_inner().next().unwrap().as_str().to_string();
            Ok(Expr::Fail(msg))
        }
        Rule::closure_expr => {
            let mut inner = pair.into_inner();
            let param = inner.next().unwrap().as_str().to_string();
            let body = parse_expr(inner.next().unwrap())?;
            Ok(Expr::Closure(param, Box::new(body)))
        }
        _ => Err(format!("Unexpected term: {:?}", pair.as_rule())),
    }
}

fn parse_derive_props(pair: Pair<Rule>) -> Result<HashMap<String, Expr>, String> {
    pair.into_inner()
        .map(|p| {
            let mut inner = p.into_inner();
            let name = inner.next().unwrap().as_str().to_string();
            let expr = parse_expr(inner.next().unwrap())?;
            Ok((name, expr))
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_operator() {
        let input = r#"
            @operator(scalar)
            Add {
                left: Scalar,
                right: Scalar
            }
        "#;
        
        let input2 = r#"
        
        // Type system
// Base types: i64, String, Bool, Float64
// Complex types: map[Type->Type], array[Type], tuple[Type]

// Shared properties for logical operators
@props(logical) {
  schema_len: i64
}

// Scalar Operators
@operator(scalar)
And {
  members: array[Scalar]
}

@operator(scalar)
Add {
  left: Scalar,
  right: Scalar
}

@operator(scalar)
Multiply {
  left: Scalar,
  right: Scalar
}

@operator(scalar)
ColumnRef {
  idx: i64
}

@operator(scalar)
Const {
  val: i64
}

@operator(scalar)
Eq {
  left: Scalar,
  right: Scalar
}

// Logical Operators
@operator(logical)
Filter {
  input: Logical,
  cond: Scalar,

  @derive_props {
    schema_len = input.schema_len
  }
}

@operator(logical)
Project {
  input: Logical,
  exprs: array[Scalar],

  @derive_props {
    schema_len = exprs.len
  }
}

@operator(logical)
Join {
  left: Logical,
  right: Logical,
  type: String,
  cond: Scalar,

  @derive_props {
    schema_len = left.schema_len + right.schema_len
  }
}

// Rules demonstrating all language features
@rule(scalar)
def constant_fold(expr: Scalar): Scalar = {
  match expr {
    // Pattern binding with @
    op @ Add(left: Const(val: x), right: Const(val: y)) => {
      Const(val: x + y)
    },
    op @ Multiply(left: Const(val: x), right: Const(val: y)) => {
      Const(val: x * y)
    },
    // Array operations
    And(members: ms) => {
      val folded = ms.map(m => constant_fold(m))
      And(members: folded)
    },
    // Wildcard pattern
    _ => expr
  }
}

@rule(scalar)
def rewrite_column_refs(expr: Scalar, index_map: map[i64, i64]): Scalar = {
  match expr {
    ColumnRef(idx: i) => {
      match index_map.get(i) {
        Some(new_idx) => ColumnRef(idx: new_idx),
        None => ColumnRef(idx: i)
      }
    },
    // Pattern for recursing through children with wildcard
    _ => expr.children(|child| rewrite_column_refs(child, index_map))
  }
}

@rule(scalar)
def has_refs_in_range(expr: Scalar, start: i64, end: i64): Bool = {
  match expr {
    ColumnRef(idx: i) => i >= start && i < end,
    _ => expr.children().any(|child| has_refs_in_range(child, start, end))
  }
}

@rule(logical)
def join_commute(expr: Logical): Logical = {
  match expr {
    Join(type: "Inner", left: l, right: r, cond: c) => {
      val left_len = l.schema_len
      val right_len = r.schema_len
      
      // Array operations: map, ++, to_map
      val refs_remap = 
        (0..left_len).map(i => (i, i + right_len)) ++
        (0..right_len).map(i => (left_len + i, i))
        .to_map()
      
      Join(
        type: "Inner",
        left: r,
        right: l,
        cond: rewrite_column_refs(c, refs_remap)
      )
    }
  }
}

@rule(logical)
def join_associate(expr: Logical): Logical = {
  match expr {
    Join(
      type: "Inner",
      left: Join(
        type: "Inner",
        left: a,
        right: b,
        cond: c1
      ),
      right: c,
      cond: c2
    ) => {
      val a_len = a.schema_len
      val b_len = b.schema_len
      val c_len = c.schema_len

      if !has_refs_in_range(c2, 0, a_len) {
        val inner_map = (a_len..(a_len + b_len + c_len))
          .map(i => (i, i - a_len))
          .to_map()

        Join(
          type: "Inner",
          left: a,
          right: Join(
            type: "Inner",
            left: b,
            right: c,
            cond: rewrite_column_refs(c2, inner_map)
          ),
          cond: c1
        )
      } else {
        fail("Cannot rewrite: outer join condition references left relation")
      }
    }
  }
}

// Example demonstrating array operations and error handling
@rule(logical)
def complex_project(expr: Logical): Logical = {
  match expr {
    project @ Project(input: rel, exprs: es) => {
      if es.len == 0 {
        fail("Empty projection list")
      }

      val mapped = es.map(e => constant_fold(e))
      val filtered = mapped.filter(e => 
        match e {
          Const(_) => false,
          _ => true
        }
      )

      if filtered.len == 0 {
        fail("All expressions folded to constants")
      }

      Project(
        input: rel,
        exprs: filtered
      )
    }
  }
}
        "#;

        // let file = parse_file(input).unwrap();

        let pairs = FluxParser::parse(Rule::file, input2).map_err(|e| e.to_string()).unwrap();
        print!("{:?}", pairs);

        // assert_eq!(file.operators.len(), 1);
        // Add more assertions...
    }
}

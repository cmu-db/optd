use crate::analyzer::hir::{BinOp, CoreData, Literal, UnaryOp, Value};
use BinOp::*;
use Literal::*;
use UnaryOp::*;

pub(super) fn eval_binary_op(left: Value, op: &BinOp, right: Value) -> Value {
    use CoreData::*;
    match (&left.0, op, &right.0) {
        (Literal(l), op, Literal(r)) => match (l, op, r) {
            (Int64(l), Add | Sub | Mul | Div | Eq | Lt, Int64(r)) => Value(Literal(match op {
                Add => Int64(l + r),
                Sub => Int64(l - r),
                Mul => Int64(l * r),
                Div => Int64(l / r),
                Eq => Bool(l == r),
                Lt => Bool(l < r),
                _ => unreachable!(),
            })),
            (Int64(l), Range, Int64(r)) => {
                Value(Array((*l..=*r).map(|n| Value(Literal(Int64(n)))).collect()))
            }
            (Float64(l), op, Float64(r)) => Value(Literal(match op {
                Add => Float64(l + r),
                Sub => Float64(l - r),
                Mul => Float64(l * r),
                Div => Float64(l / r),
                Lt => Bool(l < r),
                _ => panic!("Invalid float operation"),
            })),
            (Bool(l), op, Bool(r)) => Value(Literal(match op {
                And => Bool(*l && *r),
                Or => Bool(*l || *r),
                Eq => Bool(*l == *r),
                _ => panic!("Invalid boolean operation"),
            })),
            (String(l), op, String(r)) => Value(Literal(match op {
                Eq => Bool(l == r),
                Concat => String(l.to_string() + r),
                _ => panic!("Invalid string operation"),
            })),
            _ => panic!("Type mismatch in binary operation"),
        },
        (Array(l), Concat, Array(r)) => {
            let mut result = l.clone();
            result.extend(r.iter().cloned());
            Value(Array(result))
        }
        (Map(l), Concat, Map(r)) => {
            let mut result = l.clone();
            result.extend(r.iter().cloned());
            Value(Map(result))
        }
        _ => panic!("Invalid binary operation"),
    }
}

pub(super) fn eval_unary_op(op: &UnaryOp, expr: Value) -> Value {
    match (op, &expr.0) {
        (Neg, CoreData::Literal(Int64(x))) => Value(CoreData::Literal(Int64(-x))),
        (Neg, CoreData::Literal(Float64(x))) => Value(CoreData::Literal(Float64(-x))),
        (Not, CoreData::Literal(Bool(x))) => Value(CoreData::Literal(Bool(!x))),
        _ => panic!("Invalid unary operation or type mismatch"),
    }
}

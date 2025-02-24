use optd_core::cascades::memo::Memoize;

use crate::analyzer::hir::{
    BinOp, CoreData, Expr, LogicalOp, Materializable, PhysicalOp, ScalarOp, UnaryOp, Value,
};
use crate::analyzer::hir::{FunKind, Literal};
use BinOp::*;
use CoreData::*;
use Expr::*;
use FunKind::*;
use Literal::*;
use UnaryOp::*;

use super::context::Context;

impl Expr {
    fn eval_binary_op(left: Value, op: &BinOp, right: Value) -> Value {
        match (&left.0, op, &right.0) {
            // Integer operations
            (Literal(Int64(l)), Add, Literal(Int64(r))) => Value(Literal(Int64(l + r))),
            (Literal(Int64(l)), Sub, Literal(Int64(r))) => Value(Literal(Int64(l - r))),
            (Literal(Int64(l)), Mul, Literal(Int64(r))) => Value(Literal(Int64(l * r))),
            (Literal(Int64(l)), Div, Literal(Int64(r))) => Value(Literal(Int64(l / r))),
            (Literal(Int64(l)), Eq, Literal(Int64(r))) => Value(Literal(Bool(l == r))),
            (Literal(Int64(l)), Lt, Literal(Int64(r))) => Value(Literal(Bool(l < r))),

            // Float operations
            (Literal(Float64(l)), Add, Literal(Float64(r))) => Value(Literal(Float64(l + r))),
            (Literal(Float64(l)), Sub, Literal(Float64(r))) => Value(Literal(Float64(l - r))),
            (Literal(Float64(l)), Mul, Literal(Float64(r))) => Value(Literal(Float64(l * r))),
            (Literal(Float64(l)), Div, Literal(Float64(r))) => Value(Literal(Float64(l / r))),
            (Literal(Float64(l)), Lt, Literal(Float64(r))) => Value(Literal(Bool(l < r))),

            // Boolean operations
            (Literal(Bool(l)), And, Literal(Bool(r))) => Value(Literal(Bool(*l && *r))),
            (Literal(Bool(l)), Or, Literal(Bool(r))) => Value(Literal(Bool(*l || *r))),
            (Literal(Bool(l)), Eq, Literal(Bool(r))) => Value(Literal(Bool(*l == *r))),

            // String operations
            (Literal(String(l)), Eq, Literal(String(r))) => Value(Literal(Bool(l == r))),
            (Literal(String(l)), Concat, Literal(String(r))) => {
                Value(Literal(String(l.to_string() + r)))
            }

            // Range operation
            (Literal(Int64(l)), Range, Literal(Int64(r))) => {
                let range = (*l..=*r).map(|n| Value(Literal(Int64(n)))).collect();
                Value(Array(range))
            }

            // Array concatenation
            (Array(l), Concat, Array(r)) => {
                let mut new_array = l.clone();
                new_array.extend(r.iter().cloned());
                Value(Array(new_array))
            }

            _ => panic!("Invalid binary operation or type mismatch"),
        }
    }

    fn eval_unary_op(op: &UnaryOp, expr: Value) -> Value {
        match (op, &expr.0) {
            (Neg, Literal(Int64(x))) => Value(Literal(Int64(-x))),
            (Neg, Literal(Float64(x))) => Value(Literal(Float64(-x))),
            (Not, Literal(Bool(x))) => Value(Literal(Bool(!x))),
            _ => panic!("Invalid unary operation or type mismatch"),
        }
    }

    fn eval_core_expr<'a, M: Memoize>(
        data: &CoreData<Expr>,
        context: &mut Context,
        memo: &M,
    ) -> Vec<Value> {
        match data {
            Literal(literal) => vec![Value(Literal(literal.clone()))],
            Array(items) => {
                let initial = vec![vec![]];
                items
                    .iter()
                    .fold(initial, |result_arrays, item| {
                        let item_values = item.evaluate(context, memo);
                        result_arrays
                            .into_iter()
                            .flat_map(|arr| {
                                item_values.iter().map(move |value| {
                                    let mut new_arr = arr.clone();
                                    new_arr.push(value.clone());
                                    new_arr
                                })
                            })
                            .collect()
                    })
                    .into_iter()
                    .map(|arr| Value(Array(arr)))
                    .collect()
            }
            Tuple(items) => {
                let initial = vec![vec![]];
                items
                    .iter()
                    .fold(initial, |result_tuples, item| {
                        let item_values = item.evaluate(context, memo);
                        result_tuples
                            .into_iter()
                            .flat_map(|tup| {
                                item_values.iter().map(move |value| {
                                    let mut new_tup = tup.clone();
                                    new_tup.push(value.clone());
                                    new_tup
                                })
                            })
                            .collect()
                    })
                    .into_iter()
                    .map(|tup| Value(Tuple(tup)))
                    .collect()
            }
            Map(items) => {
                let initial = vec![vec![]];
                items
                    .iter()
                    .fold(initial, |result_maps, (k, v)| {
                        let key_values = k.evaluate(context, memo);
                        let val_values = v.evaluate(context, memo);
                        result_maps
                            .into_iter()
                            .flat_map(|map| {
                                let val_values = val_values.clone();
                                key_values.iter().flat_map(move |key| {
                                    val_values
                                        .iter()
                                        .map(|val| {
                                            let mut new_map = map.clone();
                                            new_map.push((key.clone(), val.clone()));
                                            new_map
                                        })
                                        .collect::<Vec<_>>()
                                })
                            })
                            .collect()
                    })
                    .into_iter()
                    .map(|map| Value(Map(map)))
                    .collect()
            }
            Struct(name, items) => {
                let initial = vec![vec![]];
                items
                    .iter()
                    .fold(initial, |result_structs, item| {
                        let item_values = item.evaluate(context, memo);
                        result_structs
                            .into_iter()
                            .flat_map(|st| {
                                item_values.iter().map(move |value| {
                                    let mut new_st = st.clone();
                                    new_st.push(value.clone());
                                    new_st
                                })
                            })
                            .collect()
                    })
                    .into_iter()
                    .map(|st| Value(Struct(name.clone(), st)))
                    .collect()
            }
            Function(fun_type) => vec![Value(Function(fun_type.clone()))],
            Fail(msg) => msg
                .evaluate(context, memo)
                .into_iter()
                .map(|m| Value(Fail(Box::new(m))))
                .collect(),
            Scalar(Materializable::Data(scalar_op)) => {
                let op_data_values =
                    scalar_op
                        .operator_data
                        .iter()
                        .fold(vec![vec![]], |acc, item| {
                            acc.into_iter()
                                .flat_map(|current| {
                                    item.evaluate(context, memo).into_iter().map(move |value| {
                                        let mut new_data = current.clone();
                                        new_data.push(value);
                                        new_data
                                    })
                                })
                                .collect()
                        });

                let scalar_child_values =
                    scalar_op
                        .scalar_children
                        .iter()
                        .fold(vec![vec![]], |acc, child| {
                            acc.into_iter()
                                .flat_map(|current| {
                                    child.evaluate(context, memo).into_iter().map(move |value| {
                                        let mut new_children = current.clone();
                                        new_children.push(value);
                                        new_children
                                    })
                                })
                                .collect()
                        });

                op_data_values
                    .into_iter()
                    .flat_map(|op_data| {
                        scalar_child_values.iter().map(move |scalar_children| {
                            Value(CoreData::Scalar(Materializable::Data(ScalarOp {
                                tag: scalar_op.tag.clone(),
                                operator_data: op_data.clone(),
                                scalar_children: scalar_children.clone(),
                            })))
                        })
                    })
                    .collect()
            }
            Scalar(Materializable::Group(id)) => {
                // AWAIT ON THIS memo.get_all_scalar_exprs_in_group(ScalarGroupId(*id))
                todo!()
            }
            Logical(materializable) => todo!(),
            Physical(materializable) => todo!(),
        }
    }

    pub(super) fn evaluate<'a, M: Memoize>(&self, context: &mut Context, memo: &M) -> Vec<Value> {
        match self {
            PatternMatch(_expr, _match_arms) => todo!(),

            IfThenElse(expr, r#if, r#else) => expr
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|cond| match cond.0 {
                    Literal(Bool(b)) => {
                        if b {
                            r#if.evaluate(context, memo)
                        } else {
                            r#else.evaluate(context, memo)
                        }
                    }
                    _ => panic!("Expected boolean value in if condition"),
                })
                .collect(),

            Let(ident, assignee, after) => assignee
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|value| {
                    let mut ctx = context.clone();
                    ctx.bind(ident.to_string(), value);
                    after.evaluate(&mut ctx, memo)
                })
                .collect(),

            Binary(left, bin_op, right) => left
                .evaluate(context, memo)
                .into_iter()
                .flat_map(|l| {
                    right
                        .evaluate(context, memo)
                        .into_iter()
                        .map(move |r| Self::eval_binary_op(l.clone(), bin_op, r))
                })
                .collect(),

            Unary(unary_op, expr) => expr
                .evaluate(context, memo)
                .into_iter()
                .map(|e| Self::eval_unary_op(unary_op, e))
                .collect(),

            Call(function, args) => {
                let fun_values = function.evaluate(context, memo);
                let arg_combinations = args.iter().map(|arg| arg.evaluate(context, memo)).fold(
                    vec![vec![]],
                    |acc, arg_values| {
                        acc.into_iter()
                            .flat_map(|combo| {
                                arg_values.iter().map(move |value| {
                                    let mut new_combo = combo.clone();
                                    new_combo.push(value.clone());
                                    new_combo
                                })
                            })
                            .collect::<Vec<_>>()
                    },
                );

                fun_values.into_iter().flat_map(move |fun| match &fun.0 {
                    Function(Closure(params, body)) => arg_combinations
                        .iter()
                        .flat_map(|args| {
                            let mut ctx = context.clone();
                            ctx.push_scope();
                            for (param, arg) in params.iter().zip(args.iter()) {
                                ctx.bind(param.clone(), arg.clone());
                            }
                            body.evaluate(&mut ctx, memo)
                        })
                        .collect::<Vec<_>>(),
                    Function(RustUDF(udf)) => arg_combinations
                        .iter()
                        .map(|args| udf(args.clone()))
                        .collect::<Vec<_>>(),
                    _ => panic!("Expected function value"),
                })
            }
            .collect(),

            Ref(ident) => vec![context.lookup(ident).expect("Variable not found").clone()],

            CoreExpr(core_expr) => Self::eval_core_expr(core_expr, context, memo),

            CoreVal(core_data) => vec![core_data.clone()],
        }
    }
}

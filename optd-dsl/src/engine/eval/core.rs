//! This module provides evaluation functions for core expression types, transforming
//! expressions into value streams that handle all possible evaluation paths.

use crate::{
    analyzer::hir::{CoreData, Expr, FunKind, Literal, Materializable, OperatorKind, Value},
    capture,
    engine::{
        utils::streams::{
            evaluate_all_combinations, process_result, propagate_success, ValueStream,
        },
        Context,
    },
};
use futures::StreamExt;

use CoreData::*;
use Materializable::*;

use super::operator::evaluate_operator;

/// Evaluates a core expression by generating all possible evaluation paths.
///
/// This function dispatches to specialized handlers based on the expression type,
/// generating a stream of all possible values the expression could evaluate to.
///
/// # Parameters
/// * `data` - The core expression data to evaluate
/// * `context` - The evaluation context
///
/// # Returns
/// A stream of all possible evaluation results
pub(super) fn evaluate_core_expr(data: CoreData<Expr>, context: Context) -> ValueStream {
    match data.clone() {
        Literal(lit) => evaluate_literal(lit),
        Array(items) => evaluate_collection(items, data, context),
        Tuple(items) => evaluate_collection(items, data, context),
        Struct(_, items) => evaluate_collection(items, data, context),
        Map(items) => evaluate_map(items, context),
        Function(fun_type) => evaluate_function(fun_type),
        Fail(msg) => evaluate_fail(*msg, context),
        Operator(Data(op)) => evaluate_operator(op, context),
        Operator(Group(id, kind)) => evaluate_group(id, kind),
    }
}

/// Evaluates a literal value.
fn evaluate_literal(lit: Literal) -> ValueStream {
    propagate_success(Value(CoreData::Literal(lit.clone())))
}

/// Evaluates a collection expression (Array, Tuple, or Struct).
fn evaluate_collection(
    items: Vec<Expr>,
    data_clone: CoreData<Expr>,
    context: Context,
) -> ValueStream {
    evaluate_all_combinations(items.into_iter(), context)
        .map(move |result| {
            result.map(|items| match &data_clone {
                Array(_) => Value(Array(items)),
                Tuple(_) => Value(Tuple(items)),
                Struct(name, _) => Value(Struct(name.clone(), items)),
                _ => unreachable!(),
            })
        })
        .boxed()
}

/// Evaluates a map expression by generating all combinations of keys and values.
fn evaluate_map(items: Vec<(Expr, Expr)>, context: Context) -> ValueStream {
    let keys: Vec<_> = items.iter().map(|(k, _)| k.clone()).collect();
    let values: Vec<_> = items.iter().map(|(_, v)| v.clone()).collect();

    // First evaluate all key expressions
    evaluate_all_combinations(keys.into_iter(), context.clone())
        .flat_map(move |keys_result| {
            // Process keys result
            process_result(
                keys_result,
                capture!([values, context], move |keys| {
                    // Then evaluate all value expressions
                    evaluate_all_combinations(values.into_iter(), context)
                        .map(capture!([keys], move |values_result| {
                            // Create map from keys and values
                            values_result.map(|values| {
                                Value(CoreData::Map(
                                    keys.clone().into_iter().zip(values).collect(),
                                ))
                            })
                        }))
                        .boxed()
                }),
            )
        })
        .boxed()
}

/// Evaluates a function expression.
fn evaluate_function(fun_type: FunKind) -> ValueStream {
    propagate_success(Value(CoreData::Function(fun_type)))
}

/// Evaluates a fail expression.
fn evaluate_fail(msg: Expr, context: Context) -> ValueStream {
    msg.evaluate(context)
        .map(|result| result.map(|value| Value(CoreData::Fail(Box::new(value)))))
        .boxed()
}

/// Evaluates a group expression.
fn evaluate_group(id: i64, kind: OperatorKind) -> ValueStream {
    propagate_success(Value(CoreData::Operator(Materializable::Group(id, kind))))
}

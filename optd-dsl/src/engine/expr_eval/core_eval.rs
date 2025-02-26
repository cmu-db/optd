use super::{evaluate_all_combinations, EngineError};
use crate::{
    analyzer::hir::{CoreData, Expr, Materializable, Operator, Value},
    engine::Context,
};
use futures::lock::Mutex;
use futures::{stream, StreamExt};
use futures::{FutureExt, Stream};
use std::sync::Arc;
use CoreData::*;
use Materializable::*;

/// Evaluates an operator by generating all possible combinations of its components.
///
/// This function handles three types of components that an operator might have:
/// 1. Operator data - Required base parameters for the operator
/// 2. Relational children - Optional sub-expressions that produce relations (e.g., table operations)
/// 3. Scalar children - Required sub-expressions that produce scalar values
///
/// The function generates a cartesian product of all possible values for each component,
/// then constructs operator instances for each combination using the provided value_constructor.
fn evaluate_operator(
    op: &Operator<Expr>,
    context: Context,
) -> Box<dyn Stream<Item = Result<Value, EngineError>> + Send + Unpin> {
    let kind = op.kind.clone();
    let tag = op.tag.clone();

    let op_data_exprs = op
        .operator_data
        .iter()
        .map(|expr| (*expr).clone())
        .collect::<Vec<_>>();

    let scalar_exprs = op
        .scalar_children
        .iter()
        .map(|expr| (*expr).clone())
        .collect::<Vec<_>>();

    let rel_exprs = op
        .relational_children
        .iter()
        .map(|expr| (*expr).clone())
        .collect::<Vec<_>>();

    let scalar_cache: Arc<Mutex<Option<Vec<Result<Vec<Value>, EngineError>>>>> =
        Arc::new(Mutex::new(None));
    let rel_cache: Arc<Mutex<Option<Vec<Result<Vec<Value>, EngineError>>>>> =
        Arc::new(Mutex::new(None));

    Box::new(
        evaluate_all_combinations(op_data_exprs.iter().cloned(), context.clone())
            .flat_map_unordered(None, move |op_data_result| {
                let kind = kind.clone();
                let tag = tag.clone();
                let scalar_exprs = scalar_exprs.clone();
                let rel_exprs = rel_exprs.clone();
                let context = context.clone();
                let scalar_cache = Arc::clone(&scalar_cache);
                let rel_cache = Arc::clone(&rel_cache);

                match op_data_result {
                    Ok(op_data) => async move {
                        let scalar_results = {
                            let mut cache_lock = scalar_cache.lock().await;
                            if let Some(ref cached_results) = *cache_lock {
                                cached_results.clone()
                            } else {
                                let computed_results = evaluate_all_combinations(
                                    scalar_exprs.iter().cloned(),
                                    context.clone(),
                                )
                                .collect::<Vec<_>>()
                                .await;
                                *cache_lock = Some(computed_results.clone());
                                computed_results
                            }
                        };

                        stream::iter(scalar_results).flat_map_unordered(
                            None,
                            move |scalar_result| {
                                let kind = kind.clone();
                                let tag = tag.clone();
                                let op_data = op_data.clone();
                                let rel_exprs = rel_exprs.clone();
                                let context = context.clone();
                                let rel_cache = Arc::clone(&rel_cache);

                                match scalar_result {
                                    Ok(scalar_children) => async move {
                                        let rel_results = {
                                            let mut cache_lock = rel_cache.lock().await;
                                            if let Some(ref cached_results) = *cache_lock {
                                                cached_results.clone()
                                            } else {
                                                let computed_results = evaluate_all_combinations(
                                                    rel_exprs.iter().cloned(),
                                                    context,
                                                )
                                                .collect::<Vec<_>>()
                                                .await;
                                                *cache_lock = Some(computed_results.clone());
                                                computed_results
                                            }
                                        };

                                        stream::iter(rel_results).map(move |rel_result| {
                                            rel_result.map(|rel_children| {
                                                Value(Operator(Data(Operator {
                                                    kind: kind.clone(),
                                                    tag: tag.clone(),
                                                    operator_data: op_data.clone(),
                                                    relational_children: rel_children,
                                                    scalar_children: scalar_children.clone(),
                                                })))
                                            })
                                        })
                                    }
                                    .flatten_stream()
                                    .boxed(),
                                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                                }
                            },
                        )
                    }
                    .flatten_stream()
                    .boxed(),
                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                }
            })
            .boxed(),
    )
}

pub(super) fn evaluate_core_expr(
    data: &CoreData<Expr>,
    context: Context,
) -> Box<dyn Stream<Item = Result<Value, EngineError>> + Send + Unpin> {
    match data {
        Literal(lit) => {
            let value = Value(Literal(lit.clone()));
            Box::new(stream::once(async move { Ok(value) }).boxed())
        }

        Array(items) | Tuple(items) | Struct(_, items) => {
            let data_clone = data.clone();
            Box::new(
                evaluate_all_combinations(items.iter().map(|item| (*item).clone()), context)
                    .map(move |result| {
                        result.map(|items| match &data_clone {
                            Array(_) => Value(Array(items)),
                            Tuple(_) => Value(Tuple(items)),
                            Struct(name, _) => Value(Struct(name.clone(), items)),
                            _ => unreachable!(),
                        })
                    })
                    .boxed(),
            )
        }

        Map(items) => {
            let keys: Vec<_> = items.iter().map(|(k, _)| (*k).clone()).collect();
            let values: Vec<_> = items.iter().map(|(_, v)| (*v).clone()).collect();

            let keys_cache: Arc<Mutex<Option<Vec<Result<Vec<Value>, EngineError>>>>> =
                Arc::new(Mutex::new(None));
            let values_cache: Arc<Mutex<Option<Vec<Result<Vec<Value>, EngineError>>>>> =
                Arc::new(Mutex::new(None));

            Box::new(
                async move {
                    let keys_results = {
                        let mut cache_lock = keys_cache.lock().await;
                        if let Some(ref cached_results) = *cache_lock {
                            cached_results.clone()
                        } else {
                            let computed_results =
                                evaluate_all_combinations(keys.iter().cloned(), context.clone())
                                    .collect::<Vec<_>>()
                                    .await;
                            *cache_lock = Some(computed_results.clone());
                            computed_results
                        }
                    };

                    stream::iter(keys_results).flat_map_unordered(None, move |keys_result| {
                        let values = values.clone();
                        let context = context.clone();
                        let values_cache = Arc::clone(&values_cache);

                        match keys_result {
                            Ok(keys) => async move {
                                let values_results = {
                                    let mut cache_lock = values_cache.lock().await;
                                    if let Some(ref cached_results) = *cache_lock {
                                        cached_results.clone()
                                    } else {
                                        let computed_results = evaluate_all_combinations(
                                            values.iter().cloned(),
                                            context,
                                        )
                                        .collect::<Vec<_>>()
                                        .await;
                                        *cache_lock = Some(computed_results.clone());
                                        computed_results
                                    }
                                };

                                stream::iter(values_results).map(move |values_result| {
                                    values_result.map(|values| {
                                        Value(Map(keys.clone().into_iter().zip(values).collect()))
                                    })
                                })
                            }
                            .flatten_stream()
                            .boxed(),
                            Err(e) => stream::once(async move { Err(e) }).boxed(),
                        }
                    })
                }
                .flatten_stream()
                .boxed(),
            )
        }

        Function(fun_type) => {
            let value = Value(Function(fun_type.clone()));
            Box::new(stream::once(async move { Ok(value) }).boxed())
        }

        Fail(msg) => {
            let msg_expr = (**msg).clone();
            Box::new(
                msg_expr
                    .evaluate(context)
                    .map(|result| result.map(|value| Value(Fail(Box::new(value)))))
                    .boxed(),
            )
        }

        Operator(Data(op)) => evaluate_operator(op, context),

        Operator(Group(id, kind)) => {
            let value = Value(Operator(Group(*id, *kind)));
            Box::new(stream::once(async move { Ok(value) }).boxed())
        }
    }
}

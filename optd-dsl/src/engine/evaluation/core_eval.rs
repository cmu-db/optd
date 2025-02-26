use super::expr_eval::evaluate_all_combinations;
use super::ValueStream;
use crate::engine::errors::EngineError;
use crate::engine::evaluation::stream_cache::StreamCache;
use crate::{
    analyzer::hir::{CoreData, Expr, Materializable, Operator, Value},
    engine::Context,
};
use futures::stream::Iter;
use futures::FutureExt;
use futures::{stream, StreamExt};
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
fn evaluate_operator(op: &Operator<Expr>, context: Context) -> ValueStream {
    let kind = op.kind.clone();
    let tag = op.tag.clone();

    fn extract_exprs(exprs: &[Expr]) -> Vec<Expr> {
        exprs.iter().cloned().collect()
    }

    async fn evaluate_children(
        exprs: Vec<Expr>,
        context: Context,
        cache: StreamCache<Vec<Result<Vec<Value>, EngineError>>>,
    ) -> Iter<std::vec::IntoIter<Result<Vec<Value>, EngineError>>> {
        stream::iter(
            cache
                .get_or_compute(|| {
                    evaluate_all_combinations(exprs.iter().cloned(), context).collect::<Vec<_>>()
                })
                .await,
        )
    }

    let op_data_exprs = extract_exprs(&op.operator_data);
    let scalar_exprs = extract_exprs(&op.scalar_children);
    let rel_exprs = extract_exprs(&op.relational_children);

    let scalar_cache = StreamCache::new();
    let rel_cache = StreamCache::new();

    let stream = evaluate_all_combinations(op_data_exprs.iter().cloned(), context.clone())
        .flat_map_unordered(None, move |op_data_result| {
            let tag = tag.clone();
            let scalar_exprs = scalar_exprs.clone();
            let rel_exprs = rel_exprs.clone();
            let context = context.clone();
            let scalar_cache = scalar_cache.clone();
            let rel_cache = rel_cache.clone();

            op_data_result
                .map(|op_data| {
                    async move {
                        evaluate_children(scalar_exprs.clone(), context.clone(), scalar_cache)
                            .await
                            .flat_map_unordered(None, move |scalar_result| {
                                let tag = tag.clone();
                                let op_data = op_data.clone();
                                let rel_exprs = rel_exprs.clone();
                                let context = context.clone();
                                let rel_cache = rel_cache.clone();

                                scalar_result
                                    .map(|scalar_children| {
                                        async move {
                                            evaluate_children(
                                                rel_exprs.clone(),
                                                context.clone(),
                                                rel_cache,
                                            )
                                            .await
                                            .map(
                                                move |rel_result| {
                                                    rel_result.map(|rel_children| {
                                                        Value(Operator(Data(Operator {
                                                            kind: kind.clone(),
                                                            tag: tag.clone(),
                                                            operator_data: op_data.clone(),
                                                            relational_children: rel_children,
                                                            scalar_children: scalar_children
                                                                .clone(),
                                                        })))
                                                    })
                                                },
                                            )
                                        }
                                        .flatten_stream()
                                        .boxed()
                                    })
                                    .unwrap_or_else(|e| stream::once(async move { Err(e) }).boxed())
                            })
                    }
                    .flatten_stream()
                    .boxed()
                })
                .unwrap_or_else(|e| stream::once(async move { Err(e) }).boxed())
        });

    Box::new(stream)
}

pub(super) fn evaluate_core_expr(data: &CoreData<Expr>, context: Context) -> ValueStream {
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
            let keys_cache = StreamCache::new();
            let values_cache = StreamCache::new();

            Box::new(
                async move {
                    let keys_results = keys_cache
                        .get_or_compute(|| {
                            evaluate_all_combinations(keys.iter().cloned(), context.clone())
                                .collect::<Vec<_>>()
                        })
                        .await;

                    stream::iter(keys_results).flat_map_unordered(None, move |keys_result| {
                        let values = values.clone();
                        let context = context.clone();
                        let values_cache = values_cache.clone();

                        match keys_result {
                            Ok(keys) => async move {
                                let values_results = values_cache
                                    .get_or_compute(|| {
                                        evaluate_all_combinations(values.iter().cloned(), context)
                                            .collect::<Vec<_>>()
                                    })
                                    .await;

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

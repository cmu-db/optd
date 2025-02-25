use crate::{
    analyzer::hir::{CoreData, Expr, Materializable, Operator, Value},
    interpreter::Context,
};
use anyhow::Error;
use CoreData::*;
use Materializable::*;

use super::evaluate_all_combinations;

/// Evaluates an operator by generating all possible combinations of its components.
///
/// This function handles three types of components that an operator might have:
/// 1. Operator data - Required base parameters for the operator
/// 2. Relational children - Optional sub-expressions that produce relations (e.g., table operations)
/// 3. Scalar children - Required sub-expressions that produce scalar values
///
/// The function generates a cartesian product of all possible values for each component,
/// then constructs operator instances for each combination using the provided value_constructor.
async fn evaluate_operator(
    op: &Operator<Expr>,
    context: &mut Context,
) -> Result<Vec<Value>, Error> {
    let operator_data_values = evaluate_all_combinations(op.operator_data.iter(), context).await?;
    let scalar_values = evaluate_all_combinations(op.scalar_children.iter(), context).await?;

    let relational_values = if !op.relational_children.is_empty() {
        evaluate_all_combinations(op.relational_children.iter(), context).await?
    } else {
        vec![vec![]]
    };

    let mut results = Vec::new();

    for op_data in &operator_data_values {
        for rel_children in &relational_values {
            for scalar_children_val in &scalar_values {
                results.push(Value(Operator(Data(Operator {
                    kind: op.kind.clone(),
                    tag: op.tag.clone(),
                    operator_data: op_data.clone(),
                    relational_children: rel_children.clone(),
                    scalar_children: scalar_children_val.clone(),
                }))));
            }
        }
    }

    Ok(results)
}

pub(super) async fn evaluate_core_expr(
    data: &CoreData<Expr>,
    context: &mut Context,
) -> Result<Vec<Value>, Error> {
    match data {
        Literal(lit) => Ok(vec![Value(Literal(lit.clone()))]),

        Array(items) | Tuple(items) | Struct(_, items) => {
            let item_combinations = evaluate_all_combinations(items.iter(), context).await?;

            let results = item_combinations
                .into_iter()
                .map(|items| match data {
                    Array(_) => Value(Array(items)),
                    Tuple(_) => Value(Tuple(items)),
                    Struct(name, _) => Value(Struct(name.clone(), items)),
                    _ => unreachable!(),
                })
                .collect();

            Ok(results)
        }

        Map(items) => {
            let key_comb = evaluate_all_combinations(items.iter().map(|(k, _)| k), context).await?;
            let value_comb =
                evaluate_all_combinations(items.iter().map(|(_, v)| v), context).await?;

            let mut results = Vec::new();
            for keys in &key_comb {
                for values in &value_comb {
                    results.push(Value(Map(keys
                        .clone()
                        .into_iter()
                        .zip(values.clone())
                        .collect())));
                }
            }

            Ok(results)
        }

        Function(fun_type) => Ok(vec![Value(Function(fun_type.clone()))]),

        Fail(msg) => {
            let msg_values = msg.evaluate(context).await?;
            let results = msg_values
                .into_iter()
                .map(|m| Value(Fail(Box::new(m))))
                .collect();

            Ok(results)
        }

        Operator(Data(op)) => evaluate_operator(op, context).await,

        Operator(Group(id, kind)) => Ok(vec![Value(Operator(Group(*id, *kind)))]),
    }
}

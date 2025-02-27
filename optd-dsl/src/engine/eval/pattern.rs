/*use crate::{
    analyzer::hir::{CoreData, Literal, Materializable, Operator, OperatorKind, Pattern, Value},
    utils::context::Context,
};
use anyhow::{Error, Result};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use CoreData::*;
use Literal::*;
use Materializable::*;

/// Helper function to match a series of pattern-value pairs against all contexts
///
/// This function applies pattern matching to a sequence of pattern-value pairs,
/// processing each pair against all current contexts. It enables parallel
/// matching of the same pattern-value pair across multiple contexts, while
/// maintaining sequential processing between different pairs.
///
/// Arguments:
/// * `contexts` - Current contexts to match against
/// * `pattern_value_pairs` - Iterator of (pattern, value) pairs to match
///
/// Returns a Result containing all contexts that successfully matched, or an error
async fn match_component_pairs<'a, I>(
    contexts: Vec<Context>,
    pattern_value_pairs: I,
) -> Result<Vec<Context>, Error>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
{
    let mut acc = contexts;

    for (pattern, value) in pattern_value_pairs {
        if acc.is_empty() {
            return Ok(vec![]);
        }

        let futures = acc
            .into_iter()
            .map(|ctx| match_pattern(value, pattern, ctx));

        let results = try_join_all(futures).await?;
        acc = results.into_iter().flatten().collect();
    }

    Ok(acc)
}

/// Cascades call!
async fn expand_group(_group_id: i64, _kind: OperatorKind) -> Result<Vec<Operator<Value>>, Error> {
    todo!()
}

#[async_recursion]
pub(super) async fn match_pattern(
    value: &Value,
    pattern: &Pattern,
    context: Context,
) -> Result<Vec<Context>, Error> {
    match (pattern, &value.0) {
        (Pattern::Wildcard, _) => Ok(vec![context]),

        (Pattern::Bind(ident, inner_pattern), _) => {
            let mut new_ctx = context.clone();
            new_ctx.bind(ident.to_string(), value.clone());
            match_pattern(value, inner_pattern, new_ctx).await
        }

        (Pattern::Literal(pattern_lit), Literal(value_lit)) => match (pattern_lit, value_lit) {
            (Int64(p), Int64(v)) if p == v => Ok(vec![context]),
            (Float64(p), Float64(v)) if p == v => Ok(vec![context]),
            (String(p), String(v)) if p == v => Ok(vec![context]),
            (Bool(p), Bool(v)) if p == v => Ok(vec![context]),
            (Unit, Unit) => Ok(vec![context]),
            _ => Ok(vec![]),
        },

        (Pattern::Struct(pat_name, field_patterns), Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return Ok(vec![]);
            }

            match_component_pairs(
                vec![context],
                field_patterns.iter().zip(field_values.iter()),
            )
            .await
        }

        (Pattern::Operator(op_pattern), Operator(op_value)) => match op_value {
            Data(op) => {
                if op_pattern.tag != op.tag || op_pattern.kind != op.kind {
                    return Ok(vec![]);
                }

                if op_pattern.operator_data.len() != op.operator_data.len()
                    || op_pattern.relational_children.len() != op.relational_children.len()
                    || op_pattern.scalar_children.len() != op.scalar_children.len()
                {
                    return Ok(vec![]);
                }

                let contexts_after_data = match_component_pairs(
                    vec![context],
                    op_pattern.operator_data.iter().zip(op.operator_data.iter()),
                )
                .await?;

                if contexts_after_data.is_empty() {
                    return Ok(vec![]);
                }

                let contexts_after_rel = match_component_pairs(
                    contexts_after_data,
                    op_pattern
                        .relational_children
                        .iter()
                        .zip(op.relational_children.iter()),
                )
                .await?;

                if contexts_after_rel.is_empty() {
                    return Ok(vec![]);
                }

                match_component_pairs(
                    contexts_after_rel,
                    op_pattern
                        .scalar_children
                        .iter()
                        .zip(op.scalar_children.iter()),
                )
                .await
            }

            Group(group_id, op_kind) => {
                let expanded_operators = expand_group(*group_id, *op_kind).await?;

                let futures = expanded_operators.into_iter().map(|expanded_op| {
                    let expanded_value = Value(Operator(Data(expanded_op)));
                    let context_clone = context.clone();
                    async move { match_pattern(&expanded_value, pattern, context_clone).await }
                });

                let results = try_join_all(futures).await?;
                Ok(results.into_iter().flatten().collect())
            }
        },

        _ => Ok(vec![]),
    }
}
*/

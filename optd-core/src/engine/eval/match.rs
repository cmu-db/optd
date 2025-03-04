//! This module provides pattern matching functionality for expressions.
//!
//! Pattern matching is a powerful feature that allows for destructuring and examining
//! values against patterns. This implementation supports matching against literals,
//! wildcards, bindings, structs, and operators, with special handling for operator groups.
//!
//! The pattern matching process is asynchronous to support operations like group expansion
//! that may require database access or other I/O operations.

use super::{Engine, Evaluate, Expander};
use crate::{capture, engine::utils::streams::ValueStream};
use async_recursion::async_recursion;
use futures::{future::join_all, stream, StreamExt};
use optd_dsl::analyzer::{
    context::Context,
    hir::{
        CoreData, Literal, LogicalOp, MatchArm, Materializable, Operator, Pattern, PhysicalOp,
        ScalarOp, Value,
    },
};
use Literal::*;
use Materializable::*;
use Pattern::*;

/// Expands the top-level unmaterialized group in a value if present.
/// Returns a vector of expanded values, or a singleton vector with the original value if no expansion needed.
pub(super) async fn expand_top_level<E>(value: Value, engine: Engine<E>) -> Vec<Value>
where
    E: Expander,
{
    match &value.0 {
        // For unmaterialized logical groups, expand them
        CoreData::Logical(LogicalOp(UnMaterialized(group_id))) => {
            engine.expander.expand_logical_group(*group_id).await
        }

        // For unmaterialized scalar groups, expand them
        CoreData::Scalar(ScalarOp(UnMaterialized(group_id))) => {
            engine.expander.expand_scalar_group(*group_id).await
        }

        // For unmaterialized physical goals, expand them
        CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal))) => {
            vec![engine.expander.expand_physical_goal(physical_goal).await]
        }

        // For all other types, just return the original value
        _ => vec![value],
    }
}

/// Tries to match a value against a sequence of match arms.
///
/// Attempts to match the value against each arm's pattern in order.
/// Returns the result of evaluating the first matching arm's expression.
pub(super) fn try_match_arms<E>(
    value: Value,
    match_arms: Vec<MatchArm>,
    engine: Engine<E>,
) -> ValueStream
where
    E: Expander,
{
    if match_arms.is_empty() {
        // No arms matched - this is a runtime error
        panic!("Pattern match exhausted: no pattern matched the value");
    }

    // Take the first arm
    let arm = &match_arms[0];
    let expr = arm.expr.clone();
    let remaining_arms = match_arms[1..].to_vec();

    let pattern_match_future = match_pattern(value.clone(), arm.pattern.clone(), engine.clone());

    stream::once(pattern_match_future)
        .flat_map(move |matched_contexts| {
            if !matched_contexts.is_empty() {
                // If we have matches, evaluate the arm's expression with all matched contexts
                stream::iter(matched_contexts)
                    .flat_map({
                        capture!([expr, engine], move |new_context| {
                            expr.clone()
                                .evaluate(engine.clone().with_context(new_context))
                        })
                    })
                    .boxed()
            } else {
                // If no matches, try the remaining arms
                try_match_arms(value.clone(), remaining_arms.clone(), engine.clone())
            }
        })
        .boxed()
}

/// Attempts to match a value against a pattern.
///
/// If the match succeeds, returns a vector of updated contexts with any bound variables.
/// If the match fails, returns an empty vector.
#[async_recursion]
async fn match_pattern<E>(value: Value, pattern: Pattern, engine: Engine<E>) -> Vec<Context>
where
    E: Expander,
{
    match (&pattern, &value.0) {
        // Wildcard pattern matches anything
        (Wildcard, _) => vec![engine.context.clone()],

        // Binding pattern: bind the value to the identifier and continue matching
        (Bind(ident, inner_pattern), _) => {
            let mut new_ctx = engine.context.clone();
            new_ctx.bind(ident.clone(), value.clone());
            match_pattern(
                value,
                (**inner_pattern).clone(),
                engine.with_context(new_ctx),
            )
            .await
        }

        // Literal pattern: match if literals are equal
        (Literal(pattern_lit), CoreData::Literal(value_lit)) => {
            match_literals(pattern_lit, value_lit, engine.context.clone())
        }

        (EmptyArray, CoreData::Array(arr)) if arr.is_empty() => vec![engine.context.clone()],

        // List decomposition pattern: match first element and rest of the array
        (ArrayDecomp(head_pattern, tail_pattern), CoreData::Array(arr)) => {
            match_array_decomposition(head_pattern, tail_pattern, arr, engine).await
        }

        // Struct pattern: match name and recursively match fields
        (Struct(pat_name, field_patterns), CoreData::Struct(val_name, field_values)) => {
            if pat_name != val_name || field_patterns.len() != field_values.len() {
                return vec![];
            }

            match_pattern_pairs(field_patterns.iter().zip(field_values.iter()), engine).await
        }

        // Unmaterialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_logical_group(*group_id).await;
            match_against_expanded_values(op_pattern, expanded_values, engine).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(UnMaterialized(group_id)))) => {
            let expanded_values = engine.expander.expand_scalar_group(*group_id).await;
            match_against_expanded_values(op_pattern, expanded_values, engine).await
        }

        (Operator(op_pattern), CoreData::Physical(PhysicalOp(UnMaterialized(physical_goal)))) => {
            let expanded_value = engine.expander.expand_physical_goal(physical_goal).await;
            match_against_expanded_values(op_pattern, vec![expanded_value], engine).await
        }

        // Materialized operators
        (Operator(op_pattern), CoreData::Logical(LogicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }
        (Operator(op_pattern), CoreData::Scalar(ScalarOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }
        (Operator(op_pattern), CoreData::Physical(PhysicalOp(Materialized(operator)))) => {
            match_operator_pattern(op_pattern, operator, engine).await
        }

        // No match for other combinations
        _ => vec![],
    }
}

/// Helper function to match a pattern against a collection of expanded values
async fn match_against_expanded_values<E>(
    op_pattern: &Operator<Pattern>,
    expanded_values: Vec<Value>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    // Create futures for matching against each expanded value
    let context_futures = expanded_values
        .into_iter()
        .map(|expanded_value| {
            match_pattern(expanded_value, Operator(op_pattern.clone()), engine.clone())
        })
        .collect::<Vec<_>>();

    // Await all futures and flatten the results
    join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Helper function to match literals
fn match_literals(pattern_lit: &Literal, value_lit: &Literal, context: Context) -> Vec<Context> {
    match (pattern_lit, value_lit) {
        (Int64(p), Int64(v)) if p == v => vec![context],
        (Float64(p), Float64(v)) if p == v => vec![context],
        (String(p), String(v)) if p == v => vec![context],
        (Bool(p), Bool(v)) if p == v => vec![context],
        (Unit, Unit) => vec![context],
        _ => vec![],
    }
}

/// Helper function to match array decomposition patterns
async fn match_array_decomposition<E>(
    head_pattern: &Pattern,
    tail_pattern: &Pattern,
    arr: &[Value],
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    if arr.is_empty() {
        return vec![];
    }

    // Split array into head and tail
    let head = arr[0].clone();
    let tail = Value(CoreData::Array(arr[1..].to_vec()));

    // Match head against head pattern
    let head_contexts = match_pattern(head, (*head_pattern).clone(), engine.clone()).await;
    if head_contexts.is_empty() {
        return vec![];
    }

    // For each successful head match, try to match tail
    let mut result_contexts = Vec::new();
    for head_ctx in head_contexts {
        let engine_with_head_ctx = engine.clone().with_context(head_ctx);
        let tail_contexts =
            match_pattern(tail.clone(), (*tail_pattern).clone(), engine_with_head_ctx).await;
        result_contexts.extend(tail_contexts);
    }

    result_contexts
}

/// Matches a pattern against a materialized operator.
///
/// This is the core operator matching function that matches a pattern against
/// a specific operator's components.
async fn match_operator_pattern<E>(
    op_pattern: &Operator<Pattern>,
    op: &Operator<Value>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    // Match tag and kind
    if op_pattern.tag != op.tag || op_pattern.kind != op.kind {
        return vec![];
    }

    // Match component counts
    if op_pattern.operator_data.len() != op.operator_data.len()
        || op_pattern.relational_children.len() != op.relational_children.len()
        || op_pattern.scalar_children.len() != op.scalar_children.len()
    {
        return vec![];
    }

    // Step 1: Match operator data
    let ctx_after_data = match_pattern_pairs(
        op_pattern.operator_data.iter().zip(op.operator_data.iter()),
        engine.clone(),
    )
    .await;

    if ctx_after_data.is_empty() {
        return vec![];
    }

    // Step 2: Match relational children for each context from operator data
    let contexts_after_rel = match_operator_children(
        &op_pattern.relational_children,
        &op.relational_children,
        ctx_after_data,
        engine.clone(),
    )
    .await;

    if contexts_after_rel.is_empty() {
        return vec![];
    }

    // Step 3: Match scalar children for each context from relational children
    match_operator_children(
        &op_pattern.scalar_children,
        &op.scalar_children,
        contexts_after_rel,
        engine,
    )
    .await
}

/// Helper function to match operator children components
async fn match_operator_children<E>(
    pattern_children: &[Pattern],
    op_children: &[Value],
    contexts: Vec<Context>,
    engine: Engine<E>,
) -> Vec<Context>
where
    E: Expander,
{
    let context_futures = contexts
        .into_iter()
        .map(|ctx| {
            let engine_with_ctx = engine.clone().with_context(ctx);
            match_pattern_pairs(
                pattern_children.iter().zip(op_children.iter()),
                engine_with_ctx,
            )
        })
        .collect::<Vec<_>>();

    // Await all futures and collect results
    join_all(context_futures)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Helper function to match a series of pattern-value pairs.
///
/// This function takes pattern-value pairs and tries to match them sequentially,
/// collecting all possible matching contexts.
async fn match_pattern_pairs<'a, I, E>(pairs: I, engine: Engine<E>) -> Vec<Context>
where
    I: Iterator<Item = (&'a Pattern, &'a Value)>,
    E: Expander,
{
    // Start with a set containing just the initial context
    let mut current_contexts = vec![engine.context.clone()];

    // For each pattern-value pair
    for (pattern, value) in pairs {
        // For each context, match the pattern-value pair and collect all resulting contexts
        let next_context_futures = current_contexts
            .into_iter()
            .map(|ctx| {
                let engine_with_ctx = engine.clone().with_context(ctx);
                match_pattern(value.clone(), pattern.clone(), engine_with_ctx)
            })
            .collect::<Vec<_>>();

        // Await all futures and collect all contexts
        let next_contexts = join_all(next_context_futures)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        // If no contexts matched, the entire pattern fails to match
        if next_contexts.is_empty() {
            return vec![];
        }

        // Continue with the next pattern-value pair
        current_contexts = next_contexts;
    }

    current_contexts
}

#[cfg(test)]
mod tests {
    use crate::engine::{
        eval::r#match::match_pattern,
        utils::tests::{
            bool_val, create_basic_mock_expander, create_test_engine, int_val, string_val,
            unit_val, MockExpander,
        },
        Context, Engine,
    };
    use futures::executor::block_on;
    use optd_dsl::analyzer::hir::{
        CoreData, GroupId, Literal, LogicalOp, Materializable, Operator, OperatorKind, Pattern,
        PhysicalGoal, PhysicalOp, ScalarOp, Value,
    };
    use Literal::*;
    use Materializable::*;
    use Pattern::*;

    // Test pattern matching against a literal value
    #[test]
    fn test_match_literal() {
        let engine = create_test_engine();
        let value = int_val(42);
        let pattern = Pattern::Literal(Int64(42));
        let results = block_on(match_pattern(value, pattern, engine));

        // Should produce one match with the unchanged context
        assert_eq!(results.len(), 1);
    }

    // Test pattern matching with wildcard
    #[test]
    fn test_match_wildcard() {
        let engine = create_test_engine();

        // Create various values to match against
        let values = vec![int_val(42), string_val("hello"), bool_val(true), unit_val()];

        // Create a wildcard pattern
        let pattern = Wildcard;

        // Match each value against the pattern
        for value in values {
            let results = block_on(match_pattern(value, pattern.clone(), engine.clone()));

            // Wildcard should match anything
            assert_eq!(results.len(), 1);
        }
    }

    // Test pattern matching with binding
    #[test]
    fn test_match_binding() {
        let engine = create_test_engine();
        let value = int_val(42);

        // Create a binding pattern: x @ 42
        let pattern = Bind("x".to_string(), Box::new(Pattern::Literal(Int64(42))));
        let results = block_on(match_pattern(value, pattern, engine));

        // Should produce one match
        assert_eq!(results.len(), 1);

        // Check that the variable was bound
        let context = &results[0];
        let bound_value = context.lookup("x").expect("Variable x should be bound");
        assert!(matches!(&bound_value.0, CoreData::Literal(Int64(42))));
    }

    // Test pattern matching against a struct
    #[test]
    fn test_match_struct() {
        let engine = create_test_engine();

        // Create a struct value: Person("Alice", 30)
        let person = Value(CoreData::Struct(
            "Person".to_string(),
            vec![string_val("Alice"), int_val(30)],
        ));

        // Create a struct pattern: Person(name, age)
        let pattern = Pattern::Struct(
            "Person".to_string(),
            vec![
                Bind("name".to_string(), Box::new(Wildcard)),
                Bind("age".to_string(), Box::new(Wildcard)),
            ],
        );
        let results = block_on(match_pattern(person, pattern, engine));

        // Should produce one match
        assert_eq!(results.len(), 1);

        // Check that variables were bound correctly
        let context = &results[0];
        let name = context
            .lookup("name")
            .expect("Variable name should be bound");
        let age = context.lookup("age").expect("Variable age should be bound");

        assert!(matches!(&name.0, CoreData::Literal(String(s)) if s == "Alice"));
        assert!(matches!(&age.0, CoreData::Literal(Int64(30))));
    }

    // Test pattern matching against an array decomposition
    #[test]
    fn test_match_array_decomposition() {
        let engine = create_test_engine();

        // Create an array value: [1, 2, 3]
        let array = Value(CoreData::Array(vec![int_val(1), int_val(2), int_val(3)]));

        // Create an array decomposition pattern: head :: tail
        let pattern = ArrayDecomp(
            Bind("head".to_string(), Wildcard.into()).into(),
            Bind("tail".to_string(), Wildcard.into()).into(),
        );
        let results = block_on(match_pattern(array, pattern, engine));

        // Should produce one match
        assert_eq!(results.len(), 1);

        // Check that variables were bound correctly
        let context = &results[0];
        let head = context
            .lookup("head")
            .expect("Variable head should be bound");
        let tail = context
            .lookup("tail")
            .expect("Variable tail should be bound");

        // Head should be the first element
        assert!(matches!(&head.0, CoreData::Literal(Int64(1))));

        // Tail should be the rest of the array [2, 3]
        if let CoreData::Array(tail_items) = &tail.0 {
            assert_eq!(tail_items.len(), 2);
            assert!(matches!(&tail_items[0].0, CoreData::Literal(Int64(2))));
            assert!(matches!(&tail_items[1].0, CoreData::Literal(Int64(3))));
        } else {
            panic!("Expected Array for tail, got {:?}", tail);
        }
    }

    // Test pattern matching with empty array
    #[test]
    fn test_match_empty_array() {
        let engine = create_test_engine();

        // Create an empty array value: []
        let empty_array = Value(CoreData::Array(vec![]));

        // Create an empty array pattern
        let pattern = EmptyArray;
        let results = block_on(match_pattern(empty_array, pattern, engine));

        // Should produce one match
        assert_eq!(results.len(), 1);
    }

    // Test pattern matching against a logical operator
    #[test]
    fn test_match_logical_operator() {
        // Create a logical operator
        let filter_op = Operator {
            tag: "Filter".to_string(),
            kind: OperatorKind::Logical,
            operator_data: vec![bool_val(true)], // Predicate value
            scalar_children: vec![],
            relational_children: vec![int_val(42)], // Child relation
        };

        let logical_value = Value(CoreData::Logical(LogicalOp(Materialized(filter_op))));

        // Create an operator pattern: Filter(predicate, relation)
        let op_pattern = Operator {
            tag: "Filter".to_string(),
            kind: OperatorKind::Logical,
            operator_data: vec![Bind("predicate".to_string(), Box::new(Wildcard))],
            scalar_children: vec![],
            relational_children: vec![Bind("relation".to_string(), Box::new(Wildcard))],
        };

        let pattern = Pattern::Operator(op_pattern);

        let engine = Engine::new(Context::default(), create_basic_mock_expander());
        let results = block_on(match_pattern(logical_value, pattern, engine));

        // Should produce one match
        assert_eq!(results.len(), 1);

        // Check that variables were bound correctly
        let result_context = &results[0];
        let predicate = result_context
            .lookup("predicate")
            .expect("Variable predicate should be bound");
        let relation = result_context
            .lookup("relation")
            .expect("Variable relation should be bound");

        assert!(matches!(&predicate.0, CoreData::Literal(Bool(true))));
        assert!(matches!(&relation.0, CoreData::Literal(Int64(42))));
    }

    // Test pattern matching against an expanded logical group
    #[test]
    fn test_match_expanded_logical_group() {
        // Create a MockExpander that provides two implementations for a logical group
        let expander = MockExpander::new(
            |group_id| {
                if group_id == GroupId(1) {
                    // Return two different filter operators
                    let filter1 = Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![bool_val(true)], // Predicate = true
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    let filter2 = Operator {
                        tag: "Filter".to_string(),
                        kind: OperatorKind::Logical,
                        operator_data: vec![bool_val(false)], // Predicate = false
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    vec![
                        Value(CoreData::Logical(LogicalOp(Materialized(filter1)))),
                        Value(CoreData::Logical(LogicalOp(Materialized(filter2)))),
                    ]
                } else {
                    vec![]
                }
            },
            |_| vec![],
            |_| panic!("Physical expansion not expected"),
        );

        // Create an engine with this expander
        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference: <logical_group1>
        let logical_group_ref = Value(CoreData::Logical(LogicalOp(UnMaterialized(GroupId(1)))));

        // Create a pattern that matches Filter operators with true predicate
        let op_pattern = Operator {
            tag: "Filter".to_string(),
            kind: OperatorKind::Logical,
            operator_data: vec![Pattern::Literal(Bool(true))],
            scalar_children: vec![],
            relational_children: vec![],
        };

        let pattern = Pattern::Operator(op_pattern);

        // Match the pattern against the expanded group
        let results = block_on(match_pattern(logical_group_ref, pattern, engine));

        // Should match only one of the expanded implementations (the one with true predicate)
        assert_eq!(results.len(), 1);
    }

    // Test pattern matching against an expanded scalar group
    #[test]
    fn test_match_expanded_scalar_group() {
        // Create a MockExpander that provides two implementations for a scalar group
        let expander = MockExpander::new(
            |_| vec![],
            |group_id| {
                if group_id == GroupId(2) {
                    // Return two different scalar operators
                    let add_op = Operator {
                        tag: "Add".to_string(),
                        kind: OperatorKind::Scalar,
                        operator_data: vec![int_val(1), int_val(2)], // 1 + 2
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    let mul_op = Operator {
                        tag: "Multiply".to_string(),
                        kind: OperatorKind::Scalar,
                        operator_data: vec![int_val(3), int_val(4)], // 3 * 4
                        scalar_children: vec![],
                        relational_children: vec![],
                    };

                    vec![
                        Value(CoreData::Scalar(ScalarOp(Materialized(add_op)))),
                        Value(CoreData::Scalar(ScalarOp(Materialized(mul_op)))),
                    ]
                } else {
                    vec![]
                }
            },
            |_| panic!("Physical expansion not expected"),
        );

        // Create an engine with this expander
        let engine = Engine::new(Context::default(), expander);

        // Create a scalar group reference: <scalar_group2>
        let scalar_group_ref = Value(CoreData::Scalar(ScalarOp(UnMaterialized(GroupId(2)))));

        // Create a pattern that matches Add operators
        let op_pattern = Operator {
            tag: "Add".to_string(),
            kind: OperatorKind::Scalar,
            operator_data: vec![
                Bind("left".to_string(), Box::new(Wildcard)),
                Bind("right".to_string(), Box::new(Wildcard)),
            ],
            scalar_children: vec![],
            relational_children: vec![],
        };

        let pattern = Pattern::Operator(op_pattern);

        // Match the pattern against the expanded group
        let results = block_on(match_pattern(scalar_group_ref, pattern, engine));

        // Should match only the Add operator
        assert_eq!(results.len(), 1);

        // Check that variables were bound correctly
        let context = &results[0];
        let left = context
            .lookup("left")
            .expect("Variable left should be bound");
        let right = context
            .lookup("right")
            .expect("Variable right should be bound");

        assert!(matches!(&left.0, CoreData::Literal(Int64(1))));
        assert!(matches!(&right.0, CoreData::Literal(Int64(2))));
    }

    // Test pattern matching against an expanded physical goal
    #[test]
    fn test_match_expanded_physical_goal() {
        // Create a physical goal
        let physical_goal = PhysicalGoal {
            group_id: GroupId(3),
            properties: Box::new(unit_val()),
        };

        // Create a MockExpander that provides an implementation for this physical goal
        let expander = MockExpander::new(
            |_| vec![],
            |_| vec![],
            |goal| {
                if goal.group_id == GroupId(3) {
                    // Return a hash join operator
                    let hash_join = Operator {
                        tag: "HashJoin".to_string(),
                        kind: OperatorKind::Physical,
                        operator_data: vec![int_val(42)], // Join key
                        scalar_children: vec![],
                        relational_children: vec![
                            string_val("left_table"),
                            string_val("right_table"),
                        ],
                    };

                    Value(CoreData::Physical(PhysicalOp(Materialized(hash_join))))
                } else {
                    panic!("Unexpected physical goal")
                }
            },
        );

        // Create an engine with this expander
        let engine = Engine::new(Context::default(), expander);

        // Create a physical goal reference
        let physical_ref = Value(CoreData::Physical(PhysicalOp(UnMaterialized(
            physical_goal,
        ))));

        // Create a pattern that matches HashJoin operators
        let op_pattern = Operator {
            tag: "HashJoin".to_string(),
            kind: OperatorKind::Physical,
            operator_data: vec![Bind("key".to_string(), Box::new(Wildcard))],
            scalar_children: vec![],
            relational_children: vec![
                Bind("left".to_string(), Box::new(Wildcard)),
                Bind("right".to_string(), Box::new(Wildcard)),
            ],
        };

        let pattern = Pattern::Operator(op_pattern);

        // Match the pattern against the expanded goal
        let results = block_on(match_pattern(physical_ref, pattern, engine));

        // Should match the HashJoin operator
        assert_eq!(results.len(), 1);

        // Check that variables were bound correctly
        let context = &results[0];
        let key = context.lookup("key").expect("Variable key should be bound");
        let left = context
            .lookup("left")
            .expect("Variable left should be bound");
        let right = context
            .lookup("right")
            .expect("Variable right should be bound");

        assert!(matches!(&key.0, CoreData::Literal(Int64(42))));
        assert!(matches!(&left.0, CoreData::Literal(String(s)) if s == "left_table"));
        assert!(matches!(&right.0, CoreData::Literal(String(s)) if s == "right_table"));
    }

    #[test]
    fn test_match_multiple_expanded_groups() {
        // Create a MockExpander that provides multiple implementations for a group
        let expander = MockExpander::new(
            |group_id| {
                match group_id {
                    GroupId(1) => {
                        // Return two Filter operators with different predicates
                        let filter1 = Operator {
                            tag: "Filter".to_string(),
                            kind: OperatorKind::Logical,
                            operator_data: vec![bool_val(true)],
                            scalar_children: vec![],
                            relational_children: vec![],
                        };

                        let filter2 = Operator {
                            tag: "Filter".to_string(),
                            kind: OperatorKind::Logical,
                            operator_data: vec![bool_val(false)],
                            scalar_children: vec![],
                            relational_children: vec![],
                        };

                        vec![
                            Value(CoreData::Logical(LogicalOp(Materialized(filter1)))),
                            Value(CoreData::Logical(LogicalOp(Materialized(filter2)))),
                        ]
                    }
                    _ => vec![],
                }
            },
            |_| vec![],
            |_| panic!("Physical expansion not expected"),
        );

        // Create an engine with this expander
        let engine = Engine::new(Context::default(), expander);

        // Create a logical group reference: <logical_group1>
        let logical_group_ref = Value(CoreData::Logical(LogicalOp(UnMaterialized(GroupId(1)))));

        // Create a pattern that matches any Filter operator and binds its predicate
        let pattern = Pattern::Operator(Operator {
            tag: "Filter".to_string(),
            kind: OperatorKind::Logical,
            operator_data: vec![Bind("predicate".to_string(), Box::new(Wildcard))],
            scalar_children: vec![],
            relational_children: vec![],
        });

        // Match the pattern against the expanded group
        let results = block_on(match_pattern(logical_group_ref, pattern, engine));

        // Should match both Filter operators (one with true predicate, one with false)
        assert_eq!(results.len(), 2);

        // Check that bindings contain both true and false values
        let predicates: Vec<bool> = results
            .iter()
            .map(|ctx| {
                let pred = ctx
                    .lookup("predicate")
                    .expect("Variable predicate should be bound");
                if let CoreData::Literal(Bool(b)) = pred.0 {
                    b
                } else {
                    panic!("Expected boolean predicate")
                }
            })
            .collect();

        assert!(predicates.contains(&true));
        assert!(predicates.contains(&false));
    }
}

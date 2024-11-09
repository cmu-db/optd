// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use crate::{
    heuristics::{ApplyOrder, HeuristicsOptimizer, HeuristicsOptimizerOptions},
    nodes::Value,
    optimizer::Optimizer,
    physical_property::PhysicalPropertyBuilderAny,
    tests::common::{
        column_ref, expr, list, physical_filter, physical_hash_agg, physical_nested_loop_join,
        physical_scan, physical_sort, physical_streaming_agg, MemoTestRelTyp, SortProp,
        SortPropertyBuilder,
    },
};

use pretty_assertions::assert_eq;

fn get_optimizer() -> HeuristicsOptimizer<MemoTestRelTyp> {
    HeuristicsOptimizer::new_with_rules(
        vec![],
        HeuristicsOptimizerOptions {
            apply_order: ApplyOrder::TopDown,
            enable_physical_prop_passthrough: true,
        },
        vec![].into(),
        vec![Box::new(SortPropertyBuilder) as Box<dyn PhysicalPropertyBuilderAny<MemoTestRelTyp>>]
            .into(),
    )
}

fn get_optimizer_no_passthrough() -> HeuristicsOptimizer<MemoTestRelTyp> {
    HeuristicsOptimizer::new_with_rules(
        vec![],
        HeuristicsOptimizerOptions {
            apply_order: ApplyOrder::TopDown,
            enable_physical_prop_passthrough: false,
        },
        vec![].into(),
        vec![Box::new(SortPropertyBuilder) as Box<dyn PhysicalPropertyBuilderAny<MemoTestRelTyp>>]
            .into(),
    )
}

#[test]
fn simple_required_physical_property() {
    // Test that the optimizer can add a sort node to satisfy a required sort property
    let mut optimizer = get_optimizer();
    let plan = physical_scan("t1");
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_sort(physical_scan("t1"), list(vec![column_ref("x")]))
    )
}

#[test]
fn simple_required_physical_property_satisfied() {
    // Test that the optimizer does not add a sort node if the required sort property is already satisfied
    let mut optimizer = get_optimizer();
    let plan = physical_sort(
        physical_scan("t1"),
        list(vec![column_ref("x"), column_ref("y")]),
    );
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_sort(
            physical_scan("t1"),
            list(vec![column_ref("x"), column_ref("y")])
        )
    )
}

#[test]
fn simple_required_physical_property_not_satisfied() {
    // Test that the optimizer still needs add a sort node if the required sort property is not fully satisfied
    let mut optimizer = get_optimizer();
    let plan = physical_sort(physical_scan("t1"), list(vec![column_ref("x")]));
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string(), "y".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_sort(
            physical_sort(physical_scan("t1"), list(vec![column_ref("x")])),
            list(vec![column_ref("x"), column_ref("y")])
        )
    )
}

#[test]
fn passthrough_all_the_way() {
    // Test that the optimizer can passthrough properties across multiple plan nodes to satisfy a required sort property
    let mut optimizer = get_optimizer();
    let plan = physical_filter(physical_scan("t1"), expr(Value::Bool(true)));
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_filter(
            physical_sort(physical_scan("t1"), list(vec![column_ref("x")])),
            expr(Value::Bool(true))
        )
    )
}

#[test]
fn passthrough_all_the_way_2() {
    // Test that the optimizer can passthrough properties across multiple plan nodes to satisfy a required sort property
    let mut optimizer = get_optimizer();
    let plan = physical_nested_loop_join(
        physical_filter(physical_scan("t1"), expr(Value::Bool(true))),
        physical_scan("t2"),
        expr(Value::Bool(true)),
    );
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_nested_loop_join(
            physical_filter(
                physical_sort(physical_scan("t1"), list(vec![column_ref("x")])),
                expr(Value::Bool(true))
            ),
            physical_scan("t2"),
            expr(Value::Bool(true))
        )
    )
}

struct DbgAsDisplay<'a, D>(&'a D);

impl<D: std::fmt::Display> std::fmt::Debug for DbgAsDisplay<'_, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<D: PartialEq> PartialEq for DbgAsDisplay<'_, D> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[test]
fn required_property_stream_agg() {
    // Test that the optimizer can properly add a sort node to satisfy a required sort property
    let mut optimizer = get_optimizer();
    let plan = physical_streaming_agg(
        physical_filter(physical_scan("t1"), expr(Value::Bool(true))),
        list(vec![column_ref("x"), column_ref("y")]),
    );
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec![])])
        .unwrap();
    assert_eq!(
        DbgAsDisplay(&optimized_plan),
        DbgAsDisplay(&physical_streaming_agg(
            physical_filter(
                physical_sort(
                    physical_scan("t1"),
                    list(vec![column_ref("x"), column_ref("y")])
                ),
                expr(Value::Bool(true))
            ),
            list(vec![column_ref("x"), column_ref("y")])
        ))
    )
}

#[test]
fn required_property_cannot_satisfy() {
    // Test that the optimizer can properly add a sort node to satisfy a required sort property if a node removes such
    // property.
    let mut optimizer = get_optimizer();
    let plan = physical_hash_agg(
        physical_sort(
            physical_scan("t1"),
            list(vec![column_ref("x"), column_ref("y")]),
        ),
        list(vec![column_ref("x"), column_ref("y")]),
    );
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string(), "y".to_string()])])
        .unwrap();
    assert_eq!(
        DbgAsDisplay(&optimized_plan),
        DbgAsDisplay(&physical_sort(
            physical_hash_agg(
                physical_sort(
                    physical_scan("t1"),
                    list(vec![column_ref("x"), column_ref("y")]),
                ),
                list(vec![column_ref("x"), column_ref("y")])
            ),
            list(vec![column_ref("x"), column_ref("y")])
        ))
    )
}

#[test]
fn required_property_stream_agg_cannot_passthrough() {
    // Test that the optimizer can properly add a sort node to satisfy a required sort property
    let mut optimizer = get_optimizer();
    let plan = physical_streaming_agg(
        physical_filter(physical_scan("t1"), expr(Value::Bool(true))),
        list(vec![column_ref("x"), column_ref("y")]),
    );
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec![])])
        .unwrap();
    assert_eq!(
        DbgAsDisplay(&optimized_plan),
        DbgAsDisplay(&physical_streaming_agg(
            physical_filter(
                physical_sort(
                    physical_scan("t1"),
                    list(vec![column_ref("x"), column_ref("y")])
                ),
                expr(Value::Bool(true))
            ),
            list(vec![column_ref("x"), column_ref("y")])
        ))
    )
}

#[test]
fn no_passthrough_all_the_way() {
    // Test that the optimizer can passthrough properties across multiple plan nodes to satisfy a required sort property
    // without passthrough
    let mut optimizer = get_optimizer_no_passthrough();
    let plan = physical_filter(physical_scan("t1"), expr(Value::Bool(true)));
    let optimized_plan = optimizer
        .optimize_with_required_props(plan, &[&SortProp(vec!["x".to_string()])])
        .unwrap();
    assert_eq!(
        optimized_plan,
        physical_sort(
            physical_filter(physical_scan("t1"), expr(Value::Bool(true))),
            list(vec![column_ref("x")])
        ),
    )
}

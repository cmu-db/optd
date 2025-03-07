use crate::engine::{expander::Expander, utils::streams::ValueStream, Context, Engine};
use futures::{executor::block_on_stream, stream, StreamExt};
use optd_dsl::analyzer::hir::{CoreData, Expr, Goal, GroupId, Literal, Value};
use std::{self, future::ready, sync::Arc};
use CoreData::*;
use Literal::*;

use super::streams::propagate_success;

#[derive(Clone)]
pub(crate) struct MockExpander {
    // Function to generate logical expansions for a group ID
    logical_expander: fn(GroupId) -> Vec<Value>,
    // Function to generate physical implementation for a physical goal
    physical_expander: fn(&Goal) -> Value,
    // Function to generate logical properties for a group ID
    properties_expander: fn(GroupId) -> Value,
}

impl MockExpander {
    /// Creates a new MockExpander with the specified expansion functions
    pub(crate) fn new(
        logical_expander: fn(GroupId) -> Vec<Value>,
        physical_expander: fn(&Goal) -> Value,
        properties_expander: fn(GroupId) -> Value,
    ) -> Self {
        Self {
            logical_expander,
            physical_expander,
            properties_expander,
        }
    }
}

/// A simple mock implementation of the Expander trait for testing
impl Expander for MockExpander {
    fn expand_all_exprs(&self, group_id: GroupId) -> ValueStream {
        let values = (self.logical_expander)(group_id);
        stream::iter(values).map(Ok).boxed()
    }

    fn expand_winning_expr(&self, physical_goal: &Goal) -> ValueStream {
        let value = (self.physical_expander)(physical_goal);
        propagate_success(value)
    }

    async fn expand_properties(&self, group_id: GroupId) -> Value {
        (self.properties_expander)(group_id)
    }
}

// Helper functions to create values
pub(crate) fn int_val(i: i64) -> Value {
    Value(Literal(Int64(i)))
}

pub(crate) fn string_val(s: &str) -> Value {
    Value(Literal(String(s.to_string())))
}

pub(crate) fn bool_val(b: bool) -> Value {
    Value(Literal(Bool(b)))
}

pub(crate) fn unit_val() -> Value {
    Value(Literal(Unit))
}

// Helper to wrap expressions in Arc
pub(crate) fn arc(expr: Expr) -> Arc<Expr> {
    Arc::new(expr)
}

// Helper to collect all successful values from a stream
pub(crate) fn collect_stream_values(stream: ValueStream) -> Vec<Value> {
    block_on_stream(stream).filter_map(Result::ok).collect()
}

pub(crate) fn create_basic_mock_expander() -> MockExpander {
    MockExpander::new(
        |_| vec![], // No logical expansions
        |_| panic!("Physical expansion not implemented"),
        |_| panic!("Properties expansion not implemented"),
    )
}

pub(crate) fn create_test_engine() -> Engine<MockExpander> {
    let context = Context::default();
    let expander = create_basic_mock_expander();
    Engine::new(context, expander)
}

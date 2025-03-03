use crate::engine::{expander::Expander, utils::streams::ValueStream, Context, Engine};
use futures::executor::block_on_stream;
use optd_dsl::analyzer::hir::{CoreData, Expr, GroupId, Literal, PhysicalGoal, Value};
use std::{self, future::ready, sync::Arc};
use CoreData::*;
use Literal::*;

/// A simple mock implementation of the Expander trait for testing
#[derive(Clone)]
pub(crate) struct MockExpander {
    // Function to generate logical expansions for a group ID
    logical_expander: fn(GroupId) -> Vec<Value>,
    // Function to generate scalar expansions for a group ID
    scalar_expander: fn(GroupId) -> Vec<Value>,
    // Function to generate physical implementation for a physical goal
    physical_expander: fn(&PhysicalGoal) -> Value,
}

impl MockExpander {
    /// Creates a new MockExpander with the specified expansion functions
    pub(crate) fn new(
        logical_expander: fn(GroupId) -> Vec<Value>,
        scalar_expander: fn(GroupId) -> Vec<Value>,
        physical_expander: fn(&PhysicalGoal) -> Value,
    ) -> Self {
        Self {
            logical_expander,
            scalar_expander,
            physical_expander,
        }
    }
}

impl Expander for MockExpander {
    async fn expand_logical_group(&self, group_id: GroupId) -> Vec<Value> {
        ready((self.logical_expander)(group_id)).await
    }

    async fn expand_scalar_group(&self, group_id: GroupId) -> Vec<Value> {
        ready((self.scalar_expander)(group_id)).await
    }

    async fn expand_physical_goal(&self, physical_goal: &PhysicalGoal) -> Value {
        ready((self.physical_expander)(physical_goal)).await
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
        |_| vec![], // No scalar expansions
        |_| panic!("Physical expansion not implemented"),
    )
}

pub(crate) fn create_test_engine() -> Engine<MockExpander> {
    let context = Context::default();
    let expander = create_basic_mock_expander();
    Engine::new(context, expander)
}

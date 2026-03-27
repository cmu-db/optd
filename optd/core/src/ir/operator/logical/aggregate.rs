//! The aggregate operator groups rows by a set of keys and applies an aggregate
//! function to each group.
//!
//! UNIMPLEMENTED: Currently, grouping set semantics are not handled.

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - implementation: The selected physical implementation, if any.
    /// Scalars:
    /// - exprs: The aggregate expressions to compute.
    /// - keys: The grouping keys.
    Aggregate, AggregateBorrowed {
        properties: OperatorProperties,
        metadata: AggregateMetadata {
            key_table_index: i64,
            aggregate_table_index: i64,
            implementation: Option<AggregateImplementation>,
        },
        inputs: {
            operators: [input],
            scalars: [exprs, keys],
        }
    }
);
impl_operator_conversion!(Aggregate, AggregateBorrowed);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AggregateImplementation {
    #[default]
    Hash,
}

impl Aggregate {
    pub fn new(
        key_table_index: i64,
        aggregate_table_index: i64,
        input: Arc<Operator>,
        exprs: Arc<Scalar>,
        keys: Arc<Scalar>,
        implementation: Option<AggregateImplementation>,
    ) -> Self {
        Self {
            meta: AggregateMetadata {
                key_table_index,
                aggregate_table_index,
                implementation,
            },
            common: IRCommon::new(Arc::new([input]), Arc::new([exprs, keys])),
        }
    }
}

impl Explain for AggregateBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::new();
        fields.push((".key_table_index", Pretty::display(&self.key_table_index())));
        fields.push((
            ".aggregate_table_index",
            Pretty::display(&self.aggregate_table_index()),
        ));
        fields.push((".implementation", Pretty::debug(self.implementation())));
        let exprs = self.exprs().explain(ctx, option);
        let keys = self.keys().explain(ctx, option);
        fields.push((".exprs", exprs));
        fields.push((".keys", keys));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("Aggregate", fields, children)
    }
}

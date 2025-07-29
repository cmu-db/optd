use std::sync::Arc;

use crate::ir::{
    Column, IRCommon, Scalar,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
    scalar::ColumnRef,
};

define_node!(
    Assign {
        properties: ScalarProperties,
        metadata: AssignMetadata {
            assignee: Column,
        },
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(Assign);

impl Assign {
    pub fn new(assignee: Column, expr: Arc<Scalar>) -> Self {
        Self {
            meta: AssignMetadata { assignee },
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }

    pub fn is_passthrough(&self) -> bool {
        self.expr()
            .try_bind_ref::<ColumnRef>()
            .is_ok_and(|column_ref| column_ref.column() == self.assignee())
    }
}

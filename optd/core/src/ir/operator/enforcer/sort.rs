use std::sync::Arc;

use crate::ir::{
    IRCommon, Operator,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, TupleOrdering},
};

define_node!(
    EnforcerSort {
        properties: OperatorProperties,
        metadata: EnforcerSortMetadata {
            tuple_ordering: TupleOrdering,
        },
        inputs: {
            operators: [input],
            scalars: [],
        }
    }
);
impl_operator_conversion!(EnforcerSort);

impl EnforcerSort {
    pub fn new(tuple_ordering: TupleOrdering, input: Arc<Operator>) -> Self {
        Self {
            meta: EnforcerSortMetadata { tuple_ordering },
            common: IRCommon::with_input_operators_only(Arc::new([input])),
        }
    }
}

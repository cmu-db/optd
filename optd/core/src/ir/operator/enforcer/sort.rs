use std::sync::Arc;

use crate::ir::{
    IRCommon, Operator,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, TupleOrdering},
};

define_node!(
    EnforcerSort, EnforcerSortBorrowed {
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
impl_operator_conversion!(EnforcerSort, EnforcerSortBorrowed);

impl EnforcerSort {
    pub fn new(tuple_ordering: TupleOrdering, input: Arc<Operator>) -> Self {
        Self {
            meta: EnforcerSortMetadata { tuple_ordering },
            common: IRCommon::with_input_operators_only(Arc::new([input])),
        }
    }
}

impl Explain for EnforcerSortBorrowed<'_> {
    fn explain<'a>(
        &self,
        _ctx: &crate::ir::IRContext,
        _option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        todo!()
    }
}

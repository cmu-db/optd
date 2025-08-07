use std::sync::Arc;

use pretty_xmlish::Pretty;

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
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = Vec::with_capacity(1);
        fields.push(("tuple_ordering", Pretty::display(self.tuple_ordering())));

        let metadata = self.common.explain_operator_properties(ctx, option);
        fields.extend(metadata);
        let children = self.common.explain_input_operators(ctx, option);

        Pretty::simple_record("EnforcerSort", fields, children)
    }
}

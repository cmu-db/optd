use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

define_node!(
    PhysicalFilter, PhysicalFilterBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalFilterMetadata {},
        inputs: {
            operators: [input],
            scalars: [predicate],
        }
    }
);
impl_operator_conversion!(PhysicalFilter, PhysicalFilterBorrowed);

impl PhysicalFilter {
    pub fn new(input: Arc<Operator>, predicate: Arc<Scalar>) -> Self {
        Self {
            meta: PhysicalFilterMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([predicate])),
        }
    }
}

impl Explain for PhysicalFilterBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(3);
        fields.push((".predicate", self.predicate().explain(ctx, option)));
        let children = self
            .common
            .input_operators
            .iter()
            .map(|input_op| input_op.explain(ctx, option))
            .collect();
        Pretty::simple_record("PhysicalFilter", fields, children)
    }
}

use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

define_node!(
    PhysicalProject, PhysicalProjectBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalProjectMetadata {},
        inputs: {
            operators: [input],
            scalars: [projections],
        }
    }
);
impl_operator_conversion!(PhysicalProject, PhysicalProjectBorrowed);

impl PhysicalProject {
    pub fn new(input: Arc<Operator>, projections: Arc<Scalar>) -> Self {
        Self {
            meta: PhysicalProjectMetadata {},
            common: IRCommon::new(Arc::new([input]), Arc::new([projections])),
        }
    }
}

impl Explain for PhysicalProjectBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::new();
        let projecions_explained = self.projections().explain(ctx, option);
        fields.push((".projections", projecions_explained));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        let children = self.common.explain_input_operators(ctx, option);
        Pretty::simple_record("PhysicalProject", fields, children)
    }
}

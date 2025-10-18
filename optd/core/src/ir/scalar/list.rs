use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    List, ListBorrowed {
        properties: ScalarProperties,
        metadata: ListMetadata {},
        inputs: {
            operators: [],
            scalars: members[],
        }
    }
);
impl_scalar_conversion!(List, ListBorrowed);

impl List {
    pub fn new(members: Arc<[Arc<Scalar>]>) -> Self {
        Self {
            meta: ListMetadata {},
            common: IRCommon::with_input_scalars_only(members),
        }
    }
}

impl Explain for ListBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let explained_terms = self
            .members()
            .iter()
            .map(|t| t.explain(ctx, option))
            .collect();

        Pretty::Array(explained_terms)
    }
}

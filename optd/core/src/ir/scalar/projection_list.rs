use std::sync::Arc;

use crate::ir::{
    Column, IRCommon, Scalar,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
    scalar::Assign,
};

define_node!(
    ProjectionList  {
        properties: ScalarProperties,
        metadata: ProjectionListMetadata {},
        inputs: {
            operators: [],
            scalars: members[],
        },
    },
);
impl_scalar_conversion!(ProjectionList);

impl ProjectionList {
    pub fn new(members: Arc<[Arc<Scalar>]>) -> Self {
        Self {
            meta: ProjectionListMetadata {},
            common: IRCommon::with_input_scalars_only(members),
        }
    }

    pub fn get_all_assignees(&self) -> impl Iterator<Item = Column> {
        self.members().iter().map(|member| {
            let assign = member.try_bind_ref::<Assign>().unwrap();
            assign.assignee().clone()
        })
    }
}

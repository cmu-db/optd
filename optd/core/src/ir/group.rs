use std::sync::Arc;

use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};

/// Uniquely identifies an equivalent class in the optimizer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub i64);

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "G{}", self.0)
    }
}

impl std::fmt::Debug for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "G{}", self.0)
    }
}

define_node!(
    Group, GroupBorrowed {
        properties: OperatorProperties,
        metadata: GroupMetadata {
            group_id: GroupId,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);

impl_operator_conversion!(Group, GroupBorrowed);

impl Group {
    pub fn new(group_id: GroupId, properties: Arc<OperatorProperties>) -> Self {
        Self {
            meta: GroupMetadata { group_id },
            common: IRCommon::with_properties_only(properties),
        }
    }
}

impl Explain for GroupBorrowed<'_> {
    fn explain<'a>(
        &self,
        _ctx: &super::IRContext,
        _option: &super::explain::ExplainOption,
    ) -> Pretty<'a> {
        let fields = vec![(".group_id", Pretty::display(self.group_id()))];
        Pretty::childless_record("Group", fields)
    }
}

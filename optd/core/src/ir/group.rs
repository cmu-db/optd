//! Defines the Group operator, which represents an equivalence class
//! in the query optimizer.

use crate::ir::{
    IRCommon,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::OperatorProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

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
    /// Metadata:
    /// - tuple_ordering: The tuple ordering that this enforcer imposes.
    /// Scalars: (none)
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
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = vec![(".group_id", Pretty::display(self.group_id()))];
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("Group", fields)
    }
}

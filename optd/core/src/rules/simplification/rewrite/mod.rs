mod project;
mod scalar;
mod select;

use crate::ir::{Column, Scalar, scalar::List};
use std::{collections::HashMap, sync::Arc};

pub use project::MergeProjectRulePass;
pub use scalar::ScalarSimplificationRulePass;
pub use select::{
    MergeSelectRulePass, PushSelectThroughJoinRulePass, PushSelectThroughProjectRulePass,
};

fn extract_projection_substitutions(
    table_index: i64,
    projections: &Arc<Scalar>,
) -> Option<HashMap<Column, Arc<Scalar>>> {
    let list = projections.try_borrow::<List>().ok()?;
    let mut substitutions = HashMap::with_capacity(list.members().len());
    for (idx, member) in list.members().iter().enumerate() {
        substitutions.insert(Column(table_index, idx), member.clone());
    }
    Some(substitutions)
}

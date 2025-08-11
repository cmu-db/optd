use std::sync::Arc;

use crate::ir::{
    IRCommon, Scalar,
    catalog::DataSourceId,
    macros::{define_node, impl_operator_conversion},
    properties::{Derive, OperatorProperties, OutputColumns},
    scalar::ProjectionList,
};

define_node!(
    LogicalGet, LogicalGetBorrowed {
        properties: OperatorProperties,
        metadata: LogicalGetMetadata {
            table_id: DataSourceId,
        },
        inputs: {
            operators: [],
            scalars: [projection_list],
        }
    }
);
impl_operator_conversion!(LogicalGet, LogicalGetBorrowed);

impl LogicalGet {
    pub fn new(table_id: DataSourceId, projection_list: Arc<Scalar>) -> Self {
        Self {
            meta: LogicalGetMetadata { table_id },
            common: IRCommon::with_input_scalars_only(Arc::new([projection_list])),
        }
    }
}

impl Derive<OutputColumns> for LogicalGet {
    fn derive_by_compute(&self, _ctx: &crate::ir::context::IRContext) -> OutputColumns {
        let projections = self
            .projection_list()
            .try_borrow::<ProjectionList>()
            .expect("projection_list should typecheck");
        OutputColumns::from_column_set(projections.get_all_assignees().collect())
    }

    fn derive(&self, ctx: &crate::ir::context::IRContext) -> OutputColumns {
        self.common
            .properties
            .output_columns
            .get_or_init(|| self.derive_by_compute(ctx))
            .clone()
    }
}

#[cfg(test)]
impl LogicalGet {
    pub(crate) fn mock(columns: Vec<i64>) -> Self {
        use crate::ir::{Column, convert::IntoScalar, scalar::*};

        let projections = columns
            .into_iter()
            .map(|i| Assign::new(Column(i), ColumnRef::new(Column(i)).into_scalar()).into_scalar())
            .collect::<Vec<_>>();

        Self::new(
            DataSourceId(1),
            ProjectionList::new(projections.into()).into_scalar(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{Column, context::IRContext, convert::IntoOperator, properties::GetProperty};

    use super::*;

    #[test]
    fn logical_get_construct_and_access() {
        let ctx = IRContext::with_course_tables();
        let op = LogicalGet::mock(vec![0, 1]).into_operator();
        let output_columns = op.get_property::<OutputColumns>(&ctx);
        let set = output_columns.set();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&Column(0)));
        assert!(set.contains(&Column(1)));
        assert!(op.try_borrow::<LogicalGet>().is_ok());
    }
}

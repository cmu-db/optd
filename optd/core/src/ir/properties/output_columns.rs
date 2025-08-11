use std::{collections::HashSet, ops::Deref, sync::Arc};

use itertools::Itertools;

use crate::ir::{
    ColumnSet, OperatorKind,
    operator::{LogicalGet, PhysicalTableScan},
    properties::{Derive, GetProperty, PropertyMarker},
};

#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumns(Arc<ColumnSet>);

impl OutputColumns {
    pub fn from_column_set(set: ColumnSet) -> Self {
        Self(Arc::new(set))
    }

    // pub fn set(&self) -> &ColumnSet {
    //     &self.0
    // }
}

impl std::ops::Deref for OutputColumns {
    type Target = ColumnSet;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl std::fmt::Display for OutputColumns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.deref().iter().sorted()).finish()
    }
}

impl PropertyMarker for OutputColumns {
    type Output = Self;
}

impl Derive<OutputColumns> for crate::ir::Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::Group(_) => {
                // Always derive a placeholder using the normalized expression.
                panic!("Right now group's properties should always be set.")
            }
            OperatorKind::LogicalGet(meta) => {
                LogicalGet::borrow_raw_parts(meta, &self.common).derive(ctx)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                PhysicalTableScan::borrow_raw_parts(meta, &self.common).derive(ctx)
            }
            OperatorKind::LogicalJoin(_)
            | OperatorKind::PhysicalNLJoin(_)
            | OperatorKind::LogicalSelect(_)
            | OperatorKind::PhysicalFilter(_)
            | OperatorKind::EnforcerSort(_) => {
                let set = self
                    .input_operators()
                    .iter()
                    .fold(HashSet::new(), |mut set, op| {
                        set.extend(&*op.output_columns(ctx));
                        set
                    });
                OutputColumns::from_column_set(set)
            }
            OperatorKind::MockScan(meta) => meta.spec.mocked_output_columns.clone(),
        }
    }

    fn derive(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        self.common
            .properties
            .output_columns
            .get_or_init(|| <Self as Derive<OutputColumns>>::derive_by_compute(self, ctx))
            .clone()
    }
}

impl crate::ir::Operator {
    pub fn output_columns(&self, ctx: &crate::ir::context::IRContext) -> OutputColumns {
        self.get_property::<OutputColumns>(ctx)
    }
}

// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_core::nodes::PlanNodeOrGroup;
use optd_core::optimizer::Optimizer;

use crate::plan_nodes::DfNodeType;
use crate::properties::column_ref::{ColumnRefPropertyBuilder, GroupColumnRefs};
use crate::properties::schema::{Schema, SchemaPropertyBuilder};

pub trait OptimizerExt: Optimizer<DfNodeType> {
    fn get_schema_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> Schema;
    fn get_column_ref_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> GroupColumnRefs;
}

impl<O: Optimizer<DfNodeType>> OptimizerExt for O {
    fn get_schema_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> Schema {
        self.get_logical_property::<SchemaPropertyBuilder>(root_rel, 0)
    }

    fn get_column_ref_of(&self, root_rel: PlanNodeOrGroup<DfNodeType>) -> GroupColumnRefs {
        self.get_logical_property::<ColumnRefPropertyBuilder>(root_rel, 1)
    }
}

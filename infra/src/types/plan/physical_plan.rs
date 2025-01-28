use crate::types::operator::{
    logical::LogicalOperator,
    physical::{HashJoinOperator, PhysicalFilterOperator, PhysicalOperator, TableScanOperator},
    ScalarOperator,
};
use datafusion::{
    common::{arrow::datatypes::Schema, JoinType},
    datasource::physical_plan::{CsvExecBuilder, FileScanConfig},
    execution::object_store::ObjectStoreUrl,
    physical_plan::{
        expressions::NoOp,
        filter::FilterExec,
        joins::{HashJoinExec, PartitionMode},
        ExecutionPlan,
    },
};
use std::sync::Arc;

/// TODO Add docs.
#[derive(Clone)]
pub struct PhysicalPlan {
    pub root: Arc<PhysicalOperator<PhysicalLink>>,
}

/// TODO This is hacky prototype code, DO NOT USE!
impl PhysicalPlan {
    fn as_datafusion_execution_plan(&self) -> Arc<dyn ExecutionPlan> {
        self.root.as_datafusion_execution_plan()
    }
}

/// TODO Add docs.
#[derive(Clone)]
pub enum PhysicalLink {
    PhysicalNode(Arc<PhysicalOperator<PhysicalLink>>),
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
}

/// TODO Add docs.
#[derive(Clone)]
pub enum ScalarLink {
    ScalarNode(Arc<ScalarOperator<ScalarLink>>),
}

/// TODO This is hacky prototype code, DO NOT USE!
impl PhysicalOperator<PhysicalLink> {
    fn as_datafusion_execution_plan(&self) -> Arc<dyn ExecutionPlan> {
        match self {
            PhysicalOperator::TableScan(TableScanOperator {
                table_name,
                predicate,
            }) => {
                let object_store_url = ObjectStoreUrl::local_filesystem();
                let schema = Arc::new(Schema::empty()); // TODO FIX THIS!
                let file_scan_config = FileScanConfig::new(object_store_url, schema);

                Arc::new(CsvExecBuilder::new(file_scan_config).build())
            }
            PhysicalOperator::Filter(PhysicalFilterOperator { child, predicate }) => {
                let PhysicalLink::PhysicalNode(child_operator) = child else {
                    unimplemented!("encountered a scalar operator");
                };

                let predicate_expr = Arc::new(NoOp {}); // TODO FIX THIS!
                let child_plan = child_operator.as_datafusion_execution_plan();

                Arc::new(FilterExec::try_new(predicate_expr, child_plan).unwrap())
            }
            PhysicalOperator::HashJoin(HashJoinOperator {
                join_type,
                left,
                right,
                condition,
            }) => {
                let PhysicalLink::PhysicalNode(left_operator) = left else {
                    unimplemented!("encountered a scalar operator");
                };
                let PhysicalLink::PhysicalNode(right_operator) = right else {
                    unimplemented!("encountered a scalar operator");
                };

                let left_plan = left_operator.as_datafusion_execution_plan();
                let right_plan = right_operator.as_datafusion_execution_plan();
                let condition_on = vec![]; // TODO FIX THIS!
                let join_filter = None;
                let join_type = JoinType::Inner;
                let projection = None;
                let partition_mode = PartitionMode::CollectLeft;
                let null_equals_null = true;

                Arc::new(
                    HashJoinExec::try_new(
                        left_plan,
                        right_plan,
                        condition_on,
                        join_filter,
                        &join_type,
                        projection,
                        partition_mode,
                        null_equals_null,
                    )
                    .unwrap(),
                )
            }
        }
    }
}

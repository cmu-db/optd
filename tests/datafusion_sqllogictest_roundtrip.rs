use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result as DataFusionResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};

#[derive(Debug)]
struct SubstraitRoundTripQueryPlanner;

#[async_trait]
impl QueryPlanner for SubstraitRoundTripQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let substrait_plan = datafusion_substrait::logical_plan::producer::to_substrait_plan(
            logical_plan,
            session_state,
        )?;
        let round_tripped_plan = datafusion_substrait::logical_plan::consumer::from_substrait_plan(
            session_state,
            &substrait_plan,
        )
        .await?;

        DefaultPhysicalPlanner::default()
            .create_physical_plan(&round_tripped_plan, session_state)
            .await
    }
}

struct DataFusionSubstraitDb {
    ctx: SessionContext,
}

impl DataFusionSubstraitDb {
    fn new() -> DataFusionResult<Self> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_optimizer_rules(vec![])
            .with_query_planner(Arc::new(SubstraitRoundTripQueryPlanner))
            .build();
        let ctx = SessionContext::new_with_state(state);

        register_numbers_table(&ctx)?;
        Ok(Self { ctx })
    }
}

#[async_trait]
impl AsyncDB for DataFusionSubstraitDb {
    type Error = datafusion::error::DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let dataframe = self.ctx.sql(sql).await?;
        let schema = dataframe.schema().clone();
        let batches = dataframe.collect().await?;
        let row_count = batches.iter().map(|batch| batch.num_rows() as u64).sum();

        if schema.fields().is_empty() {
            return Ok(DBOutput::StatementComplete(row_count));
        }

        let types = schema
            .fields()
            .iter()
            .map(|field| column_type(field.data_type()))
            .collect();
        let mut rows = Vec::new();

        for batch in batches {
            for row_index in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for column in batch.columns() {
                    let value = ScalarValue::try_from_array(column.as_ref(), row_index)?;
                    row.push(value.to_string());
                }
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows { types, rows })
    }

    async fn shutdown(&mut self) {}
}

#[tokio::test]
async fn sqllogictest_runs_datafusion_substrait_roundtrip() {
    let mut runner = Runner::new(|| async { DataFusionSubstraitDb::new() });

    runner
        .run_file_async("tests/slt/datafusion_substrait_roundtrip.slt")
        .await
        .unwrap();
}

fn register_numbers_table(ctx: &SessionContext) -> DataFusionResult<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;

    ctx.register_table("numbers", Arc::new(table))?;
    Ok(())
}

fn column_type(data_type: &DataType) -> DefaultColumnType {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DefaultColumnType::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

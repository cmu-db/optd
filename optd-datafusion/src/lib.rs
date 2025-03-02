// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlanProperties, Partitioning};
use datafusion::prelude::*;
use futures::StreamExt;
use std::time::SystemTime;

mod converter;
mod mock;
mod optd_utils;

/// Given a string of SQL queries, run them
pub async fn run_queries(queries: &[&str]) -> Result<()> {
    // Create a default DataFusion `SessionConfig`.
    let session_config = SessionConfig::new().with_information_schema(true);

    // Create a DataFusion `SessionContext` that uses the `optd` optimizer to help created optimized
    // `ExecutionPlan`s.
    let ctx = optd_utils::create_optd_session(Some(session_config), None, None)
        .await
        .unwrap();

    // For each query, create and optimize a physical `ExecutionPlan` and then run it.
    for (i, query) in queries.iter().enumerate() {
        let sql = ctx.sql(query).await?;

        // Start a timer to record optimization + execution time.
        let now = SystemTime::now();

        // Note that `create_physical_plan` here will call the `optd` optimizer.
        let plan = sql.create_physical_plan().await?;

        // DataFusion execution nodes will output multiple streams that are partitioned by the
        // following patterns, so just join them all into one stream for now.
        let partitions = match plan.output_partitioning() {
            Partitioning::RoundRobinBatch(c) => *c,
            Partitioning::Hash(_, h) => *h,
            Partitioning::UnknownPartitioning(p) => *p,
        };

        // Queue the record batches so that print time does not affect the execution time record.
        let mut record_batches: Vec<RecordBatch> = vec![];

        // In a separate tokio task, send batches to the next operator over the `tx` channel, and
        //  make sure to use all of the partitions.
        for i in 0..partitions {
            // Can't make this `Result<Vec<_>>` because `DataFusionError` is not `Default`.
            let result_batches: Vec<Result<RecordBatch>> =
                plan.execute(i, Default::default())?.collect().await;

            for batch in result_batches {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                record_batches.push(batch);
            }
        }

        let elapsed = now.elapsed().expect("Failed to get elapsed time");

        // Pretty print the results.
        let query_results = pretty::pretty_format_batches(&record_batches)
            .expect("Unable to format query reuslts")
            .to_string();

        println!("\n\nQuery {i}:");
        println!("{query}\n");
        println!("Execution time in Milliseconds: {}", elapsed.as_millis());
        println!("Query Results:\n{query_results}\n\n");
    }

    Ok(())
}

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty;
use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use optd_datafusion::create_df_context;
use std::{io, time::SystemTime};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a SessionContext with TPCH base tables
    let rt_config = RuntimeConfig::new();

    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let ctx = crate::create_df_context(Some(session_config.clone()), Some(rt_config.clone()), None)
        .await
        .unwrap();

    // Create a DataFrame with the input query
    let queries = io::read_to_string(io::stdin())?;
    for query in queries.split(';') {
        if query.trim().is_empty() {
            continue;
        }
        let sql = ctx.sql(query).await?;
        // Run our execution engine on the physical plan
        let df_physical_plan = sql.clone().create_physical_plan().await?;
        let plan = df_physical_plan.clone();
        println!("{:#?}", df_physical_plan.clone());
        // let df_physical_plan = df_physical_plan.children()[0].clone();
        let mut print_results: Vec<RecordBatch> = vec![];
        let now = SystemTime::now();

        // DataFusion execution nodes will output multiple streams that are partitioned by the following
        // patterns, so just join them all into one stream
        let partitions = match plan.output_partitioning() {
            Partitioning::RoundRobinBatch(c) => *c,
            Partitioning::Hash(_, h) => *h,
            Partitioning::UnknownPartitioning(p) => *p,
        };

        // In a separate tokio task, send batches to the next operator over the `tx` channel, and make
        // sure to make use of all of the partitions
        for i in 0..partitions {
            let batch_stream = plan.execute(i, Default::default()).unwrap();

            let results: Vec<_> = batch_stream.collect().await;
            for batch in results {
                let batch = batch.unwrap();
                if batch.num_rows() == 0 {
                    continue;
                }
                print_results.push(batch);
            }
        }

        match now.elapsed() {
            Ok(elapsed) => {
                // it prints '2'
                println!("Datafusion time in milliseconds: {}", elapsed.as_millis());
            }
            Err(e) => {
                // an error occurred!
                println!("Error: {e:?}");
            }
        }

        print_results.into_iter().for_each(|batch| {
            let pretty_results = pretty::pretty_format_batches(&[batch]).unwrap().to_string();
            println!("{}", pretty_results);
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_main() {
    }
}


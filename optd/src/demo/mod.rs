use crate::{
    catalog::{Catalog, iceberg::memory_catalog},
    dsl::{
        analyzer::hir::{CoreData, LogicalOp, Materializable, Udf, Value},
        compile::{Config, compile_hir},
        engine::{Continuation, Engine, EngineResponse},
        utils::retriever::{MockRetriever, Retriever},
    },
    memo::MemoryMemo,
    optimizer::{ClientRequest, OptimizeRequest, Optimizer, hir_cir::into_cir::value_to_logical},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    time::{sleep, timeout},
};
use tracing::instrument;

pub async fn properties(
    args: Vec<Value>,
    _catalog: Arc<dyn Catalog>,
    retriever: Arc<dyn Retriever>,
) -> Value {
    let arg = args[0].clone();
    let group_id = match &arg.data {
        CoreData::Logical(Materializable::Materialized(LogicalOp { group_id, .. })) => {
            group_id.unwrap()
        }
        CoreData::Logical(Materializable::UnMaterialized(group_id)) => *group_id,
        _ => panic!("Expected a logical plan"),
    };

    retriever.get_properties(group_id).await
}
#[instrument(name = "run_demo")]
async fn run_demo() {
    // Compile the HIR.
    let config = Config::new("src/demo/demo.opt".into());

    // Create a properties UDF.
    let properties_udf = Udf {
        func: Arc::new(|args, catalog, retriever| {
            Box::pin(async move { properties(args, catalog, retriever).await })
        }),
    };

    // Create the UDFs HashMap.
    let mut udfs = HashMap::new();
    udfs.insert("properties".to_string(), properties_udf);

    // Compile with the config and UDFs.
    let hir = compile_hir(config, udfs).unwrap();

    // Create necessary components.
    let memo = MemoryMemo::default();
    let catalog = Arc::new(memory_catalog());
    let retriever = Arc::new(MockRetriever::default());
    let engine = Engine::new(hir.context.clone(), catalog.clone(), retriever);

    // Launch engine to retrieve the logical plan.
    let identity: Continuation<Value, Value> = Arc::new(|value| Box::pin(async move { value }));
    let logical_plan = match engine.launch("input", vec![], identity).await {
        EngineResponse::Return(value, _) => value_to_logical(&value),
        _ => panic!("Unexpected engine response"),
    };

    // Start optimizer.
    let optimize_channel = Optimizer::launch(memo, catalog, hir);
    let (tx, mut rx) = mpsc::channel(1);
    optimize_channel
        .send(ClientRequest::Optimize(OptimizeRequest {
            plan: logical_plan.clone(),
            physical_tx: tx.clone(),
        }))
        .await
        .unwrap();

    // Timeout after 5 seconds.
    let timeout_duration = Duration::from_secs(5);
    let result = timeout(timeout_duration, async {
        while let Some(response) = rx.recv().await {
            println!("Received response: {:?}", response);
        }
    })
    .await;

    match result {
        Ok(_) => println!("Finished receiving responses."),
        Err(_) => println!("Timed out after 5 seconds."),
    }

    // Dump the memo (debug utility).
    optimize_channel
        .send(ClientRequest::DumpMemo)
        .await
        .unwrap();
    sleep(Duration::from_secs(10)).await;
}

#[cfg(test)]
mod demo {
    use super::*;
    // TODO Consider remove tracing_subscriber dependency in optd when we have a proper cli.
    #[cfg(feature = "subscriber")]
    use tracing_subscriber::{EnvFilter, fmt};

    #[tokio::test]
    async fn test_optimizer_demo() {
        #[cfg(feature = "subscriber")]
        {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            fmt()
                .with_env_filter(filter)
                .pretty()
                .with_ansi(true)
                .init();
        }
        run_demo().await
    }
}

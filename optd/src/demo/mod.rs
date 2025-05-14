use crate::{
    catalog::iceberg::memory_catalog,
    dsl::{
        analyzer::hir::Value,
        compile::{Config, compile_hir},
        engine::{Continuation, Engine, EngineResponse},
        utils::retriever::MockRetriever,
    },
    memo::MemoryMemo,
    optimizer::{OptimizeRequest, Optimizer, hir_cir::into_cir::value_to_logical},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

async fn run_demo() {
    // Compile the HIR.
    let config = Config::new("src/demo/demo.opt".into());
    let udfs = HashMap::new();
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
        .send(OptimizeRequest {
            plan: logical_plan,
            physical_tx: tx,
        })
        .await
        .unwrap();

    // Timeout after 2 seconds.
    let timeout_duration = Duration::from_secs(2);
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
}

#[cfg(test)]
mod demo {
    use super::*;

    #[tokio::test]
    async fn test_optimizer_demo() {
        run_demo().await
    }
}

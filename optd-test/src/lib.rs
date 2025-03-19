#![cfg(test)]

use futures::channel::mpsc;
use optd_core::{
    memo::Memoize,
    optimizer::{OptimizeRequest, Optimizer},
};

mod hir;
mod mock;
mod rules;
mod utils;

use mock::MockMemo;
use utils::*;

#[tokio::test]
async fn test_simple() {
    let memo = MockMemo::new();
    let hir = hir::generate_simple_commute_hir();

    let mut request_tx = Optimizer::launch(memo, hir);

    let plan = create_join(
        create_filter(create_scan("t1"), vec![]),
        create_scan("t2"),
        vec![],
    );

    let (optimized_plans_tx, optimized_plans_rx) = mpsc::channel(0);

    let new_request = OptimizeRequest {
        logical_plan: plan,
        response_tx: optimized_plans_tx,
    };

    request_tx.start_send(new_request).unwrap();
}

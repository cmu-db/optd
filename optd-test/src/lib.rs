#![cfg(test)]

use optd_core::{memo::Memoize, optimizer::Optimizer};

mod hir;
mod mock;
mod rules;

use mock::MockMemo;

#[test]
fn test_simple() {
    let memo = MockMemo::new();
    let hir = hir::generate_simple_commute_hir();

    let request_tx = Optimizer::launch(memo, hir);

    // TODO: Read in logical plans from DataFusion and send it to the optimizer.

    todo!()
}

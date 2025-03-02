use cascades::memo::Memoize;
use iceberg::Catalog;
use std::sync::Arc;

#[allow(dead_code)]
pub mod cascades;
pub mod catalog;
pub mod cost_model;
pub mod operators;
pub mod plans;
pub mod storage;

#[cfg(test)]
pub(crate) mod test_utils;

/// The `optd` optimizer.
#[derive(Debug)]
pub struct Optimizer<M, C> {
    pub memo: Arc<M>,
    pub catalog: Arc<C>,
}

impl<M, C> Optimizer<M, C>
where
    M: Memoize,
    C: Catalog,
{
    pub fn new(memo: Arc<M>, catalog: Arc<C>) -> Self {
        Self { memo, catalog }
    }
}

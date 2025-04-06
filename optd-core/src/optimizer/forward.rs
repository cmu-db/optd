use crate::{
    error::Error,
    memo::{ForwardResult, Memoize},
};

use super::Optimizer;

impl<M: Memoize> Optimizer<M> {
    pub(super) async fn handle_forward_result(
        &mut self,
        _result: ForwardResult,
    ) -> Result<(), Error> {
        todo!()
    }
}

use optd_core::rel_node::RelNodeMeta;
use pretty_xmlish::Pretty;

use crate::cost::{COMPUTE_COST, IO_COST, ROW_COUNT};

pub trait Insertable<'a> {
    fn with_meta(self, meta: &RelNodeMeta) -> Self;
}

impl<'a> Insertable<'a> for Vec<(&'a str, Pretty<'a>)> {
    // FIXME: this assumes we are using OptCostModel
    fn with_meta(mut self, meta: &RelNodeMeta) -> Self {
        self.push((
            "cost",
            Pretty::display(&format!(
                "weighted={:.2},row_cnt={:.2},compute={:.2},io={:.2}",
                meta.cost.0[0],
                meta.cost.0[ROW_COUNT],
                meta.cost.0[COMPUTE_COST],
                meta.cost.0[IO_COST],
            )),
        ));
        self
    }
}

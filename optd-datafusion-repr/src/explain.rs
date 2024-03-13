use optd_core::rel_node::RelNodeMeta;
use pretty_xmlish::Pretty;

use crate::cost::ROW_COUNT;

pub trait Insertable<'a> {
    fn with_meta(self, meta: &RelNodeMeta) -> Self;
}

impl<'a> Insertable<'a> for Vec<(&'a str, Pretty<'a>)> {
    fn with_meta(mut self, meta: &RelNodeMeta) -> Self {
        self.push(("rows", Pretty::display(&meta.cost.0[ROW_COUNT])));
        self
    }
}

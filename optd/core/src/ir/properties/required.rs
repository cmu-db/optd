use std::sync::Arc;

use crate::ir::{
    Operator,
    context::IRContext,
    properties::{PropertyMarker, TupleOrdering},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Required {
    pub tuple_ordering: TupleOrdering,
}

impl std::fmt::Display for Required {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"tuple_ordering", &self.tuple_ordering)
            .finish()
    }
}

impl PropertyMarker for Arc<Required> {
    type Output = Self;
}

impl crate::ir::properties::TrySatisfy<Arc<Required>> for Operator {
    fn try_satisfy(
        &self,
        property: &Arc<Required>,
        ctx: &IRContext,
    ) -> Option<std::sync::Arc<[Arc<Required>]>> {
        self.try_satisfy(&property.tuple_ordering, ctx)
            .map(|inputs_required| {
                inputs_required
                    .iter()
                    .map(|p| {
                        Arc::new(Required {
                            tuple_ordering: p.clone(),
                        })
                    })
                    .collect()
            })
    }
}

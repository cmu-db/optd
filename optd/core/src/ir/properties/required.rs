use crate::ir::{
    Operator,
    context::IRContext,
    properties::{PropertyMarker, TupleOrdering},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Required {
    tuple_ordering: TupleOrdering,
}

impl PropertyMarker for Required {}

impl crate::ir::properties::TrySatisfy<Required> for Operator {
    fn try_satisfy(
        &self,
        property: &Required,
        ctx: &IRContext,
    ) -> Option<std::sync::Arc<[Required]>> {
        self.try_satisfy(&property.tuple_ordering, ctx)
            .map(|inputs_required| {
                inputs_required
                    .iter()
                    .map(|p| Required {
                        tuple_ordering: p.clone(),
                    })
                    .collect()
            })
    }
}

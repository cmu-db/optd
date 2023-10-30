use crate::{
    plan_nodes::{
        LogicalFilter, LogicalJoin, LogicalScan, OptRelNode, OptRelNodeRef, OptRelNodeTyp,
        PhysicalFilter, PhysicalNestedLoopJoin, PhysicalScan,
    },
    rel_node::{RelNodeTyp, Value},
};

use super::Rule;

pub struct PhysicalConversionRule {}

impl Rule<OptRelNodeTyp> for PhysicalConversionRule {
    fn matches(&self, typ: OptRelNodeTyp, _data: Option<Value>) -> bool {
        typ.is_logical()
    }

    fn apply(&self, input: OptRelNodeRef) -> Vec<OptRelNodeRef> {
        match input.typ {
            OptRelNodeTyp::Join(_) => {
                vec![
                    PhysicalNestedLoopJoin::new(LogicalJoin::from_rel_node(input).unwrap())
                        .into_rel_node(),
                ]
            }
            OptRelNodeTyp::Scan => {
                vec![PhysicalScan::new(LogicalScan::from_rel_node(input).unwrap()).into_rel_node()]
            }
            OptRelNodeTyp::Filter => {
                vec![
                    PhysicalFilter::new(LogicalFilter::from_rel_node(input).unwrap())
                        .into_rel_node(),
                ]
            }
            _ => vec![],
        }
    }

    fn is_impl_rule(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "physical_conversion"
    }
}

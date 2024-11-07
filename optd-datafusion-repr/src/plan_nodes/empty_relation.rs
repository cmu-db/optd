use std::sync::Arc;

use bincode;
use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use super::{
    ArcDfPlanNode, ArcDfPredNode, ConstantPred, DfNodeType, DfPlanNode, DfReprPlanNode,
    DfReprPredNode,
};
use crate::explain::Insertable;
use crate::properties::schema::Schema;

#[derive(Clone, Debug)]
pub struct LogicalEmptyRelation(pub ArcDfPlanNode);

impl DfReprPlanNode for LogicalEmptyRelation {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::EmptyRelation {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, _meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalEmptyRelation",
            vec![("produce_one_row", self.produce_one_row().to_string().into())],
        )
    }
}

impl LogicalEmptyRelation {
    pub fn new(produce_one_row: bool, schema: Schema) -> LogicalEmptyRelation {
        let serialized_data: Arc<[u8]> = bincode::serialize(&schema).unwrap().into_iter().collect();
        LogicalEmptyRelation(
            DfPlanNode {
                typ: DfNodeType::EmptyRelation,
                children: vec![],
                predicates: vec![
                    ConstantPred::bool(produce_one_row).into_pred_node(),
                    ConstantPred::serialized(serialized_data).into_pred_node(),
                ],
            }
            .into(),
        )
    }

    pub fn produce_one_row(&self) -> bool {
        ConstantPred::from_pred_node(self.0.predicates[0].clone())
            .unwrap()
            .value()
            .as_bool()
    }

    pub fn empty_relation_schema(&self) -> Schema {
        decode_empty_relation_schema(&self.0.predicates[1])
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalEmptyRelation(pub ArcDfPlanNode);

impl DfReprPlanNode for PhysicalEmptyRelation {
    fn into_plan_node(self) -> ArcDfPlanNode {
        self.0
    }

    fn from_plan_node(plan_node: ArcDfPlanNode) -> Option<Self> {
        if plan_node.typ != DfNodeType::PhysicalEmptyRelation {
            return None;
        }
        Some(Self(plan_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        let mut fields = vec![("produce_one_row", self.produce_one_row().to_string().into())];
        if let Some(meta_map) = meta_map {
            fields = fields.with_meta(self.0.get_meta(meta_map));
        }
        Pretty::childless_record("PhysicalEmptyRelation", fields)
    }
}

impl PhysicalEmptyRelation {
    pub fn produce_one_row(&self) -> bool {
        ConstantPred::from_pred_node(self.0.predicates[0].clone())
            .unwrap()
            .value()
            .as_bool()
    }

    pub fn empty_relation_schema(&self) -> Schema {
        decode_empty_relation_schema(&self.0.predicates[1])
    }
}

pub fn decode_empty_relation_schema(pred: &ArcDfPredNode) -> Schema {
    let serialized_data = ConstantPred::from_pred_node(pred.clone())
        .unwrap()
        .value()
        .as_slice();
    bincode::deserialize(serialized_data.as_ref()).unwrap()
}

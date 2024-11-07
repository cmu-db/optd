use arrow_schema::DataType;
use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use super::data_type_pred::DataTypePred;
use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct CastPred(pub ArcDfPredNode);

impl CastPred {
    pub fn new(child: ArcDfPredNode, cast_to: DataType) -> Self {
        CastPred(
            DfPredNode {
                typ: DfPredType::Cast,
                children: vec![child, DataTypePred::new(cast_to).into_pred_node()],
                data: None,
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn cast_to(&self) -> DataType {
        DataTypePred::from_pred_node(self.0.child(1))
            .unwrap()
            .data_type()
    }
}

impl DfReprPredNode for CastPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::Cast) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "Cast",
            vec![
                ("cast_to", format!("{}", self.cast_to()).into()),
                ("child", self.child().explain(meta_map)),
            ],
            vec![],
        )
    }
}

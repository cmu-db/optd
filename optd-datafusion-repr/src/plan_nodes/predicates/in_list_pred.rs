use optd_core::nodes::{PlanNodeMetaMap, Value};
use pretty_xmlish::Pretty;

use super::ListPred;
use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

#[derive(Clone, Debug)]
pub struct InListPred(pub ArcDfPredNode);

impl InListPred {
    pub fn new(child: ArcDfPredNode, list: ListPred, negated: bool) -> Self {
        InListPred(
            DfPredNode {
                typ: DfPredType::InList,
                children: vec![child, list.into_pred_node()],
                data: Some(Value::Bool(negated)),
            }
            .into(),
        )
    }

    pub fn child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn list(&self) -> ListPred {
        ListPred::from_pred_node(self.0.child(1)).unwrap()
    }

    /// `true` for `NOT IN`.
    pub fn negated(&self) -> bool {
        self.0.data.as_ref().unwrap().as_bool()
    }
}

impl DfReprPredNode for InListPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::InList) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            "InList",
            vec![
                ("expr", self.child().explain(meta_map)),
                ("list", self.list().explain(meta_map)),
                ("negated", self.negated().to_string().into()),
            ],
            vec![],
        )
    }
}

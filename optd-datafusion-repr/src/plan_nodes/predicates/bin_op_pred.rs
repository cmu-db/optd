use optd_core::nodes::PlanNodeMetaMap;
use pretty_xmlish::Pretty;

use crate::plan_nodes::{ArcDfPredNode, DfPredNode, DfPredType, DfReprPredNode};

/// The pattern of storing numerical, comparison, and logical operators in the same type with is_*()
/// functions     to distinguish between them matches how datafusion::logical_expr::Operator does
/// things I initially thought about splitting BinOpType into three "subenums". However, having two
/// nested levels of     types leads to some really confusing code
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub enum BinOpType {
    // numerical
    Add,
    Sub,
    Mul,
    Div,
    Mod,

    // comparison
    Eq,
    Neq,
    Gt,
    Lt,
    Geq,
    Leq,
}

impl std::fmt::Display for BinOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl BinOpType {
    pub fn is_numerical(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod
        )
    }

    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Geq | Self::Leq
        )
    }
}

#[derive(Clone, Debug)]
pub struct BinOpPred(pub ArcDfPredNode);

impl BinOpPred {
    pub fn new(left: ArcDfPredNode, right: ArcDfPredNode, op_type: BinOpType) -> Self {
        BinOpPred(
            DfPredNode {
                typ: DfPredType::BinOp(op_type),
                children: vec![left, right],
                data: None,
            }
            .into(),
        )
    }

    pub fn left_child(&self) -> ArcDfPredNode {
        self.0.child(0)
    }

    pub fn right_child(&self) -> ArcDfPredNode {
        self.0.child(1)
    }

    pub fn op_type(&self) -> BinOpType {
        if let DfPredType::BinOp(op_type) = self.0.typ {
            op_type
        } else {
            panic!("not a bin op")
        }
    }
}

impl DfReprPredNode for BinOpPred {
    fn into_pred_node(self) -> ArcDfPredNode {
        self.0
    }

    fn from_pred_node(pred_node: ArcDfPredNode) -> Option<Self> {
        if !matches!(pred_node.typ, DfPredType::BinOp(_)) {
            return None;
        }
        Some(Self(pred_node))
    }

    fn explain(&self, meta_map: Option<&PlanNodeMetaMap>) -> Pretty<'static> {
        Pretty::simple_record(
            self.op_type().to_string(),
            vec![],
            vec![
                self.left_child().explain(meta_map),
                self.right_child().explain(meta_map),
            ],
        )
    }
}

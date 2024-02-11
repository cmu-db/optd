use optd_core::rel_node::{RelNode, Value};
use pretty_xmlish::Pretty;

use super::{macros::define_plan_node, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalLimit(pub PlanNode);

impl OptRelNode for LogicalLimit {
    fn into_rel_node(self) -> OptRelNodeRef {
        self.0.into_rel_node()
    }

    fn from_rel_node(rel_node: OptRelNodeRef) -> Option<Self> {
        if rel_node.typ != OptRelNodeTyp::Limit {
            return None;
        }
        PlanNode::from_rel_node(rel_node).map(Self)
    }

    fn dispatch_explain(&self) -> Pretty<'static> {
        Pretty::childless_record(
            "LogicalLimit",
            vec![
                ("skip", self.skip().to_string().into()),
                (
                    "fetch",
                    (if let Some(x) = self.fetch() {
                        x.to_string()
                    } else {
                        "None".to_owned()
                    })
                    .into(),
                ),
            ],
        )
    }
}

impl LogicalLimit {
    pub fn new(skip: i64, fetch: Option<i64>) -> LogicalLimit {
        LogicalLimit(PlanNode(
            RelNode {
                typ: OptRelNodeTyp::EmptyRelation,
                children: vec![],
                data: Some(Value::Pair(
                    Value::Int(skip).into(),
                    Value::Option(
                        (if let Some(x) = fetch {
                            Some(Value::Int(x).into())
                        } else {
                            None
                        }),
                    )
                    .into(),
                )),
            }
            .into(),
        ))
    }

    pub fn skip(&self) -> i64 {
        self.clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_pair()
            .0
            .as_i64()
    }

    pub fn fetch(&self) -> Option<i64> {
        let option = self
            .clone()
            .into_rel_node()
            .data
            .as_ref()
            .unwrap()
            .as_pair()
            .1
            .as_option();

        if let Some(x) = option {
            Some(x.as_i64())
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct PhysicalLimit(pub PlanNode);

define_plan_node!(
    PhysicalLimit : PlanNode,
    Limit, [
        { 0, left: PlanNode }
    ], [
    ]
);

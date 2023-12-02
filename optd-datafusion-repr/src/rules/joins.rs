use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use optd_core::optimizer::Optimizer;
use optd_core::rel_node::RelNode;
use optd_core::rules::{Rule, RuleMatcher};

use super::macros::{define_impl_rule, define_rule};
use crate::plan_nodes::{
    BinOpExpr, BinOpType, ColumnRefExpr, Expr, ExprList, JoinType, LogicalJoin, LogicalProjection,
    OptRelNode, OptRelNodeTyp, PhysicalHashJoin, PlanNode,
};
use crate::properties::schema::SchemaPropertyBuilder;

// A join B -> B join A
define_rule!(
    JoinCommuteRule,
    apply_join_commute,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_join_commute(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinCommuteRulePicks { left, right, cond }: JoinCommuteRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    fn rewrite_column_refs(expr: Expr, left_size: usize, right_size: usize) -> Expr {
        let expr = expr.into_rel_node();
        if let Some(expr) = ColumnRefExpr::from_rel_node(expr.clone()) {
            let index = expr.index();
            if index < left_size {
                return ColumnRefExpr::new(index + right_size).into_expr();
            } else {
                return ColumnRefExpr::new(index - left_size).into_expr();
            }
        }
        let children = expr.children.clone();
        let children = children
            .into_iter()
            .map(|x| {
                rewrite_column_refs(Expr::from_rel_node(x).unwrap(), left_size, right_size)
                    .into_rel_node()
            })
            .collect_vec();
        Expr::from_rel_node(
            RelNode {
                typ: expr.typ.clone(),
                children,
                data: expr.data.clone(),
            }
            .into(),
        )
        .unwrap()
    }

    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
    let cond = rewrite_column_refs(
        Expr::from_rel_node(cond.into()).unwrap(),
        left_schema.len(),
        right_schema.len(),
    );
    let node = LogicalJoin::new(
        PlanNode::from_group(right.into()),
        PlanNode::from_group(left.into()),
        cond,
        JoinType::Inner,
    );
    let mut proj_expr = Vec::with_capacity(left_schema.0.len() + right_schema.0.len());
    for i in 0..left_schema.len() {
        proj_expr.push(ColumnRefExpr::new(right_schema.len() + i).into_expr());
    }
    for i in 0..right_schema.len() {
        proj_expr.push(ColumnRefExpr::new(i).into_expr());
    }
    let node =
        LogicalProjection::new(node.into_plan_node(), ExprList::new(proj_expr)).into_rel_node();
    vec![node.as_ref().clone()]
}

// (A join B) join C -> A join (B join C)
define_rule!(
    JoinAssocRule,
    apply_join_assoc,
    (
        Join(JoinType::Inner),
        (Join(JoinType::Inner), a, b, [cond1]),
        c,
        [cond2]
    )
);

fn apply_join_assoc(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    JoinAssocRulePicks {
        a,
        b,
        c,
        cond1,
        cond2,
    }: JoinAssocRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    fn rewrite_column_refs(expr: Expr, a_size: usize) -> Option<Expr> {
        let expr = expr.into_rel_node();
        if let Some(expr) = ColumnRefExpr::from_rel_node(expr.clone()) {
            let index = expr.index();
            if index < a_size {
                return None;
            } else {
                return Some(ColumnRefExpr::new(index - a_size).into_expr());
            }
        }
        let children = expr.children.clone();
        let children = children
            .into_iter()
            .map(|x| rewrite_column_refs(Expr::from_rel_node(x).unwrap(), a_size))
            .collect::<Option<Vec<_>>>()?;
        Some(
            Expr::from_rel_node(
                RelNode {
                    typ: expr.typ.clone(),
                    children: children
                        .into_iter()
                        .map(|x| x.into_rel_node())
                        .collect_vec(),
                    data: expr.data.clone(),
                }
                .into(),
            )
            .unwrap(),
        )
    }
    let a_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(a.clone()), 0);
    let b_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(b.clone()), 0);
    let c_schema = optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(c.clone()), 0);
    let cond2 = Expr::from_rel_node(cond2.into()).unwrap();
    let Some(cond2) = rewrite_column_refs(cond2, a_schema.len()) else {
        return vec![];
    };
    let node = RelNode {
        typ: OptRelNodeTyp::Join(JoinType::Inner),
        children: vec![
            a.into(),
            RelNode {
                typ: OptRelNodeTyp::Join(JoinType::Inner),
                children: vec![b.into(), c.into(), cond2.into_rel_node()],
                data: None,
            }
            .into(),
            cond1.into(),
        ],
        data: None,
    };
    vec![node]
}

define_impl_rule!(
    HashJoinRule,
    apply_hash_join,
    (Join(JoinType::Inner), left, right, [cond])
);

fn apply_hash_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    HashJoinRulePicks { left, right, cond }: HashJoinRulePicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    if let OptRelNodeTyp::BinOp(BinOpType::Eq) = cond.typ {
        let left_schema =
            optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(left.clone()), 0);
        // let right_schema =
        //     optimizer.get_property::<SchemaPropertyBuilder>(Arc::new(right.clone()), 0);
        let op = BinOpExpr::from_rel_node(Arc::new(cond.clone())).unwrap();
        let left_expr: Expr = op.left_child();
        let right_expr = op.right_child();
        let Some(mut left_expr) = ColumnRefExpr::from_rel_node(left_expr.into_rel_node()) else {
            return vec![];
        };
        let Some(mut right_expr) = ColumnRefExpr::from_rel_node(right_expr.into_rel_node()) else {
            return vec![];
        };
        let can_convert = if left_expr.index() < left_schema.0.len()
            && right_expr.index() >= left_schema.0.len()
        {
            true
        } else if right_expr.index() < left_schema.0.len()
            && left_expr.index() >= left_schema.0.len()
        {
            (left_expr, right_expr) = (right_expr, left_expr);
            true
        } else {
            false
        };

        if can_convert {
            let right_expr = ColumnRefExpr::new(right_expr.index() - left_schema.0.len());
            let node = PhysicalHashJoin::new(
                PlanNode::from_group(left.into()),
                PlanNode::from_group(right.into()),
                ExprList::new(vec![left_expr.into_expr()]),
                ExprList::new(vec![right_expr.into_expr()]),
                JoinType::Inner,
            );
            return vec![node.into_rel_node().as_ref().clone()];
        }
    }
    vec![]
}

// (Proj A) join B -> (Proj (A join B))
define_rule!(
    ProjectionPullUpJoin,
    apply_projection_pull_up_join,
    (
        Join(JoinType::Inner),
        (Projection, left, [list]),
        right,
        [cond]
    )
);

struct ProjectionMapping {
    forward: Vec<usize>,
    backward: Vec<Option<usize>>,
}

impl ProjectionMapping {
    pub fn build(mapping: Vec<usize>) -> Option<Self> {
        let mut backward = vec![];
        for (i, &x) in mapping.iter().enumerate() {
            if x >= backward.len() {
                backward.resize(x + 1, None);
            }
            backward[x] = Some(i);
        }
        Some(Self {
            forward: mapping,
            backward,
        })
    }

    pub fn projection_col_refers_to(&self, col: usize) -> usize {
        self.forward[col]
    }

    pub fn original_col_maps_to(&self, col: usize) -> Option<usize> {
        self.backward[col]
    }
}

fn apply_projection_pull_up_join(
    optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectionPullUpJoinPicks {
        left,
        right,
        list,
        cond,
    }: ProjectionPullUpJoinPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    let list = ExprList::from_rel_node(Arc::new(list)).unwrap();

    fn compute_column_mapping(list: ExprList) -> Option<ProjectionMapping> {
        let mut mapping = vec![];
        for expr in list.to_vec() {
            let col_expr = ColumnRefExpr::from_rel_node(expr.into_rel_node())?;
            mapping.push(col_expr.index());
        }
        ProjectionMapping::build(mapping)
    }

    let Some(mapping) = compute_column_mapping(list.clone()) else {
        return vec![];
    };

    fn rewrite_condition(
        cond: Expr,
        mapping: &ProjectionMapping,
        left_schema_size: usize,
        projection_schema_size: usize,
    ) -> Expr {
        if cond.typ() == OptRelNodeTyp::ColumnRef {
            let col = ColumnRefExpr::from_rel_node(cond.into_rel_node()).unwrap();
            let idx = col.index();
            if idx < projection_schema_size {
                let col = mapping.projection_col_refers_to(col.index());
                return ColumnRefExpr::new(col).into_expr();
            } else {
                let col = col.index();
                return ColumnRefExpr::new(col - projection_schema_size + left_schema_size)
                    .into_expr();
            }
        }
        let expr = cond.into_rel_node();
        let mut children = Vec::with_capacity(expr.children.len());
        for child in &expr.children {
            children.push(
                rewrite_condition(
                    Expr::from_rel_node(child.clone()).unwrap(),
                    mapping,
                    left_schema_size,
                    projection_schema_size,
                )
                .into_rel_node(),
            );
        }
        let expr = Expr::from_rel_node(
            RelNode {
                typ: expr.typ.clone(),
                children,
                data: expr.data.clone(),
            }
            .into(),
        )
        .unwrap();
        expr
    }

    let left = Arc::new(left.clone());
    let right = Arc::new(right.clone());

    // TODO(chi): support capture projection node.
    let projection =
        LogicalProjection::new(PlanNode::from_group(left.clone()), list.clone()).into_rel_node();
    let left_schema = optimizer.get_property::<SchemaPropertyBuilder>(left.clone(), 0);
    let projection_schema = optimizer.get_property::<SchemaPropertyBuilder>(projection.clone(), 0);
    let right_schema = optimizer.get_property::<SchemaPropertyBuilder>(right.clone(), 0);
    let mut new_projection_exprs = list.to_vec();
    for i in 0..right_schema.len() {
        let col: Expr = ColumnRefExpr::new(i + left_schema.len()).into_expr();
        new_projection_exprs.push(col);
    }
    let node = LogicalProjection::new(
        LogicalJoin::new(
            PlanNode::from_group(left),
            PlanNode::from_group(right),
            rewrite_condition(
                Expr::from_rel_node(Arc::new(cond)).unwrap(),
                &mapping,
                left_schema.len(),
                projection_schema.len(),
            ),
            JoinType::Inner,
        )
        .into_plan_node(),
        ExprList::new(new_projection_exprs),
    );
    vec![node.into_rel_node().as_ref().clone()]
}

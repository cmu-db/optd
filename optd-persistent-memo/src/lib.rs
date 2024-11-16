use std::collections::{HashMap, HashSet};

use optd_core::{
    cascades::{self, ExprId, GroupId, Memo, PredId},
    nodes::{self, NodeType},
};
use optd_persistent::{self, BackendManager, MemoStorage, StorageResult};

/// A memo table implementation based on the `optd-persistent` crate storage.
pub struct PersistentMemo<T: NodeType> {
    storage: optd_persistent::BackendManager,

    // TODO: This is a hacky workaround to keep track of which expressions
    // are physical and which are logical, so we know which table to check
    // in the ORM.
    // Obviously this defeats the purpose of the ORM.
    physical_expressions: HashSet<ExprId>,

    // --
    // TODO: Below this: Stuff we need to move into the memotable
    // Storing this stuff defeats the purpose of the ORM.
    // --

    // Predicate stuff.
    pred_id_to_pred_node: HashMap<PredId, nodes::ArcPredNode<T>>,
    pred_node_to_pred_id: HashMap<nodes::ArcPredNode<T>, PredId>,
    expr_group_id_counter: usize,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

type OrmLogicalExpr = optd_persistent::entities::logical_expression::Model;
type OrmPhysicalExpr = optd_persistent::entities::physical_expression::Model;

impl<T: NodeType> PersistentMemo<T> {
    pub fn new(database_url: Option<&str>) -> StorageResult<Self> {
        Ok(PersistentMemo {
            storage: futures_lite::future::block_on(BackendManager::new(database_url))?,
            physical_expressions: HashSet::new(),
            pred_id_to_pred_node: HashMap::new(),
            pred_node_to_pred_id: HashMap::new(),
            expr_group_id_counter: 0,
            merged_group_mapping: HashMap::new(),
            dup_expr_mapping: HashMap::new(),
        })
    }

    fn next_pred_id(&mut self) -> PredId {
        let id = self.expr_group_id_counter;
        self.expr_group_id_counter += 1;
        PredId(id)
    }

    fn node_to_logical_expr(&self, plan_node: nodes::ArcPlanNode<T>) -> OrmLogicalExpr {
        todo!()
    }

    fn node_to_physical_expr(&self, plan_node: nodes::ArcPlanNode<T>) -> OrmPhysicalExpr {
        todo!()
    }
}

impl<T: NodeType> Memo<T> for PersistentMemo<T> {
    fn add_new_expr(&mut self, plan_node: nodes::ArcPlanNode<T>) -> (GroupId, ExprId) {
        if plan_node.typ.is_logical() {
            let logical_expr = self.node_to_logical_expr(plan_node.clone());
            let (g_id, e_id) = futures_lite::future::block_on(self.storage.add_logical_expression(
                logical_expr,
                vec![], // TODO: Unused in both our reference impls
            ))
            .unwrap();

            (
                GroupId(g_id.try_into().unwrap()),
                ExprId(e_id.try_into().unwrap()),
            )
        } else {
            panic!("Inserting physical expressions into new groups is not supported---something went wrong.")
        }
    }

    fn add_expr_to_group(
        &mut self,
        plan_node: nodes::PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        let nodes::PlanNodeOrGroup::PlanNode(plan_node) = plan_node else {
            todo!("group merging");
        };

        if plan_node.typ.is_logical() {
            let logical_expr = self.node_to_logical_expr(plan_node.clone());
            futures_lite::future::block_on(self.storage.add_logical_expression_to_group(
                group_id.0.try_into().unwrap(),
                logical_expr,
                vec![], // TODO: Unused in both our reference impls
            ))
            .unwrap();
            // TODO: add_logical_expression_to_group does not return an expr, though it should
            Some(ExprId(0))
        } else {
            let physical_expr = self.node_to_physical_expr(plan_node.clone());
            futures_lite::future::block_on(self.storage.add_physical_expression_to_group(
                group_id.0.try_into().unwrap(),
                physical_expr,
                vec![], // TODO: Unused in both our reference impls
            ))
            .unwrap();
            // TODO: add_physical_expression_to_group does not return an expr, though it should
            Some(ExprId(0))
        }
    }

    fn add_new_pred(&mut self, pred_node: nodes::ArcPredNode<T>) -> PredId {
        let pred_id = self.next_pred_id();
        if let Some(id) = self.pred_node_to_pred_id.get(&pred_node) {
            return *id;
        }
        self.pred_node_to_pred_id.insert(pred_node.clone(), pred_id);
        self.pred_id_to_pred_node.insert(pred_id, pred_node);
        pred_id
    }

    fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        todo!()
    }

    fn get_expr_memoed(&self, expr_id: ExprId) -> cascades::ArcMemoPlanNode<T> {
        todo!()
    }

    fn get_all_group_ids(&self) -> Vec<GroupId> {
        todo!()
    }

    fn get_group(&self, group_id: GroupId) -> &cascades::Group {
        todo!()
    }

    fn get_pred(&self, pred_id: PredId) -> nodes::ArcPredNode<T> {
        self.pred_id_to_pred_node[&pred_id].clone()
    }

    fn update_group_info(&mut self, group_id: GroupId, group_info: cascades::GroupInfo) {
        todo!()
    }

    fn estimated_plan_space(&self) -> usize {
        todo!()
    }
}

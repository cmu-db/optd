mod backend_manager;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use backend_manager::MemoBackendManager;
use futures_lite::future;
use optd_core::{
    cascades::{self, ArcMemoPlanNode, ExprId, GroupId, GroupInfo, Memo, MemoPlanNode, PredId},
    nodes::{self, ArcPlanNode, NodeType, PlanNodeOrGroup},
};
use optd_persistent::{self, BackendManager, MemoStorage, StorageResult};

/// A memo table implementation based on the `optd-persistent` crate storage.
pub struct PersistentMemo<T: NodeType> {
    storage: MemoBackendManager,

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
    // TODO: Instead of wrapping usize in all of our data structures, consider picking a fixed-size integer type.
    expr_group_id_counter: usize,

    // We update all group IDs in the memo table upon group merging, but
    // there might be edge cases that some tasks still hold the old group ID.
    // In this case, we need this mapping to redirect to the merged group ID.
    merged_group_mapping: HashMap<GroupId, GroupId>,
    dup_expr_mapping: HashMap<ExprId, ExprId>,
}

type OrmLogicalExpr = optd_persistent::entities::logical_expression::Model;
type OrmPhysicalExpr = optd_persistent::entities::physical_expression::Model;
type OrmGroup = optd_persistent::entities::cascades_group::Model;

impl<T: NodeType> PersistentMemo<T> {
    pub fn new(database_url: Option<&str>) -> StorageResult<Self> {
        Ok(PersistentMemo {
            storage: future::block_on(MemoBackendManager::new(database_url))?,
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

    fn reduce_group(&self, group_id: GroupId) -> GroupId {
        self.merged_group_mapping[&group_id]
    }

    fn add_new_group_expr_inner(
        &mut self,
        plan_node: ArcPlanNode<T>,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<(GroupId, ExprId)> {
        // Get children group IDs
        let children_group_ids = plan_node
            .children
            .iter()
            .map(|child| {
                match child {
                    // TODO: can I remove reduce?
                    PlanNodeOrGroup::Group(group) => self.reduce_group(*group),
                    PlanNodeOrGroup::PlanNode(child) => {
                        // No merge / modification to the memo should occur for the following
                        // operation
                        let (group, _) = self
                            .add_new_group_expr_inner(child.clone(), None)
                            .expect("should not trigger merge group");
                        self.reduce_group(group) // TODO: can I remove?
                    }
                }
            })
            .map(|x| x.0.try_into().unwrap())
            .collect::<Vec<_>>();

        let typ_id = 0; // TODO: typ to i16
        let is_logical = plan_node.typ.is_logical();

        // If expression is already stored
        if let Some((group_id, expr_id)) =
            future::block_on(self.storage.lookup_expr(typ_id, &children_group_ids)).unwrap()
        {
            if let Some(add_to_group_id) = add_to_group_id {
                let add_to_group_id = self.reduce_group(add_to_group_id);
                // TODO: support for group merging
                // self.merge_group_inner(add_to_group_id, group_id);
                return Ok((add_to_group_id, ExprId(expr_id.try_into().unwrap())));
            }
            return Ok((
                GroupId(group_id.try_into().unwrap()),
                ExprId(expr_id.try_into().unwrap()),
            ));
        }

        let res = future::block_on(self.storage.insert_expr(
            typ_id,
            &children_group_ids,
            add_to_group_id.map(|x| x.0.try_into().unwrap()),
        ))
        .unwrap();

        Ok((
            GroupId(res.0.try_into().unwrap()),
            ExprId(res.1.try_into().unwrap()),
        ))
    }
}

impl<T: NodeType> Memo<T> for PersistentMemo<T> {
    fn add_new_expr(&mut self, plan_node: nodes::ArcPlanNode<T>) -> (GroupId, ExprId) {
        let (group_id, expr_id) = self
            .add_new_group_expr_inner(plan_node, None)
            .expect("should not trigger merge group");
        (group_id, expr_id)
    }

    fn add_expr_to_group(
        &mut self,
        plan_node: nodes::PlanNodeOrGroup<T>,
        group_id: GroupId,
    ) -> Option<ExprId> {
        match plan_node {
            PlanNodeOrGroup::Group(input_group) => {
                let input_group = self.reduce_group(input_group);
                let group_id = self.reduce_group(group_id);
                // TODO: Group merging
                // self.merge_group_inner(input_group, group_id);
                None
            }
            PlanNodeOrGroup::PlanNode(plan_node) => {
                let reduced_group_id = self.reduce_group(group_id);
                let (returned_group_id, expr_id) = self
                    .add_new_group_expr_inner(plan_node, Some(reduced_group_id))
                    .unwrap();
                assert_eq!(returned_group_id, reduced_group_id);
                Some(expr_id)
            }
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
        GroupId(
            future::block_on(self.storage.get_expr_by_id(expr_id.0.try_into().unwrap()))
                .unwrap()
                .unwrap()
                .0
                .try_into()
                .unwrap(),
        )
    }

    fn get_expr_memoed(&self, mut expr_id: ExprId) -> cascades::ArcMemoPlanNode<T> {
        while let Some(new_expr_id) = self.dup_expr_mapping.get(&expr_id) {
            expr_id = *new_expr_id;
        }

        let expr_id = expr_id.0.try_into().unwrap();

        let (_, typ_id, children_group_ids) =
            future::block_on(self.storage.get_expr_by_id(expr_id))
                .unwrap()
                .expect("expr not found in database");

        MemoPlanNode {
            typ: T::from_i16(typ_id), // TODO: from_i16
            children: children_group_ids
                .iter()
                .map(|group_id| GroupId(*group_id.try_into().unwrap()))
                .collect(),
            predicates: vec![], // TODO
        }
    }

    fn get_all_group_ids(&self) -> Vec<GroupId> {
        future::block_on(self.storage.get_all_group_ids())
            .unwrap()
            .iter()
            .map(|id| GroupId((*id).try_into().unwrap()))
            .collect()
    }

    fn get_group(&self, group_id: GroupId) -> &cascades::Group {
        let g_id = group_id.0.try_into().unwrap();
        let orm_group = future::block_on(self.storage.get_group(g_id)).unwrap();

        let new_group = cascades::Group {
            group_exprs: orm_group
                .group_exprs
                .iter()
                .map(|expr_id| ExprId((*expr_id).try_into().unwrap()))
                .collect(),
            info: GroupInfo {
                winner: cascades::Winner::Unknown, // TODO
            },
            properties: Arc::new([]), // TODO
        };
        Box::leak(Box::new(new_group)) // TODO: Memo table trait should probably return Arcs?
    }

    fn get_pred(&self, pred_id: PredId) -> nodes::ArcPredNode<T> {
        self.pred_id_to_pred_node[&pred_id].clone()
    }

    fn update_group_info(&mut self, group_id: GroupId, group_info: cascades::GroupInfo) {
        // TODO: This might require a bigger redesign
        todo!()
    }

    fn estimated_plan_space(&self) -> usize {
        future::block_on(self.storage.get_expr_count()).unwrap() as usize
    }
}

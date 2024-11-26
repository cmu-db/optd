mod backend_manager;

use core::panic;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use backend_manager::{BackendWinnerInfo, MemoBackendManager, PredicateData};
use futures_lite::future;
use optd_core::{
    cascades::{self, ExprId, GroupId, GroupInfo, Memo, MemoPlanNode, PredId, WinnerInfo},
    cost::Statistics,
    nodes::{
        self, ArcPlanNode, NodeType, PlanNodeOrGroup, PredNode, SerializedNodeTag,
        SerializedPredTag,
    },
};
use optd_persistent::{self, entities::predicate, StorageResult};

/// A memo table implementation based on the `optd-persistent` crate storage.
pub struct PersistentMemo<T: NodeType> {
    /// Struct that allows access the ORM
    storage: MemoBackendManager,

    /// Allows us to conveniently tie the PersistentMemo to NodeType
    _p: PhantomData<T>,

    // TODO: This is an in-memory solution for mapping logical/physical expression IDs to
    // the actual expression ID. Needs to be redesigned for persistence.
    log_id_to_expr_id: HashMap<i32, ExprId>,
    phys_id_to_expr_id: HashMap<i32, ExprId>,
    expr_id_to_log_phys_id: HashMap<ExprId, (i32, bool)>,

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
            _p: PhantomData,
            expr_group_id_counter: 0,
            merged_group_mapping: HashMap::new(),
            dup_expr_mapping: HashMap::new(),
            log_id_to_expr_id: HashMap::new(),
            phys_id_to_expr_id: HashMap::new(),
            expr_id_to_log_phys_id: HashMap::new(),
        })
    }

    fn next_id(&mut self) -> usize {
        let id = self.expr_group_id_counter;
        self.expr_group_id_counter += 1;
        id
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

        let typ_id = plan_node.typ.clone().into().0 as i16;
        let is_logical = plan_node.typ.is_logical();

        // If expression is already stored
        if let Some((group_id, expr_id)) = future::block_on(self.storage.lookup_expr(
            typ_id,
            is_logical,
            &children_group_ids,
        ))
        .unwrap()
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
            is_logical,
            &children_group_ids,
            add_to_group_id.map(|x| x.0.try_into().unwrap()),
        ))
        .unwrap();

        let predicate_ids = plan_node
            .predicates
            .iter()
            .map(|x| self.add_new_pred(x.clone()).0.try_into().unwrap())
            .collect::<Vec<i32>>();

        let unified_expr_id: ExprId = if is_logical {
            if let None = self.log_id_to_expr_id.get(&res.1) {
                let expr_id = ExprId(self.next_id());
                self.log_id_to_expr_id.insert(res.1, expr_id);
                future::block_on(
                    self.storage
                        .link_logical_expr_to_predicates(res.1, &predicate_ids),
                )
                .unwrap();
            }
            self.log_id_to_expr_id.get(&res.1).unwrap().clone()
        } else {
            if let None = self.phys_id_to_expr_id.get(&res.1) {
                future::block_on(
                    self.storage
                        .link_physical_expr_to_predicates(res.1, &predicate_ids),
                )
                .unwrap();
                let expr_id = ExprId(self.next_id());
                self.phys_id_to_expr_id.insert(res.1, expr_id);
            }
            self.phys_id_to_expr_id.get(&res.1).unwrap().clone()
        };

        Ok((GroupId(res.0.try_into().unwrap()), unified_expr_id))
    }

    fn add_new_pred_inner(&mut self, pred_node: nodes::ArcPredNode<T>) -> PredId {
        let mut children_pred_ids: Vec<i32> = Vec::with_capacity(pred_node.children.len());
        for child in pred_node.children.iter() {
            let id = self.add_new_pred_inner(child.clone());
            children_pred_ids.push(id.0.try_into().unwrap());
        }

        let typ_id = pred_node.typ.clone().into().0 as i32;
        let data = serde_json::to_value(backend_manager::PredicateData {
            children_ids: children_pred_ids.clone(),
            data: pred_node.data.clone(),
        })
        .unwrap();

        let pred_id = if let Some(id) =
            future::block_on(self.storage.lookup_pred(typ_id, data.clone())).unwrap()
        {
            id
        } else {
            future::block_on(self.storage.insert_pred(typ_id, data)).unwrap()
        };

        future::block_on(
            self.storage
                .insert_pred_children(pred_id, &children_pred_ids),
        )
        .unwrap();

        PredId(pred_id.try_into().unwrap())
    }

    fn get_pred_inner(&self, pred_id: i32) -> nodes::ArcPredNode<T> {
        let pred = future::block_on(self.storage.get_pred(pred_id))
            .unwrap()
            .expect("pred should exist");

        let Ok(typ) = T::PredType::try_from(SerializedPredTag(pred.variant as u16)) else {
            panic!("invalid node type");
        };
        let PredicateData { children_ids, data } = serde_json::from_value(pred.data).unwrap();
        let mut children = Vec::with_capacity(children_ids.len());
        for child_id in children_ids {
            children.push(self.get_pred_inner(child_id));
        }

        Arc::new(PredNode {
            typ,
            children,
            data,
        })
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
        let pred_id = self.add_new_pred_inner(pred_node);
        pred_id
    }

    fn get_group_id(&self, expr_id: ExprId) -> GroupId {
        let (expr_id, is_logical) = self
            .expr_id_to_log_phys_id
            .get(&expr_id)
            .expect("expr id was not found in expr->logphys mapping");
        GroupId(
            future::block_on(self.storage.get_expr_by_id(*expr_id, *is_logical))
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

        let (expr_id, is_logical) = self
            .expr_id_to_log_phys_id
            .get(&expr_id)
            .expect("expr id was not found in expr->logphys mapping");

        let (_, typ_id, children_group_ids) =
            future::block_on(self.storage.get_expr_by_id(*expr_id, *is_logical))
                .unwrap()
                .expect("expr not found in database");

        let Ok(typ) = T::try_from(SerializedNodeTag(typ_id as u16)) else {
            panic!("invalid node type");
        };

        // TODO: The order of predicates are not preserved.
        let predicates = future::block_on(self.storage.get_pred_ids_in_expr(*expr_id, *is_logical))
            .unwrap()
            .iter()
            .map(|&id| PredId(id.try_into().unwrap()))
            .collect();

        MemoPlanNode {
            typ: typ,
            children: children_group_ids
                .iter()
                .map(|group_id| GroupId((*group_id).try_into().unwrap()))
                .collect(),
            predicates,
        }
        .into()
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
                winner: match orm_group.winner {
                    Some(winner) => cascades::Winner::Full(WinnerInfo {
                        expr_id: ExprId(self.phys_id_to_expr_id[&winner.physical_expr_id].0),
                        total_weighted_cost: winner.total_weighted_cost,
                        operation_weighted_cost: winner.operation_weighted_cost,
                        total_cost: winner.total_cost,
                        operation_cost: winner.operation_cost,
                        statistics: todo!(),
                    }),
                    None => cascades::Winner::Unknown,
                },
            },
            properties: Arc::new([]), // TODO
        };
        Box::leak(Box::new(new_group)) // TODO: Memo table trait should probably return Arcs?
    }

    fn get_pred(&self, pred_id: PredId) -> nodes::ArcPredNode<T> {
        let pred_id = pred_id.0.try_into().unwrap();
        self.get_pred_inner(pred_id)
    }

    fn update_group_info(&mut self, group_id: GroupId, group_info: cascades::GroupInfo) {
        // TODO: What of in_progress, is_optimized

        match group_info.winner {
            cascades::Winner::Unknown => {}
            cascades::Winner::Impossible => {
                panic!("Impossible winner not supported in persistent memo yet");
            }
            cascades::Winner::Full(mut info) => {
                let phys_expr_id = self
                    .expr_id_to_log_phys_id
                    .get(&info.expr_id)
                    .expect("winner expr id not found in expr->logphys mapping")
                    .0;

                let backend_info = BackendWinnerInfo {
                    physical_expr_id: phys_expr_id,
                    total_weighted_cost: info.total_weighted_cost,
                    operation_weighted_cost: info.operation_weighted_cost,
                    total_cost: info.total_cost,
                    operation_cost: info.operation_cost,
                    // TODO: Statistics
                };

                let group_id = group_id.0.try_into().unwrap();
                future::block_on(self.storage.update_winner(group_id, Some(backend_info))).unwrap();
            }
        }
    }

    fn estimated_plan_space(&self) -> usize {
        future::block_on(self.storage.get_expr_count()).unwrap() as usize
    }
}

use std::{collections::HashMap, sync::Arc};

use itertools::Itertools;
use tokio::sync::watch;
use tracing::{info, instrument, trace};

use crate::{
    ir::{
        Group, GroupId, IRCommon, IRContext, Operator,
        convert::IntoOperator,
        properties::{OperatorProperties, Required, TrySatisfy},
        rule::{OperatorPattern, Rule, RuleSet},
    },
    memo::{CostedExpr, Exploration, MemoGroupExpr, MemoTable, Optimization, Status, WithId},
    rules::EnforceTupleOrderingRule,
};

pub struct Cascades {
    pub memo: tokio::sync::RwLock<MemoTable>,
    pub ctx: IRContext,
    pub rule_set: RuleSet,
}

impl Cascades {
    pub fn new(ctx: IRContext, rule_set: RuleSet) -> Self {
        Self {
            memo: tokio::sync::RwLock::new(MemoTable::new(ctx.clone())),
            ctx,
            rule_set,
        }
    }

    pub async fn optimize(
        self: &Arc<Self>,
        plan: &Arc<Operator>,
        required: Arc<Required>,
    ) -> Option<Arc<Operator>> {
        let group_id = self.insert_new_operator(plan).await;
        let fut = self.find_best_costed_expr_for(group_id, required);
        let rx = fut.await;
        let best_root = {
            rx.borrow()
                .costed_exprs
                .iter()
                .min_by(|x, y| x.total_cost.as_f64().total_cmp(&y.total_cost.as_f64()))
                .cloned()
        }?;

        let properties = {
            let reader = self.memo.read().await;
            reader
                .get_memo_group(&group_id)
                .exploration
                .borrow()
                .properties
                .clone()
        };

        let best_plan = self
            .extract_best_group_expr(&best_root, group_id, properties)
            .await?;
        Some(best_plan)
    }

    fn extract_best_group_expr(
        self: &Arc<Self>,
        best_root: &CostedExpr,
        group_id: GroupId,
        properties: Arc<OperatorProperties>,
    ) -> impl Future<Output = Option<Arc<Operator>>> + Send {
        Box::pin(async move {
            let expr = best_root.group_expr.key();

            let input_groups = expr.input_operators();
            let mut input_operators = Vec::with_capacity(input_groups.len());
            for (group_id, (required, index)) in
                input_groups.iter().zip(best_root.input_requirements.iter())
            {
                let rx = self
                    .find_best_costed_expr_for(*group_id, required.clone())
                    .await;

                let properties = {
                    let reader = self.memo.read().await;
                    reader
                        .get_memo_group(group_id)
                        .exploration
                        .borrow()
                        .properties
                        .clone()
                };

                let best_costed = rx.borrow().costed_exprs.get(*index).cloned()?;

                let input_op = self
                    .extract_best_group_expr(&best_costed, *group_id, properties)
                    .await?;
                input_operators.push(input_op);
            }
            let input_scalars = {
                let reader = self.memo.read().await;
                expr.input_scalars()
                    .iter()
                    .map(|id| reader.get_scalar(id).unwrap())
                    .collect()
            };

            let common =
                IRCommon::new_with_properties(input_operators.into(), input_scalars, properties);
            Some(Arc::new(Operator::from_raw_parts(
                Some(group_id),
                expr.kind().clone(),
                common,
            )))
        })
    }

    async fn insert_new_operator(self: &Arc<Self>, plan: &Arc<Operator>) -> GroupId {
        let mut writer = self.memo.write().await;
        writer
            .insert_new_operator(plan.clone())
            .unwrap_or_else(|group_id| group_id)
    }
}

// Optimization.
impl Cascades {
    async fn find_best_costed_expr_for(
        self: &Arc<Self>,
        group_id: GroupId,
        required: Arc<Required>,
    ) -> watch::Receiver<Optimization> {
        let cascades = self.clone();
        let mut rx = cascades
            .spawn_optimize_group(group_id, required.clone())
            .await;

        loop {
            {
                let res = rx.wait_for(|state| state.status == Status::Complete).await;
                if res.is_ok() {
                    break;
                }
            }
            rx = self
                .clone()
                .spawn_optimize_group(group_id, required.clone())
                .await;
        }
        rx
    }

    #[instrument(name = "enforce", skip_all)]
    async fn explore_enforcers(
        &self,
        group_id: GroupId,
        required: &Arc<Required>,
        properties: &Arc<OperatorProperties>,
    ) {
        let group = Group::new(group_id, properties.clone()).into_operator();
        let enforcer_rule = EnforceTupleOrderingRule::new(required.tuple_ordering.clone());
        let new_enforcers = enforcer_rule.transform(&group, &self.ctx).unwrap();
        let total_produced = new_enforcers.len();
        let mut newly_produced = 0;
        {
            let mut writer = self.memo.write().await;
            for op in new_enforcers {
                if writer.insert_operator_into_group(op, group_id).is_ok() {
                    newly_produced += 1;
                }
            }
        }
        info!(
            rule = enforcer_rule.name(),
            total_produced, newly_produced, "applied"
        )
    }

    // clippy: the compiler cannot derive `Send` bounds when using `async fn`. (alternative: pinbox.)
    #[allow(clippy::manual_async_fn)]
    fn spawn_optimize_group(
        self: Arc<Self>,
        group_id: GroupId,
        required: Arc<Required>,
    ) -> impl Future<Output = watch::Receiver<Optimization>> + Send {
        async move {
            let (tx, not_started) = {
                let mut writer = self.memo.write().await;
                let group = writer.get_memo_group_mut(&group_id);
                let tx = group
                    .optimizations
                    .entry(required.clone())
                    .or_insert(watch::Sender::default())
                    .clone();
                drop(writer);
                let not_started = tx.send_if_modified(|state| {
                    let not_started = state.status == Status::NotStarted;
                    not_started.then(|| state.status = Status::InProgress);
                    not_started
                });
                (tx, not_started)
            };
            let rx = tx.subscribe();
            if not_started {
                let required = required.clone();
                tokio::spawn(
                    async move { Box::pin(self.optimize_group(group_id, required, tx)).await },
                );
            }
            rx
        }
    }

    #[instrument(parent = None, skip(self, required, tx), fields(required = %required))]
    async fn optimize_group(
        self: Arc<Self>,
        group_id: GroupId,
        required: Arc<Required>,
        tx: watch::Sender<Optimization>,
    ) {
        let mut rx = self.clone().spawn_explore_group(group_id).await;
        let properties = rx.borrow().properties.clone();
        self.explore_enforcers(group_id, &required, &properties)
            .await;
        let mut index = 0;
        loop {
            let next_expr = {
                let Ok(x) = rx
                    .wait_for(|x| index < x.exprs.len() || x.status == Status::Complete)
                    .await
                else {
                    return;
                };

                let Some(next_expr) = x.exprs.get(index).cloned() else {
                    break;
                };
                next_expr
            };

            if let Some(costed) = self
                .optimize_expr(group_id, &required, next_expr, &properties)
                .await
            {
                tx.send_if_modified(|x| {
                    x.costed_exprs.push(costed);
                    false
                });
            }
            index += 1;
        }

        tx.send_if_modified(|state| {
            let in_progress = state.status == Status::InProgress;
            if in_progress {
                state.status = Status::Complete;
            }
            in_progress
        });
        info!("optimized");
    }

    #[instrument(name = "expr", skip_all, fields(id = %expr.id()))]
    async fn optimize_expr(
        self: &Arc<Self>,
        group_id: GroupId,
        required: &Arc<Required>,
        expr: WithId<Arc<MemoGroupExpr>>,
        properties: &Arc<OperatorProperties>,
    ) -> Option<CostedExpr> {
        let operator = {
            let reader = self.memo.read().await;
            reader.get_operator_one_level(expr.key(), properties.clone(), group_id)
        };
        let op_cost = self.ctx.cm.compute_operator_cost(&operator, &self.ctx)?;

        let inputs_required = operator.try_satisfy(required, &self.ctx)?;

        let mut best_inputs = Vec::with_capacity(operator.input_operators().len());
        let mut best_input_costs = Vec::with_capacity(operator.input_operators().len());
        for (input_group_id, input_required) in expr
            .key()
            .input_operators()
            .iter()
            .zip(inputs_required.iter())
        {
            if input_group_id.eq(&group_id) && input_required == required {
                trace!("self optimization avoided");
                return None;
            }

            let rx = self
                .clone()
                .find_best_costed_expr_for(*input_group_id, input_required.clone())
                .await;
            let state = rx.borrow();
            let (index, costed_expr) = state
                .costed_exprs
                .iter()
                .enumerate()
                .min_by(|(_, x), (_, y)| x.total_cost.as_f64().total_cmp(&y.total_cost.as_f64()))?;
            best_inputs.push((input_required.clone(), index));
            best_input_costs.push(costed_expr.total_cost);
        }

        let total_cost =
            self.ctx
                .cm
                .compute_total_with_input_costs(&operator, &best_input_costs, &self.ctx)?;
        info!(%op_cost, %total_cost, "optimized");
        Some(CostedExpr::new(
            expr,
            op_cost,
            total_cost,
            best_inputs.into(),
        ))
    }
}

// Exploration.
impl Cascades {
    async fn get_all_group_exprs_in(
        self: &Arc<Self>,
        group_id: GroupId,
    ) -> watch::Receiver<Exploration> {
        let mut rx = self.clone().spawn_explore_group(group_id).await;
        loop {
            {
                let res = rx.wait_for(|state| state.status == Status::Complete).await;
                if res.is_ok() {
                    break;
                }
            }
            rx = self.clone().spawn_explore_group(group_id).await;
        }
        rx
    }

    // clippy: the compiler cannot derive `Send` bounds when using `async fn`. (alternative: pinbox.)
    #[allow(clippy::manual_async_fn)]
    fn spawn_explore_group(
        self: Arc<Self>,
        group_id: GroupId,
    ) -> impl Future<Output = watch::Receiver<Exploration>> + Send {
        async move {
            let (tx, not_started) = {
                let reader = self.memo.read().await;
                let tx = &reader.get_memo_group(&group_id).exploration.clone();
                let not_started = tx.send_if_modified(|state| {
                    let not_started = state.status == Status::NotStarted;
                    not_started.then(|| state.status = Status::InProgress);
                    not_started
                });
                (tx.clone(), not_started)
            };
            let rx = tx.subscribe();

            if not_started {
                tokio::spawn(async move { Box::pin(self.explore_group(group_id, tx)).await });
            }
            rx
        }
    }

    #[instrument(parent = None, skip(self, tx))]
    async fn explore_group(self: Arc<Self>, group_id: GroupId, tx: watch::Sender<Exploration>) {
        let properties = tx.borrow().properties.clone();
        let mut index = 0;

        loop {
            let next_expr = {
                let state = tx.borrow();
                if state.status == Status::Obsolete {
                    return;
                }
                let Some(expr) = state.exprs.get(index).cloned() else {
                    break;
                };
                expr
            };

            self.explore_expr(group_id, next_expr, &properties).await;
            // increment the counter.
            index += 1;
        }

        // Exploration is complete.
        tx.send_if_modified(|state| {
            let in_progress = state.status == Status::InProgress;
            if in_progress {
                state.status = Status::Complete;
            }
            in_progress
        });
        info!("explored");
    }

    #[instrument(name = "expr", skip_all, fields(id = %expr.id()))]
    async fn explore_expr(
        self: &Arc<Self>,
        group_id: GroupId,
        expr: WithId<Arc<MemoGroupExpr>>,
        properties: &Arc<OperatorProperties>,
    ) {
        for rule in self.rule_set.iter() {
            let all_bindings = self
                .explore_all_bindings(group_id, &expr, properties, rule.pattern())
                .await;

            let bindings_count = all_bindings.as_ref().map(|v| v.len()).unwrap_or(0);
            info!(rule = rule.name(), %bindings_count, "matched");

            if bindings_count > 0 {
                let mut total_produced = 0;
                let mut newly_produced = 0;
                for binding in all_bindings.iter().flatten() {
                    let new_operators = rule.transform(binding, &self.ctx).unwrap();
                    total_produced += new_operators.len();
                    {
                        let mut writer = self.memo.write().await;
                        for op in new_operators {
                            if writer
                                .insert_operator_into_group(op.clone(), group_id)
                                .is_ok()
                            {
                                newly_produced += 1;
                            }
                        }
                    }
                }
                info!(
                    rule = rule.name(),
                    total_produced, newly_produced, "applied"
                )
            }
        }
    }

    #[instrument(skip_all, fields(top = %expr.id()))]
    async fn explore_all_bindings(
        self: &Arc<Self>,
        group_id: GroupId,
        expr: &WithId<Arc<MemoGroupExpr>>,
        properties: &Arc<OperatorProperties>,
        pattern: &OperatorPattern,
    ) -> Option<Vec<Arc<Operator>>> {
        let input_group_ids = expr.key().input_operators();
        let input_scalars = expr.key().input_scalars();
        if !pattern.top_matches(expr.key().kind()) {
            return None;
        }
        let input_patterns = pattern.input_operator_patterns();
        let mut input_bindings_map: HashMap<usize, Vec<_>> =
            HashMap::with_capacity(input_patterns.len());
        for (index, input_pattern) in input_patterns {
            // early return if input length mismatch.
            let input_group_id = input_group_ids.get(*index)?;

            let rx = self.get_all_group_exprs_in(*input_group_id).await;
            let input_exprs = rx.borrow().exprs.clone();
            let input_properties = rx.borrow().properties.clone();
            assert_eq!(rx.borrow().status, Status::Complete);

            let input_bindings = input_bindings_map.entry(*index).or_default();
            for input_expr in input_exprs {
                let cascades = self.clone();
                let fut = Box::pin(cascades.explore_all_bindings(
                    *input_group_id,
                    &input_expr,
                    &input_properties,
                    input_pattern,
                ));
                if let Some(input_bindings_from_expr) = fut.await {
                    input_bindings.extend(input_bindings_from_expr);
                }
            }
            if input_bindings.is_empty() {
                return None;
            }
        }

        let (input_choices, input_scalars) = {
            let reader = self.memo.read().await;
            let input_choices = input_group_ids
                .iter()
                .enumerate()
                .map(|(i, input_group_id)| {
                    match input_bindings_map.remove(&i) {
                        Some(v) => v,
                        None => {
                            // get group

                            let properties = reader
                                .get_memo_group(input_group_id)
                                .exploration
                                .borrow()
                                .properties
                                .clone();
                            vec![Group::new(*input_group_id, properties).into_operator()]
                        }
                    }
                })
                .collect_vec();
            let input_scalars = input_scalars
                .iter()
                .map(|group_id| reader.get_scalar(group_id).unwrap())
                .collect::<Arc<[_]>>();
            (input_choices, input_scalars)
        };

        Some(
            input_choices
                .into_iter()
                .multi_cartesian_product()
                .map(|input_operators| {
                    Arc::new(Operator::from_raw_parts(
                        Some(group_id),
                        expr.key().kind().clone(),
                        IRCommon::new_with_properties(
                            input_operators.into(),
                            input_scalars.clone(),
                            properties.clone(),
                        ),
                    ))
                })
                .collect_vec(),
        )
    }
}

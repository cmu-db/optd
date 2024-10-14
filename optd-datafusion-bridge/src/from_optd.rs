use std::{collections::HashMap, sync::Arc};

use anyhow::{bail, Context, Result};
use async_recursion::async_recursion;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    datasource::source_as_provider,
    logical_expr::Operator,
    physical_expr,
    physical_plan::{
        self,
        aggregates::AggregateMode,
        expressions::create_aggregate_expr,
        joins::{
            utils::{ColumnIndex, JoinFilter},
            CrossJoinExec, PartitionMode,
        },
        projection::ProjectionExec,
        AggregateExpr, ExecutionPlan, PhysicalExpr,
    },
    scalar::ScalarValue,
};
use optd_core::rel_node::RelNodeMetaMap;
use optd_datafusion_repr::{
    plan_nodes::{
        BinOpType, ConstantType, Expr, FuncType, JoinType, LogOpType, OptRelNode, OptRelNodeRef,
        OptRelNodeTyp, PhysicalAgg, PhysicalBetweenExpr, PhysicalBinOpExpr, PhysicalCastExpr,
        PhysicalColumnRefExpr, PhysicalConstantExpr, PhysicalEmptyRelation, PhysicalExprList,
        PhysicalFilter, PhysicalFuncExpr, PhysicalHashJoin, PhysicalInListExpr, PhysicalLikeExpr,
        PhysicalLimit, PhysicalLogOpExpr, PhysicalNestedLoopJoin, PhysicalProjection, PhysicalScan,
        PhysicalSort, PhysicalSortOrderExpr, PlanNode, SortOrderType,
    },
    properties::schema::Schema as OptdSchema,
};

use crate::{physical_collector::CollectorExec, OptdPlanContext};

fn from_optd_schema(optd_schema: OptdSchema) -> Schema {
    let match_type = |typ: &ConstantType| typ.into_data_type();
    let mut fields = Vec::with_capacity(optd_schema.len());
    for field in optd_schema.fields {
        fields.push(Field::new(
            field.name,
            match_type(&field.typ),
            field.nullable,
        ));
    }
    Schema::new(fields)
}

impl OptdPlanContext<'_> {
    #[async_recursion]
    async fn conv_from_optd_table_scan(
        &mut self,
        node: PhysicalScan,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let source = self.tables.get(node.table().as_ref()).unwrap();
        let provider = source_as_provider(source)?;
        let plan = provider.scan(self.session_state, None, &[], None).await?;
        Ok(plan)
    }

    fn conv_from_optd_sort_order_expr(
        &mut self,
        sort_expr: PhysicalSortOrderExpr,
        context: &SchemaRef,
    ) -> Result<physical_expr::PhysicalSortExpr> {
        let expr = Self::conv_from_optd_expr(sort_expr.child(), context)?;
        Ok(physical_expr::PhysicalSortExpr {
            expr,
            options: match sort_expr.order() {
                SortOrderType::Asc => datafusion::arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                SortOrderType::Desc => datafusion::arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
        })
    }

    fn conv_from_optd_agg_expr(
        &mut self,
        expr: Expr,
        context: &SchemaRef,
    ) -> Result<Arc<dyn AggregateExpr>> {
        let expr = PhysicalFuncExpr::from_rel_node(expr.into_rel_node()).unwrap();
        let typ = expr.func();
        let FuncType::Agg(func) = typ else {
            unreachable!()
        };
        let args = expr
            .children()
            .to_vec()
            .into_iter()
            .map(|expr| Self::conv_from_optd_expr(expr, context))
            .collect::<Result<Vec<_>>>()?;
        Ok(create_aggregate_expr(
            &func,
            false,
            &args,
            &[],
            context,
            "<agg_func>",
        )?)
    }

    fn conv_from_optd_expr(expr: Expr, context: &SchemaRef) -> Result<Arc<dyn PhysicalExpr>> {
        match expr.typ() {
            OptRelNodeTyp::PhysicalColumnRef => {
                let expr = PhysicalColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let idx = expr.index();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Column::new("<expr>", idx),
                ))
            }
            OptRelNodeTyp::PhysicalConstant(typ) => {
                let expr = PhysicalConstantExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let value = expr.value();
                let value = match typ {
                    ConstantType::Bool => ScalarValue::Boolean(Some(value.as_bool())),
                    ConstantType::UInt8 => ScalarValue::UInt8(Some(value.as_u8())),
                    ConstantType::UInt16 => ScalarValue::UInt16(Some(value.as_u16())),
                    ConstantType::UInt32 => ScalarValue::UInt32(Some(value.as_u32())),
                    ConstantType::UInt64 => ScalarValue::UInt64(Some(value.as_u64())),
                    ConstantType::Int8 => ScalarValue::Int8(Some(value.as_i8())),
                    ConstantType::Int16 => ScalarValue::Int16(Some(value.as_i16())),
                    ConstantType::Int32 => ScalarValue::Int32(Some(value.as_i32())),
                    ConstantType::Int64 => ScalarValue::Int64(Some(value.as_i64())),
                    ConstantType::Float64 => ScalarValue::Float64(Some(value.as_f64())),
                    ConstantType::Decimal => {
                        ScalarValue::Decimal128(Some(value.as_f64() as i128), 20, 0)
                        // TODO(chi): no hard code decimal
                    }
                    ConstantType::Date => ScalarValue::Date32(Some(value.as_i64() as i32)),
                    ConstantType::IntervalMonthDateNano => {
                        ScalarValue::IntervalMonthDayNano(Some(value.as_i128()))
                    }
                    ConstantType::Utf8String => ScalarValue::Utf8(Some(value.as_str().to_string())),
                    ConstantType::Any => unimplemented!(),
                };
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::Literal::new(value),
                ))
            }
            OptRelNodeTyp::PhysicalFunc(_) => {
                let expr = PhysicalFuncExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let func = expr.func();
                let args = expr
                    .children()
                    .to_vec()
                    .into_iter()
                    .map(|expr| Self::conv_from_optd_expr(expr, context))
                    .collect::<Result<Vec<_>>>()?;
                match func {
                    FuncType::Scalar(func) => {
                        Ok(datafusion::physical_expr::functions::create_physical_expr(
                            &func,
                            &args,
                            context,
                            &physical_expr::execution_props::ExecutionProps::new(),
                        )?)
                    }
                    FuncType::Case => {
                        let when_expr = args[0].clone();
                        let then_expr = args[1].clone();
                        let else_expr = args[2].clone();
                        Ok(physical_expr::expressions::case(
                            None,
                            vec![(when_expr, then_expr)],
                            Some(else_expr),
                        )?)
                    }
                    _ => unreachable!(),
                }
            }
            OptRelNodeTyp::PhysicalLogOp(typ) => {
                let expr = PhysicalLogOpExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let mut children = expr.children().into_iter();
                let first_expr = Self::conv_from_optd_expr(children.next().unwrap(), context)?;
                let op = match typ {
                    LogOpType::And => Operator::And,
                    LogOpType::Or => Operator::Or,
                };
                children.try_fold(first_expr, |acc, expr| {
                    let expr = Self::conv_from_optd_expr(expr, context)?;
                    Ok(
                        Arc::new(datafusion::physical_plan::expressions::BinaryExpr::new(
                            acc, op, expr,
                        )) as Arc<dyn PhysicalExpr>,
                    )
                })
            }
            OptRelNodeTyp::PhysicalBinOp(op) => {
                let expr = PhysicalBinOpExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let left = Self::conv_from_optd_expr(expr.left_child(), context)?;
                let right = Self::conv_from_optd_expr(expr.right_child(), context)?;
                let op = match op {
                    BinOpType::Eq => Operator::Eq,
                    BinOpType::Neq => Operator::NotEq,
                    BinOpType::Leq => Operator::LtEq,
                    BinOpType::Lt => Operator::Lt,
                    BinOpType::Geq => Operator::GtEq,
                    BinOpType::Gt => Operator::Gt,
                    BinOpType::Add => Operator::Plus,
                    BinOpType::Sub => Operator::Minus,
                    BinOpType::Mul => Operator::Multiply,
                    BinOpType::Div => Operator::Divide,
                    op => unimplemented!("{}", op),
                };
                Ok(
                    Arc::new(datafusion::physical_plan::expressions::BinaryExpr::new(
                        left, op, right,
                    )) as Arc<dyn PhysicalExpr>,
                )
            }
            OptRelNodeTyp::PhysicalBetween => {
                // TODO: should we just convert between to x <= c1 and x >= c2?
                let expr = PhysicalBetweenExpr::from_rel_node(expr.into_rel_node()).unwrap();
                Self::conv_from_optd_expr(
                    PhysicalLogOpExpr::new(
                        LogOpType::And,
                        PhysicalExprList::new(vec![
                            PhysicalBinOpExpr::new(expr.child(), expr.lower(), BinOpType::Geq)
                                .into_expr(),
                            PhysicalBinOpExpr::new(expr.child(), expr.upper(), BinOpType::Leq)
                                .into_expr(),
                        ]),
                    )
                    .into_expr(),
                    context,
                )
            }
            OptRelNodeTyp::PhysicalCast => {
                let expr = PhysicalCastExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let child = Self::conv_from_optd_expr(expr.child(), context)?;
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::CastExpr::new(
                        child,
                        expr.cast_to(),
                        None,
                    ),
                ))
            }
            OptRelNodeTyp::PhysicalLike => {
                let expr = PhysicalLikeExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let child = Self::conv_from_optd_expr(expr.child(), context)?;
                let pattern = Self::conv_from_optd_expr(expr.pattern(), context)?;
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::LikeExpr::new(
                        expr.negated(),
                        expr.case_insensitive(),
                        child,
                        pattern,
                    ),
                ))
            }
            OptRelNodeTyp::PhysicalInList => {
                let expr = PhysicalInListExpr::from_rel_node(expr.into_rel_node()).unwrap();
                let child = Self::conv_from_optd_expr(expr.child(), context)?;
                let list = expr
                    .list()
                    .to_vec()
                    .into_iter()
                    .map(|expr| Self::conv_from_optd_expr(expr, context))
                    .collect::<Result<Vec<_>>>()?;
                let negated = expr.negated();
                Ok(Arc::new(
                    datafusion::physical_plan::expressions::InListExpr::new(
                        child, list, negated, None,
                    ),
                ))
            }
            _ => unimplemented!("{}", expr.into_rel_node()),
        }
    }

    #[async_recursion]
    async fn conv_from_optd_projection(
        &mut self,
        node: PhysicalProjection,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.conv_from_optd_plan_node(node.child(), meta).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .enumerate()
            .map(|(idx, expr)| {
                Ok((
                    Self::conv_from_optd_expr(expr, &input_exec.schema())?,
                    format!("col{}", idx),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(
            Arc::new(ProjectionExec::try_new(physical_exprs, input_exec)?)
                as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn conv_from_optd_filter(
        &mut self,
        node: PhysicalFilter,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.conv_from_optd_plan_node(node.child(), meta).await?;
        let physical_expr = Self::conv_from_optd_expr(node.cond(), &input_exec.schema())?;
        Ok(
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                physical_expr,
                input_exec,
            )?) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn conv_from_optd_limit(
        &mut self,
        node: PhysicalLimit,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let child = self.conv_from_optd_plan_node(node.child(), meta).await?;

        // Limit skip/fetch expressions are only allowed to be constant int
        assert!(node.skip().typ() == OptRelNodeTyp::PhysicalConstant(ConstantType::UInt64));
        // Conversion from u64 -> usize could fail (also the case in into_optd)
        let skip = PhysicalConstantExpr::from_rel_node(node.skip().into_rel_node())
            .unwrap()
            .value()
            .as_u64()
            .try_into()
            .unwrap();

        assert!(node.fetch().typ() == OptRelNodeTyp::PhysicalConstant(ConstantType::UInt64));
        let fetch = PhysicalConstantExpr::from_rel_node(node.fetch().into_rel_node())
            .unwrap()
            .value()
            .as_u64();
        let fetch_opt: Option<usize> = if fetch == u64::MAX {
            None
        } else {
            Some(fetch.try_into().unwrap())
        };

        Ok(
            Arc::new(datafusion::physical_plan::limit::GlobalLimitExec::new(
                child, skip, fetch_opt,
            )) as Arc<dyn ExecutionPlan>,
        )
    }

    #[async_recursion]
    async fn conv_from_optd_sort(
        &mut self,
        node: PhysicalSort,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.conv_from_optd_plan_node(node.child(), meta).await?;
        let physical_exprs = node
            .exprs()
            .to_vec()
            .into_iter()
            .map(|expr| {
                self.conv_from_optd_sort_order_expr(
                    PhysicalSortOrderExpr::from_rel_node(expr.into_rel_node()).unwrap(),
                    &input_exec.schema(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(
            Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
                physical_exprs,
                input_exec,
            )) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    #[async_recursion]
    async fn conv_from_optd_agg(
        &mut self,
        node: PhysicalAgg,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let input_exec = self.conv_from_optd_plan_node(node.child(), meta).await?;
        let agg_exprs = node
            .aggrs()
            .to_vec()
            .into_iter()
            .map(|expr| self.conv_from_optd_agg_expr(expr, &input_exec.schema()))
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = node
            .groups()
            .to_vec()
            .into_iter()
            .map(|expr| {
                Ok((
                    Self::conv_from_optd_expr(expr, &input_exec.schema())?,
                    "<agg_expr>".to_string(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let group_exprs = physical_plan::aggregates::PhysicalGroupBy::new_single(group_exprs);
        let agg_num = agg_exprs.len();
        let schema = input_exec.schema().clone();
        Ok(Arc::new(
            datafusion::physical_plan::aggregates::AggregateExec::try_new(
                AggregateMode::Single,
                group_exprs,
                agg_exprs,
                vec![None; agg_num],
                vec![None; agg_num],
                input_exec,
                schema,
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    #[async_recursion]
    async fn conv_from_optd_nested_loop_join(
        &mut self,
        node: PhysicalNestedLoopJoin,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let left_exec = self.conv_from_optd_plan_node(node.left(), meta).await?;
        let right_exec = self.conv_from_optd_plan_node(node.right(), meta).await?;
        let filter_schema = {
            let fields = left_exec
                .schema()
                .fields()
                .into_iter()
                .chain(right_exec.schema().fields().into_iter())
                .cloned()
                .collect::<Vec<_>>();
            Schema::new_with_metadata(fields, HashMap::new())
        };

        let physical_expr =
            Self::conv_from_optd_expr(node.cond(), &Arc::new(filter_schema.clone()))?;

        if let JoinType::Cross = node.join_type() {
            return Ok(Arc::new(CrossJoinExec::new(left_exec, right_exec))
                as Arc<dyn ExecutionPlan + 'static>);
        }

        let join_type = match node.join_type() {
            JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
            JoinType::LeftOuter => datafusion::logical_expr::JoinType::Left,
            _ => unimplemented!(),
        };

        let mut column_idxs = vec![];
        for i in 0..left_exec.schema().fields().len() {
            column_idxs.push(ColumnIndex {
                index: i,
                side: physical_plan::joins::utils::JoinSide::Left,
            });
        }
        for i in 0..right_exec.schema().fields().len() {
            column_idxs.push(ColumnIndex {
                index: i,
                side: physical_plan::joins::utils::JoinSide::Right,
            });
        }

        Ok(Arc::new(
            datafusion::physical_plan::joins::NestedLoopJoinExec::try_new(
                left_exec,
                right_exec,
                Some(JoinFilter::new(physical_expr, column_idxs, filter_schema)),
                &join_type,
            )?,
        ) as Arc<dyn ExecutionPlan + 'static>)
    }

    #[async_recursion]
    async fn conv_from_optd_hash_join(
        &mut self,
        node: PhysicalHashJoin,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan + 'static>> {
        let left_exec = self.conv_from_optd_plan_node(node.left(), meta).await?;
        let right_exec = self.conv_from_optd_plan_node(node.right(), meta).await?;
        let join_type = match node.join_type() {
            JoinType::Inner => datafusion::logical_expr::JoinType::Inner,
            _ => unimplemented!(),
        };
        let left_exprs = PhysicalExprList::from_rel_node(node.left_keys().into_rel_node())
            .unwrap()
            .to_vec();
        let right_exprs = PhysicalExprList::from_rel_node(node.right_keys().into_rel_node())
            .unwrap()
            .to_vec();
        assert_eq!(left_exprs.len(), right_exprs.len());
        let mut on = Vec::with_capacity(left_exprs.len());
        for (left_expr, right_expr) in left_exprs.into_iter().zip(right_exprs.into_iter()) {
            let Some(left_expr) = PhysicalColumnRefExpr::from_rel_node(left_expr.into_rel_node())
            else {
                bail!("left expr is not physical column ref")
            };
            let Some(right_expr) = PhysicalColumnRefExpr::from_rel_node(right_expr.into_rel_node())
            else {
                bail!("right expr is not physical column ref")
            };
            on.push((
                physical_expr::expressions::Column::new(
                    left_exec.schema().field(left_expr.index()).name(),
                    left_expr.index(),
                ),
                physical_expr::expressions::Column::new(
                    right_exec.schema().field(right_expr.index()).name(),
                    right_expr.index(),
                ),
            ));
        }
        Ok(
            Arc::new(datafusion::physical_plan::joins::HashJoinExec::try_new(
                left_exec,
                right_exec,
                on,
                None,
                &join_type,
                PartitionMode::CollectLeft,
                false,
            )?) as Arc<dyn ExecutionPlan + 'static>,
        )
    }

    async fn conv_from_optd_plan_node(
        &mut self,
        node: PlanNode,
        meta: &RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rel_node = node.into_rel_node();

        let group_id = meta
            .get(&(rel_node.as_ref() as *const _ as usize))
            .expect("group id not found")
            .group_id;
        let rel_node_dbg = rel_node.clone();
        let bare = match &rel_node.typ {
            OptRelNodeTyp::PhysicalScan => {
                self.conv_from_optd_table_scan(PhysicalScan::from_rel_node(rel_node).unwrap())
                    .await?
            }
            OptRelNodeTyp::PhysicalProjection => {
                self.conv_from_optd_projection(
                    PhysicalProjection::from_rel_node(rel_node).unwrap(),
                    meta,
                )
                .await?
            }
            OptRelNodeTyp::PhysicalFilter => {
                self.conv_from_optd_filter(PhysicalFilter::from_rel_node(rel_node).unwrap(), meta)
                    .await?
            }
            OptRelNodeTyp::PhysicalSort => {
                self.conv_from_optd_sort(PhysicalSort::from_rel_node(rel_node).unwrap(), meta)
                    .await?
            }
            OptRelNodeTyp::PhysicalAgg => {
                self.conv_from_optd_agg(PhysicalAgg::from_rel_node(rel_node).unwrap(), meta)
                    .await?
            }
            OptRelNodeTyp::PhysicalNestedLoopJoin(_) => {
                self.conv_from_optd_nested_loop_join(
                    PhysicalNestedLoopJoin::from_rel_node(rel_node).unwrap(),
                    meta,
                )
                .await?
            }
            OptRelNodeTyp::PhysicalHashJoin(_) => {
                self.conv_from_optd_hash_join(
                    PhysicalHashJoin::from_rel_node(rel_node).unwrap(),
                    meta,
                )
                .await?
            }
            OptRelNodeTyp::PhysicalEmptyRelation => {
                let physical_node = PhysicalEmptyRelation::from_rel_node(rel_node).unwrap();
                let schema = physical_node.empty_relation_schema();
                let datafusion_schema: Schema = from_optd_schema(schema);
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                    physical_node.produce_one_row(),
                    Arc::new(datafusion_schema),
                )) as Arc<dyn ExecutionPlan>
            }
            OptRelNodeTyp::PhysicalLimit => {
                self.conv_from_optd_limit(PhysicalLimit::from_rel_node(rel_node).unwrap(), meta)
                    .await?
            }
            typ => unimplemented!("{}", typ),
        };

        let optimizer = self.optimizer.as_ref().unwrap();
        if optimizer.adaptive_enabled() {
            let bare_with_collector: Result<Arc<dyn ExecutionPlan>> =
                Ok(Arc::new(CollectorExec::new(
                    bare,
                    group_id,
                    self.optimizer.as_ref().unwrap().runtime_statistics.clone(),
                )) as Arc<dyn ExecutionPlan>);
            bare_with_collector.with_context(|| format!("when processing {}", rel_node_dbg))
        } else {
            Ok(bare)
        }
    }

    pub async fn conv_from_optd(
        &mut self,
        root_rel: OptRelNodeRef,
        meta: RelNodeMetaMap,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // dbg!(root_rel.clone());
        self.conv_from_optd_plan_node(PlanNode::from_rel_node(root_rel).unwrap(), &meta)
            .await
    }
}

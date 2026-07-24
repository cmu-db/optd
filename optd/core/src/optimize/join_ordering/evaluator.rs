//! Candidate-cost evaluation strategies.
//!
//! [`CardinalityEvaluator`] is the allocation-free path used by [`DefaultCostModel`]: it derives
//! the output cardinality and local join cost directly from child properties. The
//! [`MaterializingEvaluator`] remains the exact compatibility path for arbitrary [`CostModel`]
//! implementations whose semantics can depend on concrete operator handles.

use std::sync::Arc;

use super::OptimizeResult;
use super::plan::{PlanProperties, materialize_candidate_join};
use crate::analysis::{cross_product_profile, join_profile_from_conjuncts};
use crate::cost::{
    CostModel, DefaultCostModel, join_algorithm_class_for_conjuncts,
    join_algorithm_cost_for_profiles,
};
use crate::hypergraph::QueryHypergraph;
use crate::optimize::OptimizeError;
use crate::{
    AnalysisContext, CardinalityEstimationV1, CardinalityProfile, JoinType, Operator, QueryContext,
};

pub(super) struct EvaluatedPlan<C> {
    pub(super) cost: C,
    pub(super) properties: PlanProperties,
    pub(super) materialized: Option<Operator>,
}

pub(super) struct CandidateInput<'a, C> {
    pub(super) cost: &'a C,
    pub(super) properties: &'a PlanProperties,
    pub(super) materialized: Option<Operator>,
}

pub(super) struct JoinSpec<'a> {
    pub(super) join_type: &'a JoinType,
    pub(super) edge_indices: &'a [usize],
    pub(super) hypergraph: &'a QueryHypergraph,
    pub(super) cardinality_required: bool,
}

pub(super) trait CandidateEvaluator<M: CostModel>: Send + Sync {
    fn evaluate_leaf(
        &self,
        cost_model: &M,
        root: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        cardinality_required: bool,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>>;

    /// Whether candidate evaluation must first recover concrete child operators.
    fn requires_materialized_inputs(&self) -> bool;

    fn evaluate_join(
        &self,
        cost_model: &M,
        spec: JoinSpec<'_>,
        outer: CandidateInput<'_, M::Cost>,
        inner: CandidateInput<'_, M::Cost>,
        ctx: &mut QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>>;
}

/// Property-based evaluator for the built-in cardinality cost model.
///
/// Join alternatives remain as recipes during search. Only the winning recipe is materialized,
/// eliminating query-arena allocation and analysis-cache entries for rejected alternatives.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CardinalityEvaluator;

impl CandidateEvaluator<DefaultCostModel> for CardinalityEvaluator {
    fn evaluate_leaf(
        &self,
        cost_model: &DefaultCostModel,
        root: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        _cardinality_required: bool,
    ) -> OptimizeResult<EvaluatedPlan<f64>> {
        let cost = cost_model.total_cost(root, ctx, analyses)?;
        let cardinality =
            CardinalityEstimationV1::get_shared(ctx, analyses, root).map_err(cardinality_error)?;
        Ok(EvaluatedPlan {
            cost,
            properties: PlanProperties {
                cardinality: Some(cardinality),
            },
            materialized: Some(root),
        })
    }

    fn requires_materialized_inputs(&self) -> bool {
        false
    }

    fn evaluate_join(
        &self,
        cost_model: &DefaultCostModel,
        spec: JoinSpec<'_>,
        outer: CandidateInput<'_, f64>,
        inner: CandidateInput<'_, f64>,
        ctx: &mut QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<f64>> {
        let outer_profile = required_cardinality(outer.properties, "outer")?;
        let inner_profile = required_cardinality(inner.properties, "inner")?;
        let predicates = spec
            .edge_indices
            .iter()
            .filter_map(|&index| spec.hypergraph.edges[index].predicate)
            .collect::<Vec<_>>();

        let output = if predicates.is_empty() && *spec.join_type == JoinType::Inner {
            cross_product_profile(outer_profile, inner_profile)
        } else {
            join_profile_from_conjuncts(
                outer_profile,
                inner_profile,
                spec.join_type.clone(),
                &predicates,
                ctx,
            )
        };
        let local_cost = join_algorithm_cost_for_profiles(
            outer_profile,
            inner_profile,
            &output,
            join_algorithm_class_for_conjuncts(&predicates, ctx),
        );
        // Match `CostModel::total_cost_from_children` exactly, including the fold order. This is
        // important even for scalar costs because callers rely on semantic parity with the
        // materializing evaluator.
        let cost = cost_model.add(
            cost_model.add(cost_model.add(cost_model.zero(), local_cost), *outer.cost),
            *inner.cost,
        );

        Ok(EvaluatedPlan {
            cost,
            properties: PlanProperties {
                cardinality: Some(Arc::new(output)),
            },
            materialized: None,
        })
    }
}

fn required_cardinality<'a>(
    properties: &'a PlanProperties,
    input: &str,
) -> OptimizeResult<&'a CardinalityProfile> {
    properties
        .cardinality
        .as_deref()
        .ok_or_else(|| OptimizeError::PassError {
            pass: "JoinOrdering",
            message: format!("cardinality evaluator received {input} input without a profile"),
        })
}

fn cardinality_error(error: crate::AnalysisError) -> OptimizeError {
    OptimizeError::PassError {
        pass: "JoinOrdering",
        message: error.to_string(),
    }
}

/// Compatibility evaluator that preserves materialize-then-cost semantics for custom models.
///
/// Cardinality profiles are computed lazily when the selected enumerator ranks candidates with
/// them. Exact DPhyp can therefore use an independent custom cost model without paying for—or
/// requiring catalog metadata for—an otherwise unused property.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct MaterializingEvaluator;

impl<M: CostModel> CandidateEvaluator<M> for MaterializingEvaluator {
    fn evaluate_leaf(
        &self,
        cost_model: &M,
        root: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        cardinality_required: bool,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>> {
        let cost = cost_model.total_cost(root, ctx, analyses)?;
        let cardinality = cardinality_required
            .then(|| CardinalityEstimationV1::get_shared(ctx, analyses, root))
            .transpose()
            .map_err(cardinality_error)?;
        Ok(EvaluatedPlan {
            cost,
            properties: PlanProperties { cardinality },
            materialized: Some(root),
        })
    }

    fn requires_materialized_inputs(&self) -> bool {
        true
    }

    fn evaluate_join(
        &self,
        cost_model: &M,
        spec: JoinSpec<'_>,
        outer: CandidateInput<'_, M::Cost>,
        inner: CandidateInput<'_, M::Cost>,
        ctx: &mut QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<EvaluatedPlan<M::Cost>> {
        let outer_root = outer
            .materialized
            .expect("the materializing evaluator requires an outer operator");
        let inner_root = inner
            .materialized
            .expect("the materializing evaluator requires an inner operator");
        let root = materialize_candidate_join(
            outer_root,
            inner_root,
            spec.join_type.clone(),
            spec.edge_indices,
            spec.hypergraph,
            ctx,
        );
        let cost = cost_model.total_cost_from_children(
            root,
            &[outer.cost.clone(), inner.cost.clone()],
            ctx,
            analyses,
        )?;
        let cardinality = spec
            .cardinality_required
            .then(|| CardinalityEstimationV1::get_shared(ctx, analyses, root))
            .transpose()
            .map_err(cardinality_error)?;
        Ok(EvaluatedPlan {
            cost,
            properties: PlanProperties { cardinality },
            materialized: Some(root),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::DataType;

    use super::*;
    use crate::analysis::connecting_edge_indices;
    use crate::{
        BinaryOp, Column, ColumnData, CrossProduct, ExprData, Join, NaryOp, OperatorData,
        ScalarValue, Scan, TableRef, build_hypergraph, nodeset_singleton,
    };

    #[derive(Debug, Clone, Copy)]
    enum TestJoinKind {
        Inner,
        LeftOuter,
        RightOuter,
        FullOuter,
        LeftSemi,
        LeftAnti,
        Single,
        LeftMark { nullable: bool },
    }

    impl TestJoinKind {
        fn join_type(self, ctx: &mut QueryContext) -> JoinType {
            match self {
                Self::Inner => JoinType::Inner,
                Self::LeftOuter => JoinType::LeftOuter,
                Self::RightOuter => JoinType::RightOuter,
                Self::FullOuter => JoinType::FullOuter,
                Self::LeftSemi => JoinType::LeftSemi,
                Self::LeftAnti => JoinType::LeftAnti,
                Self::Single => JoinType::Single,
                Self::LeftMark { nullable } => JoinType::LeftMark {
                    marker: ColumnData::new("match", DataType::Boolean).add(ctx),
                    nullable,
                },
            }
        }
    }

    struct TwoInputs {
        outer: Operator,
        inner: Operator,
        outer_key: Column,
        outer_value: Column,
        inner_key: Column,
        inner_value: Column,
    }

    fn add_two_inputs(ctx: &mut QueryContext) -> TwoInputs {
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(ctx);
        let outer_value = ColumnData::new("outer_value", DataType::Int64).add(ctx);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(ctx);
        let inner_value = ColumnData::new("inner_value", DataType::Int64).add(ctx);
        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer"),
            columns: vec![outer_key, outer_value],
        })
        .add(ctx);
        let inner = OperatorData::Scan(Scan {
            table: TableRef::bare("inner"),
            columns: vec![inner_key, inner_value],
        })
        .add(ctx);
        TwoInputs {
            outer,
            inner,
            outer_key,
            outer_value,
            inner_key,
            inner_value,
        }
    }

    fn binary_predicate(
        ctx: &mut QueryContext,
        op: BinaryOp,
        left: Column,
        right: Column,
    ) -> crate::Expr {
        ExprData::Binary {
            op,
            left: ExprData::ColumnRef(left).add(ctx),
            right: ExprData::ColumnRef(right).add(ctx),
        }
        .add(ctx)
    }

    fn evaluate_both(
        ctx: &mut QueryContext,
        analyses: &mut AnalysisContext,
        root: Operator,
        outer: Operator,
        inner: Operator,
        join_type: JoinType,
    ) -> (EvaluatedPlan<f64>, EvaluatedPlan<f64>) {
        let hypergraph = build_hypergraph(ctx, analyses, root);
        let edge_indices =
            connecting_edge_indices(&nodeset_singleton(0), &nodeset_singleton(1), &hypergraph);
        assert!(!edge_indices.is_empty());

        let model = DefaultCostModel;
        let deferred = CardinalityEvaluator;
        let deferred_outer = deferred
            .evaluate_leaf(&model, outer, ctx, analyses, true)
            .unwrap();
        let deferred_inner = deferred
            .evaluate_leaf(&model, inner, ctx, analyses, true)
            .unwrap();
        let cached_outer = CardinalityEstimationV1::get_shared(ctx, analyses, outer).unwrap();
        assert!(Arc::ptr_eq(
            deferred_outer
                .properties
                .cardinality
                .as_ref()
                .expect("deferred leaves carry cardinality"),
            &cached_outer,
        ));
        let deferred_result = deferred
            .evaluate_join(
                &model,
                JoinSpec {
                    join_type: &join_type,
                    edge_indices: &edge_indices,
                    hypergraph: &hypergraph,
                    cardinality_required: true,
                },
                CandidateInput {
                    cost: &deferred_outer.cost,
                    properties: &deferred_outer.properties,
                    materialized: deferred_outer.materialized,
                },
                CandidateInput {
                    cost: &deferred_inner.cost,
                    properties: &deferred_inner.properties,
                    materialized: deferred_inner.materialized,
                },
                ctx,
                analyses,
            )
            .unwrap();

        let materializing = MaterializingEvaluator;
        let materialized_outer = materializing
            .evaluate_leaf(&model, outer, ctx, analyses, true)
            .unwrap();
        let materialized_inner = materializing
            .evaluate_leaf(&model, inner, ctx, analyses, true)
            .unwrap();
        let materialized_result = materializing
            .evaluate_join(
                &model,
                JoinSpec {
                    join_type: &join_type,
                    edge_indices: &edge_indices,
                    hypergraph: &hypergraph,
                    cardinality_required: true,
                },
                CandidateInput {
                    cost: &materialized_outer.cost,
                    properties: &materialized_outer.properties,
                    materialized: materialized_outer.materialized,
                },
                CandidateInput {
                    cost: &materialized_inner.cost,
                    properties: &materialized_inner.properties,
                    materialized: materialized_inner.materialized,
                },
                ctx,
                analyses,
            )
            .unwrap();

        (deferred_result, materialized_result)
    }

    fn assert_profile_and_cost_parity(
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
        deferred: &EvaluatedPlan<f64>,
        materialized: &EvaluatedPlan<f64>,
        case: impl std::fmt::Debug,
    ) {
        let root = materialized
            .materialized
            .expect("compatibility evaluation materializes a root");
        let analyzed = CardinalityEstimationV1::get_shared(ctx, analyses, root).unwrap();
        let deferred_profile = deferred
            .properties
            .cardinality
            .as_deref()
            .expect("deferred evaluation carries cardinality");
        assert_eq!(deferred_profile, analyzed.as_ref(), "{case:?}");
        assert_eq!(
            deferred.cost.to_bits(),
            materialized.cost.to_bits(),
            "{case:?}"
        );
    }

    #[test]
    fn cardinality_evaluator_matches_materialized_join_types_bit_for_bit() {
        let cases = [
            TestJoinKind::Inner,
            TestJoinKind::LeftOuter,
            TestJoinKind::RightOuter,
            TestJoinKind::FullOuter,
            TestJoinKind::LeftSemi,
            TestJoinKind::LeftAnti,
            TestJoinKind::Single,
            TestJoinKind::LeftMark { nullable: false },
            TestJoinKind::LeftMark { nullable: true },
        ];

        for case in cases {
            let mut ctx = QueryContext::new();
            let inputs = add_two_inputs(&mut ctx);
            let equality =
                binary_predicate(&mut ctx, BinaryOp::Eq, inputs.outer_key, inputs.inner_key);
            let residual = binary_predicate(
                &mut ctx,
                BinaryOp::Gt,
                inputs.outer_value,
                inputs.inner_value,
            );
            let on = ExprData::Nary {
                op: NaryOp::And,
                exprs: vec![equality, residual],
            }
            .add(&mut ctx);
            let join_type = case.join_type(&mut ctx);
            let root = OperatorData::Join(Join {
                join_type: join_type.clone(),
                on,
                outer: inputs.outer,
                inner: inputs.inner,
            })
            .add(&mut ctx);
            let mut analyses = crate::test_analyses(&ctx);

            let (deferred, materialized) = evaluate_both(
                &mut ctx,
                &mut analyses,
                root,
                inputs.outer,
                inputs.inner,
                join_type,
            );
            assert_profile_and_cost_parity(&ctx, &mut analyses, &deferred, &materialized, case);
        }
    }

    #[test]
    fn cardinality_evaluator_matches_cross_product_frequency_scaling() {
        let mut ctx = QueryContext::new();
        let inputs = add_two_inputs(&mut ctx);
        let root = OperatorData::CrossProduct(CrossProduct {
            outer: inputs.outer,
            inner: inputs.inner,
        })
        .add(&mut ctx);
        let mut analyses = crate::test_analyses(&ctx);

        let (deferred, materialized) = evaluate_both(
            &mut ctx,
            &mut analyses,
            root,
            inputs.outer,
            inputs.inner,
            JoinType::Inner,
        );
        assert_profile_and_cost_parity(
            &ctx,
            &mut analyses,
            &deferred,
            &materialized,
            "cross product",
        );

        let output = deferred.properties.cardinality.as_deref().unwrap();
        let outer = CardinalityEstimationV1::get_shared(&ctx, &mut analyses, inputs.outer).unwrap();
        assert!(
            output.columns[&inputs.outer_key].frequency.value
                > outer.columns[&inputs.outer_key].frequency.value,
            "cross products must scale a column's frequency by the opposite input",
        );
        assert!(matches!(
            materialized.materialized.unwrap().get(&ctx),
            OperatorData::CrossProduct(_)
        ));
    }

    #[test]
    fn predicate_free_non_inner_join_matches_literal_true_materialization() {
        let mut ctx = QueryContext::new();
        let inputs = add_two_inputs(&mut ctx);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
        let root = OperatorData::Join(Join {
            join_type: JoinType::LeftOuter,
            on,
            outer: inputs.outer,
            inner: inputs.inner,
        })
        .add(&mut ctx);
        let mut analyses = crate::test_analyses(&ctx);

        let (deferred, materialized) = evaluate_both(
            &mut ctx,
            &mut analyses,
            root,
            inputs.outer,
            inputs.inner,
            JoinType::LeftOuter,
        );
        assert_profile_and_cost_parity(
            &ctx,
            &mut analyses,
            &deferred,
            &materialized,
            "predicate-free left outer join",
        );
        let OperatorData::Join(join) = materialized.materialized.unwrap().get(&ctx) else {
            panic!("a predicate-free outer join must retain its join type");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(matches!(
            join.on.get(&ctx),
            ExprData::Literal(ScalarValue::Boolean(true))
        ));
    }
}

//! Functional-dependency-aware grouping keys; output grouping columns remain unchanged.

use std::collections::BTreeSet;

use crate::{Expr, ExprData, LogicalFacts, QueryContext};

/// Reduce keys only for estimation: every removed value is determined by retained keys.
/// A deterministic elimination order also handles cyclic dependencies without dropping
/// all representatives. Unknown/non-column expressions remain on the fallback path.
pub(super) fn independent_grouping_keys(
    keys: &[Expr],
    facts: &LogicalFacts,
    ctx: &QueryContext,
) -> Vec<Expr> {
    let value_for_key = |key: Expr| match key.get(ctx) {
        ExprData::ColumnRef(column) => facts.canonical_value_for_column(*column),
        _ => None,
    };
    let values = keys
        .iter()
        .filter_map(|key| value_for_key(*key))
        .collect::<BTreeSet<_>>();
    let mut retained = values.clone();
    if !facts.functional_dependencies.is_empty() || !facts.constraints.literal_equalities.is_empty()
    {
        for value in values {
            let mut others = retained.clone();
            others.remove(&value);
            if facts
                .functional_dependency_closure(&others)
                .contains(&value)
            {
                retained.remove(&value);
            }
        }
    }
    let mut seen = BTreeSet::new();
    keys.iter()
        .copied()
        .filter(|key| {
            value_for_key(*key).is_none_or(|value| retained.contains(&value) && seen.insert(value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use crate::{
        Aggregation, AnalysisContext, CardinalityEstimationV1, Catalog, Column, ColumnData,
        ColumnStatistics, ConstScan, CrossProduct, ExprData, MemoryCatalog, Operator, OperatorData,
        QueryContext, Scan, TableRef, TableStatistics, UniqueKey,
    };

    fn fixture(unique: bool) -> (QueryContext, Arc<MemoryCatalog>, Operator, [Column; 2]) {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let attr = ColumnData::new("attr", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![id, attr],
        })
        .add(&mut ctx);
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attr", DataType::Int64, false),
        ]));
        catalog
            .create_table(TableRef::bare("t"), schema, None)
            .unwrap();
        let mut constraints = crate::TableConstraints::default();
        if unique {
            constraints.unique_keys =
                crate::BoundedVec::try_new(vec![UniqueKey::try_new(vec!["id".into()]).unwrap()])
                    .unwrap();
        }
        catalog
            .set_table_statistics(
                TableRef::bare("t"),
                TableStatistics {
                    row_count: Some(100),
                    size_bytes: None,
                    column_statistics: [("id", 100), ("attr", 10)]
                        .into_iter()
                        .map(|(name, ndv)| {
                            (
                                name.into(),
                                ColumnStatistics {
                                    lower_bound: None,
                                    upper_bound: None,
                                    frequency: Some(100),
                                    distinct: Some(ndv),
                                    sketches: None,
                                    distribution: None,
                                },
                            )
                        })
                        .collect(),
                    column_groups: vec![],
                    constraints,
                },
            )
            .unwrap();
        (ctx, catalog, scan, [id, attr])
    }

    fn group(ctx: &mut QueryContext, input: Operator, columns: &[Column]) -> Operator {
        let keys = columns
            .iter()
            .map(|col| ExprData::ColumnRef(*col).add(ctx))
            .collect();
        OperatorData::Aggregation(Aggregation {
            input,
            keys,
            aggregates: vec![],
        })
        .add(ctx)
    }

    fn triplicate(ctx: &mut QueryContext, input: Operator) -> Operator {
        let copies = OperatorData::ConstScan(ConstScan {
            columns: vec![],
            rows: vec![vec![], vec![], vec![]],
        })
        .add(ctx);
        OperatorData::CrossProduct(CrossProduct {
            outer: input,
            inner: copies,
        })
        .add(ctx)
    }

    #[test]
    fn declared_key_avoids_group_inflation_after_row_duplication() {
        let (mut ctx, catalog, scan, [id, attr]) = fixture(true);
        let duplicated = triplicate(&mut ctx, scan);
        let by_key = group(&mut ctx, duplicated, &[id]);
        let by_tuple = group(&mut ctx, duplicated, &[attr, id]);
        let mut analyses = AnalysisContext::new(catalog);
        let key = analyses
            .get::<CardinalityEstimationV1>(&ctx, by_key)
            .unwrap();
        let tuple = analyses
            .get::<CardinalityEstimationV1>(&ctx, by_tuple)
            .unwrap();
        assert_eq!(key.rows.value, 100.0);
        assert_eq!(tuple.rows.value, key.rows.value);
        assert_eq!(
            tuple.columns.len(),
            2,
            "estimation must not remove output columns"
        );
    }

    #[test]
    fn dependencies_survive_filters_aliases_and_renames() {
        let (mut ctx, catalog, scan, [id, attr]) = fixture(true);
        let duplicated = triplicate(&mut ctx, scan);
        let predicate = ExprData::Binary {
            op: crate::BinaryOp::Lt,
            left: ExprData::ColumnRef(id).add(&mut ctx),
            right: ExprData::Literal(crate::ScalarValue::Int64(50)).add(&mut ctx),
        }
        .add(&mut ctx);
        let filtered = OperatorData::Selection(crate::Selection {
            input: duplicated,
            predicate,
        })
        .add(&mut ctx);
        let alias = ColumnData::new("alias", DataType::Int64).add(&mut ctx);
        let expression = ExprData::ColumnRef(id).add(&mut ctx);
        let mapped = OperatorData::Map(crate::Map {
            input: filtered,
            computations: vec![(alias, expression)],
        })
        .add(&mut ctx);
        let projected = OperatorData::Projection(crate::Projection {
            input: mapped,
            columns: vec![alias, attr],
        })
        .add(&mut ctx);
        let renamed_id = ColumnData::new("renamed_id", DataType::Int64).add(&mut ctx);
        let renamed_attr = ColumnData::new("renamed_attr", DataType::Int64).add(&mut ctx);
        let renamed = OperatorData::Rename(crate::Rename {
            alias: "renamed".into(),
            input: projected,
            defs: vec![(renamed_id, alias), (renamed_attr, attr)],
        })
        .add(&mut ctx);
        let by_key = group(&mut ctx, renamed, &[renamed_id]);
        let by_tuple = group(&mut ctx, renamed, &[renamed_attr, renamed_id]);
        let mut analyses = AnalysisContext::new(catalog);
        let key = analyses
            .get::<CardinalityEstimationV1>(&ctx, by_key)
            .unwrap();
        let tuple = analyses
            .get::<CardinalityEstimationV1>(&ctx, by_tuple)
            .unwrap();
        assert!(key.rows.value < 100.0);
        assert_eq!(tuple.rows.value, key.rows.value);
    }

    #[test]
    fn self_join_instances_do_not_share_key_dependencies() {
        let (mut ctx, catalog, left, [left_id, _]) = fixture(true);
        let right_id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let right_attr = ColumnData::new("attr", DataType::Int64).add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![right_id, right_attr],
        })
        .add(&mut ctx);
        let product = OperatorData::CrossProduct(CrossProduct {
            outer: left,
            inner: right,
        })
        .add(&mut ctx);
        let grouped = group(&mut ctx, product, &[left_id, right_attr]);
        let profile = AnalysisContext::new(catalog)
            .get::<CardinalityEstimationV1>(&ctx, grouped)
            .unwrap();
        assert_eq!(profile.rows.value, 1_000.0);
    }

    #[test]
    fn inner_join_equalities_connect_dependencies_but_outer_joins_do_not() {
        use super::independent_grouping_keys;
        use crate::{Join, JoinType, LogicalFactsAnalysis};
        let (mut ctx, catalog, left, [left_id, left_attr]) = fixture(true);
        let right_id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let right_attr = ColumnData::new("attr", DataType::Int64).add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![right_id, right_attr],
        })
        .add(&mut ctx);
        let on = ExprData::Binary {
            op: crate::BinaryOp::Eq,
            left: ExprData::ColumnRef(left_id).add(&mut ctx),
            right: ExprData::ColumnRef(right_id).add(&mut ctx),
        }
        .add(&mut ctx);
        let left_keys = [left_id, left_attr].map(|col| ExprData::ColumnRef(col).add(&mut ctx));
        let right_keys = [right_id, right_attr].map(|col| ExprData::ColumnRef(col).add(&mut ctx));
        let cross_keys = [left_id, right_attr].map(|col| ExprData::ColumnRef(col).add(&mut ctx));
        let mut analyses = AnalysisContext::new(catalog);
        for (join_type, left_count, right_count, cross_count) in [
            (JoinType::Inner, 1, 1, 1),
            (JoinType::LeftOuter, 1, 2, 2),
            (JoinType::RightOuter, 2, 1, 2),
            (JoinType::FullOuter, 2, 2, 2),
            (JoinType::Single, 1, 2, 2),
        ] {
            let join = OperatorData::Join(Join {
                join_type,
                outer: left,
                inner: right,
                on,
            })
            .add(&mut ctx);
            let facts = analyses.get::<LogicalFactsAnalysis>(&ctx, join).unwrap();
            assert_eq!(
                independent_grouping_keys(&left_keys, &facts, &ctx).len(),
                left_count
            );
            assert_eq!(
                independent_grouping_keys(&right_keys, &facts, &ctx).len(),
                right_count
            );
            assert_eq!(
                independent_grouping_keys(&cross_keys, &facts, &ctx).len(),
                cross_count
            );
        }
    }

    #[test]
    fn aggregation_keys_determine_aggregate_values_after_duplication() {
        let (mut ctx, catalog, scan, [id, attr]) = fixture(false);
        let sum = ColumnData::new("sum", DataType::Int64).add(&mut ctx);
        let key = ExprData::ColumnRef(attr).add(&mut ctx);
        let arg = ExprData::ColumnRef(id).add(&mut ctx);
        let aggregate = OperatorData::Aggregation(Aggregation {
            input: scan,
            keys: vec![key],
            aggregates: vec![(
                sum,
                crate::AggregateExpr::Func {
                    func: crate::AggregateFunction::Sum,
                    arg,
                    distinct: false,
                },
            )],
        })
        .add(&mut ctx);
        let duplicated = triplicate(&mut ctx, aggregate);
        let regrouped = group(&mut ctx, duplicated, &[attr, sum]);
        let profile = AnalysisContext::new(catalog)
            .get::<CardinalityEstimationV1>(&ctx, regrouped)
            .unwrap();
        assert_eq!(profile.rows.value, 10.0);
        assert_eq!(profile.columns.len(), 2);
    }

    #[test]
    fn composite_keys_require_every_component_to_be_available() {
        let (mut ctx, catalog, scan, [id, attr]) = fixture(false);
        let mut statistics = catalog
            .table_by_ref(&TableRef::bare("t"))
            .unwrap()
            .statistics
            .unwrap();
        statistics.constraints.unique_keys = crate::BoundedVec::try_new(vec![
            UniqueKey::try_new(vec!["id".into(), "attr".into()]).unwrap(),
        ])
        .unwrap();
        catalog
            .set_table_statistics(TableRef::bare("t"), statistics)
            .unwrap();
        let partial = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![id],
        })
        .add(&mut ctx);
        let mut analyses = AnalysisContext::new(catalog);
        let facts = analyses
            .get::<crate::LogicalFactsAnalysis>(&ctx, scan)
            .unwrap();
        assert_eq!(facts.functional_dependencies.len(), 1);
        assert_eq!(facts.functional_dependencies[0].determinants.len(), 2);
        let keys = [id, attr].map(|col| ExprData::ColumnRef(col).add(&mut ctx));
        assert_eq!(
            super::independent_grouping_keys(&keys, &facts, &ctx).len(),
            2
        );
        let partial_facts = analyses
            .get::<crate::LogicalFactsAnalysis>(&ctx, partial)
            .unwrap();
        assert!(partial_facts.functional_dependencies.is_empty());
    }

    #[test]
    fn semi_join_right_literal_constraint_is_not_charged_twice() {
        use crate::{BinaryOp, Join, JoinType, ScalarValue, Selection};
        let (mut ctx, catalog, left, [_, left_key]) = fixture(false);
        let right_key = ColumnData::new("attr", DataType::Int64).add(&mut ctx);
        let right = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![right_key],
        })
        .add(&mut ctx);
        let left_ref = ExprData::ColumnRef(left_key).add(&mut ctx);
        let right_ref = ExprData::ColumnRef(right_key).add(&mut ctx);
        let five = ExprData::Literal(ScalarValue::Int64(5)).add(&mut ctx);
        let right_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: right_ref,
            right: five,
        }
        .add(&mut ctx);
        let right = OperatorData::Selection(Selection {
            input: right,
            predicate: right_predicate,
        })
        .add(&mut ctx);
        let mut analyses = AnalysisContext::new(catalog);
        for op in [BinaryOp::Eq, BinaryOp::IsNotDistinctFrom] {
            for reversed in [false, true] {
                let on = ExprData::Binary {
                    op,
                    left: if reversed { right_ref } else { left_ref },
                    right: if reversed { left_ref } else { right_ref },
                }
                .add(&mut ctx);
                let semi = OperatorData::Join(Join {
                    join_type: JoinType::LeftSemi,
                    on,
                    outer: left,
                    inner: right,
                })
                .add(&mut ctx);
                let repeated = ExprData::Binary {
                    op: BinaryOp::Eq,
                    left: left_ref,
                    right: five,
                }
                .add(&mut ctx);
                let filtered = OperatorData::Selection(Selection {
                    input: semi,
                    predicate: repeated,
                })
                .add(&mut ctx);
                let before = analyses.get::<CardinalityEstimationV1>(&ctx, semi).unwrap();
                let after = analyses
                    .get::<CardinalityEstimationV1>(&ctx, filtered)
                    .unwrap();
                assert!(before.rows.value > 0.0);
                assert_eq!(after.rows.value, before.rows.value);
                let grouped = group(&mut ctx, semi, &[left_key]);
                let groups = analyses
                    .get::<CardinalityEstimationV1>(&ctx, grouped)
                    .unwrap();
                assert_eq!(groups.rows.value, 1.0);
                assert_eq!(groups.rows.upper, Some(1.0));
                let marker = ColumnData::new("mark", DataType::Boolean).add(&mut ctx);
                for join_type in [
                    JoinType::LeftAnti,
                    JoinType::LeftMark {
                        marker,
                        nullable: false,
                    },
                    JoinType::LeftOuter,
                    JoinType::RightOuter,
                    JoinType::FullOuter,
                    JoinType::Single,
                ] {
                    let other = OperatorData::Join(crate::Join {
                        join_type,
                        on,
                        outer: left,
                        inner: right,
                    })
                    .add(&mut ctx);
                    let facts = analyses
                        .get::<crate::LogicalFactsAnalysis>(&ctx, other)
                        .unwrap();
                    let key = facts.canonical_value_for_column(left_key).unwrap();
                    assert!(!facts.constraints.literal_equalities.contains_key(&key));
                }
            }
        }
    }

    #[test]
    fn observed_unique_ndv_does_not_create_a_dependency() {
        let (mut ctx, catalog, scan, [id, attr]) = fixture(false);
        let duplicated = triplicate(&mut ctx, scan);
        let by_tuple = group(&mut ctx, duplicated, &[id, attr]);
        let tuple = AnalysisContext::new(catalog)
            .get::<CardinalityEstimationV1>(&ctx, by_tuple)
            .unwrap();
        assert_eq!(tuple.rows.value, 300.0);
    }
}

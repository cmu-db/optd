//! Semi/anti estimation contracts: input populations, clause policy, and profile boundaries.

use super::*;
use crate::{Aggregation, ColumnData, Join, MemoryCatalog, Selection, TableStatistics};
use arrow_schema::{DataType, Field, Schema};

struct Fixture {
    ctx: QueryContext,
    analyses: AnalysisContext,
    scans: [Operator; 2],
    keys: [Column; 2],
    buckets: [Column; 2],
}

impl Fixture {
    fn new() -> Self {
        let mut ctx = QueryContext::new();
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("bucket", DataType::Int64, false),
        ]));
        let mut scan = |name: &str, ndv| {
            let table = TableRef::bare(name);
            let key = ColumnData::new("key", DataType::Int64).add(&mut ctx);
            let bucket = ColumnData::new("bucket", DataType::Int64).add(&mut ctx);
            catalog
                .create_table(table.clone(), schema.clone(), None)
                .unwrap();
            let column = |distinct| ColumnStatistics {
                lower_bound: None,
                upper_bound: None,
                frequency: Some(1_000),
                distinct: Some(distinct),
                sketches: None,
                distribution: None,
            };
            catalog
                .set_table_statistics(
                    table.clone(),
                    TableStatistics {
                        row_count: Some(1_000),
                        size_bytes: None,
                        column_statistics: [
                            ("key".into(), column(ndv)),
                            ("bucket".into(), column(4)),
                        ]
                        .into_iter()
                        .collect(),
                        column_groups: Vec::new(),
                        constraints: Default::default(),
                    },
                )
                .unwrap();
            let op = OperatorData::Scan(Scan {
                table,
                columns: vec![key, bucket],
            })
            .add(&mut ctx);
            (op, key, bucket)
        };
        let (left, left_key, left_bucket) = scan("left_t", 1_000);
        let (right, right_key, right_bucket) = scan("right_t", 500);
        Self {
            ctx,
            analyses: AnalysisContext::new(catalog),
            scans: [left, right],
            keys: [left_key, right_key],
            buckets: [left_bucket, right_bucket],
        }
    }

    fn filter(&mut self, side: usize) -> Operator {
        let column = ExprData::ColumnRef(self.buckets[side]).add(&mut self.ctx);
        let literal = ExprData::Literal(ScalarValue::Int64(0)).add(&mut self.ctx);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: column,
            right: literal,
        }
        .add(&mut self.ctx);
        OperatorData::Selection(Selection {
            input: self.scans[side],
            predicate,
        })
        .add(&mut self.ctx)
    }

    fn profile(&mut self, op: Operator) -> CardinalityProfile {
        self.analyses
            .get::<CardinalityEstimationV1>(&self.ctx, op)
            .unwrap()
    }

    fn join(
        &mut self,
        inputs: [Operator; 2],
        keys: [Column; 2],
        join_type: JoinType,
    ) -> CardinalityProfile {
        let on = equality(&mut self.ctx, keys);
        let op = OperatorData::Join(Join {
            outer: inputs[0],
            inner: inputs[1],
            on,
            join_type,
        })
        .add(&mut self.ctx);
        self.profile(op)
    }
}

fn equality(ctx: &mut QueryContext, keys: [Column; 2]) -> Expr {
    let left = ExprData::ColumnRef(keys[0]).add(ctx);
    let right = ExprData::ColumnRef(keys[1]).add(ctx);
    ExprData::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }
    .add(ctx)
}

#[test]
fn filtering_inputs_preserves_lhs_domain_and_caps_rhs_domain_by_rows() {
    let mut f = Fixture::new();
    let filtered = [f.filter(0), f.filter(1)];
    for (inputs, semi_rows, anti_rows) in [
        (f.scans, 500.0, 500.0),
        ([filtered[0], f.scans[1]], 125.0, 125.0),
        ([f.scans[0], filtered[1]], 250.0, 750.0),
        (filtered, 62.5, 187.5),
    ] {
        let semi = f.join(inputs, f.keys, JoinType::LeftSemi);
        let anti = f.join(inputs, f.keys, JoinType::LeftAnti);
        assert!(
            (semi.rows.value - semi_rows).abs() < 1e-9,
            "semi: {:?}",
            semi.rows
        );
        assert!(
            (anti.rows.value - anti_rows).abs() < 1e-9,
            "anti: {:?}",
            anti.rows
        );
    }
}

#[test]
fn filters_inner_joins_and_grouping_keep_population_adjusted_ndvs() {
    let mut f = Fixture::new();
    let filtered = [f.filter(0), f.filter(1)];
    for (side, expected) in [(0, 250.0), (1, 218.75)] {
        let actual = f.profile(filtered[side]).columns[&f.keys[side]]
            .distinct
            .value;
        assert!((actual - expected).abs() < 1e-9);
    }
    let inner = f.join([filtered[0], f.scans[1]], f.keys, JoinType::Inner);
    assert!((inner.rows.value - 500.0).abs() < 1e-9);
    for (side, expected) in [(0, 250.0), (1, 218.75)] {
        let key = ExprData::ColumnRef(f.keys[side]).add(&mut f.ctx);
        let group = OperatorData::Aggregation(Aggregation {
            input: filtered[side],
            keys: vec![key],
            aggregates: Vec::new(),
        })
        .add(&mut f.ctx);
        assert!((f.profile(group).rows.value - expected).abs() < 1e-9);
    }
}

#[test]
fn aliases_preserve_base_identity_but_computations_do_not() {
    let mut f = Fixture::new();
    let input = f.filter(0);
    let alias = ColumnData::new("key", DataType::Int64).add(&mut f.ctx);
    let renamed = OperatorData::Rename(crate::Rename {
        alias: "renamed".into(),
        input,
        defs: vec![(alias, f.keys[0])],
    })
    .add(&mut f.ctx);
    assert_eq!(
        f.join(
            [renamed, f.scans[1]],
            [alias, f.keys[1]],
            JoinType::LeftAnti
        )
        .rows
        .value,
        125.0
    );
    let computed = ColumnData::new("key", DataType::Int64).add(&mut f.ctx);
    let source = ExprData::ColumnRef(f.keys[0]).add(&mut f.ctx);
    let cast = ExprData::Cast {
        expr: source,
        ty: DataType::Int64,
    }
    .add(&mut f.ctx);
    let mapped = OperatorData::Map(crate::Map {
        input,
        computations: vec![(computed, cast)],
    })
    .add(&mut f.ctx);
    let profile = f.profile(mapped);
    assert_eq!(
        profile.columns[&computed].value,
        Some(ValueId::Derived(computed))
    );
    assert!((profile.columns[&computed].distinct.value - 250.0).abs() < 1e-9);
    // A same-named computed column must not acquire the base column's larger domain.
    assert_eq!(
        f.join(
            [mapped, f.scans[1]],
            [computed, f.keys[1]],
            JoinType::LeftAnti
        )
        .rows
        .value,
        0.0
    );
}

#[test]
fn base_hll_and_unique_domains_survive_filtering_without_reviving_sketches() {
    for use_hll in [false, true] {
        let mut f = Fixture::new();
        let table = TableRef::bare("left_t");
        let mut statistics = f
            .analyses
            .catalog()
            .table_by_ref(&table)
            .unwrap()
            .statistics
            .unwrap();
        let column = statistics.column_statistics.get_mut("key").unwrap();
        column.distinct = None;
        let original_ndv = if use_hll {
            let mut sketches = ColumnSketches::new(1_000).hll();
            for value in 0..1_000 {
                sketches.observe(&ScalarValue::Int64(value));
            }
            let ndv = sketches
                .distinct_values
                .as_ref()
                .unwrap()
                .estimate()
                .min(1_000.0);
            column.sketches = Some(sketches);
            ndv
        } else {
            statistics.constraints.unique_keys =
                vec![crate::UniqueKey::try_new(vec!["key".into()]).unwrap()]
                    .try_into()
                    .unwrap();
            1_000.0
        };
        f.analyses
            .catalog()
            .set_table_statistics(table, statistics)
            .unwrap();
        let filtered = f.filter(0);
        let anti = f.join([filtered, f.scans[1]], f.keys, JoinType::LeftAnti);
        assert!((anti.rows.value - 250.0 * (1.0 - 500.0 / original_ndv)).abs() < 1e-9);
        assert!(f.profile(filtered).columns[&f.keys[0]].sketches.is_none());
    }
}

fn relation(column: Column, rows: f64, ndv: f64) -> CardinalityProfile {
    let rows = Estimate::exact(rows);
    let mut key = ColumnProfile::unknown(&rows);
    key.distinct = Estimate::exact(ndv);
    CardinalityProfile::new(rows, [(column, key)].into_iter().collect())
}

fn mcv(profile: &mut CardinalityProfile, column: Column, entries: &[(i64, usize)]) {
    profile.columns.get_mut(&column).unwrap().distribution =
        Some(Arc::new(crate::ColumnDistributionStatistics {
            null_count: None,
            histogram: None,
            most_common_values: Some(
                crate::MostCommonValues::try_new(
                    entries
                        .iter()
                        .map(|&(value, frequency)| crate::MostCommonValue {
                            value: ScalarValue::Int64(value),
                            frequency,
                        })
                        .collect(),
                    crate::StatisticsCompleteness::Partial,
                    crate::StatisticsProvenance::FullScan,
                )
                .unwrap(),
            ),
        }));
}

fn estimate_pair(
    ctx: &QueryContext,
    left: &CardinalityProfile,
    right: &CardinalityProfile,
    on: Expr,
) -> (f64, f64) {
    let config = CardinalityEstimationConfig::default();
    let semi = join_profile_from_predicate_with_config(
        left,
        right,
        JoinType::LeftSemi,
        on,
        ctx,
        &config,
        None,
    );
    let anti = join_profile_from_predicate_with_config(
        left,
        right,
        JoinType::LeftAnti,
        on,
        ctx,
        &config,
        None,
    );
    assert!((semi.rows.value + anti.rows.value - left.rows.value).abs() < 1e-9);
    for profile in [&semi, &anti] {
        for column in profile.columns.values() {
            assert!(column.distinct.value <= column.frequency.value);
            assert!(column.frequency.value <= profile.rows.value);
        }
    }
    (semi.rows.value, anti.rows.value)
}

#[test]
fn mcv_matches_count_left_mass_once_regardless_of_right_duplicates() {
    let mut f = Fixture::new();
    let on = equality(&mut f.ctx, f.keys);
    let mut entries = vec![(1, 1), (2, 901)];
    entries.extend((3..=100).map(|value| (value, 1)));
    let mut left = relation(f.keys[0], 1_000.0, 100.0);
    mcv(&mut left, f.keys[0], &entries);
    let mut sketches = ColumnSketches::new(1_000).space_saving(128).unwrap();
    for &(value, count) in &entries {
        for _ in 0..count {
            sketches.observe(&ScalarValue::Int64(value));
        }
    }
    left.columns.get_mut(&f.keys[0]).unwrap().sketches = Some(Arc::new(sketches));
    for duplicates in [1, 1_000] {
        let mut right = relation(f.keys[1], duplicates as f64, 1.0);
        mcv(&mut right, f.keys[1], &[(1, duplicates)]);
        assert_eq!(estimate_pair(&f.ctx, &left, &right, on), (1.0, 999.0));
    }
    // A post-join distribution proxy is not evidence of current matched-row mass.
    left.rows.source = EstimateSource::Derived;
    left.columns.get_mut(&f.keys[0]).unwrap().sketches = None;
    let mut right = relation(f.keys[1], 1.0, 1.0);
    mcv(&mut right, f.keys[1], &[(1, 1)]);
    assert_eq!(estimate_pair(&f.ctx, &left, &right, on), (10.0, 990.0));
}

#[test]
fn partial_mcvs_separate_known_mass_from_uncertain_non_null_mass() {
    let mut f = Fixture::new();
    let forward = equality(&mut f.ctx, f.keys);
    let reversed = equality(&mut f.ctx, [f.keys[1], f.keys[0]]);
    for (unknown, paired_mcv, expected) in [
        (false, true, 1_000.0 * (0.2 + 0.6 * 9.0 / 99.0)),
        (true, true, 500.0),
        (false, false, 80.0),
    ] {
        let mut left = relation(f.keys[0], 1_000.0, 100.0);
        let mut right = relation(f.keys[1], 10_000.0, 10.0);
        left.columns.get_mut(&f.keys[0]).unwrap().frequency = Estimate::exact(800.0);
        if unknown {
            left.columns.get_mut(&f.keys[0]).unwrap().distinct = Estimate::default(100.0);
        }
        mcv(&mut left, f.keys[0], &[(1, 200), (2, 300)]);
        if paired_mcv {
            mcv(&mut right, f.keys[1], &[(1, 1_000), (3, 2_000)]);
        }
        for on in [forward, reversed] {
            assert!((estimate_pair(&f.ctx, &left, &right, on).0 - expected).abs() < 1e-9);
        }
    }
}

#[test]
fn unknown_domains_use_half_coverage_subject_to_rhs_and_inner_caps() {
    let mut f = Fixture::new();
    let on = equality(&mut f.ctx, f.keys);
    for (ld, rd, ln, lv, rn, rv, expected) in [
        (true, false, 1_000.0, 100.0, 1_000.0, 100.0, 500.0),
        (false, true, 1_000.0, 100.0, 1_000.0, 100.0, 500.0),
        (true, true, 1_000.0, 100.0, 1_000.0, 100.0, 500.0),
        (true, false, 1_000.0, 1_000.0, 1.0, 1.0, 1.0),
        (false, true, 100.0, 5.0, 10.0, 10.0, 100.0),
    ] {
        let mut left = relation(f.keys[0], ln, lv);
        let mut right = relation(f.keys[1], rn, rv);
        if ld {
            left.columns.get_mut(&f.keys[0]).unwrap().distinct = Estimate::default(lv);
        }
        if rd {
            right.columns.get_mut(&f.keys[1]).unwrap().distinct = Estimate::default(rv);
        }
        let semi = estimate_pair(&f.ctx, &left, &right, on).0;
        let inner = join_profile_from_predicate_with_config(
            &left,
            &right,
            JoinType::Inner,
            on,
            &f.ctx,
            &CardinalityEstimationConfig::default(),
            None,
        );
        assert_eq!(semi, expected);
        assert!(semi <= inner.rows.value + 1e-9);
    }
}

#[test]
fn empty_inputs_and_null_safe_equality_obey_existence_semantics() {
    let mut f = Fixture::new();
    let ordinary = equality(&mut f.ctx, f.keys);
    let l = ExprData::ColumnRef(f.keys[0]).add(&mut f.ctx);
    let r = ExprData::ColumnRef(f.keys[1]).add(&mut f.ctx);
    let null_safe = ExprData::Binary {
        op: BinaryOp::IsNotDistinctFrom,
        left: l,
        right: r,
    }
    .add(&mut f.ctx);
    let always = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut f.ctx);
    let mut left = relation(f.keys[0], 100.0, 10.0);
    left.columns.get_mut(&f.keys[0]).unwrap().frequency = Estimate::exact(50.0);
    for rows in [0.0, 10.0] {
        let mut right = relation(f.keys[1], rows, 0.0);
        right.columns.get_mut(&f.keys[1]).unwrap().frequency = Estimate::exact(0.0);
        for (on, expected) in [(ordinary, 0.0), (null_safe, 50.0), (always, 100.0)] {
            assert_eq!(
                estimate_pair(&f.ctx, &left, &right, on).0,
                if rows == 0.0 { 0.0 } else { expected }
            );
            assert_eq!(
                estimate_pair(&f.ctx, &relation(f.keys[0], 0.0, 0.0), &right, on),
                (0.0, 0.0)
            );
        }
    }
}

#[test]
fn conjuncts_multiply_without_fanout_or_duplicate_equality_charges() {
    let mut f = Fixture::new();
    let eq = equality(&mut f.ctx, f.keys);
    let bucket = ExprData::ColumnRef(f.buckets[0]).add(&mut f.ctx);
    let zero = ExprData::Literal(ScalarValue::Int64(0)).add(&mut f.ctx);
    let residual = ExprData::Binary {
        op: BinaryOp::Eq,
        left: bucket,
        right: zero,
    }
    .add(&mut f.ctx);
    let left = f.profile(f.scans[0]);
    let right = f.profile(f.scans[1]);
    for predicates in [vec![eq, residual], vec![eq, eq, residual]] {
        let on = ExprData::Nary {
            op: NaryOp::And,
            exprs: predicates,
        }
        .add(&mut f.ctx);
        assert_eq!(estimate_pair(&f.ctx, &left, &right, on), (125.0, 875.0));
    }
}

#[test]
fn foreign_key_guarantees_require_a_complete_referenced_population() {
    let mut f = Fixture::new();
    let left_table = TableRef::bare("left_t");
    let right_table = TableRef::bare("right_t");
    let mut left = f
        .analyses
        .catalog()
        .table_by_ref(&left_table)
        .unwrap()
        .statistics
        .unwrap();
    left.column_statistics.get_mut("key").unwrap().distinct = Some(500);
    left.constraints.foreign_keys = vec![
        crate::ForeignKey::try_new(vec!["key".into()], right_table.clone(), vec!["key".into()])
            .unwrap(),
    ]
    .try_into()
    .unwrap();
    f.analyses
        .catalog()
        .set_table_statistics(left_table, left)
        .unwrap();
    let mut right = f
        .analyses
        .catalog()
        .table_by_ref(&right_table)
        .unwrap()
        .statistics
        .unwrap();
    right.row_count = Some(500);
    for column in right.column_statistics.values_mut() {
        column.frequency = Some(500);
    }
    right.constraints.unique_keys = vec![crate::UniqueKey::try_new(vec!["key".into()]).unwrap()]
        .try_into()
        .unwrap();
    f.analyses
        .catalog()
        .set_table_statistics(right_table, right)
        .unwrap();
    let filtered = [f.filter(0), f.filter(1)];
    for (inputs, expected) in [
        (f.scans, 1_000.0),
        ([filtered[0], f.scans[1]], 250.0),
        ([f.scans[0], filtered[1]], 250.0),
    ] {
        assert_eq!(
            f.join(inputs, f.keys, JoinType::LeftSemi).rows.value,
            expected
        );
    }
}

#[test]
fn sparse_samples_are_not_join_key_presence_evidence() {
    use arrow_array::{Int64Array, RecordBatch};
    let mut f = Fixture::new();
    for (name, keys) in [("left_t", vec![1_i64, 2]), ("right_t", vec![3, 4])] {
        let table = TableRef::bare(name);
        let schema = f.analyses.catalog().table_by_ref(&table).unwrap().schema;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vec![0_i64, 0])),
            ],
        )
        .unwrap();
        let sample = optd_sampling::TableSample::try_new(
            schema,
            vec![batch],
            2,
            1_000,
            optd_sampling::SampleProvenance::Past,
        )
        .unwrap();
        f.analyses
            .catalog()
            .set_table_samples(table, sample)
            .unwrap();
    }
    assert_eq!(
        f.join(f.scans, f.keys, JoinType::LeftSemi).rows.value,
        500.0
    );
    assert_eq!(
        f.join(f.scans, f.keys, JoinType::LeftAnti).rows.value,
        500.0
    );
}

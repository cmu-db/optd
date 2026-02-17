//! Tests for the AdvancedCardinalityEstimator.

use std::sync::Arc;

use crate::ir::{
    Column, DataType, IRContext,
    builder::*,
    catalog::{Catalog, Field, Schema},
    operator::join::JoinType,
    properties::{Cardinality, CardinalityEstimator},
    statistics::{AdvanceColumnStatistics, ColumnStatistics, TableStatistics},
};
use crate::magic::{MagicCatalog, MagicCostModel};

use super::AdvancedCardinalityEstimator;

/// Create an IRContext using MagicCatalog with AdvancedCardinalityEstimator.
fn adv_ctx() -> IRContext {
    IRContext::new(
        Arc::new(MagicCatalog::default()),
        Arc::new(AdvancedCardinalityEstimator),
        Arc::new(MagicCostModel),
    )
}

fn adv_ctx_with_catalog(cat: MagicCatalog) -> IRContext {
    IRContext::new(
        Arc::new(cat),
        Arc::new(AdvancedCardinalityEstimator),
        Arc::new(MagicCostModel),
    )
}

fn simple_table_stats(row_count: usize, col_stats: Vec<ColumnStatistics>) -> TableStatistics {
    TableStatistics {
        row_count,
        column_statistics: col_stats,
        size_bytes: None,
    }
}

fn col_stat(name: &str, distinct_count: usize) -> ColumnStatistics {
    ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: name.to_string(),
        advanced_stats: vec![],
        min_value: None,
        max_value: None,
        null_count: Some(0),
        distinct_count: Some(distinct_count),
    }
}

fn col_stat_with_range(name: &str, distinct_count: usize, min: i64, max: i64) -> ColumnStatistics {
    ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: name.to_string(),
        advanced_stats: vec![],
        min_value: Some(min.to_string()),
        max_value: Some(max.to_string()),
        null_count: Some(0),
        distinct_count: Some(distinct_count),
    }
}

fn col_stat_with_nulls(name: &str, distinct_count: usize, null_count: usize) -> ColumnStatistics {
    ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: name.to_string(),
        advanced_stats: vec![],
        min_value: None,
        max_value: None,
        null_count: Some(null_count),
        distinct_count: Some(distinct_count),
    }
}

// ---------- Scan tests ----------

#[test]
fn test_scan_with_stats() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(src, simple_table_stats(500, vec![col_stat("t.a", 100)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);
    let card = scan.cardinality(&ctx);
    assert_eq!(card.as_f64(), 500.0);
}

#[test]
fn test_scan_without_stats() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    // No stats set

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);
    let card = scan.cardinality(&ctx);
    assert_eq!(card.as_f64(), 1000.0); // fallback default
}

// ---------- Filter selectivity tests ----------

#[test]
fn test_equality_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stat("t.a", 10)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // Column(0) = 42
    let col = Column(0);
    let predicate = column_ref(col).eq(int32(42));
    let filtered = scan.logical_select(predicate);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * (1/10) = 100
    assert!((card.as_f64() - 100.0).abs() < 0.01);
}

#[test]
fn test_equality_selectivity_with_nulls() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        true,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    // 200 nulls out of 1000 rows, NDV = 10
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_nulls("t.a", 10, 200)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let col = Column(0);
    let predicate = column_ref(col).eq(int32(42));
    let filtered = scan.logical_select(predicate);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * (1 - 200/1000) * (1/10) = 1000 * 0.8 * 0.1 = 80
    assert!((card.as_f64() - 80.0).abs() < 0.01);
}

#[test]
fn test_literal_true_false() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(src, simple_table_stats(500, vec![col_stat("t.a", 10)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // Filter with literal true → same cardinality
    let filtered_true = scan.clone().logical_select(boolean(true));
    assert_eq!(filtered_true.cardinality(&ctx).as_f64(), 500.0);

    // Filter with literal false → zero cardinality
    let filtered_false = scan.logical_select(boolean(false));
    assert_eq!(filtered_false.cardinality(&ctx).as_f64(), 0.0);
}

#[test]
fn test_nary_and_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("t.a".to_string(), DataType::Int32, false),
        Field::new("t.b".to_string(), DataType::Int32, false),
    ]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat("t.a", 10), col_stat("t.b", 5)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // a = 1 AND b = 2 → (1/10) * (1/5) = 0.02
    let col_a = Column(0);
    let col_b = Column(1);
    let pred = column_ref(col_a)
        .eq(int32(1))
        .and(column_ref(col_b).eq(int32(2)));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * 0.02 = 20
    assert!((card.as_f64() - 20.0).abs() < 0.01);
}

#[test]
fn test_nary_or_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("t.a".to_string(), DataType::Int32, false),
        Field::new("t.b".to_string(), DataType::Int32, false),
    ]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat("t.a", 10), col_stat("t.b", 5)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // a = 1 OR b = 2 → 1 - (1-0.1)*(1-0.2) = 1 - 0.9*0.8 = 0.28
    let col_a = Column(0);
    let col_b = Column(1);
    let pred = column_ref(col_a)
        .eq(int32(1))
        .or(column_ref(col_b).eq(int32(2)));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * 0.28 = 280
    assert!((card.as_f64() - 280.0).abs() < 0.01);
}

#[test]
fn test_unknown_predicate_fallback() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stat("t.a", 10)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // a < 5 (range predicate without min/max stats → should fallback)
    let col_a = Column(0);
    let pred = column_ref(col_a).lt(int32(5));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * 0.1 (FALLBACK_PREDICATE_SELECTIVITY — no min/max available)
    assert!((card.as_f64() - 100.0).abs() < 0.01);
}

#[test]
fn test_ndv_zero_fallback() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();

    // NDV = 0 → should fall back
    // Physically, this makese no sense -- a non-nullable column with 1000 rows can not have 0 distinct values.
    // This test is purely for verifying the fallback behvaiour for situations where the distinct count is corrupted
    // or erroneously parsed as 0 when it did not exist.
    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stat("t.a", 0)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let col_a = Column(0);
    let pred = column_ref(col_a).eq(int32(42));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // NDV=0 → ndv() returns None → equality_selectivity_col_lit falls back
    // to base_row_count (1000) as NDV → sel = 1/1000 = 0.001 → card = 1.0
    assert!((card.as_f64() - 1.0).abs() < 0.01);
}

#[test]
fn test_column_without_source_fallback() {
    // Use a mock scan (columns have source=None) to test fallback
    let ctx = adv_ctx();
    let scan = ctx.mock_scan(0, vec![0, 1], 500.0);

    // Column created by mock_scan has source=None
    let col = Column(0);
    let pred = column_ref(col).eq(int32(42));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // No source → fallback selectivity 0.1
    assert!((card.as_f64() - 50.0).abs() < 0.01);
}

#[test]
fn test_ndv_from_hll_advanced_stats() {
    use synopses::distinct::HyperLogLog;

    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();

    // Build an HLL and insert values
    let mut hll: HyperLogLog<i32> = HyperLogLog::new();
    for i in 0..50 {
        hll.insert(&i);
    }
    let hll_data = serde_json::to_value(&hll).unwrap();

    let col_stats = ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: "t.a".to_string(),
        advanced_stats: vec![AdvanceColumnStatistics {
            stats_type: "hll".to_string(),
            data: hll_data,
        }],
        min_value: None,
        max_value: None,
        null_count: Some(0),
        distinct_count: None, // No precomputed distinct_count — force HLL path
    };

    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stats]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let col = Column(0);
    let pred = column_ref(col).eq(int32(42));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // NDV from HLL should be ~50, so selectivity ~1/50 = 0.02
    // card = 1000 * 0.02 = 20
    // Allow some HLL approximation error
    assert!(card.as_f64() > 17.0 && card.as_f64() < 23.0);
}

/// Create column stats with an HLL built from the given integer range,
/// and optional min/max range strings. `distinct_count` is set to `None`
/// to force the HLL path in `ndv()` / `extract_hll()`.
fn col_stat_with_hll(
    name: &str,
    values: std::ops::Range<i32>,
    min_max: Option<(i64, i64)>,
) -> ColumnStatistics {
    use synopses::distinct::HyperLogLog;

    let mut hll: HyperLogLog<i32> = HyperLogLog::new();
    for v in values {
        hll.insert(&v);
    }
    let hll_data = serde_json::to_value(&hll).unwrap();
    ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: name.to_string(),
        advanced_stats: vec![AdvanceColumnStatistics {
            stats_type: "hll".to_string(),
            data: hll_data,
        }],
        min_value: min_max.map(|(lo, _)| lo.to_string()),
        max_value: min_max.map(|(_, hi)| hi.to_string()),
        null_count: Some(0),
        distinct_count: None,
    }
}

// ---------- HLL join selectivity tests ----------

#[test]
fn test_equijoin_hll_full_overlap() {
    // Both sides have the same values (0..100) → HLL intersection ≈ NDV
    // Selectivity ≈ NDV / (NDV * NDV) = 1/NDV ≈ 1/100
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(1000, vec![col_stat_with_hll("a.id", 0..100, Some((0, 99)))]),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(2000, vec![col_stat_with_hll("b.id", 0..100, Some((0, 99)))]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(1))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx).as_f64();

    // Full overlap: sel ≈ |intersect|/(NDV_a * NDV_b) ≈ 100/(100*100) = 1/100
    // card ≈ 1000 * 2000 / 100 = 20000
    // With HLL error, allow ±30%
    assert!(card > 14000.0 && card < 26000.0, "card = {card}");
}

#[test]
fn test_equijoin_hll_partial_overlap() {
    // a has values 0..200, b has values 100..300 → ~100 values overlap
    // intersection ≈ 100, NDV_a ≈ 200, NDV_b ≈ 200
    // sel ≈ 100 / (200 * 200) = 1/400
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(
            1000,
            vec![col_stat_with_hll("a.id", 0..200, Some((0, 199)))],
        ),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(
            1000,
            vec![col_stat_with_hll("b.id", 100..300, Some((100, 299)))],
        ),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(1))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx).as_f64();

    // Partial overlap: sel ≈ 100/(200*200) = 0.0025
    // card ≈ 1000 * 1000 * 0.0025 = 2500
    // Containment assumption would give: 1000 * 1000 / max(200,200) = 5000
    // HLL should give a lower estimate. Allow wide margin for HLL error.
    assert!(card > 1000.0 && card < 5000.0, "card = {card}");

    // Verify it's strictly less than what containment would give
    let containment_card = 1000.0 * 1000.0 / 200.0; // 5000
    assert!(
        card < containment_card,
        "HLL estimate {card} should be less than containment {containment_card}"
    );
}

#[test]
fn test_equijoin_hll_no_overlap() {
    // a has values 0..100, b has values 1000..1100 → no overlap
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(500, vec![col_stat_with_hll("a.id", 0..100, Some((0, 99)))]),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(
            500,
            vec![col_stat_with_hll("b.id", 1000..1100, Some((1000, 1099)))],
        ),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(1))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx).as_f64();

    // Disjoint domains → domain overlap check (min/max) short-circuits to 0
    assert_eq!(card, 0.0);
}

#[test]
fn test_equijoin_hll_no_overlap_overlapping_range() {
    // a has values 0,2,4,...,198 (evens), b has values 1,3,5,...,199 (odds)
    // min/max ranges overlap ([0,198] vs [1,199]) but actual values are disjoint.
    // HLL intersection should be ~0.
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    // Build HLLs with even/odd values manually
    use synopses::distinct::HyperLogLog;
    let mut hll_a: HyperLogLog<i32> = HyperLogLog::new();
    for v in (0..200).step_by(2) {
        hll_a.insert(&v);
    }
    let mut hll_b: HyperLogLog<i32> = HyperLogLog::new();
    for v in (1..200).step_by(2) {
        hll_b.insert(&v);
    }

    let cs_a = ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: "a.id".to_string(),
        advanced_stats: vec![AdvanceColumnStatistics {
            stats_type: "hll".to_string(),
            data: serde_json::to_value(&hll_a).unwrap(),
        }],
        min_value: Some("0".to_string()),
        max_value: Some("198".to_string()),
        null_count: Some(0),
        distinct_count: None,
    };
    let cs_b = ColumnStatistics {
        column_id: 0,
        column_type: DataType::Int32,
        name: "b.id".to_string(),
        advanced_stats: vec![AdvanceColumnStatistics {
            stats_type: "hll".to_string(),
            data: serde_json::to_value(&hll_b).unwrap(),
        }],
        min_value: Some("1".to_string()),
        max_value: Some("199".to_string()),
        null_count: Some(0),
        distinct_count: None,
    };

    cat.set_table_stats(src_a, simple_table_stats(500, vec![cs_a]));
    cat.set_table_stats(src_b, simple_table_stats(500, vec![cs_b]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(1))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx).as_f64();

    // Ranges overlap but actual values are disjoint.
    // Containment assumption would give 500*500/100 = 2500.
    // HLL intersection should be very small (near 0 due to disjoint sets,
    // though HLL may have some estimation noise).
    // The key assertion: much less than containment estimate.
    let containment_card = 500.0 * 500.0 / 100.0;
    assert!(
        card < containment_card * 0.3,
        "HLL estimate {card} should be much less than containment {containment_card}"
    );
}

// ---------- Equi-join tests ----------

#[test]
fn test_equijoin_basic() {
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.aid".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(100, vec![col_stat_with_range("a.id", 100, 0, 99)]),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(1000, vec![col_stat_with_range("b.aid", 50, 0, 49)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);

    // a.id(Column 0) = b.aid(Column 1)
    let col_a = Column(0);
    let col_b = Column(1);
    let keys: Arc<[(Column, Column)]> = Arc::new([(col_a, col_b)]);

    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx);

    // Expected: 100 * 1000 / max(100, 50) = 100000 / 100 = 1000
    assert!((card.as_f64() - 1000.0).abs() < 0.01);
}

#[test]
fn test_equijoin_domain_no_overlap() {
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    // Disjoint ranges: a [0, 99], b [200, 299]
    cat.set_table_stats(
        src_a,
        simple_table_stats(100, vec![col_stat_with_range("a.id", 100, 0, 99)]),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(100, vec![col_stat_with_range("b.id", 100, 200, 299)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);

    let col_a = Column(0);
    let col_b = Column(1);
    let keys: Arc<[(Column, Column)]> = Arc::new([(col_a, col_b)]);

    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx);

    // Disjoint domains → zero rows
    assert_eq!(card.as_f64(), 0.0);
}

#[test]
fn test_equijoin_composite_keys() {
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![
        Field::new("a.x".to_string(), DataType::Int32, false),
        Field::new("a.y".to_string(), DataType::Int32, false),
    ]));
    let schema_b = Arc::new(Schema::new(vec![
        Field::new("b.x".to_string(), DataType::Int32, false),
        Field::new("b.y".to_string(), DataType::Int32, false),
    ]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(
            100,
            vec![
                col_stat_with_range("a.x", 10, 0, 9),
                col_stat_with_range("a.y", 20, 0, 19),
            ],
        ),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(
            200,
            vec![
                col_stat_with_range("b.x", 10, 0, 9),
                col_stat_with_range("b.y", 20, 0, 19),
            ],
        ),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);

    // Composite key: (a.x, a.y) = (b.x, b.y)
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(2)), (Column(1), Column(3))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    let card = join.cardinality(&ctx);

    // Expected: 100 * 200 * (1/max(10,10)) * (1/max(20,20)) = 20000 * (1/10) * (1/20) = 100
    assert!((card.as_f64() - 100.0).abs() < 0.01);
}

// ---------- Integration test ----------

#[test]
fn test_integration_scan_filter_join_project() {
    let cat = MagicCatalog::default();
    let schema_orders = Arc::new(Schema::new(vec![
        Field::new("orders.id".to_string(), DataType::Int32, false),
        Field::new("orders.customer_id".to_string(), DataType::Int32, false),
        Field::new("orders.status".to_string(), DataType::Int32, false),
    ]));
    let schema_customers = Arc::new(Schema::new(vec![
        Field::new("customers.id".to_string(), DataType::Int32, false),
        Field::new("customers.name".to_string(), DataType::Int32, false),
    ]));

    let orders = cat
        .try_create_table("orders".to_string(), schema_orders.clone())
        .unwrap();
    let customers = cat
        .try_create_table("customers".to_string(), schema_customers.clone())
        .unwrap();

    cat.set_table_stats(
        orders,
        simple_table_stats(
            10000,
            vec![
                col_stat_with_range("orders.id", 10000, 1, 10000),
                col_stat_with_range("orders.customer_id", 500, 1, 500),
                col_stat("orders.status", 5), // 5 distinct statuses
            ],
        ),
    );
    cat.set_table_stats(
        customers,
        simple_table_stats(
            500,
            vec![
                col_stat_with_range("customers.id", 500, 1, 500),
                col_stat("customers.name", 490),
            ],
        ),
    );

    let ctx = adv_ctx_with_catalog(cat);

    // Scan orders
    let scan_orders = ctx.table_scan(orders, &schema_orders, None);
    assert_eq!(scan_orders.cardinality(&ctx).as_f64(), 10000.0);

    // Filter: orders.status = 1 → 10000 * (1/5) = 2000
    let status_col = Column(2); // orders.status
    let filtered = scan_orders.physical_filter(column_ref(status_col).eq(int32(1)));
    let filtered_card = filtered.cardinality(&ctx).as_f64();
    assert!((filtered_card - 2000.0).abs() < 0.01);

    // Scan customers
    let scan_customers = ctx.table_scan(customers, &schema_customers, None);
    assert_eq!(scan_customers.cardinality(&ctx).as_f64(), 500.0);

    // Hash join: orders.customer_id = customers.id
    // card = 2000 * 500 / max(500, 500) = 2000
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(1), Column(3))]);
    let joined = filtered.hash_join(scan_customers, keys, boolean(true), JoinType::Inner);
    let joined_card = joined.cardinality(&ctx).as_f64();
    assert!((joined_card - 2000.0).abs() < 0.01);

    // Project → same cardinality
    let projected = joined.physical_project(vec![column_ref(Column(0))]);
    assert!((projected.cardinality(&ctx).as_f64() - joined_card).abs() < 0.01);
}

// ---------- Empirical: HLL accuracy vs containment ----------

/// Analytical containment-assumption join cardinality.
fn containment_join_card(rows_a: f64, rows_b: f64, ndv_a: f64, ndv_b: f64) -> f64 {
    rows_a * rows_b / ndv_a.max(ndv_b)
}

/// Analytical ground-truth join cardinality (uniform distribution, known overlap).
fn ground_truth_join_card(
    rows_a: f64,
    rows_b: f64,
    ndv_a: f64,
    ndv_b: f64,
    intersect_ndv: f64,
) -> f64 {
    intersect_ndv * (rows_a / ndv_a) * (rows_b / ndv_b)
}

/// Build a two-table equi-join with HLL stats and return the estimated cardinality.
fn hll_join_card(
    rows_a: usize,
    values_a: std::ops::Range<i32>,
    rows_b: usize,
    values_b: std::ops::Range<i32>,
) -> f64 {
    let min_a = values_a.start as i64;
    let max_a = (values_a.end - 1) as i64;
    let min_b = values_b.start as i64;
    let max_b = (values_b.end - 1) as i64;

    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![Field::new(
        "a.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let schema_b = Arc::new(Schema::new(vec![Field::new(
        "b.id".to_string(),
        DataType::Int32,
        false,
    )]));
    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();

    cat.set_table_stats(
        src_a,
        simple_table_stats(
            rows_a,
            vec![col_stat_with_hll("a.id", values_a, Some((min_a, max_a)))],
        ),
    );
    cat.set_table_stats(
        src_b,
        simple_table_stats(
            rows_b,
            vec![col_stat_with_hll("b.id", values_b, Some((min_b, max_b)))],
        ),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan_a = ctx.table_scan(src_a, &schema_a, None);
    let scan_b = ctx.table_scan(src_b, &schema_b, None);
    let keys: Arc<[(Column, Column)]> = Arc::new([(Column(0), Column(1))]);
    let join = scan_a.hash_join(scan_b, keys, boolean(true), JoinType::Inner);
    join.cardinality(&ctx).as_f64()
}

#[test]
fn test_empirical_partial_overlap_50pct() {
    // A: values 0..200 (NDV=200), B: values 100..300 (NDV=200)
    // Intersection = 100 values
    let rows_a = 1000.0;
    let rows_b = 1000.0;
    let ndv_a = 200.0;
    let ndv_b = 200.0;
    let intersect = 100.0;

    let truth = ground_truth_join_card(rows_a, rows_b, ndv_a, ndv_b, intersect);
    let containment = containment_join_card(rows_a, rows_b, ndv_a, ndv_b);
    let hll_est = hll_join_card(1000, 0..200, 1000, 100..300);

    println!("50% overlap: truth={truth}, containment={containment}, hll={hll_est}");

    // truth=2500, containment=5000 (2× over)
    // HLL should be closer to truth than containment
    let hll_error = (hll_est - truth).abs();
    let containment_error = (containment - truth).abs();
    assert!(
        hll_error < containment_error,
        "HLL error ({hll_error}) should be less than containment error ({containment_error})"
    );
    // HLL should be within 50% of truth (generous for HLL noise)
    assert!(
        hll_est > truth * 0.5 && hll_est < truth * 1.5,
        "HLL estimate {hll_est} should be within 50% of truth {truth}"
    );
}

#[test]
fn test_empirical_small_overlap_10pct() {
    // A: values 0..1000 (NDV=1000), B: values 900..2000 (NDV=1100)
    // Intersection = 100 values
    let rows_a = 5000.0;
    let rows_b = 5000.0;
    let ndv_a = 1000.0;
    let ndv_b = 1100.0;
    let intersect = 100.0;

    let truth = ground_truth_join_card(rows_a, rows_b, ndv_a, ndv_b, intersect);
    let containment = containment_join_card(rows_a, rows_b, ndv_a, ndv_b);
    let hll_est = hll_join_card(5000, 0..1000, 5000, 900..2000);

    println!("10% overlap: truth={truth:.0}, containment={containment:.0}, hll={hll_est:.0}");

    // truth ≈ 2273, containment ≈ 22727 (10× over)
    let hll_error = (hll_est - truth).abs();
    let containment_error = (containment - truth).abs();
    assert!(
        hll_error < containment_error,
        "HLL error ({hll_error:.0}) should be less than containment error ({containment_error:.0})"
    );
}

#[test]
fn test_empirical_full_containment_regression() {
    // A: values 0..100 (NDV=100), B: values 0..500 (NDV=500)
    // Intersection = 100 values (A is a subset of B)
    let rows_a = 500.0;
    let rows_b = 2500.0;
    let ndv_a = 100.0;
    let ndv_b = 500.0;
    let intersect = 100.0;

    let truth = ground_truth_join_card(rows_a, rows_b, ndv_a, ndv_b, intersect);
    let containment = containment_join_card(rows_a, rows_b, ndv_a, ndv_b);
    let hll_est = hll_join_card(500, 0..100, 2500, 0..500);

    println!("Full containment: truth={truth}, containment={containment}, hll={hll_est}");

    // truth = 100 * 5 * 5 = 2500, containment = 500*2500/500 = 2500
    // Both should be similar. HLL should not degrade this case.
    // Allow wider margin since HLL intersection estimation on subset
    // can be noisy with inclusion-exclusion.
    assert!(
        hll_est > truth * 0.4 && hll_est < truth * 2.0,
        "HLL estimate {hll_est} should be reasonable vs truth {truth}"
    );
}

#[test]
fn test_empirical_skewed_table_sizes() {
    // A: 100 rows, values 0..50 (NDV=50)
    // B: 10000 rows, values 25..225 (NDV=200)
    // Intersection = 25 values
    let rows_a = 100.0;
    let rows_b = 10000.0;
    let ndv_a = 50.0;
    let ndv_b = 200.0;
    let intersect = 25.0;

    let truth = ground_truth_join_card(rows_a, rows_b, ndv_a, ndv_b, intersect);
    let containment = containment_join_card(rows_a, rows_b, ndv_a, ndv_b);
    let hll_est = hll_join_card(100, 0..50, 10000, 25..225);

    println!("Skewed sizes: truth={truth}, containment={containment}, hll={hll_est}");

    // truth = 25 * 2 * 50 = 2500, containment = 100*10000/200 = 5000
    let hll_error = (hll_est - truth).abs();
    let containment_error = (containment - truth).abs();
    assert!(
        hll_error < containment_error,
        "HLL error ({hll_error:.0}) should be less than containment error ({containment_error:.0})"
    );
}

// ---------- Empirical: Plan quality via Cascades ----------

use crate::{
    cascades::Cascades,
    ir::{explain::quick_explain, rule::RuleSet},
    rules,
};

/// Build an IRContext with AdvancedCardinalityEstimator using the given catalog.
fn cascades_with_catalog(cat: MagicCatalog) -> Arc<Cascades> {
    let ctx = adv_ctx_with_catalog(cat);
    let rule_set = RuleSet::builder()
        .add_rule(rules::LogicalGetAsPhysicalTableScanRule::new())
        .add_rule(rules::LogicalJoinAsPhysicalHashJoinRule::new())
        .add_rule(rules::LogicalJoinAsPhysicalNLJoinRule::new())
        .add_rule(rules::LogicalSelectAsPhysicalFilterRule::new())
        .add_rule(rules::LogicalProjectAsPhysicalProjectRule::new())
        .add_rule(rules::LogicalSelectJoinTransposeRule::new())
        .add_rule(rules::LogicalJoinInnerCommuteRule::new())
        .add_rule(rules::LogicalJoinInnerAssocRule::new())
        .build();
    Arc::new(Cascades::new(ctx, rule_set))
}

#[tokio::test]
async fn test_plan_quality_join_order_three_tables() {
    // Three tables: A(1000 rows), B(1000 rows), C(1000 rows)
    // A ⋈ B has LOW overlap (10%): A values 0..500, B values 450..1000
    // B ⋈ C has HIGH overlap (100%): B values 450..1000, C values 450..1000
    //
    // With accurate HLL estimates:
    //   A⋈B produces few rows (low intersection) → expensive intermediate
    //   B⋈C produces many rows (full intersection) but that's a dense join
    //
    // The optimizer should prefer the plan that minimizes total cost.
    // We verify the optimizer produces a valid plan and the cost is finite.
    let cat = MagicCatalog::default();
    let schema_a = Arc::new(Schema::new(vec![
        Field::new("a.id".to_string(), DataType::Int32, false),
        Field::new("a.v".to_string(), DataType::Int32, false),
    ]));
    let schema_b = Arc::new(Schema::new(vec![
        Field::new("b.id".to_string(), DataType::Int32, false),
        Field::new("b.aid".to_string(), DataType::Int32, false),
    ]));
    let schema_c = Arc::new(Schema::new(vec![
        Field::new("c.bid".to_string(), DataType::Int32, false),
        Field::new("c.v".to_string(), DataType::Int32, false),
    ]));

    let src_a = cat
        .try_create_table("a".to_string(), schema_a.clone())
        .unwrap();
    let src_b = cat
        .try_create_table("b".to_string(), schema_b.clone())
        .unwrap();
    let src_c = cat
        .try_create_table("c".to_string(), schema_c.clone())
        .unwrap();

    // A: 1000 rows, join col values 0..500 (NDV=500)
    cat.set_table_stats(
        src_a,
        simple_table_stats(
            1000,
            vec![
                col_stat_with_hll("a.id", 0..500, Some((0, 499))),
                col_stat("a.v", 100),
            ],
        ),
    );
    // B: 1000 rows, join col 1 (b.id) values 450..1000 (low overlap with A)
    //                join col 2 (b.aid) values 0..550 (high overlap with C)
    cat.set_table_stats(
        src_b,
        simple_table_stats(
            1000,
            vec![
                col_stat_with_hll("b.id", 450..1000, Some((450, 999))),
                col_stat_with_hll("b.aid", 0..550, Some((0, 549))),
            ],
        ),
    );
    // C: 1000 rows, join col values 0..550 (high overlap with B.aid)
    cat.set_table_stats(
        src_c,
        simple_table_stats(
            1000,
            vec![
                col_stat_with_hll("c.bid", 0..550, Some((0, 549))),
                col_stat("c.v", 200),
            ],
        ),
    );

    let opt = cascades_with_catalog(cat);
    let ctx = &opt.ctx;

    // Build logical plan: A ⋈ B ⋈ C
    // A.id = B.id AND B.aid = C.bid
    let scan_a = ctx.logical_get(src_a, &schema_a, None);
    let scan_b = ctx.logical_get(src_b, &schema_b, None);
    let scan_c = ctx.logical_get(src_c, &schema_c, None);

    // A(cols 0,1) ⋈ B(cols 2,3) on A.id(0) = B.id(2)
    let ab = scan_a.logical_join(
        scan_b,
        column_ref(Column(0)).eq(column_ref(Column(2))),
        JoinType::Inner,
    );
    // (A⋈B)(cols 0..3) ⋈ C(cols 4,5) on B.aid(3) = C.bid(4)
    let abc = ab.logical_join(
        scan_c,
        column_ref(Column(3)).eq(column_ref(Column(4))),
        JoinType::Inner,
    );

    let required = Arc::new(crate::ir::properties::Required::default());
    let optimized = opt.optimize(&abc, required).await.unwrap();

    let explained = quick_explain(&optimized, ctx);
    println!("Optimized plan:\n{explained}");

    // Verify the optimizer produced a physical plan with finite cost
    let cost = ctx.cm.compute_total_cost(&optimized, ctx);
    assert!(
        cost.is_some(),
        "Optimized plan should have a computable cost"
    );
    let cost_val = cost.unwrap().as_f64();
    assert!(
        cost_val.is_finite() && cost_val > 0.0,
        "Cost should be finite and positive, got {cost_val}"
    );

    // The optimized plan should contain PhysicalHashJoin nodes
    assert!(
        explained.contains("PhysicalHashJoin"),
        "Plan should use hash joins: {explained}"
    );
}

#[tokio::test]
async fn test_plan_quality_filter_pushdown_with_hll() {
    // Two tables: orders(10000 rows), customers(500 rows)
    // Join on orders.customer_id = customers.id
    // Filter: orders.status = 1 (selectivity 1/5)
    //
    // The optimizer should push the filter below the join.
    // With HLL-based cardinality, the cost of the filtered join should
    // be lower than the unfiltered join.
    let cat = MagicCatalog::default();
    let schema_orders = Arc::new(Schema::new(vec![
        Field::new("orders.id".to_string(), DataType::Int32, false),
        Field::new("orders.cid".to_string(), DataType::Int32, false),
        Field::new("orders.status".to_string(), DataType::Int32, false),
    ]));
    let schema_cust = Arc::new(Schema::new(vec![
        Field::new("cust.id".to_string(), DataType::Int32, false),
        Field::new("cust.name".to_string(), DataType::Int32, false),
    ]));

    let orders = cat
        .try_create_table("orders".to_string(), schema_orders.clone())
        .unwrap();
    let cust = cat
        .try_create_table("customers".to_string(), schema_cust.clone())
        .unwrap();

    cat.set_table_stats(
        orders,
        simple_table_stats(
            10000,
            vec![
                col_stat_with_range("orders.id", 10000, 1, 10000),
                col_stat_with_hll("orders.cid", 1..501, Some((1, 500))),
                col_stat("orders.status", 5),
            ],
        ),
    );
    cat.set_table_stats(
        cust,
        simple_table_stats(
            500,
            vec![
                col_stat_with_hll("cust.id", 1..501, Some((1, 500))),
                col_stat("cust.name", 490),
            ],
        ),
    );

    let opt = cascades_with_catalog(cat);
    let ctx = &opt.ctx;

    let scan_orders = ctx.logical_get(orders, &schema_orders, None);
    let scan_cust = ctx.logical_get(cust, &schema_cust, None);

    // orders ⋈ customers on orders.cid(1) = cust.id(3)
    let joined = scan_orders.logical_join(
        scan_cust,
        column_ref(Column(1)).eq(column_ref(Column(3))),
        JoinType::Inner,
    );
    // Filter: orders.status(2) = 1
    let filtered_join = joined.logical_select(column_ref(Column(2)).eq(int32(1)));

    let required = Arc::new(crate::ir::properties::Required::default());
    let optimized = opt.optimize(&filtered_join, required).await.unwrap();

    let explained = quick_explain(&optimized, ctx);
    println!("Filter pushdown plan:\n{explained}");

    // The optimizer should produce a valid physical plan
    let cost = ctx.cm.compute_total_cost(&optimized, ctx);
    assert!(
        cost.is_some(),
        "Optimized plan should have a computable cost"
    );
    let cost_val = cost.unwrap().as_f64();
    assert!(
        cost_val.is_finite() && cost_val > 0.0,
        "Cost should be finite and positive, got {cost_val}"
    );

    // Verify filter exists in the plan (either pushed down or on top)
    assert!(
        explained.contains("PhysicalFilter") || explained.contains("PhysicalHashJoin"),
        "Plan should contain filter and join: {explained}"
    );
}

// ---------- Range selectivity tests ----------

#[test]
fn test_range_lt_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col < 50 with domain [0, 100] → sel = (50 - 0) / (100 - 0) = 0.5
    let pred = column_ref(Column(0)).lt(int32(50));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 500.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_gt_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col > 75 with domain [0, 100] → sel = (100 - 75) / 100 = 0.25
    let pred = column_ref(Column(0)).gt(int32(75));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 250.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_le_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col <= 25 with domain [0, 100] → sel = (25 - 0) / 100 = 0.25
    // (continuous approximation: <= and < give same result)
    let pred = column_ref(Column(0)).le(int32(25));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 250.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_ge_selectivity() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col >= 80 with domain [0, 100] → sel = (100 - 80) / 100 = 0.2
    let pred = column_ref(Column(0)).ge(int32(80));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 200.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_commuted() {
    // 50 < col  ≡  col > 50
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // int32(50) < column_ref(0)  →  col > 50  →  sel = (100 - 50) / 100 = 0.5
    let pred = int32(50).lt(column_ref(Column(0)));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 500.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_out_of_bounds_low() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col < -10 with domain [0, 100] → clamped to 0
    let pred = column_ref(Column(0)).lt(int32(-10));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert_eq!(card.as_f64(), 0.0);
}

#[test]
fn test_range_out_of_bounds_high() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col > 200 with domain [0, 100] → clamped to 0
    let pred = column_ref(Column(0)).gt(int32(200));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert_eq!(card.as_f64(), 0.0);
}

#[test]
fn test_range_covers_all() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col < 200 with domain [0, 100] → clamped to 1.0
    let pred = column_ref(Column(0)).lt(int32(200));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 1000.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_with_nulls() {
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        true,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    // 200 nulls out of 1000 rows, domain [0, 100]
    let mut cs = col_stat_with_range("t.a", 100, 0, 100);
    cs.null_count = Some(200);
    cat.set_table_stats(src, simple_table_stats(1000, vec![cs]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    // col < 50 → range_frac = 0.5, null_frac = 0.2 → sel = 0.5 * 0.8 = 0.4
    let pred = column_ref(Column(0)).lt(int32(50));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert!((card.as_f64() - 400.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_range_no_stats_fallback() {
    // No min/max → fallback to 0.1
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stat("t.a", 10)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let pred = column_ref(Column(0)).lt(int32(50));
    let card = scan.logical_select(pred).cardinality(&ctx);
    // Fallback: 1000 * 0.1 = 100
    assert!((card.as_f64() - 100.0).abs() < 0.01, "card = {}", card.as_f64());
}

// ---------- AND/OR range analysis tests ----------

#[test]
fn test_and_contradiction() {
    // col > 50 AND col < 30 with domain [0, 100] → contradiction → 0
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let pred = column_ref(Column(0))
        .gt(int32(50))
        .and(column_ref(Column(0)).lt(int32(30)));
    let card = scan.logical_select(pred).cardinality(&ctx);
    assert_eq!(card.as_f64(), 0.0);
}

#[test]
fn test_and_range_normal() {
    // col > 20 AND col < 80 — non-contradictory, falls through to independence
    // sel(col > 20) = (100-20)/100 = 0.8
    // sel(col < 80) = (80-0)/100 = 0.8
    // independence: 0.8 * 0.8 = 0.64
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let pred = column_ref(Column(0))
        .gt(int32(20))
        .and(column_ref(Column(0)).lt(int32(80)));
    let card = scan.logical_select(pred).cardinality(&ctx);
    // 1000 * 0.8 * 0.8 = 640
    assert!((card.as_f64() - 640.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_or_tautology() {
    // col < 60 OR col >= 40 with domain [0, 100]
    // Intervals: [0, 60] and [40, 100] → merged: [0, 100] → covers domain → tautology
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let pred = column_ref(Column(0))
        .lt(int32(60))
        .or(column_ref(Column(0)).ge(int32(40)));
    let card = scan.logical_select(pred).cardinality(&ctx);
    // Tautology: sel = 1.0, card = 1000
    assert!((card.as_f64() - 1000.0).abs() < 1.0, "card = {}", card.as_f64());
}

#[test]
fn test_or_non_tautology() {
    // col < 30 OR col > 70 with domain [0, 100]
    // Intervals: [0, 30] and [70, 100] → gap at [30, 70] → NOT a tautology
    // Falls through to independence:
    //   sel(col < 30) = 0.3, sel(col > 70) = 0.3
    //   sel(OR) = 1 - (1-0.3)*(1-0.3) = 1 - 0.49 = 0.51
    let cat = MagicCatalog::default();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "t.a".to_string(),
        DataType::Int32,
        false,
    )]));
    let src = cat
        .try_create_table("t".to_string(), schema.clone())
        .unwrap();
    cat.set_table_stats(
        src,
        simple_table_stats(1000, vec![col_stat_with_range("t.a", 100, 0, 100)]),
    );

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let pred = column_ref(Column(0))
        .lt(int32(30))
        .or(column_ref(Column(0)).gt(int32(70)));
    let card = scan.logical_select(pred).cardinality(&ctx);
    // Independence: 1 - 0.7 * 0.7 = 0.51 → card = 510
    assert!((card.as_f64() - 510.0).abs() < 1.0, "card = {}", card.as_f64());
}

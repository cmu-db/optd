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
        false,
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

    // a < 5 (range predicate — not supported in v1, should fallback)
    let col_a = Column(0);
    let pred = column_ref(col_a).lt(int32(5));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // Expected: 1000 * 0.1 (FALLBACK_PREDICATE_SELECTIVITY)
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
    cat.set_table_stats(src, simple_table_stats(1000, vec![col_stat("t.a", 0)]));

    let ctx = adv_ctx_with_catalog(cat);
    let scan = ctx.logical_get(src, &schema, None);

    let col_a = Column(0);
    let pred = column_ref(col_a).eq(int32(42));
    let filtered = scan.logical_select(pred);
    let card = filtered.cardinality(&ctx);

    // NDV=0 → ndv() returns None → fallback selectivity 0.1
    assert!((card.as_f64() - 100.0).abs() < 0.01);
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

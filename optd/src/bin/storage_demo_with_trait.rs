use diesel::prelude::*;

use dotenvy::dotenv;
use optd::storage::{
    models::{
        common::JoinType,
        logical_expr::LogicalExpr,
        logical_operators::{LogicalFilter, LogicalJoin, LogicalOpKind, LogicalScan},
        physical_operators::PhysicalOpKind,
    },
    StorageManager,
};

fn demo(mut storage: StorageManager) -> anyhow::Result<()> {
    // - LogicalFilter (on: t1.v2 = 'foo')
    //   - LogicalJoin (inner, on t1.v1 = t2.v1)
    //     - LogicalScan (t1)
    //     - LogicalScan (t2)
    let scan1 = LogicalExpr::Scan(LogicalScan {
        table_name: "t1".to_string(),
    });

    let scan2 = LogicalExpr::Scan(LogicalScan {
        table_name: "t2".to_string(),
    });

    let (scan_1_expr_id, scan_1_group) = storage.add_logical_expr(scan1);
    let (scan_2_expr_id, scan_2_group) = storage.add_logical_expr(scan2);

    let join = LogicalExpr::Join(LogicalJoin {
        join_type: JoinType::Inner,
        left: scan_1_group,
        right: scan_2_group,
        join_cond: "t1.v1 = t2.v1".to_string(),
    });

    let join_alt = LogicalExpr::Join(LogicalJoin {
        join_type: JoinType::Inner,
        left: scan_2_group,
        right: scan_1_group,
        join_cond: "t1.v1 = t2.v1".to_string(),
    });

    let (join_expr_id, join_group) = storage.add_logical_expr(join);

    let join_alt_expr_id = storage.add_logical_expr_to_group(join_alt, join_group);

    let filter = LogicalExpr::Filter(LogicalFilter {
        child: join_group,
        predicate: "t1.v2 = 'foo'".to_string(),
    });

    let (filter_expr_id, filter_group) = storage.add_logical_expr(filter);

    let exprs = storage.get_all_logical_exprs_in_group(filter_group);
    assert_eq!(exprs[0].id, filter_expr_id);
    assert_eq!(
        storage.get_logical_expr_identifiers(&exprs[0].inner),
        Some((filter_expr_id, filter_group))
    );
    let child_group = match &exprs[0].inner {
        LogicalExpr::Filter(filter) => {
            println!("filter: {:?}", filter);
            filter.child
        }
        _ => unreachable!(),
    };

    let exprs = storage.get_all_logical_exprs_in_group(child_group);
    assert_eq!(
        storage.get_logical_expr_identifiers(&exprs[0].inner),
        Some((join_expr_id, join_group))
    );
    assert_eq!(
        storage.get_logical_expr_identifiers(&exprs[1].inner),
        Some((join_alt_expr_id, join_group))
    );

    let (left_group, right_group) = match &exprs[0].inner {
        LogicalExpr::Join(join) => {
            println!("join: {:?}", join);
            (join.left, join.right)
        }
        _ => unreachable!(),
    };

    let (left_group_alt, right_group_alt) = match &exprs[1].inner {
        LogicalExpr::Join(join) => {
            println!("join: {:?}", join);
            (join.left, join.right)
        }
        _ => unreachable!(),
    };

    assert_eq!(left_group, right_group_alt);
    assert_eq!(right_group, left_group_alt);

    let exprs = storage.get_all_logical_exprs_in_group(left_group);
    assert_eq!(exprs[0].id, scan_1_expr_id);
    assert_eq!(
        storage.get_logical_expr_identifiers(&exprs[0].inner),
        Some((scan_1_expr_id, scan_1_group))
    );
    match &exprs[0].inner {
        LogicalExpr::Scan(scan) => {
            println!("scan: {:?}", scan);
        }
        _ => unreachable!(),
    }

    let exprs = storage.get_all_logical_exprs_in_group(right_group);
    assert_eq!(exprs[0].id, scan_2_expr_id);
    assert_eq!(
        storage.get_logical_expr_identifiers(&exprs[0].inner),
        Some((scan_2_expr_id, scan_2_group))
    );
    match &exprs[0].inner {
        LogicalExpr::Scan(scan) => {
            println!("scan: {:?}", scan);
        }
        _ => unreachable!(),
    }

    assert_eq!(child_group, join_group);

    Ok(())
}

fn main() -> anyhow::Result<()> {
    // CREATE TABLE t1(v1 INTEGER, v2 TEXT);
    // CREATE TABLE t2(v1 INTEGER, v2 TEXT);
    // SELECT * from t1 inner join t2 on t1.v1 = t2.v1 where t1.v2 = 'foo';
    // - LogicalFilter (on: t1.v2 = 'foo')
    //   - LogicalJoin (inner, on t1.v1 = t2.v1)
    //     - LogicalScan (t1)
    //     - LogicalScan (t2)
    dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut storage = StorageManager::new(&database_url)?;
    storage.migration_run()?;
    {
        use optd::storage::schema::logical_op_kinds::dsl::*;
        let kinds = logical_op_kinds
            .select(LogicalOpKind::as_select())
            .load(&mut storage.conn)?;

        println!("logical operator support (n={})", kinds.len());
        for kind in kinds {
            println!("+ {}", kind.name);
        }
    }

    {
        use optd::storage::schema::physical_op_kinds::dsl::*;
        let descs = physical_op_kinds
            .select(PhysicalOpKind::as_select())
            .load(&mut storage.conn)?;

        println!("physical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }
    // manual_demo(storage)?;
    demo(storage)?;
    Ok(())
}

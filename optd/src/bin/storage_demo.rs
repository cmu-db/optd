use std::env;

use diesel::prelude::*;
use dotenvy::dotenv;
use optd::storage::{
    models::{
        common::JoinType,
        logical_expr::LogicalExprId,
        logical_operators::{LogicalFilter, LogicalJoin, LogicalOpDesc, LogicalOpDescId},
        physical_operators::PhysicalOpDesc,
        rel_group::RelGroupId,
    },
    schema::{logical_filters, logical_joins, logical_op_descs, logical_scans},
    StorageManager,
};

fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut storage = StorageManager::new(&database_url)?;
    storage.migration_run()?;
    {
        use optd::storage::schema::logical_op_descs::dsl::*;
        let descs = logical_op_descs
            .select(LogicalOpDesc::as_select())
            .load(&mut storage.conn)?;

        println!("logical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }

    {
        use optd::storage::schema::physical_op_descs::dsl::*;
        let descs = physical_op_descs
            .select(PhysicalOpDesc::as_select())
            .load(&mut storage.conn)?;

        println!("physical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }

    {
        use optd::storage::schema::physical_op_descs::dsl::*;
        let descs = physical_op_descs
            .select(PhysicalOpDesc::as_select())
            .load(&mut storage.conn)?;

        println!("physical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }

    // CREATE TABLE t1(v1 INTEGER, v2 TEXT);
    // CREATE TABLE t2(v1 INTEGER, v2 TEXT);
    // SELECT * from t1 inner join t2 on t1.v1 = t2.v1 where t1.v2 = 'foo';
    // - LogicalFilter (on: t1.v2 = 'foo')
    //   - LogicalJoin (inner, on t1.v1 = t2.v1)
    //     - LogicalScan (t1)
    //     - LogicalScan (t2)

    let logical_scan_id = logical_op_descs::table
        .filter(logical_op_descs::name.eq("LogicalScan"))
        .select(logical_op_descs::id)
        .first::<LogicalOpDescId>(&mut storage.conn)?;

    let logical_join_id = logical_op_descs::table
        .filter(logical_op_descs::name.eq("LogicalJoin"))
        .select(logical_op_descs::id)
        .first::<LogicalOpDescId>(&mut storage.conn)?;

    let logical_filter_id = logical_op_descs::table
        .filter(logical_op_descs::name.eq("LogicalFilter"))
        .select(logical_op_descs::id)
        .first::<LogicalOpDescId>(&mut storage.conn)?;

    let scan1_group_id = {
        // - LogicalScan (t1)
        use optd::storage::schema::logical_exprs;
        use optd::storage::schema::rel_groups;
        let rel_group_id = diesel::insert_into(rel_groups::table)
            .default_values()
            .returning(rel_groups::id)
            .get_result::<RelGroupId>(&mut storage.conn)?;

        println!("created group_id={:?}", rel_group_id);

        let logical_expr_id = diesel::insert_into(logical_exprs::table)
            .values((
                logical_exprs::logical_op_desc_id.eq(logical_scan_id),
                logical_exprs::group_id.eq(rel_group_id),
            ))
            .returning(logical_exprs::id)
            .get_result::<LogicalExprId>(&mut storage.conn)?;

        diesel::insert_into(logical_scans::table)
            .values((
                logical_scans::logical_expr_id.eq(logical_expr_id),
                logical_scans::table_name.eq("t1"),
            ))
            .execute(&mut storage.conn)?;
        rel_group_id
    };

    let scan2_group_id = {
        // - LogicalScan (t2)
        use optd::storage::schema::logical_exprs;
        use optd::storage::schema::rel_groups;
        let rel_group_id = diesel::insert_into(rel_groups::table)
            .default_values()
            .returning(rel_groups::id)
            .get_result::<RelGroupId>(&mut storage.conn)?;

        println!("created group_id={:?}", rel_group_id);

        let logical_expr_id = diesel::insert_into(logical_exprs::table)
            .values((
                logical_exprs::logical_op_desc_id.eq(logical_scan_id),
                logical_exprs::group_id.eq(rel_group_id),
            ))
            .returning(logical_exprs::id)
            .get_result::<LogicalExprId>(&mut storage.conn)?;

        diesel::insert_into(logical_scans::table)
            .values((
                logical_scans::logical_expr_id.eq(logical_expr_id),
                logical_scans::table_name.eq("t2"),
            ))
            .execute(&mut storage.conn)?;
        rel_group_id
    };

    let join_group_id = {
        // - LogicalJoin (inner, on t1.v1 = t2.v1)
        use optd::storage::schema::logical_exprs;
        use optd::storage::schema::rel_groups;
        let rel_group_id = diesel::insert_into(rel_groups::table)
            .default_values()
            .returning(rel_groups::id)
            .get_result::<RelGroupId>(&mut storage.conn)?;

        println!("created group_id={:?}", rel_group_id);

        let logical_expr_id = diesel::insert_into(logical_exprs::table)
            .values((
                logical_exprs::logical_op_desc_id.eq(logical_join_id),
                logical_exprs::group_id.eq(rel_group_id),
            ))
            .returning(logical_exprs::id)
            .get_result::<LogicalExprId>(&mut storage.conn)?;

        let join = LogicalJoin {
            logical_expr_id,
            join_type: JoinType::Inner,
            left: scan1_group_id,
            right: scan2_group_id,
            join_cond: "t1.v1 = t2.v1".to_string(),
        };

        diesel::insert_into(logical_joins::table)
            .values(join)
            .execute(&mut storage.conn)?;

        rel_group_id
    };

    {
        // - LogicalFilter (on: t1.v2 = 'foo')
        use optd::storage::schema::logical_exprs;
        use optd::storage::schema::rel_groups;
        let rel_group_id = diesel::insert_into(rel_groups::table)
            .default_values()
            .returning(rel_groups::id)
            .get_result::<RelGroupId>(&mut storage.conn)?;

        println!("created group_id={:?}", rel_group_id);

        let logical_expr_id = diesel::insert_into(logical_exprs::table)
            .values((
                logical_exprs::logical_op_desc_id.eq(logical_filter_id),
                logical_exprs::group_id.eq(rel_group_id),
            ))
            .returning(logical_exprs::id)
            .get_result::<LogicalExprId>(&mut storage.conn)?;

        let filter = LogicalFilter {
            logical_expr_id,
            child: join_group_id,
            predicate: "t1.v2 = 'foo'".to_string(),
        };

        diesel::insert_into(logical_filters::table)
            .values(filter)
            .execute(&mut storage.conn)?;
    };

    // Run `sqlite3 test_memo.db` and follow query gives you all logical exprs in the database.
    // select l.id, l.group_id, l.created_at, desc.name
    // from logical_exprs as l, logical_op_descs as desc
    // where l.logical_op_desc_id = desc.id;

    Ok(())
}

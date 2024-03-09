use anyhow::Result;
use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use optd_sqlplannertest::DatafusionDb;
use postgres_db::PostgresDb;

use crate::{
    benchmark::Benchmark,
    tpch::{TpchConfig, TpchKit, TPCH_KIT_POSTGRES},
};

mod benchmark;
mod cardtest;
mod datafusion_db_cardtest;
mod postgres_db;
mod shell;
mod tpch;

#[tokio::main]
async fn main() -> Result<()> {
    let pg_db = PostgresDb::build(true).await?;
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![Box::new(pg_db)];
    let cardtest_runner = CardtestRunner::new(databases).await?;
    let tpch_config = TpchConfig {
        database: String::from(TPCH_KIT_POSTGRES),
        scale_factor: 1,
        seed: 15721,
    };
    let tpch_benchmark = Benchmark::Tpch(tpch_config.clone());
    cardtest_runner.load_databases(tpch_benchmark).await?;
    let qerrors = cardtest_runner.eval_qerrors("select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;").await?;
    println!("qerrors: {:?}", qerrors);
    Ok(())
}

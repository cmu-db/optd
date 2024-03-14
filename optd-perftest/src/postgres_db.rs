use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBHelper,
    shell,
    tpch::{TpchConfig, TpchKit},
};
use async_trait::async_trait;
use regex::Regex;
use std::fs;
use tokio_postgres::{Client, NoTls};

/// This dbname is assumed to always exist
const DEFAULT_DBNAME: &str = "postgres";

pub struct PostgresDb {}

/// Conventions I keep for methods of this class:
///   - Functions should be idempotent. For instance, start_postgres() should not fail if Postgres is already running
///       - For instance, this is why "createdb" is _not_ a function
///   - Stop and start functions should be separate
///   - Setup should be done in build() unless it requires more information (like benchmark)
impl PostgresDb {
    pub fn new() -> Self {
        Self {}
    }

    /// Create a connection to a Postgres database
    async fn connect_to_db(dbname: &str) -> anyhow::Result<Client> {
        let (client, connection) =
            tokio_postgres::connect(&format!("host=localhost dbname={}", dbname), NoTls)
                .await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
            println!("dropping connection");
        });
        Ok(client)
    }

    /// Check whether a certain database exists
    async fn get_does_db_exist(client: &Client, dbname: &str) -> anyhow::Result<bool> {
        let result = client.query(&format!("SELECT FROM pg_database WHERE datname = '{}'", dbname), &[]).await?;
        Ok(result.len() > 0)
    }

    async fn load_benchmark_data(benchmark: &Benchmark) -> anyhow::Result<()> {
        let dbname = benchmark.get_dbname();
        // since we don't know whether dbname exists at this point, we have to connect to the default database
        let default_db_client = Self::connect_to_db(DEFAULT_DBNAME).await?;
        // determine whether we should load the data
        // if so, get the state of the system to one where data can be loaded (i.e. `dbname` doesn't exist)
        let does_db_exist = Self::get_does_db_exist(&default_db_client, &dbname).await?;
        let should_load = if benchmark.is_readonly() {
            !does_db_exist
        } else {
            if does_db_exist {
                default_db_client.query(&format!("DROP DATABASE {}", dbname), &[]).await?;
            }
            true
        };
        if should_load {
            log::debug!("[start] loading benchmark data");
            default_db_client.query(&format!("CREATE DATABASE {}", dbname), &[]).await?;
            drop(default_db_client);
            // now that we've created `dbname`, we can connect to that
            let client = Self::connect_to_db(&dbname).await?;
            match benchmark {
                Benchmark::Tpch(tpch_config) => Self::load_tpch_data(&client, &dbname, tpch_config).await?,
                _ => unimplemented!(),
            };
            log::debug!("[end] loading benchmark data");
        } else {
            log::debug!("[skip] loading benchmark data");
        }
        Ok(())
    }

    /// Load the TPC-H data assuming the dbname database already exists
    async fn load_tpch_data(client: &Client, dbname: &str, tpch_config: &TpchConfig) -> anyhow::Result<()> {
        // load the schema
        let tpch_kit = TpchKit::build()?;
        // TODO(phw2): factor out psql and change function doc
        shell::run_command_with_status_check(&format!(
            "psql {} -f {}",
            dbname,
            tpch_kit.schema_fpath.to_str().unwrap()
        ))?;
        // load the tables
        tpch_kit.gen_tables(tpch_config)?;
        let tbl_fpath_iter = tpch_kit.get_tbl_fpath_iter(tpch_config).unwrap();
        for tbl_fpath in tbl_fpath_iter {
            let tbl_name = tbl_fpath.file_stem().unwrap().to_str().unwrap();
            let copy_table_cmd = format!(
                "\\copy {} from {} csv delimiter '|'",
                tbl_name,
                tbl_fpath.to_str().unwrap()
            );
            shell::run_command_with_status_check(&format!(
                "psql {} -c \"{}\"",
                dbname, copy_table_cmd
            ))?;
        }
        Ok(())
    }
}

#[async_trait]
impl CardtestRunnerDBHelper for PostgresDb {
    fn get_name(&self) -> &str {
        "Postgres"
    }

    async fn eval_benchmark_estcards(
        &self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        Self::load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = Self::connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => Self::eval_tpch_estcards(&client, tpch_config).await,
        }
    }

    async fn eval_benchmark_truecards(
        &self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        Self::load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = Self::connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => Self::eval_tpch_truecards(&client, tpch_config).await,
        }
    }
}

/// This impl has helpers for ```impl CardtestRunnerDBHelper for PostgresDb```
impl PostgresDb {
    async fn eval_tpch_estcards(client: &Client, tpch_config: &TpchConfig) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build()?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut estcards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = Self::eval_query_estcard(client, &sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_tpch_truecards(client: &Client, tpch_config: &TpchConfig) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build()?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut truecards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let truecard = Self::eval_query_truecard(client, &sql).await?;
            truecards.push(truecard);
        }

        Ok(truecards)
    }

    async fn eval_query_estcard(client: &Client, sql: &str) -> anyhow::Result<usize> {
        let result = client
            .query(&format!("EXPLAIN {}", sql), &[])
            .await?;
        // the first line contains the explain of the root node
        let first_explain_line: &str = result.first().unwrap().get(0);
        let estcard = PostgresDb::extract_row_count(first_explain_line).unwrap();
        Ok(estcard)
    }

    async fn eval_query_truecard(client: &Client, sql: &str) -> anyhow::Result<usize> {
        let rows = client.query(sql, &[]).await?;
        let truecard = rows.len();
        Ok(truecard)
    }

    /// Extract the row count from a line of an EXPLAIN output
    fn extract_row_count(explain_line: &str) -> Option<usize> {
        let re = Regex::new(r"rows=(\d+)").unwrap();
        if let Some(caps) = re.captures(explain_line) {
            if let Some(matched) = caps.get(1) {
                let rows_str = matched.as_str();
                match rows_str.parse::<usize>() {
                    Ok(row_cnt) => Some(row_cnt),
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

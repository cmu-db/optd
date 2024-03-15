use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBHelper,
    tpch::{TpchConfig, TpchKit},
};
use async_trait::async_trait;
use regex::Regex;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use tokio_postgres::{Client, NoTls};

/// This dbname is assumed to always exist
const DEFAULT_DBNAME: &str = "postgres";

pub struct PostgresDb {
    workspace_dpath: PathBuf,
}

/// Conventions I keep for methods of this class:
///   - Functions should be idempotent. For instance, start_postgres() should not fail if Postgres is already running
///       - For instance, this is why "createdb" is _not_ a function
///   - Stop and start functions should be separate
///   - Setup should be done in build() unless it requires more information (like benchmark)
impl PostgresDb {
    pub fn new<P: AsRef<Path>>(workspace_dpath: P) -> Self {
        Self {
            workspace_dpath: PathBuf::from(workspace_dpath.as_ref()),
        }
    }

    /// Create a connection to a Postgres database
    async fn connect_to_db(dbname: &str) -> anyhow::Result<Client> {
        let (client, connection) =
            tokio_postgres::connect(&format!("host=localhost dbname={}", dbname), NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(client)
    }

    /// Check whether a certain database exists
    async fn get_does_db_exist(client: &Client, dbname: &str) -> anyhow::Result<bool> {
        let result = client
            .query(
                &format!("SELECT FROM pg_database WHERE datname = '{}'", dbname),
                &[],
            )
            .await?;
        Ok(!result.is_empty())
    }

    // Retrieves the location of pgdata
    async fn get_pgdata_dpath_str(client: &Client) -> anyhow::Result<String> {
        let row = client
            .query_one(
                "SELECT setting FROM pg_settings WHERE name = 'data_directory'",
                &[],
            )
            .await?;
        let pgdata_location: String = row.get("setting");
        Ok(pgdata_location)
    }

    async fn load_benchmark_data(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        let dbname = benchmark.get_dbname();
        // since we don't know whether dbname exists at this point, we have to connect to the default database
        let default_db_client = Self::connect_to_db(DEFAULT_DBNAME).await?;
        let pgdata_dpath_str = Self::get_pgdata_dpath_str(&default_db_client).await?;
        let pgdata_dpath = PathBuf::from(pgdata_dpath_str);
        let done_fname = format!("{}_done", dbname);
        let done_fpath = pgdata_dpath.join(done_fname);
        // determine whether we should load the data
        let should_load = if benchmark.is_readonly() {
            // we use the existence of done_fpath to indicate that loading was finished rather than
            // whether dbname exists as it's possible for dbname to be created by only partially loaded
            !done_fpath.exists()
        } else {
            true
        };
        if should_load {
            log::debug!("[start] loading benchmark data");
            // it's possible for the db to exist or not after we have determined we should load the data
            let does_db_exist = Self::get_does_db_exist(&default_db_client, &dbname).await?;
            if does_db_exist {
                default_db_client
                    .query(&format!("DROP DATABASE {}", dbname), &[])
                    .await?;
            }
            default_db_client
                .query(&format!("CREATE DATABASE {}", dbname), &[])
                .await?;
            drop(default_db_client);
            // now that we've created `dbname`, we can connect to that
            let client = Self::connect_to_db(&dbname).await?;
            match benchmark {
                Benchmark::Tpch(tpch_config) => self.load_tpch_data(&client, tpch_config).await?,
                _ => unimplemented!(),
            };
            File::create(done_fpath).await?;
            log::debug!("[end] loading benchmark data");
        } else {
            log::debug!("[skip] loading benchmark data");
        }
        Ok(())
    }

    /// Load the TPC-H data to the database that client is connected to
    async fn load_tpch_data(
        &self,
        client: &Client,
        tpch_config: &TpchConfig,
    ) -> anyhow::Result<()> {
        // set up TpchKit
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;

        // load the schema
        // we need to call make to ensure that the schema file exists
        // tpch_kit.make(TPCH_KIT_POSTGRES);
        let sql = fs::read_to_string(tpch_kit.schema_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;

        // load the tables
        tpch_kit.gen_tables(tpch_config)?;
        for tbl_fpath in tpch_kit.get_tbl_fpath_iter(tpch_config)? {
            let tbl_fpath_str = &tbl_fpath.to_str().unwrap();
            let tbl_name = TpchKit::get_tbl_name_from_tbl_fpath(&tbl_fpath);
            client
                .query(
                    &format!(
                        "COPY {} FROM '{}' WITH (FORMAT csv, DELIMITER '|')",
                        tbl_name, tbl_fpath_str
                    ),
                    &[],
                )
                .await?;
        }

        Ok(())
    }
}

#[async_trait]
impl CardtestRunnerDBHelper for PostgresDb {
    fn get_name(&self) -> &str {
        "Postgres"
    }

    async fn eval_benchmark_estcards(&self, benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = Self::connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_estcards(&client, tpch_config).await,
        }
    }

    async fn eval_benchmark_truecards(&self, benchmark: &Benchmark) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = Self::connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_truecards(&client, tpch_config).await,
        }
    }
}

/// This impl has helpers for ```impl CardtestRunnerDBHelper for PostgresDb```
impl PostgresDb {
    async fn eval_tpch_estcards(
        &self,
        client: &Client,
        tpch_config: &TpchConfig,
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut estcards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = Self::eval_query_estcard(client, &sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_tpch_truecards(
        &self,
        client: &Client,
        tpch_config: &TpchConfig,
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
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
        let result = client.query(&format!("EXPLAIN {}", sql), &[]).await?;
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

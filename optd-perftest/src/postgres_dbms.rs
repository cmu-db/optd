use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBMSHelper,
    tpch::{TpchConfig, TpchKit},
    truecard_cache::DBMSTruecardCache,
};
use async_trait::async_trait;
use futures::Sink;
use lazy_static::lazy_static;
use regex::Regex;

use std::{
    fs,
    io::Cursor,
    path::{Path, PathBuf},
};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_postgres::{Client, NoTls, Row};

/// The name of the Postgres DBMS (as opposed to the DataFusion DBMS for instance)
pub const POSTGRES_DBMS_NAME: &str = "Postgres";

/// This dbname is assumed to always exist
const DEFAULT_DBNAME: &str = "postgres";

pub struct PostgresDBMS {
    workspace_dpath: PathBuf,
    pguser: String,
    pgpassword: String,
    truecard_cache: DBMSTruecardCache,
}

/// Conventions I keep for methods of this class:
///   - Functions should be idempotent. For instance, start_postgres() should not fail if Postgres is already running
///       - For instance, this is why "createdb" is _not_ a function
///   - Stop and start functions should be separate
///   - Setup should be done in build() unless it requires more information (like benchmark)
impl PostgresDBMS {
    pub fn build<P: AsRef<Path>>(
        workspace_dpath: P,
        pguser: &str,
        pgpassword: &str,
    ) -> anyhow::Result<Self> {
        let workspace_dpath = PathBuf::from(workspace_dpath.as_ref());
        let truecard_cache = DBMSTruecardCache::build(&workspace_dpath, POSTGRES_DBMS_NAME)?;
        let pg_dbms = Self {
            workspace_dpath,
            pguser: String::from(pguser),
            pgpassword: String::from(pgpassword),
            truecard_cache,
        };
        Ok(pg_dbms)
    }

    /// Create a connection to a Postgres database
    async fn connect_to_db(&self, dbname: &str) -> anyhow::Result<Client> {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=localhost user={} password={} dbname={}",
                self.pguser, self.pgpassword, dbname
            ),
            NoTls,
        )
        .await?;
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

    async fn load_benchmark_data(&self, benchmark: &Benchmark) -> anyhow::Result<()> {
        let dbname = benchmark.get_dbname();
        // since we don't know whether dbname exists at this point, we have to connect to the default database
        let defaultdb_client = self.connect_to_db(DEFAULT_DBNAME).await?;
        let pgdata_dones_dpath = self.workspace_dpath.join("pgdata_dones");
        if !pgdata_dones_dpath.exists() {
            fs::create_dir(&pgdata_dones_dpath)?;
        }
        let done_fname = format!("{}_done", dbname);
        let done_fpath = pgdata_dones_dpath.join(done_fname);
        // determine whether we should load the data
        let should_load = if benchmark.is_readonly() {
            // if the db doesn't even exist then we clearly need to load it
            if !Self::get_does_db_exist(&defaultdb_client, &dbname).await? {
                // there may be a done_fpath left over from before. we need to make sure to delete it since it's
                // now known to be inaccurate
                if done_fpath.exists() {
                    fs::remove_file(&done_fpath)?;
                }
                true
            } else {
                // if the db does exist, we use done_fpath to determine if we need to load it since it's possible
                // for the db to be created but only partially loaded
                !done_fpath.exists()
            }
        } else {
            true
        };
        if should_load {
            log::debug!("[start] loading benchmark data");
            // it's possible for the db to exist or not after we have determined we should load the data
            let does_db_exist = Self::get_does_db_exist(&defaultdb_client, &dbname).await?;
            if does_db_exist {
                defaultdb_client
                    .query(&format!("DROP DATABASE {}", dbname), &[])
                    .await?;
            }
            defaultdb_client
                .query(&format!("CREATE DATABASE {}", dbname), &[])
                .await?;
            drop(defaultdb_client);
            // now that we've created `dbname`, we can connect to that
            let client = self.connect_to_db(&dbname).await?;
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
            Self::copy_from_stdin(client, tbl_fpath).await?;
        }

        // create stats
        // you need to do VACUUM FULL ANALYZE and not just ANALYZE to make sure the stats are created in a deterministic way
        // this is standard practice for postgres benchmarking
        client.query("VACUUM FULL ANALYZE", &[]).await?;

        Ok(())
    }

    /// Load a file into Postgres by sending the bytes through the network
    /// Unlike COPY ... FROM, COPY ... FROM STDIN works even if the Postgres process is on another machine or container
    async fn copy_from_stdin<P: AsRef<Path>>(
        client: &tokio_postgres::Client,
        tbl_fpath: P,
    ) -> anyhow::Result<()> {
        // read file
        let mut file = File::open(&tbl_fpath).await?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).await?;
        let cursor = Cursor::new(data);

        // run copy from statement
        let tbl_name = TpchKit::get_tbl_name_from_tbl_fpath(&tbl_fpath);
        let stmt = client
            .prepare(&format!(
                "COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER '|')",
                tbl_name
            ))
            .await?;
        let sink = client.copy_in(&stmt).await?;
        futures::pin_mut!(sink);
        sink.as_mut().start_send(cursor)?;
        sink.finish().await?;

        Ok(())
    }
}

#[async_trait]
impl CardtestRunnerDBMSHelper for PostgresDBMS {
    fn get_name(&self) -> &str {
        POSTGRES_DBMS_NAME
    }

    async fn eval_benchmark_estcards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = self.connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => self.eval_tpch_estcards(&client, tpch_config).await,
        }
    }

    async fn eval_benchmark_truecards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        self.load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = self.connect_to_db(&dbname).await?;
        match benchmark {
            Benchmark::Test => unimplemented!(),
            Benchmark::Tpch(tpch_config) => {
                self.eval_tpch_truecards(&client, tpch_config, &dbname)
                    .await
            }
        }
    }
}

/// This impl has helpers for ```impl CardtestRunnerDBMSHelper for PostgresDBMS```
impl PostgresDBMS {
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
            let estcard = self.eval_query_estcard(client, &sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_tpch_truecards(
        &mut self,
        client: &Client,
        tpch_config: &TpchConfig,
        dbname: &str, // used by truecard_cache
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_config)?;

        let mut truecards = vec![];
        for sql_fpath in tpch_kit.get_sql_fpath_ordered_iter(tpch_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let truecard = match self.truecard_cache.get_truecard(dbname, &sql) {
                Some(truecard) => truecard,
                None => {
                    let truecard = self.eval_query_truecard(client, &sql).await?;
                    self.truecard_cache.insert_truecard(dbname, &sql, truecard);
                    truecard
                }
            };
            truecards.push(truecard);
        }

        Ok(truecards)
    }

    fn log_explain(&self, explain_rows: &[Row]) {
        let explain_lines: Vec<&str> = explain_rows.iter().map(|row| row.get(0)).collect();
        let explain_str = explain_lines.join("\n");
        log::info!("{} {}", self.get_name(), explain_str);
    }

    async fn eval_query_estcard(&self, client: &Client, sql: &str) -> anyhow::Result<usize> {
        let explain_rows = client.query(&format!("EXPLAIN {}", sql), &[]).await?;
        self.log_explain(&explain_rows);
        // the first line contains the explain of the root node
        let first_explain_line: &str = explain_rows.first().unwrap().get(0);
        let estcard = PostgresDBMS::extract_row_count(first_explain_line).unwrap();
        Ok(estcard)
    }

    async fn eval_query_truecard(&self, client: &Client, sql: &str) -> anyhow::Result<usize> {
        let rows = client.query(sql, &[]).await?;
        let truecard = rows.len();
        Ok(truecard)
    }

    /// Extract the row count from a line of an EXPLAIN output
    fn extract_row_count(explain_line: &str) -> Option<usize> {
        lazy_static! {
            static ref ROWS_RE: Regex = Regex::new(r"rows=(\d+)").unwrap();
        }
        if let Some(caps) = ROWS_RE.captures(explain_line) {
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

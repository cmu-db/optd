use crate::{
    benchmark::Benchmark,
    cardtest::CardtestRunnerDBMSHelper,
    job::{JobKit, JobKitConfig},
    tpch::{TpchKit, TpchKitConfig},
    truecard::{TruecardCache, TruecardGetter},
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

/// Conceptually, this struct represents a "thing that creates connections to Postgres"
/// Thus, it's ok to have multiple copies of this struct at once
#[derive(Clone)]
pub struct PostgresDBMS {
    workspace_dpath: PathBuf,
    pguser: String,
    pgpassword: String,
}

impl PostgresDBMS {
    pub fn build<P: AsRef<Path>>(
        workspace_dpath: P,
        pguser: &str,
        pgpassword: &str,
    ) -> anyhow::Result<Self> {
        let workspace_dpath = PathBuf::from(workspace_dpath.as_ref());
        let pg_dbms = Self {
            workspace_dpath,
            pguser: String::from(pguser),
            pgpassword: String::from(pgpassword),
        };
        Ok(pg_dbms)
    }

    /// Create a connection to a Postgres database
    pub async fn connect_to_db(&self, dbname: &str) -> anyhow::Result<Client> {
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
                Benchmark::Tpch(tpch_kit_config) => {
                    self.load_tpch_data(&client, tpch_kit_config).await?
                }
                Benchmark::Job(job_kit_config) => {
                    self.load_job_data(&client, job_kit_config).await?
                }
                Benchmark::Joblight(job_kit_config) => {
                    self.load_job_data(&client, job_kit_config).await?
                }
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
        tpch_kit_config: &TpchKitConfig,
    ) -> anyhow::Result<()> {
        // set up TpchKit
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;

        // load the schema
        // we need to call make to ensure that the schema file exists
        // tpch_kit.make(TPCH_KIT_POSTGRES);
        let sql = fs::read_to_string(tpch_kit.schema_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;

        // load the tables
        tpch_kit.gen_tables(tpch_kit_config)?;
        for tbl_fpath in tpch_kit.get_tbl_fpath_vec(tpch_kit_config, "tbl")? {
            Self::copy_from_stdin(client, tbl_fpath, "|", "\\").await?;
        }

        // load the constraints and indexes
        let sql = fs::read_to_string(tpch_kit.constraints_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;
        let sql = fs::read_to_string(tpch_kit.indexes_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;

        // create stats
        // you need to do VACUUM FULL ANALYZE and not just ANALYZE to make sure the stats are created in a deterministic way
        // this is standard practice for postgres benchmarking
        client.query("VACUUM FULL ANALYZE", &[]).await?;

        Ok(())
    }

    /// Load the JOB data to the database that client is connected to
    async fn load_job_data(
        &self,
        client: &Client,
        job_kit_config: &JobKitConfig,
    ) -> anyhow::Result<()> {
        // set up TpchKit
        let job_kit = JobKit::build(&self.workspace_dpath)?;

        // load the schema
        // we need to call make to ensure that the schema file exists
        let sql = fs::read_to_string(job_kit.schema_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;

        // load the tables
        job_kit.download_tables(job_kit_config)?;
        for tbl_fpath in job_kit.get_tbl_fpath_vec("csv")? {
            Self::copy_from_stdin(client, tbl_fpath, ",", "\\").await?;
        }

        // load the indexes
        let sql = fs::read_to_string(job_kit.indexes_fpath.to_str().unwrap())?;
        client.batch_execute(&sql).await?;

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
        delimiter: &str,
        escape: &str,
    ) -> anyhow::Result<()> {
        // Setup
        let mut file = File::open(&tbl_fpath).await?;
        // Internally, File::read() seems to read at most 2MB at a time, so I set BUFFER_SIZE to be that.
        const BUFFER_SIZE: usize = 2 * 1024 * 1024;
        let mut extra_bytes_buffer = vec![];

        // Read the file BUFFER_SIZE at a time, sending a copy statement accordingly.
        // BUFFER_SIZE must be < 1GB because sending a single statement that is >1GB in size
        //   causes Postgres to cancel the transaction.
        loop {
            // Add the extra bytes from last time and then read from the file to fill the buffer to at most BUFFER_SIZE
            let mut buffer = vec![0u8; BUFFER_SIZE];
            let num_extra_bytes = extra_bytes_buffer.len();
            buffer.splice(0..num_extra_bytes, extra_bytes_buffer);
            let num_bytes_read = file.read(&mut buffer[num_extra_bytes..]).await?;
            let num_bytes_in_buffer = num_extra_bytes + num_bytes_read;

            if num_bytes_in_buffer == 0 {
                break;
            }

            // Truncate here to handle when file.read() encounters the end of the file.
            buffer.truncate(num_bytes_in_buffer);

            // Find the last newline in the buffer. Copy the extra data out and truncate the buffer.
            extra_bytes_buffer = vec![];
            let last_newline_idx = buffer.iter().rposition(|&x| x == b'\n');
            // It's possible that the buffer doesn't contain any newlines if it only has the very last line of the file.
            // In that case, we'll just assume it's the last line of the file and not truncate the buffer.
            // It's also possible that we have a line that's too long, but it's easier to just let Postgres throw an
            //   error if this happens.
            if let Some(last_newline_idx) = last_newline_idx {
                let extra_bytes_start_idx = last_newline_idx + 1;
                // Since we truncated buffer earlier, &buffer[extra_bytes_start_idx..] will not contain
                //   any bytes *not* in the file.
                extra_bytes_buffer.extend_from_slice(&buffer[extra_bytes_start_idx..]);
                buffer.truncate(extra_bytes_start_idx);
            }

            // Execute a COPY FROM STDIN statement with the buffer
            let cursor = Cursor::new(buffer);
            let tbl_name = TpchKit::get_tbl_name_from_tbl_fpath(&tbl_fpath);
            let stmt = client
                .prepare(&format!(
                    "COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER '{}', ESCAPE '{}')",
                    tbl_name, delimiter, escape
                ))
                .await?;
            let sink = client.copy_in(&stmt).await?;
            futures::pin_mut!(sink);
            sink.as_mut().start_send(cursor)?;
            sink.finish().await?;
        }

        Ok(())
    }

    async fn eval_tpch_estcards(
        &self,
        client: &Client,
        tpch_kit_config: &TpchKitConfig,
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_kit_config)?;

        let mut estcards = vec![];
        for (_, sql_fpath) in tpch_kit.get_sql_fpath_ordered_iter(tpch_kit_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(client, &sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
    }

    async fn eval_job_estcards(
        &self,
        client: &Client,
        job_kit_config: &JobKitConfig,
    ) -> anyhow::Result<Vec<usize>> {
        let job_kit = JobKit::build(&self.workspace_dpath)?;

        let mut estcards = vec![];
        for (_, sql_fpath) in job_kit.get_sql_fpath_ordered_iter(job_kit_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let estcard = self.eval_query_estcard(client, &sql).await?;
            estcards.push(estcard);
        }

        Ok(estcards)
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

    async fn eval_tpch_truecards(
        &mut self,
        client: &Client,
        tpch_kit_config: &TpchKitConfig,
        data_and_queries_name: &str, // used by truecard_cache
        truecard_cache: &mut TruecardCache,
    ) -> anyhow::Result<Vec<usize>> {
        let tpch_kit = TpchKit::build(&self.workspace_dpath)?;
        tpch_kit.gen_queries(tpch_kit_config)?;

        let mut truecards = vec![];
        for (query_id, sql_fpath) in tpch_kit.get_sql_fpath_ordered_iter(tpch_kit_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let truecard = match truecard_cache.get_truecard(data_and_queries_name, &query_id) {
                Some(truecard) => truecard,
                None => {
                    let truecard = self.eval_query_truecard(client, &sql).await?;
                    truecard_cache.insert_truecard(data_and_queries_name, &query_id, truecard);
                    truecard
                }
            };
            truecards.push(truecard);
        }

        Ok(truecards)
    }

    async fn eval_job_truecards(
        &mut self,
        client: &Client,
        job_kit_config: &JobKitConfig,
        data_and_queries_name: &str, // used by truecard_cache
        truecard_cache: &mut TruecardCache,
    ) -> anyhow::Result<Vec<usize>> {
        let job_kit = JobKit::build(&self.workspace_dpath)?;

        let mut truecards = vec![];
        for (query_id, sql_fpath) in job_kit.get_sql_fpath_ordered_iter(job_kit_config)? {
            let sql = fs::read_to_string(sql_fpath)?;
            let truecard = match truecard_cache.get_truecard(data_and_queries_name, &query_id) {
                Some(truecard) => truecard,
                None => {
                    let truecard = self.eval_query_truecard(client, &sql).await?;
                    truecard_cache.insert_truecard(data_and_queries_name, &query_id, truecard);
                    truecard
                }
            };
            truecards.push(truecard);
        }

        Ok(truecards)
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
            Benchmark::Tpch(tpch_kit_config) => {
                self.eval_tpch_estcards(&client, tpch_kit_config).await
            }
            Benchmark::Job(job_kit_config) | Benchmark::Joblight(job_kit_config) => {
                self.eval_job_estcards(&client, job_kit_config).await
            }
        }
    }
}

#[async_trait]
impl TruecardGetter for PostgresDBMS {
    async fn get_benchmark_truecards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>> {
        // load truecards from saved file
        let truecard_cache_fpath = self.workspace_dpath.join("truecard_cache.json");
        let mut truecard_cache = TruecardCache::build(truecard_cache_fpath)?;

        // if necessary, actually execute the queries
        // it's ok to call load_benchmark_data() even though we might have already called it in
        //   get_benchmark_estcards() because the second call to load_benchmark_data() will
        //   simply do nothing as the dbname will already exist
        self.load_benchmark_data(benchmark).await?;
        let dbname = benchmark.get_dbname();
        let client = self.connect_to_db(&dbname).await?;
        let data_and_queries_name = benchmark.get_data_and_queries_name();
        // all "eval_*" functions should add the truecards they find to the truecard cache
        match benchmark {
            Benchmark::Tpch(tpch_kit_config) => {
                self.eval_tpch_truecards(
                    &client,
                    tpch_kit_config,
                    &data_and_queries_name,
                    &mut truecard_cache,
                )
                .await
            }
            Benchmark::Job(job_kit_config) | Benchmark::Joblight(job_kit_config) => {
                self.eval_job_truecards(
                    &client,
                    job_kit_config,
                    &data_and_queries_name,
                    &mut truecard_cache,
                )
                .await
            }
        }
        // note that truecard_cache will save itself when it goes out of scope
    }
}

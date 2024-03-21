use clap::{Parser, Subcommand};
use optd_perftest::cardtest;
use optd_perftest::shell;
use optd_perftest::tpch::{TpchConfig, TPCH_KIT_POSTGRES};
use prettytable::{format, Cell, Row, Table};
use std::{fs, iter};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    #[clap(default_value = "optd_perftest_workspace")]
    #[clap(
        help = "The directory where artifacts required for performance testing (such as pgdata or TPC-H queries) are generated. See comment of parse_pathstr() to see what paths are allowed (TLDR: absolute and relative both ok)."
    )]
    workspace: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Cardtest {
        #[arg(long)]
        #[clap(default_value = "0.01")]
        scale_factor: f64,

        #[arg(long)]
        #[clap(default_value = "15721")]
        seed: i32,

        #[arg(long)]
        #[clap(value_delimiter = ',', num_args = 1..)]
        // this is the current list of all queries that work in perftest
        #[clap(default_value = "2,3,5,7,8,9,10,11,12,13,14,17")]
        query_ids: Vec<u32>,

        #[arg(long)]
        #[clap(default_value = "default_user")]
        #[clap(help = "The name of a user with superuser privileges")]
        pguser: String,

        #[arg(long)]
        #[clap(default_value = "password")]
        #[clap(help = "The name of a user with superuser privileges")]
        pgpassword: String,
    },
}

// q-errors are always >= 1.0 so two decimal points is enough
fn fmt_qerror(qerror: f64) -> String {
    format!("{:.2}", qerror)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    let workspace_dpath = shell::parse_pathstr(&cli.workspace)?;
    if !workspace_dpath.exists() {
        fs::create_dir(&workspace_dpath)?;
    }

    match cli.command {
        Commands::Cardtest {
            scale_factor,
            seed,
            query_ids,
            pguser,
            pgpassword,
        } => {
            let tpch_config = TpchConfig {
                dbms: String::from(TPCH_KIT_POSTGRES),
                scale_factor,
                seed,
                query_ids: query_ids.clone(),
            };
            let qerrors_alldbs =
                cardtest::cardtest(&workspace_dpath, &pguser, &pgpassword, tpch_config).await?;
            println!();
            println!(" Aggregate Q-Error Comparison");
            let mut agg_qerror_table = Table::new();
            agg_qerror_table.set_titles(prettytable::row![
                "DBMS", "Median", "# Inf", "Mean", "Min", "Max"
            ]);
            for (dbms, qerrors) in &qerrors_alldbs {
                if !qerrors.is_empty() {
                    let finite_qerrors: Vec<f64> = qerrors
                        .clone()
                        .into_iter()
                        .filter(|&qerror| qerror.is_finite())
                        .collect();
                    let ninf_qerrors = qerrors.len() - finite_qerrors.len();
                    let mean_qerror =
                        finite_qerrors.iter().sum::<f64>() / finite_qerrors.len() as f64;
                    let min_qerror = qerrors
                        .iter()
                        .min_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap();
                    let median_qerror = statistical::median(qerrors);
                    let max_qerror = qerrors
                        .iter()
                        .max_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap();
                    agg_qerror_table.add_row(prettytable::row![
                        dbms,
                        fmt_qerror(median_qerror),
                        ninf_qerrors,
                        fmt_qerror(mean_qerror),
                        fmt_qerror(*min_qerror),
                        fmt_qerror(*max_qerror),
                    ]);
                } else {
                    agg_qerror_table
                        .add_row(prettytable::row![dbms, "N/A", "N/A", "N/A", "N/A", "N/A"]);
                }
            }
            agg_qerror_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
            agg_qerror_table.printstd();

            let mut per_query_qerror_table = Table::new();
            println!();
            println!(" Per-Query Q-Error Comparison");
            let title_cells = iter::once(Cell::new("Query #"))
                .chain(qerrors_alldbs.keys().map(|dbms| Cell::new(dbms)))
                .collect();
            per_query_qerror_table.set_titles(Row::new(title_cells));
            for (i, query_id) in query_ids.iter().enumerate() {
                let mut row_cells = vec![];
                row_cells.push(prettytable::cell!(query_id));
                for qerrors in qerrors_alldbs.values() {
                    let qerror = qerrors.get(i).unwrap();
                    row_cells.push(prettytable::cell!(fmt_qerror(*qerror)));
                }
                per_query_qerror_table.add_row(Row::new(row_cells));
            }
            per_query_qerror_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
            per_query_qerror_table.printstd();
        }
    }

    Ok(())
}

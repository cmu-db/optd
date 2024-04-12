use clap::{Parser, Subcommand};
use optd_perftest::cardtest;
use optd_perftest::shell;
use optd_perftest::tpch::{TpchConfig, TPCH_KIT_POSTGRES};
use prettytable::{format, Table};
use std::fs;

#[derive(Parser)]
struct Cli {
    #[clap(long)]
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
        #[clap(long)]
        #[clap(default_value = "0.01")]
        scale_factor: f64,

        #[clap(long)]
        #[clap(default_value = "15721")]
        seed: i32,

        #[clap(long)]
        #[clap(value_delimiter = ',', num_args = 1..)]
        // this is the current list of all queries that work in perftest
        #[clap(default_value = "2,3,5,6,7,8,9,10,11,12,13,14,17,19")]
        #[clap(help = "The queries to get the Q-error of")]
        query_ids: Vec<u32>,

        #[clap(long)]
        #[clap(action)]
        #[clap(help = "Whether to use the cached optd stats/cache generated stats")]
        // this is an option because you want to make it true whenever you update the
        //   code for how stats are generated in optd, in order to not use cached stats
        // I found that I almost always want to use the cache though, which is why the
        //   system will use the cache by default
        rebuild_cached_optd_stats: bool,

        #[clap(long)]
        #[clap(default_value = "default_user")]
        #[clap(help = "The name of a user with superuser privileges")]
        pguser: String,

        #[clap(long)]
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
            rebuild_cached_optd_stats,
            pguser,
            pgpassword,
        } => {
            let tpch_config = TpchConfig {
                dbms: String::from(TPCH_KIT_POSTGRES),
                scale_factor,
                seed,
                query_ids: query_ids.clone(),
            };
            let cardinfo_alldbs = cardtest::cardtest(
                &workspace_dpath,
                rebuild_cached_optd_stats,
                &pguser,
                &pgpassword,
                tpch_config,
            )
            .await?;
            println!();
            println!(" Aggregate Q-Error Comparison");
            let mut agg_qerror_table = Table::new();
            agg_qerror_table.set_titles(prettytable::row![
                "DBMS", "Median", "# Inf", "Mean", "Min", "Max"
            ]);
            for (dbms, cardinfos) in &cardinfo_alldbs {
                if !cardinfos.is_empty() {
                    let qerrors: Vec<f64> =
                        cardinfos.iter().map(|cardinfo| cardinfo.qerror).collect();
                    let finite_qerrors: Vec<f64> = qerrors
                        .clone()
                        .into_iter()
                        .filter(|qerror| qerror.is_finite())
                        .collect();
                    let ninf_qerrors = qerrors.len() - finite_qerrors.len();
                    let mean_qerror =
                        finite_qerrors.iter().sum::<f64>() / finite_qerrors.len() as f64;
                    let min_qerror = qerrors
                        .iter()
                        .min_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap();
                    let median_qerror = statistical::median(&qerrors);
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

            println!();
            println!(" Per-Query Cardinality Info");
            println!(" ===========================");
            for (i, query_id) in query_ids.iter().enumerate() {
                println!(" Query {}", query_id);
                let mut this_query_cardinfo_table = Table::new();
                this_query_cardinfo_table.set_titles(prettytable::row![
                    "DBMS",
                    "Q-Error",
                    "Est. Card.",
                    "True Card."
                ]);
                for (dbms, cardinfos) in &cardinfo_alldbs {
                    let this_query_cardinfo = cardinfos.get(i).unwrap();
                    this_query_cardinfo_table.add_row(prettytable::row![
                        dbms,
                        this_query_cardinfo.qerror,
                        this_query_cardinfo.estcard,
                        this_query_cardinfo.truecard
                    ]);
                }
                this_query_cardinfo_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                this_query_cardinfo_table.printstd();
            }
        }
    }

    Ok(())
}

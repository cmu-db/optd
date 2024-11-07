use std::fs;
use std::path::Path;

use clap::{Parser, Subcommand, ValueEnum};
use optd_perfbench::benchmark::Benchmark;
use optd_perfbench::cardbench::Cardinfo;
use optd_perfbench::job::JobKitConfig;
use optd_perfbench::tpch::{TpchKitConfig, TPCH_KIT_POSTGRES};
use optd_perfbench::{cardbench, job, shell, tpch};
use prettytable::{format, Table};

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    #[clap(default_value = "optd_perfbench_workspace")]
    #[clap(
        help = "The directory where artifacts required for performance testing (such as pgdata or TPC-H queries) are generated. See comment of parse_pathstr() to see what paths are allowed (TLDR: absolute and relative both ok)."
    )]
    workspace: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum BenchmarkName {
    Tpch,
    Job,
    Joblight,
}

#[derive(Subcommand)]
enum Commands {
    Cardbench {
        #[clap(value_enum)]
        #[clap(default_value = "tpch")]
        benchmark_name: BenchmarkName,

        #[clap(long)]
        #[clap(default_value = "0.01")]
        scale_factor: f64,

        #[clap(long)]
        #[clap(default_value = "15721")]
        seed: i32,

        #[clap(long)]
        #[clap(value_delimiter = ',', num_args = 1..)]
        // This is the current list of all queries that work in perfbench
        #[clap(default_value = None)]
        #[clap(help = "The queries to get the Q-error of")]
        query_ids: Vec<String>,

        #[clap(long)]
        #[clap(action)]
        #[clap(help = "Whether to use the cached optd stats/cache generated stats")]
        // This is an option because you want to make it true whenever you update the
        //   code for how stats are generated in optd, in order to not use cached stats.
        // I found that I almost always want to use the cache though, which is why the
        //   system will use the cache by default.
        rebuild_cached_optd_stats: bool,

        #[clap(long)]
        #[clap(action)]
        #[clap(help = "Whether to enable adaptivity for optd")]
        adaptive: bool,

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

/// Q-errors are always >= 1.0 so two decimal points is enough
fn fmt_qerror(qerror: f64) -> String {
    format!("{:.2}", qerror)
}

fn percentile(sorted_v: &[f64], percentile: f64) -> f64 {
    let idx = ((percentile / 100.0) * (sorted_v.len() - 1) as f64).round() as usize;
    sorted_v[idx]
}

/// cardbench::cardbench_core() expects sanitized inputs and returns outputs in their simplest form.
/// This function wraps around cardbench::cardbench_core() to sanitize the inputs and print the
/// outputs nicely.
#[allow(clippy::too_many_arguments)]
async fn cardbench<P: AsRef<Path>>(
    workspace_dpath: P,
    benchmark_name: BenchmarkName,
    scale_factor: f64,
    seed: i32,
    query_ids: Vec<String>,
    rebuild_cached_optd_stats: bool,
    pguser: String,
    pgpassword: String,
    adaptive: bool,
) -> anyhow::Result<()> {
    let query_ids = if query_ids.is_empty() {
        Vec::from(match benchmark_name {
            BenchmarkName::Tpch => tpch::WORKING_QUERY_IDS,
            BenchmarkName::Job => job::WORKING_JOB_QUERY_IDS,
            BenchmarkName::Joblight => job::WORKING_JOBLIGHT_QUERY_IDS,
        })
        .into_iter()
        .map(String::from)
        .collect()
    } else {
        query_ids
    };

    let benchmark = match benchmark_name {
        BenchmarkName::Tpch => Benchmark::Tpch(TpchKitConfig {
            dbms: String::from(TPCH_KIT_POSTGRES),
            scale_factor,
            seed,
            query_ids: query_ids.clone(),
        }),
        BenchmarkName::Job => Benchmark::Job(JobKitConfig {
            query_ids: query_ids.clone(),
            is_light: false,
        }),
        BenchmarkName::Joblight => Benchmark::Joblight(JobKitConfig {
            query_ids: query_ids.clone(),
            is_light: true,
        }),
    };

    let cardinfo_alldbs = cardbench::cardbench_core(
        &workspace_dpath,
        rebuild_cached_optd_stats,
        &pguser,
        &pgpassword,
        benchmark,
        adaptive,
    )
    .await?;
    let sorted_cardinfo_alldbs = {
        let mut vec: Vec<(String, Vec<Cardinfo>)> = cardinfo_alldbs.into_iter().collect();
        vec.sort_by(|a, b| a.0.cmp(&b.0));
        vec
    };

    println!();
    println!(" Per-Query Cardinality Information");
    println!("===================================");
    for (i, query_id) in query_ids.iter().enumerate() {
        println!(" Query {}", query_id);
        let mut this_query_cardinfo_table = Table::new();
        this_query_cardinfo_table.set_titles(prettytable::row![
            "DBMS",
            "Q-Error",
            "Est. Card.",
            "True Card."
        ]);
        for (dbms, cardinfos) in &sorted_cardinfo_alldbs {
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

    println!();
    println!(" Aggregate Q-Error Information");
    let mut agg_qerror_table = Table::new();
    agg_qerror_table.set_titles(prettytable::row![
        "DBMS", "Median", "P90", "P95", "P99", "# Inf", "Mean", "Min", "Max"
    ]);
    for (dbms, cardinfos) in &sorted_cardinfo_alldbs {
        if !cardinfos.is_empty() {
            let qerrors: Vec<f64> = cardinfos.iter().map(|cardinfo| cardinfo.qerror).collect();
            let finite_qerrors: Vec<f64> = qerrors
                .clone()
                .into_iter()
                .filter(|qerror| qerror.is_finite())
                .collect();
            let mut sorted_qerrors = finite_qerrors.clone();
            sorted_qerrors.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let median_qerror = percentile(&sorted_qerrors, 50.0);
            let p90_qerror = percentile(&sorted_qerrors, 90.0);
            let p95_qerror = percentile(&sorted_qerrors, 95.0);
            let p99_qerror = percentile(&sorted_qerrors, 99.0);
            let ninf_qerrors = qerrors.len() - finite_qerrors.len();
            let mean_qerror = finite_qerrors.iter().sum::<f64>() / finite_qerrors.len() as f64;
            let min_qerror = qerrors
                .iter()
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max_qerror = finite_qerrors
                .iter()
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            agg_qerror_table.add_row(prettytable::row![
                dbms,
                fmt_qerror(median_qerror),
                fmt_qerror(p90_qerror),
                fmt_qerror(p95_qerror),
                fmt_qerror(p99_qerror),
                ninf_qerrors,
                fmt_qerror(mean_qerror),
                fmt_qerror(*min_qerror),
                fmt_qerror(*max_qerror),
            ]);
        } else {
            agg_qerror_table.add_row(prettytable::row![dbms, "N/A", "N/A", "N/A", "N/A", "N/A"]);
        }
    }
    agg_qerror_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    agg_qerror_table.printstd();

    println!();
    println!(" Comparative Q-Error Information");
    let mut best_qerror_infos = vec![];
    for (dbms, cardinfos) in &sorted_cardinfo_alldbs {
        for (i, cardinfo) in cardinfos.iter().enumerate() {
            if i >= best_qerror_infos.len() {
                assert!(i == best_qerror_infos.len());
                best_qerror_infos.push((cardinfo.qerror, vec![dbms.clone()]));
            } else {
                let (best_qerror, _) = best_qerror_infos[i];

                if cardinfo.qerror < best_qerror {
                    *best_qerror_infos.get_mut(i).unwrap() = (cardinfo.qerror, vec![dbms.clone()]);
                } else if cardinfo.qerror == best_qerror {
                    best_qerror_infos.get_mut(i).unwrap().1.push(dbms.clone());
                }
            }
        }
    }
    let mut cmp_qerror_table = Table::new();
    cmp_qerror_table.set_titles(prettytable::row!["DBMS", "# Best", "# Tied Best"]);
    for (dbms, _) in sorted_cardinfo_alldbs {
        let num_best = best_qerror_infos
            .iter()
            .filter(|(_, dbmss)| dbmss.len() == 1 && dbmss.contains(&dbms))
            .count();
        let num_tied_best = best_qerror_infos
            .iter()
            .filter(|(_, dbmss)| dbmss.len() > 1 && dbmss.contains(&dbms))
            .count();
        cmp_qerror_table.add_row(prettytable::row![dbms, num_best, num_tied_best]);
    }
    cmp_qerror_table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    cmp_qerror_table.printstd();

    Ok(())
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
        Commands::Cardbench {
            benchmark_name,
            scale_factor,
            seed,
            query_ids,
            rebuild_cached_optd_stats,
            pguser,
            pgpassword,
            adaptive,
        } => {
            cardbench(
                workspace_dpath,
                benchmark_name,
                scale_factor,
                seed,
                query_ids,
                rebuild_cached_optd_stats,
                pguser,
                pgpassword,
                adaptive,
            )
            .await
        }
    }
}

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use datafusion::functions_aggregate::expr_fn::{avg, sum, count};
use datafusion::functions::datetime::expr_fn::date_trunc;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(
    name = "nyc_tlc_datafusion",
    about = "NYC TLC Yellow Taxi 2025 analytics using DataFusion DataFrame API + SQL"
)]
struct Args {
    /// Folder containing the 2025 yellow parquet files (12 files, one per month)
    /// Example: ./data/yellow/2025
    #[arg(long, default_value = "./data/yellow/2025")]
    data_dir: String,

    /// Year to analyze (mostly informational / validation)
    #[arg(long, default_value_t = 2025)]
    year: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let data_dir = PathBuf::from(&args.data_dir);
    validate_data_dir(&data_dir, args.year)?;

    let ctx = SessionContext::new();

    // Register ALL parquet files in the directory as one table
    ctx.register_parquet(
        "yellow",
        data_dir.to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await
    .context("register_parquet failed")?;

    println!("Loaded table 'yellow' from: {}", data_dir.display());
    println!("Running aggregations for year {}...\n", args.year);

    // ---------------------------------------------------
    // Aggregation 1: Trips and revenue by pickup month
    // ---------------------------------------------------
    println!("==============================");
    println!("Aggregation 1 (DataFrame API): Trips and revenue by pickup month");
    println!("==============================");

    let df = ctx.table("yellow").await?;


    // pickup_month = date_trunc('month', tpep_pickup_datetime)
   let pickup_month =
    date_trunc(lit("month"), col("tpep_pickup_datetime")).alias("pickup_month");

    let agg1_df = df
        .aggregate(
            vec![pickup_month],
            vec![
                count(lit(1)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;

    print_df("Aggregation 1 (DataFrame API)", agg1_df).await?;

    println!("\n==============================");
    println!("Aggregation 1 (SQL): Trips and revenue by pickup month");
    println!("==============================");

    let agg1_sql = r#"
        SELECT
            date_trunc('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM yellow
        GROUP BY 1
        ORDER BY 1 ASC
    "#;

    let agg1_sql_df = ctx.sql(agg1_sql).await?;
    print_df("Aggregation 1 (SQL)", agg1_sql_df).await?;

    // ---------------------------------------------------
    // Aggregation 2: Tip behavior by payment type
    // ---------------------------------------------------
    println!("\n==============================");
    println!("Aggregation 2 (DataFrame API): Tip behavior by payment type");
    println!("==============================");

    let df2 = ctx.table("yellow").await?;

    // tip_rate = SUM(tip_amount) / SUM(total_amount)
    // Step 1: aggregate sums separately
let agg2_base = df2.aggregate(
    vec![col("payment_type")],
    vec![
        count(lit(1)).alias("trip_count"),
        avg(col("tip_amount")).alias("avg_tip_amount"),
        sum(col("tip_amount")).alias("sum_tip_amount"),
        sum(col("total_amount")).alias("sum_total_amount"),
    ],
)?;

// Step 2: compute tip_rate in a projection (and optionally drop helper columns)
let agg2_df = agg2_base
    .select(vec![
        col("payment_type"),
        col("trip_count"),
        col("avg_tip_amount"),
        (col("sum_tip_amount") / col("sum_total_amount")).alias("tip_rate"),
    ])?
    .sort(vec![col("trip_count").sort(false, true)])?;

    print_df("Aggregation 2 (DataFrame API)", agg2_df).await?;

    println!("\n==============================");
    println!("Aggregation 2 (SQL): Tip behavior by payment type");
    println!("==============================");

    let agg2_sql = r#"
        SELECT
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM yellow
        GROUP BY 1
        ORDER BY trip_count DESC
    "#;

    let agg2_sql_df = ctx.sql(agg2_sql).await?;
    print_df("Aggregation 2 (SQL)", agg2_sql_df).await?;

    println!("\n✅ All aggregations completed successfully.");
    Ok(())
}

fn validate_data_dir(data_dir: &Path, year: i32) -> Result<()> {
    if !data_dir.exists() {
        return Err(anyhow!(
            "Data directory does not exist: {}",
            data_dir.display()
        ));
    }
    if !data_dir.is_dir() {
        return Err(anyhow!(
            "Data path is not a directory: {}",
            data_dir.display()
        ));
    }

    // Warning-only check: expect 12 files following TLC naming pattern
    let mut missing = Vec::new();
    for m in 1..=12 {
        let fname = format!("yellow_tripdata_{}-{:02}.parquet", year, m);
        if !data_dir.join(&fname).exists() {
            missing.push(fname);
        }
    }

    if !missing.is_empty() {
        eprintln!(
            "⚠️  Some expected files are missing in {}:",
            data_dir.display()
        );
        for f in &missing {
            eprintln!("   - {}", f);
        }
        eprintln!("\nTip: you can test with 1 month first and later add all 12 months.\n");
    }

    Ok(())
}

async fn print_df(title: &str, df: DataFrame) -> Result<()> {
    let batches = df.collect().await?;
    let formatted = pretty_format_batches(&batches)?;
    println!("\n--- {} ---", title);
    println!("{}", formatted);
    Ok(())
}
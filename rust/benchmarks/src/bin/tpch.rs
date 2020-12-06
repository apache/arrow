// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Benchmark derived from TPC-H. This is not an official TPC-H benchmark.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::{CsvFile, MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::csv::CsvExec;
use datafusion::prelude::*;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct BenchmarkOpt {
    /// Query number
    #[structopt(short, long)]
    query: usize,

    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of threads to use for parallel execution
    #[structopt(short = "c", long = "concurrency", default_value = "2")]
    concurrency: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "4096")]
    batch_size: usize,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "csv")]
    file_format: String,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,
}

#[derive(Debug, StructOpt)]
struct ConvertOpt {
    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "TPC-H", about = "TPC-H Benchmarks.")]
enum TpchOpt {
    Benchmark(BenchmarkOpt),
    Convert(ConvertOpt),
}

const TABLES: &[&str] = &["lineitem", "orders"];

#[tokio::main]
async fn main() -> Result<()> {
    match TpchOpt::from_args() {
        TpchOpt::Benchmark(opt) => benchmark(opt).await,
        TpchOpt::Convert(opt) => convert_tbl(opt).await,
    }
}

async fn benchmark(opt: BenchmarkOpt) -> Result<()> {
    println!("Running benchmarks with the following options: {:?}", opt);
    let config = ExecutionConfig::new()
        .with_concurrency(opt.concurrency)
        .with_batch_size(opt.batch_size);
    let mut ctx = ExecutionContext::with_config(config);

    // register tables
    for table in TABLES {
        let table_provider =
            get_table(opt.path.to_str().unwrap(), table, opt.file_format.as_str())?;
        if opt.mem_table {
            println!("Loading table '{}' into memory", table);
            let start = Instant::now();

            let memtable =
                MemTable::load(table_provider.as_ref(), opt.batch_size).await?;
            println!(
                "Loaded table '{}' into memory in {} ms",
                table,
                start.elapsed().as_millis()
            );
            ctx.register_table(table, Box::new(memtable));
        } else {
            ctx.register_table(table, table_provider);
        }
    }

    // run benchmark
    for i in 0..opt.iterations {
        let start = Instant::now();
        let plan = create_logical_plan(&mut ctx, opt.query)?;
        execute_query(&mut ctx, &plan, opt.debug).await?;
        println!(
            "Query {} iteration {} took {} ms",
            opt.query,
            i,
            start.elapsed().as_millis()
        );
    }

    Ok(())
}

fn create_logical_plan(ctx: &mut ExecutionContext, query: usize) -> Result<LogicalPlan> {
    match query {
        1 => ctx.create_logical_plan(
            "select
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity),
                    sum(l_extendedprice),
                    sum(l_extendedprice * (1 - l_discount)),
                    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
                    avg(l_quantity),
                    avg(l_extendedprice),
                    avg(l_discount),
                    count(*)
                from
                    lineitem
                where
                    l_shipdate <= '1998-12-01'
                group by
                    l_returnflag,
                    l_linestatus
                order by
                    l_returnflag,
                    l_linestatus",
        ),

        12 => ctx.create_logical_plan(
            "SELECT
                l_shipmode,
                sum(case
                    when o_orderpriority = '1-URGENT'
                        OR o_orderpriority = '2-HIGH'
                        then 1
                    else 0
                end) as high_line_count,
                sum(case
                    when o_orderpriority <> '1-URGENT'
                        AND o_orderpriority <> '2-HIGH'
                        then 1
                    else 0
                end) AS low_line_count
            FROM
                lineitem JOIN orders ON l_orderkey = o_orderkey
            WHERE (l_shipmode = 'MAIL' OR l_shipmode = 'SHIP')
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= '1994-01-01'
                AND l_receiptdate < '1995-01-01'
            GROUP BY
                l_shipmode
            ORDER BY
                l_shipmode",
        ),

        _ => unimplemented!("unsupported query"),
    }
}

async fn execute_query(
    ctx: &mut ExecutionContext,
    plan: &LogicalPlan,
    debug: bool,
) -> Result<()> {
    if debug {
        println!("Logical plan:\n{:?}", plan);
    }
    let plan = ctx.optimize(&plan)?;
    if debug {
        println!("Optimized logical plan:\n{:?}", plan);
    }
    let physical_plan = ctx.create_physical_plan(&plan)?;
    let result = ctx.collect(physical_plan).await?;
    if debug {
        pretty::print_batches(&result)?;
    }
    Ok(())
}

async fn convert_tbl(opt: ConvertOpt) -> Result<()> {
    for table in TABLES {
        let schema = get_schema(table);

        let path = format!("{}/{}.tbl", opt.input_path.to_str().unwrap(), table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .file_extension(".tbl");

        let ctx = ExecutionContext::new();
        let csv = Arc::new(CsvExec::try_new(&path, options, None, 4096)?);
        let output_path = opt.output_path.to_str().unwrap().to_owned();

        match opt.file_format.as_str() {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => ctx.write_parquet(csv, output_path).await?,
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Invalid output format: {}",
                    other
                )))
            }
        }
    }

    Ok(())
}

fn get_table(
    path: &str,
    table: &str,
    table_format: &str,
) -> Result<Box<dyn TableProvider + Send + Sync>> {
    match table_format {
        // dbgen creates .tbl ('|' delimited) files
        "tbl" => {
            let path = format!("{}/{}.tbl", path, table);
            let schema = get_schema(table);
            let options = CsvReadOptions::new()
                .schema(&schema)
                .delimiter(b'|')
                .file_extension(".tbl");

            Ok(Box::new(CsvFile::try_new(&path, options)?))
        }
        "csv" => {
            let path = format!("{}/{}", path, table);
            let schema = get_schema(table);
            let options = CsvReadOptions::new().schema(&schema).has_header(true);

            Ok(Box::new(CsvFile::try_new(&path, options)?))
        }
        "parquet" => {
            let path = format!("{}/{}", path, table);
            Ok(Box::new(ParquetTable::try_new(&path)?))
        }
        other => {
            unimplemented!("Invalid file format '{}'", other);
        }
    }
}

fn get_schema(table: &str) -> Schema {
    match table {
        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::UInt32, true),
            Field::new("l_partkey", DataType::UInt32, true),
            Field::new("l_suppkey", DataType::UInt32, true),
            Field::new("l_linenumber", DataType::UInt32, true),
            Field::new("l_quantity", DataType::Float64, true),
            Field::new("l_extendedprice", DataType::Float64, true),
            Field::new("l_discount", DataType::Float64, true),
            Field::new("l_tax", DataType::Float64, true),
            Field::new("l_returnflag", DataType::Utf8, true),
            Field::new("l_linestatus", DataType::Utf8, true),
            Field::new("l_shipdate", DataType::Utf8, true),
            Field::new("l_commitdate", DataType::Utf8, true),
            Field::new("l_receiptdate", DataType::Utf8, true),
            Field::new("l_shipinstruct", DataType::Utf8, true),
            Field::new("l_shipmode", DataType::Utf8, true),
            Field::new("l_comment", DataType::Utf8, true),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::UInt32, true),
            Field::new("custkey", DataType::UInt32, true),
            Field::new("o_orderstatus", DataType::Utf8, true),
            Field::new("o_totalprice", DataType::Float64, true),
            Field::new("o_orderdate", DataType::Utf8, true),
            Field::new("o_orderpriority", DataType::Utf8, true),
            Field::new("o_clerk", DataType::Utf8, true),
            Field::new("o_shippriority", DataType::Utf8, true),
            Field::new("o_comment", DataType::Utf8, true),
        ]),

        _ => unimplemented!(),
    }
}

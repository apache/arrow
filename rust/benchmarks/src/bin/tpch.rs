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
use std::process;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty;
use datafusion::datasource::parquet::ParquetTable;
use datafusion::datasource::{CsvFile, MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::physical_plan::csv::{CsvExec, CsvReadOptions};

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

    let path = opt.path.to_str().unwrap();

    let tableprovider: Box<dyn TableProvider + Send + Sync> =
        match opt.file_format.as_str() {
            // dbgen creates .tbl ('|' delimited) files
            "tbl" => {
                let path = format!("{}/lineitem.tbl", path);
                let schema = lineitem_schema();
                let options = CsvReadOptions::new()
                    .schema(&schema)
                    .delimiter(b'|')
                    .file_extension(".tbl");

                Box::new(CsvFile::try_new(&path, options)?)
            }
            "csv" => {
                let path = format!("{}/lineitem", path);
                let schema = lineitem_schema();
                let options = CsvReadOptions::new().schema(&schema).has_header(true);

                Box::new(CsvFile::try_new(&path, options)?)
            }
            "parquet" => {
                let path = format!("{}/lineitem", path);
                Box::new(ParquetTable::try_new(&path)?)
            }
            other => {
                println!("Invalid file format '{}'", other);
                process::exit(-1);
            }
        };

    if opt.mem_table {
        println!("Loading data into memory");
        let start = Instant::now();

        let memtable = MemTable::load(tableprovider.as_ref(), opt.batch_size).await?;
        println!(
            "Loaded data into memory in {} ms",
            start.elapsed().as_millis()
        );

        ctx.register_table("lineitem", Box::new(memtable));
    } else {
        ctx.register_table("lineitem", tableprovider);
    }

    let sql = match opt.query {
        1 => {
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
                l_linestatus"
        }

        _ => unimplemented!("unsupported query"),
    };

    for i in 0..opt.iterations {
        let start = Instant::now();
        execute_sql(&mut ctx, sql, opt.debug).await?;
        println!(
            "Query {} iteration {} took {} ms",
            opt.query,
            i,
            start.elapsed().as_millis()
        );
    }

    Ok(())
}

async fn execute_sql(ctx: &mut ExecutionContext, sql: &str, debug: bool) -> Result<()> {
    let plan = ctx.create_logical_plan(sql)?;
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
    let schema = lineitem_schema();

    let path = format!("{}/lineitem.tbl", opt.input_path.to_str().unwrap());
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

    Ok(())
}

fn lineitem_schema() -> Schema {
    Schema::new(vec![
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
    ])
}

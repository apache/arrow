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
//!
//! This is a modified version of the DataFusion version of these benchmarks.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty;
use ballista::prelude::*;
use datafusion::prelude::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct BenchmarkOpt {
    /// Ballista executor host
    #[structopt(long = "host")]
    host: String,

    /// Ballista executor port
    #[structopt(long = "port")]
    port: u16,

    /// Query number
    #[structopt(long)]
    query: usize,

    /// Activate debug mode to see query results
    #[structopt(long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(long = "iterations", default_value = "1")]
    iterations: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(long = "batch-size", default_value = "32768")]
    batch_size: usize,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, long = "path")]
    path: PathBuf,

    /// File format: `csv`, `tbl` or `parquet`
    #[structopt(long = "format")]
    file_format: String,
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

    /// Compression to use when writing Parquet files
    #[structopt(short = "c", long = "compression", default_value = "snappy")]
    compression: String,

    /// Number of partitions to produce
    #[structopt(short = "p", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "4096")]
    batch_size: usize,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "TPC-H", about = "TPC-H Benchmarks.")]
enum TpchOpt {
    Benchmark(BenchmarkOpt),
    Convert(ConvertOpt),
}

const TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    match TpchOpt::from_args() {
        TpchOpt::Benchmark(opt) => benchmark(opt).await.map(|_| ()),
        TpchOpt::Convert(opt) => convert_tbl(opt).await,
    }
}

async fn benchmark(opt: BenchmarkOpt) -> Result<()> {
    println!("Running benchmarks with the following options: {:?}", opt);

    let mut settings = HashMap::new();
    settings.insert("batch.size".to_owned(), format!("{}", opt.batch_size));

    let ctx = BallistaContext::remote(opt.host.as_str(), opt.port, settings);

    // register tables with Ballista context
    let path = opt.path.to_str().unwrap();
    let file_format = opt.file_format.as_str();
    for table in TABLES {
        match file_format {
            // dbgen creates .tbl ('|' delimited) files without header
            "tbl" => {
                let path = format!("{}/{}.tbl", path, table);
                let schema = get_schema(table);
                let options = CsvReadOptions::new()
                    .schema(&schema)
                    .delimiter(b'|')
                    .has_header(false)
                    .file_extension(".tbl");
                ctx.register_csv(table, &path, options)?;
            }
            "csv" => {
                let path = format!("{}/{}", path, table);
                let schema = get_schema(table);
                let options = CsvReadOptions::new().schema(&schema).has_header(true);
                ctx.register_csv(table, &path, options)?;
            }
            "parquet" => {
                let path = format!("{}/{}", path, table);
                ctx.register_parquet(table, &path)?;
            }
            other => {
                unimplemented!("Invalid file format '{}'", other);
            }
        }
    }

    let mut millis = vec![];

    // run benchmark
    let sql = get_query_sql(opt.query)?;
    println!("Running benchmark with query {}:\n {}", opt.query, sql);
    for i in 0..opt.iterations {
        let start = Instant::now();
        let df = ctx.sql(&sql)?;
        let mut batches = vec![];
        let mut stream = df.collect().await?;
        while let Some(result) = stream.next().await {
            let batch = result?;
            batches.push(batch);
        }
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        millis.push(elapsed as f64);
        println!("Query {} iteration {} took {:.1} ms", opt.query, i, elapsed);
        if opt.debug {
            pretty::print_batches(&batches)?;
        }
    }

    let avg = millis.iter().sum::<f64>() / millis.len() as f64;
    println!("Query {} avg time: {:.2} ms", opt.query, avg);

    Ok(())
}

fn get_query_sql(query: usize) -> Result<String> {
    if query > 0 && query < 23 {
        let filename = format!("queries/q{}.sql", query);
        Ok(fs::read_to_string(&filename).expect("failed to read query"))
    } else {
        Err(BallistaError::General(
            "invalid query. Expected value between 1 and 22".to_owned(),
        ))
    }
}

async fn convert_tbl(opt: ConvertOpt) -> Result<()> {
    let output_root_path = Path::new(&opt.output_path);
    for table in TABLES {
        let start = Instant::now();
        let schema = get_schema(table);

        let input_path = format!("{}/{}.tbl", opt.input_path.to_str().unwrap(), table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .file_extension(".tbl");

        let config = ExecutionConfig::new().with_batch_size(opt.batch_size);
        let mut ctx = ExecutionContext::with_config(config);

        // build plan to read the TBL file
        let mut csv = ctx.read_csv(&input_path, options)?;

        // optionally, repartition the file
        if opt.partitions > 1 {
            csv = csv.repartition(Partitioning::RoundRobinBatch(opt.partitions))?
        }

        // create the physical plan
        let csv = csv.to_logical_plan();
        let csv = ctx.optimize(&csv)?;
        let csv = ctx.create_physical_plan(&csv)?;

        let output_path = output_root_path.join(table);
        let output_path = output_path.to_str().unwrap().to_owned();

        println!(
            "Converting '{}' to {} files in directory '{}'",
            &input_path, &opt.file_format, &output_path
        );
        match opt.file_format.as_str() {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => {
                let compression = match opt.compression.as_str() {
                    "none" => Compression::UNCOMPRESSED,
                    "snappy" => Compression::SNAPPY,
                    "brotli" => Compression::BROTLI,
                    "gzip" => Compression::GZIP,
                    "lz4" => Compression::LZ4,
                    "lz0" => Compression::LZO,
                    "zstd" => Compression::ZSTD,
                    other => {
                        return Err(BallistaError::NotImplemented(format!(
                            "Invalid compression format: {}",
                            other
                        )))
                    }
                };
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .build();
                ctx.write_parquet(csv, output_path, Some(props)).await?
            }
            other => {
                return Err(BallistaError::NotImplemented(format!(
                    "Invalid output format: {}",
                    other
                )))
            }
        }
        println!("Conversion completed in {} ms", start.elapsed().as_millis());
    }

    Ok(())
}

fn get_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int32, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Float64, false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int32, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Float64, false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int32, false),
            Field::new("ps_suppkey", DataType::Int32, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Float64, false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int32, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int32, false),
            Field::new("o_custkey", DataType::Int32, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int32, false),
            Field::new("l_partkey", DataType::Int32, false),
            Field::new("l_suppkey", DataType::Int32, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

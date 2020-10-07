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

#![allow(bare_trait_objects)]

use arrow::util::pretty;
use clap::{crate_version, App, Arg};
use datafusion::error::Result;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use rustyline::Editor;
use std::env;
use std::path::Path;
use std::time::Instant;

#[tokio::main]
pub async fn main() {
    let matches = App::new("DataFusion")
        .version(crate_version!())
        .about(
            "DataFusion is an in-memory query engine that uses Apache Arrow \
             as the memory model. It supports executing SQL queries against CSV and \
             Parquet files as well as querying directly against in-memory data.",
        )
        .arg(
            Arg::with_name("data-path")
                .help("Path to your data, default to current directory")
                .short("p")
                .long("data-path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch-size")
                .help("The batch size of each query, default value is 1048576")
                .short("c")
                .long("batch-size")
                .takes_value(true),
        )
        .get_matches();

    if let Some(path) = matches.value_of("data-path") {
        let p = Path::new(path);
        env::set_current_dir(&p).unwrap();
    };

    let batch_size = matches
        .value_of("batch-size")
        .map(|size| size.parse::<usize>().unwrap())
        .unwrap_or(1_048_576);

    let mut ctx =
        ExecutionContext::with_config(ExecutionConfig::new().with_batch_size(batch_size));

    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if is_exit_command(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(&mut ctx, query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push_str(" ");
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

async fn exec_and_print(ctx: &mut ExecutionContext, sql: String) -> Result<()> {
    let now = Instant::now();

    let df = ctx.sql(&sql)?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!(
            "0 rows in set. Query took {} seconds.",
            now.elapsed().as_secs()
        );
        return Ok(());
    }

    pretty::print_batches(&results)?;

    let row_count: usize = results.iter().map(|b| b.num_rows()).sum();

    if row_count > 1 {
        println!(
            "{} row in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    } else {
        println!(
            "{} rows in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    }

    Ok(())
}

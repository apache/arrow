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

#[macro_use]
extern crate clap;

use clap::{App, Arg};
use std::path::Path;
use std::env;
use rustyline::Editor;
use datafusion::execution::context::ExecutionContext;
use std::cell::RefMut;
use datafusion::execution::relation::Relation;
use arrow::array::{BinaryArray, Float64Array, BooleanArray, ArrayRef};
use arrow::datatypes::DataType;
use prettytable::{Table, Cell, Row};

fn main() {
    let matches = App::new("DataFusion")
        .version(crate_version!())
        .about("DataFusion is an in-memory query engine that uses Apache Arrow \
         as the memory model. It supports executing SQL queries against CSV and \
         Parquet files as well as querying directly against in-memory data.")
        .arg(Arg::with_name("data-path")
            .help("Path to your data, default to current directory")
            .short("p")
            .long("data-path")
            .takes_value(true))
        .get_matches();

    let data_path = matches.value_of("data-path")
        .map(|p| Path::new(p))
        .unwrap_or(env::current_dir().unwrap().as_path());

    let mut ctx = ExecutionContext::new();

    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if line.trim_end().ends_with(";") => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                println!("{}", query);
                match ctx.sql(&query, 1024) {
                    Ok(result) => {
                        let mut result = result.borrow_mut();
                        print_result(result);
                    },
                    Err(err) => {
                        println!("{:?}", err);
                    },
                };
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push_str("\n");
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn print_result(mut results: RefMut<Relation>) {
    let mut row_count = 0;
    let mut table = Table::new();
    while let Some(batch) = results.next().unwrap() {
        row_count += batch.num_rows();

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&str_value(column.clone(), row)));
            }
            table.add_row(Row::new(cells));
        }
    }
    table.printstd();

    println!("{} row(s) in set.", row_count);
}

fn str_value(column: ArrayRef, row: usize) -> String {
    match column.data_type() {
        DataType::Utf8 => column.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .get_string(row),
        DataType::Boolean => column.as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row).to_string(),
        _ => column.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row).to_string(),
    }
}
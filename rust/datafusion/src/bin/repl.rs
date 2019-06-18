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

#[macro_use]
extern crate clap;

use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use clap::{App, Arg};
use datafusion::error::{ExecutionError, Result};
use datafusion::execution::context::ExecutionContext;
use datafusion::execution::relation::Relation;
use prettytable::{Cell, Row, Table};
use rustyline::Editor;
use std::cell::RefMut;
use std::env;
use std::path::Path;

fn main() {
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

    let mut ctx = ExecutionContext::new();

    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone());
                match exec_and_print(&mut ctx, query, batch_size) {
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

fn exec_and_print(
    ctx: &mut ExecutionContext,
    sql: String,
    batch_size: usize,
) -> Result<()> {
    let relation = ctx.sql(&sql, batch_size)?;
    print_result(relation.borrow_mut())?;

    Ok(())
}

fn print_result(mut results: RefMut<Relation>) -> Result<()> {
    let mut row_count = 0;
    let mut table = Table::new();
    let schema = results.schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(&field.name()));
    }
    table.add_row(Row::new(header));

    while let Some(batch) = results.next().unwrap() {
        row_count += batch.num_rows();

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(&str_value(column.clone(), row)?));
            }
            table.add_row(Row::new(cells));
        }
    }
    table.printstd();

    if row_count > 1 {
        println!("{} rows in set.", row_count);
    } else {
        println!("{} row in set.", row_count);
    }

    Ok(())
}

macro_rules! make_string {
    ($array_type:ty, $column: ident, $row: ident) => {{
        Ok($column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap()
            .value($row)
            .to_string())
    }};
}

fn str_value(column: ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Utf8 => Ok(column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .get_string(row)),
        DataType::Boolean => make_string!(BooleanArray, column, row),
        DataType::Int16 => make_string!(Int16Array, column, row),
        DataType::Int32 => make_string!(Int32Array, column, row),
        DataType::Int64 => make_string!(Int64Array, column, row),
        DataType::UInt8 => make_string!(UInt8Array, column, row),
        DataType::UInt16 => make_string!(UInt16Array, column, row),
        DataType::UInt32 => make_string!(UInt32Array, column, row),
        DataType::UInt64 => make_string!(UInt64Array, column, row),
        DataType::Float16 => make_string!(Float32Array, column, row),
        DataType::Float32 => make_string!(Float32Array, column, row),
        DataType::Float64 => make_string!(Float64Array, column, row),
        DataType::Timestamp(unit) if *unit == TimeUnit::Second => {
            make_string!(TimestampSecondArray, column, row)
        }
        DataType::Timestamp(unit) if *unit == TimeUnit::Millisecond => {
            make_string!(TimestampMillisecondArray, column, row)
        }
        DataType::Timestamp(unit) if *unit == TimeUnit::Microsecond => {
            make_string!(TimestampMicrosecondArray, column, row)
        }
        DataType::Timestamp(unit) if *unit == TimeUnit::Nanosecond => {
            make_string!(TimestampNanosecondArray, column, row)
        }
        DataType::Date32(_) => make_string!(Date32Array, column, row),
        DataType::Date64(_) => make_string!(Date64Array, column, row),
        DataType::Time32(unit) if *unit == TimeUnit::Second => {
            make_string!(Time32SecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            make_string!(Time32MillisecondArray, column, row)
        }
        DataType::Time32(unit) if *unit == TimeUnit::Microsecond => {
            make_string!(Time64MicrosecondArray, column, row)
        }
        DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            make_string!(Time64NanosecondArray, column, row)
        }
        _ => Err(ExecutionError::ExecutionError(format!(
            "Unsupported {:?} type for repl.",
            column.data_type()
        ))),
    }
}

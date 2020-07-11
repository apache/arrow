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

use clap::{App, Arg};
use serde_json::Value;

use arrow::util::integration_util::{ArrowJson, ArrowJsonBatch, ArrowJsonSchema};

use arrow::array::*;
use arrow::datatypes::{DataType, DateUnit, IntervalUnit, Schema};
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use hex::decode;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    eprintln!("{:?}", args);

    let matches = App::new("rust arrow-json-integration-test")
        .arg(Arg::with_name("integration")
            .long("integration"))
        .arg(Arg::with_name("arrow")
            .long("arrow")
            .help("path to ARROW file")
            .takes_value(true))
        .arg(Arg::with_name("json")
            .long("json")
            .help("path to JSON file")
            .takes_value(true))
        .arg(Arg::with_name("mode")
            .long("mode")
            .help("mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)")
            .takes_value(true)
            .default_value("VALIDATE"))
        .arg(Arg::with_name("verbose")
            .long("verbose")
            .help("enable/disable verbose mode"))
        .get_matches();

    let arrow_file = matches
        .value_of("arrow")
        .expect("must provide path to arrow file");
    let json_file = matches
        .value_of("json")
        .expect("must provide path to json file");
    let mode = matches.value_of("mode").unwrap();
    let verbose = true; //matches.value_of("verbose").is_some();

    match mode {
        "JSON_TO_ARROW" => json_to_arrow(json_file, arrow_file, verbose),
        "ARROW_TO_JSON" => arrow_to_json(arrow_file, json_file, verbose),
        "VALIDATE" => validate(arrow_file, json_file, verbose),
        _ => panic!(format!("mode {} not supported", mode)),
    }
}

fn json_to_arrow(json_name: &str, arrow_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", json_name, arrow_name);
    }

    let (schema, batches) = read_json_file(json_name)?;

    let arrow_file = File::create(arrow_name)?;
    let mut writer = FileWriter::try_new(arrow_file, &schema)?;

    for b in batches {
        writer.write(&b)?;
    }

    Ok(())
}

fn record_batch_from_json(
    schema: &Schema,
    json_batch: ArrowJsonBatch,
) -> Result<RecordBatch> {
    let mut columns = vec![];

    for (field, json_col) in schema.fields().iter().zip(json_batch.columns) {
        let col: ArrayRef = match field.data_type() {
            DataType::Null => Arc::new(NullArray::new(json_col.count)),
            DataType::Boolean => {
                let mut b = BooleanBuilder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_bool().unwrap()),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Int8 => {
                let mut b = Int8Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_i64().unwrap() as i8),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Int16 => {
                let mut b = Int16Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_i64().unwrap() as i16),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Int32
            | DataType::Date32(DateUnit::Day)
            | DataType::Time32(_)
            | DataType::Interval(IntervalUnit::YearMonth) => {
                let mut b = Int32Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_i64().unwrap() as i32),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                let array = Arc::new(b.finish()) as ArrayRef;
                arrow::compute::cast(&array, field.data_type()).unwrap()
            }
            DataType::Int64
            | DataType::Date64(DateUnit::Millisecond)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(IntervalUnit::DayTime) => {
                let mut b = Int64Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(
                            value
                                .as_str()
                                .unwrap()
                                .parse()
                                .expect("Unable to parse string as i64"),
                        ),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                let array = Arc::new(b.finish()) as ArrayRef;
                arrow::compute::cast(&array, field.data_type()).unwrap()
            }
            DataType::UInt8 => {
                let mut b = UInt8Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_u64().unwrap() as u8),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::UInt16 => {
                let mut b = UInt16Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_u64().unwrap() as u16),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::UInt32 => {
                let mut b = UInt32Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_u64().unwrap() as u32),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::UInt64 => {
                let mut b = UInt64Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(
                            value
                                .as_str()
                                .unwrap()
                                .parse()
                                .expect("Unable to parse string as u64"),
                        ),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Float32 => {
                let mut b = Float32Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_f64().unwrap() as f32),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_f64().unwrap()),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Binary => {
                let mut b = BinaryBuilder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => {
                            let v = decode(value.as_str().unwrap()).unwrap();
                            b.append_value(&v)
                        }
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::LargeBinary => {
                let mut b = LargeBinaryBuilder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => {
                            let v = decode(value.as_str().unwrap()).unwrap();
                            b.append_value(&v)
                        }
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::Utf8 => {
                let mut b = StringBuilder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_str().unwrap()),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::LargeUtf8 => {
                let mut b = LargeStringBuilder::new(json_col.count);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => b.append_value(value.as_str().unwrap()),
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            DataType::FixedSizeBinary(len) => {
                let mut b = FixedSizeBinaryBuilder::new(json_col.count, *len);
                for (is_valid, value) in json_col
                    .validity
                    .as_ref()
                    .unwrap()
                    .iter()
                    .zip(json_col.data.unwrap())
                {
                    match is_valid {
                        1 => {
                            let v = hex::decode(value.as_str().unwrap()).unwrap();
                            b.append_value(&v)
                        }
                        _ => b.append_null(),
                    }
                    .unwrap();
                }
                Arc::new(b.finish())
            }
            t => {
                return Err(ArrowError::JsonError(format!(
                    "data type {:?} not supported",
                    t
                )))
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
}

fn arrow_to_json(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", arrow_name, json_name);
    }

    let arrow_file = File::open(arrow_name)?;
    let mut reader = FileReader::try_new(arrow_file)?;

    let mut fields = vec![];
    for f in reader.schema().fields() {
        fields.push(f.to_json());
    }
    let schema = ArrowJsonSchema { fields };

    let mut batches = vec![];
    while let Ok(Some(batch)) = reader.next_batch() {
        batches.push(ArrowJsonBatch::from_batch(&batch));
    }

    let arrow_json = ArrowJson {
        schema,
        batches,
        dictionaries: None,
    };

    let json_file = File::create(json_name)?;
    serde_json::to_writer(&json_file, &arrow_json).unwrap();

    Ok(())
}

fn validate(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Validating {} and {}", arrow_name, json_name);
    }

    // open JSON file
    let (json_schema, json_batches) = read_json_file(json_name)?;

    // open Arrow file
    let arrow_file = File::open(arrow_name)?;
    let mut arrow_reader = FileReader::try_new(arrow_file)?;
    let arrow_schema = arrow_reader.schema().as_ref().to_owned();

    // compare schemas
    if json_schema != arrow_schema {
        return Err(ArrowError::ComputeError(format!(
            "Schemas do not match. JSON: {:?}. Arrow: {:?}",
            json_schema, arrow_schema
        )));
    }

    // compare number of batches
    assert!(
        json_batches.len() == arrow_reader.num_batches(),
        "JSON batches and Arrow batches are unequal"
    );

    if verbose {
        eprintln!(
            "Schemas match. JSON file has {} batches.",
            json_batches.len()
        );
    }

    for json_batch in &json_batches {
        if let Some(arrow_batch) = arrow_reader.next_batch()? {
            // compare batches
            assert!(arrow_batch.num_columns() == json_batch.num_columns());
            assert!(arrow_batch.num_rows() == json_batch.num_rows());

            // TODO compare in more detail
            eprintln!(
                "Basic validation of {} and {} PASSES",
                arrow_name, json_name
            );
        } else {
            return Err(ArrowError::ComputeError(
                "no more arrow batches left".to_owned(),
            ));
        }
    }

    if let Some(_) = arrow_reader.next_batch()? {
        return Err(ArrowError::ComputeError(
            "no more json batches left".to_owned(),
        ));
    }

    Ok(())
}

fn read_json_file(json_name: &str) -> Result<(Schema, Vec<RecordBatch>)> {
    let json_file = File::open(json_name)?;
    let reader = BufReader::new(json_file);
    let arrow_json: Value = serde_json::from_reader(reader).unwrap();
    let schema = Schema::from(&arrow_json["schema"])?;
    let mut batches = vec![];
    for b in arrow_json["batches"].as_array().unwrap() {
        let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
        let batch = record_batch_from_json(&schema, json_batch)?;
        batches.push(batch);
    }
    Ok((schema, batches))
}

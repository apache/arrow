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

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use clap::{App, Arg};
use hex::decode;
use serde_json::Value;

use arrow::array::*;
use arrow::datatypes::{DataType, DateUnit, Field, IntervalUnit, Schema};
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use arrow::{
    buffer::Buffer,
    buffer::MutableBuffer,
    datatypes::ToByteSlice,
    util::{bit_util, integration_util::*},
};

fn main() -> Result<()> {
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

    let json_file = read_json_file(json_name)?;

    let arrow_file = File::create(arrow_name)?;
    let mut writer = FileWriter::try_new(arrow_file, &json_file.schema)?;

    for b in json_file.batches {
        writer.write(&b)?;
    }

    Ok(())
}

fn record_batch_from_json(
    schema: &Schema,
    json_batch: ArrowJsonBatch,
    json_dictionaries: Option<&HashMap<i64, ArrowJsonDictionaryBatch>>,
) -> Result<RecordBatch> {
    let mut columns = vec![];

    for (field, json_col) in schema.fields().iter().zip(json_batch.columns) {
        let col = array_from_json(field, json_col, json_dictionaries)?;
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), columns)
}

/// Construct an Arrow array from a partially typed JSON column
fn array_from_json(
    field: &Field,
    json_col: ArrowJsonColumn,
    dictionaries: Option<&HashMap<i64, ArrowJsonDictionaryBatch>>,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Null => Ok(Arc::new(NullArray::new(json_col.count))),
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                    1 => b.append_value(value.as_i64().ok_or_else(|| {
                        ArrowError::JsonError(format!(
                            "Unable to get {:?} as int64",
                            value
                        ))
                    })? as i8),
                    _ => b.append_null(),
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            let array = Arc::new(b.finish()) as ArrayRef;
            arrow::compute::cast(&array, field.data_type())
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
                    1 => b.append_value(match value {
                        Value::Number(n) => n.as_i64().unwrap(),
                        Value::String(s) => {
                            s.parse().expect("Unable to parse string as i64")
                        }
                        _ => panic!("Unable to parse {:?} as number", value),
                    }),
                    _ => b.append_null(),
                }?;
            }
            let array = Arc::new(b.finish()) as ArrayRef;
            arrow::compute::cast(&array, field.data_type())
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
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
                }?;
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::List(child_field) => {
            let null_buf = create_null_buf(&json_col);
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(
                &child_field,
                children.get(0).unwrap().clone(),
                dictionaries,
            )?;
            let offsets: Vec<i32> = json_col
                .offset
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect();
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .offset(0)
                .add_buffer(Buffer::from(&offsets.to_byte_slice()))
                .add_child_data(child_array.data())
                .null_bit_buffer(null_buf)
                .build();
            Ok(Arc::new(ListArray::from(list_data)))
        }
        DataType::LargeList(child_field) => {
            let null_buf = create_null_buf(&json_col);
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(
                &child_field,
                children.get(0).unwrap().clone(),
                dictionaries,
            )?;
            let offsets: Vec<i64> = json_col
                .offset
                .unwrap()
                .iter()
                .map(|v| match v {
                    Value::Number(n) => n.as_i64().unwrap(),
                    Value::String(s) => s.parse::<i64>().unwrap(),
                    _ => panic!("64-bit offset must be either string or number"),
                })
                .collect();
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .offset(0)
                .add_buffer(Buffer::from(&offsets.to_byte_slice()))
                .add_child_data(child_array.data())
                .null_bit_buffer(null_buf)
                .build();
            Ok(Arc::new(LargeListArray::from(list_data)))
        }
        DataType::FixedSizeList(child_field, _) => {
            let children = json_col.children.clone().unwrap();
            let child_array = array_from_json(
                &child_field,
                children.get(0).unwrap().clone(),
                dictionaries,
            )?;
            let null_buf = create_null_buf(&json_col);
            let list_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .add_child_data(child_array.data())
                .null_bit_buffer(null_buf)
                .build();
            Ok(Arc::new(FixedSizeListArray::from(list_data)))
        }
        DataType::Struct(fields) => {
            // construct struct with null data
            let null_buf = create_null_buf(&json_col);
            let mut array_data = ArrayData::builder(field.data_type().clone())
                .len(json_col.count)
                .null_bit_buffer(null_buf);

            for (field, col) in fields.iter().zip(json_col.children.unwrap()) {
                let array = array_from_json(field, col, dictionaries)?;
                array_data = array_data.add_child_data(array.data());
            }

            let array = StructArray::from(array_data.build());
            Ok(Arc::new(array))
        }
        DataType::Dictionary(key_type, value_type) => {
            let dict_id = field.dict_id().ok_or_else(|| {
                ArrowError::JsonError(format!(
                    "Unable to find dict_id for field {:?}",
                    field
                ))
            })?;
            // find dictionary
            let dictionary = dictionaries
                .ok_or_else(|| {
                    ArrowError::JsonError(format!(
                        "Unable to find any dictionaries for field {:?}",
                        field
                    ))
                })?
                .get(&dict_id);
            match dictionary {
                Some(dictionary) => dictionary_array_from_json(
                    field, json_col, key_type, value_type, dictionary,
                ),
                None => Err(ArrowError::JsonError(format!(
                    "Unable to find dictionary for field {:?}",
                    field
                ))),
            }
        }
        t => Err(ArrowError::JsonError(format!(
            "data type {:?} not supported",
            t
        ))),
    }
}

fn dictionary_array_from_json(
    field: &Field,
    json_col: ArrowJsonColumn,
    dict_key: &DataType,
    dict_value: &DataType,
    dictionary: &ArrowJsonDictionaryBatch,
) -> Result<ArrayRef> {
    match dict_key {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let null_buf = create_null_buf(&json_col);

            // build the key data into a buffer, then construct values separately
            let key_field = Field::new_dict(
                "key",
                dict_key.clone(),
                field.is_nullable(),
                field
                    .dict_id()
                    .expect("Dictionary fields must have a dict_id value"),
                field
                    .dict_is_ordered()
                    .expect("Dictionary fields must have a dict_is_ordered value"),
            );
            let keys = array_from_json(&key_field, json_col, None)?;
            // note: not enough info on nullability of dictionary
            let value_field = Field::new("value", dict_value.clone(), true);
            println!("dictionary value type: {:?}", dict_value);
            let values =
                array_from_json(&value_field, dictionary.data.columns[0].clone(), None)?;

            // convert key and value to dictionary data
            let dict_data = ArrayData::builder(field.data_type().clone())
                .len(keys.len())
                .add_buffer(keys.data().buffers()[0].clone())
                .null_bit_buffer(null_buf)
                .add_child_data(values.data())
                .build();

            let array = match dict_key {
                DataType::Int8 => {
                    Arc::new(Int8DictionaryArray::from(dict_data)) as ArrayRef
                }
                DataType::Int16 => Arc::new(Int16DictionaryArray::from(dict_data)),
                DataType::Int32 => Arc::new(Int32DictionaryArray::from(dict_data)),
                DataType::Int64 => Arc::new(Int64DictionaryArray::from(dict_data)),
                DataType::UInt8 => Arc::new(UInt8DictionaryArray::from(dict_data)),
                DataType::UInt16 => Arc::new(UInt16DictionaryArray::from(dict_data)),
                DataType::UInt32 => Arc::new(UInt32DictionaryArray::from(dict_data)),
                DataType::UInt64 => Arc::new(UInt64DictionaryArray::from(dict_data)),
                _ => unreachable!(),
            };
            Ok(array)
        }
        _ => Err(ArrowError::JsonError(format!(
            "Dictionary key type {:?} not supported",
            dict_key
        ))),
    }
}

/// A helper to create a null buffer from a Vec<bool>
fn create_null_buf(json_col: &ArrowJsonColumn) -> Buffer {
    let num_bytes = bit_util::ceil(json_col.count, 8);
    let mut null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
    json_col
        .validity
        .clone()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(i, v)| {
            let null_slice = null_buf.data_mut();
            if *v != 0 {
                bit_util::set_bit(null_slice, i);
            }
        });
    null_buf.freeze()
}

fn arrow_to_json(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", arrow_name, json_name);
    }

    let arrow_file = File::open(arrow_name)?;
    let reader = FileReader::try_new(arrow_file)?;

    let mut fields: Vec<ArrowJsonField> = vec![];
    for f in reader.schema().fields() {
        fields.push(ArrowJsonField::from(f));
    }
    let schema = ArrowJsonSchema { fields };

    let batches = reader
        .map(|batch| Ok(ArrowJsonBatch::from_batch(&batch?)))
        .collect::<Result<Vec<_>>>()?;

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
    let json_file = read_json_file(json_name)?;

    // open Arrow file
    let arrow_file = File::open(arrow_name)?;
    let mut arrow_reader = FileReader::try_new(arrow_file)?;
    let arrow_schema = arrow_reader.schema().as_ref().to_owned();

    // compare schemas
    if json_file.schema != arrow_schema {
        return Err(ArrowError::ComputeError(format!(
            "Schemas do not match. JSON: {:?}. Arrow: {:?}",
            json_file.schema, arrow_schema
        )));
    }

    let json_batches = &json_file.batches;

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

    for json_batch in json_batches {
        if let Some(Ok(arrow_batch)) = arrow_reader.next() {
            // compare batches
            let num_columns = arrow_batch.num_columns();
            assert!(num_columns == json_batch.num_columns());
            assert!(arrow_batch.num_rows() == json_batch.num_rows());

            for i in 0..num_columns {
                assert_eq!(
                    arrow_batch.column(i).data(),
                    json_batch.column(i).data(),
                    "Arrow and JSON batch columns not the same"
                );
            }
        } else {
            return Err(ArrowError::ComputeError(
                "no more arrow batches left".to_owned(),
            ));
        }
    }

    if arrow_reader.next().is_some() {
        return Err(ArrowError::ComputeError(
            "no more json batches left".to_owned(),
        ));
    }

    Ok(())
}

struct ArrowFile {
    schema: Schema,
    // we can evolve this into a concrete Arrow type
    // this is temporarily not being read from
    _dictionaries: HashMap<i64, ArrowJsonDictionaryBatch>,
    batches: Vec<RecordBatch>,
}

fn read_json_file(json_name: &str) -> Result<ArrowFile> {
    let json_file = File::open(json_name)?;
    let reader = BufReader::new(json_file);
    let arrow_json: Value = serde_json::from_reader(reader).unwrap();
    let schema = Schema::from(&arrow_json["schema"])?;
    // read dictionaries
    let mut dictionaries = HashMap::new();
    if let Some(dicts) = arrow_json.get("dictionaries") {
        for d in dicts
            .as_array()
            .expect("Unable to get dictionaries as array")
        {
            let json_dict: ArrowJsonDictionaryBatch = serde_json::from_value(d.clone())
                .expect("Unable to get dictionary from JSON");
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }

    let mut batches = vec![];
    for b in arrow_json["batches"].as_array().unwrap() {
        let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
        let batch = record_batch_from_json(&schema, json_batch, Some(&dictionaries))?;
        batches.push(batch);
    }
    Ok(ArrowFile {
        schema,
        _dictionaries: dictionaries,
        batches,
    })
}

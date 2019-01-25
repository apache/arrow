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

use parquet::Deserialize;

#[derive(Deserialize)]
struct Abc {
    a: String,
}

#[derive(Deserialize)]
struct Def {
    #[parquet(rename = "!@£$%^&*(")]
    a: String,
}

// #[derive(Deserialize)]
// struct Ghi {
// 	#[parquet(rename = 123)]
//     a: String,
// }

#[derive(Deserialize)]
struct Jkl<M> {
    a: M,
}

use std::str::FromStr;
use std::{collections::HashMap, env, fs, mem, path::PathBuf, thread};

use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{
    types::{Group, List, Map, Root, Row, Value},
    Deserialize,
};
use parquet::schema::types::Type;

macro_rules! list {
    ( $( $e:expr ), * ) => {
        {
            #[allow(unused_mut)]
            let mut result = Vec::new();
            $(
                result.push($e);
            )*
            List::from(result)
        }
    }
}

macro_rules! map {
    ( $( ($k:expr, $v:expr) ), * ) => {
        {
            #[allow(unused_mut)]
            let mut result = HashMap::new();
            $(
                result.insert($k, $v);
            )*
            Map::from(result)
        }
    }
}

#[test]
fn test_file_reader_rows_nonnullable_derived() {
    #[derive(PartialEq, Deserialize, Debug)]
    struct RowDerived {
        #[parquet(rename = "ID")]
        id: i64,
        #[parquet(rename = "Int_Array")]
        int_array: List<i32>,
        int_array_array: List<List<i32>>,
        #[parquet(rename = "Int_Map")]
        int_map: Map<String, i32>,
        int_map_array: List<Map<String, i32>>,
        #[parquet(rename = "nested_Struct")]
        nested_struct: RowDerivedInner,
    }

    #[derive(PartialEq, Deserialize, Debug)]
    struct RowDerivedInner {
        a: i32,
        #[parquet(rename = "B")]
        b: List<i32>,
        c: RowDerivedInnerInner,
        #[parquet(rename = "G")]
        g: Map<String, ((List<f64>,),)>,
    }

    #[derive(PartialEq, Deserialize, Debug)]
    struct RowDerivedInnerInner {
        #[parquet(rename = "D")]
        d: List<List<RowDerivedInnerInnerInner>>,
    }

    #[derive(PartialEq, Deserialize, Debug)]
    struct RowDerivedInnerInnerInner {
        e: i32,
        f: String,
    }

    let rows = test_file_reader_rows::<RowDerived>("nonnullable.impala.parquet", None).unwrap();

    let expected_rows: Vec<RowDerived> = vec![RowDerived {
        id: 8,
        int_array: list![-1],
        int_array_array: list![list![-1, -2], list![]],
        int_map: map![("k1".to_string(), -1)],
        int_map_array: list![map![], map![("k1".to_string(), 1i32)], map![], map![]],
        nested_struct: RowDerivedInner {
            a: -1,
            b: list![-1],
            c: RowDerivedInnerInner {
                d: list![list![RowDerivedInnerInnerInner {
                    e: -1,
                    f: "nonnullable".to_string()
                }]],
            },
            g: map![],
        },
    }];

    assert_eq!(rows, expected_rows);
}

fn test_file_reader_rows<T>(file_name: &str, schema: Option<Type>) -> Result<Vec<T>, ParquetError>
where
    T: Deserialize,
{
    let file = get_test_file(file_name);
    let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
    // println!("<Root<T> as Deserialize>::Reader: {}", mem::size_of::<<Root<T> as Deserialize>::Reader>());
    let iter = file_reader.get_row_iter(schema)?;
    Ok(iter.collect())
}

fn get_test_file(file_name: &str) -> fs::File {
    let file = fs::File::open(get_test_path(file_name).as_path());
    if file.is_err() {
        panic!("Test file {} not found", file_name)
    }
    file.unwrap()
}

fn get_test_path(file_name: &str) -> PathBuf {
    let result = env::var("PARQUET_TEST_DATA");
    if result.is_err() {
        panic!("Please point PARQUET_TEST_DATA environment variable to the test data directory");
    }
    let mut pathbuf = PathBuf::from_str(&result.unwrap()).unwrap();
    pathbuf.push(file_name);
    pathbuf
}

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

use std::{collections::HashMap, env, fs, path::PathBuf, str::FromStr};

use parquet::{
    errors::ParquetError,
    file::reader::{FileReader, RowGroupReader, SerializedFileReader},
    record::{
        types::{List, Map},
        Record,
    },
};

#[allow(dead_code)]
#[derive(Record)]
struct Abc {
    a: String,
}

#[allow(dead_code)]
#[derive(Record)]
struct Def {
    #[parquet(rename = "!@£$%^&*(")]
    a: String,
}

// #[derive(Record)]
// struct Ghi {
// 	#[parquet(rename = 123)]
//     a: String,
// }

#[allow(dead_code)]
#[derive(Record)]
struct Jkl<M> {
    a: M,
}

#[allow(dead_code)]
#[derive(Record)]
struct Mno {}

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
    #[derive(PartialEq, Record, Debug)]
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

    #[derive(PartialEq, Record, Debug)]
    struct RowDerivedInner {
        a: i32,
        #[parquet(rename = "B")]
        b: List<i32>,
        c: RowDerivedInnerInner,
        #[parquet(rename = "G")]
        g: Map<String, ((List<f64>,),)>,
    }

    #[derive(PartialEq, Record, Debug)]
    struct RowDerivedInnerInner {
        #[parquet(rename = "D")]
        d: List<List<RowDerivedInnerInnerInner>>,
    }

    #[derive(PartialEq, Record, Debug)]
    struct RowDerivedInnerInnerInner {
        e: i32,
        f: String,
    }

    let rows =
        test_file_reader_rows::<RowDerived>("nonnullable.impala.parquet", None).unwrap();

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

#[test]
fn test_file_reader_rows_projection_derived() {
    #[derive(PartialEq, Record, Debug)]
    struct SparkSchema {
        c: f64,
        b: i32,
    }

    let rows =
        test_file_reader_rows::<SparkSchema>("nested_maps.snappy.parquet", None).unwrap();

    let expected_rows = vec![
        SparkSchema { c: 1.0, b: 1 },
        SparkSchema { c: 1.0, b: 1 },
        SparkSchema { c: 1.0, b: 1 },
        SparkSchema { c: 1.0, b: 1 },
        SparkSchema { c: 1.0, b: 1 },
        SparkSchema { c: 1.0, b: 1 },
    ];

    assert_eq!(rows, expected_rows);
}

#[test]
fn test_file_reader_rows_projection_map_derived() {
    #[derive(PartialEq, Record, Debug)]
    struct SparkSchema {
        a: Option<Map<String, Option<Map<i32, bool>>>>,
    }

    let rows =
        test_file_reader_rows::<SparkSchema>("nested_maps.snappy.parquet", None).unwrap();

    let expected_rows = vec![
        SparkSchema {
            a: Some(map![("a".to_string(), Some(map![(1, true), (2, false)]))]),
        },
        SparkSchema {
            a: Some(map![("b".to_string(), Some(map![(1, true)]))]),
        },
        SparkSchema {
            a: Some(map![("c".to_string(), None)]),
        },
        SparkSchema {
            a: Some(map![("d".to_string(), Some(map![]))]),
        },
        SparkSchema {
            a: Some(map![("e".to_string(), Some(map![(1, true)]))]),
        },
        SparkSchema {
            a: Some(map![(
                "f".to_string(),
                Some(map![(3, true), (4, false), (5, true)])
            )]),
        },
    ];

    assert_eq!(rows, expected_rows);
}

#[test]
fn test_file_reader_rows_projection_list_derived() {
    #[derive(PartialEq, Record, Debug)]
    struct SparkSchema {
        a: Option<List<Option<List<Option<List<Option<String>>>>>>>,
    }

    let rows = test_file_reader_rows::<SparkSchema>("nested_lists.snappy.parquet", None)
        .unwrap();

    let expected_rows = vec![
        SparkSchema {
            a: Some(list![
                Some(list![
                    Some(list![Some("a".to_string()), Some("b".to_string())]),
                    Some(list![Some("c".to_string())])
                ]),
                Some(list![None, Some(list![Some("d".to_string())])])
            ]),
        },
        SparkSchema {
            a: Some(list![
                Some(list![
                    Some(list![Some("a".to_string()), Some("b".to_string())]),
                    Some(list![Some("c".to_string()), Some("d".to_string())])
                ]),
                Some(list![None, Some(list![Some("e".to_string())])])
            ]),
        },
        SparkSchema {
            a: Some(list![
                Some(list![
                    Some(list![Some("a".to_string()), Some("b".to_string())]),
                    Some(list![Some("c".to_string()), Some("d".to_string())]),
                    Some(list![Some("e".to_string())])
                ]),
                Some(list![None, Some(list![Some("f".to_string())])])
            ]),
        },
    ];

    assert_eq!(rows, expected_rows);
}

#[test]
fn test_file_reader_rows_invalid_projection_derived() {
    #[derive(PartialEq, Record, Debug)]
    struct SparkSchema {
        key: i32,
        value: bool,
    }

    let res = test_file_reader_rows::<SparkSchema>("nested_maps.snappy.parquet", None);

    assert_eq!(
        res.unwrap_err(),
        ParquetError::General("Types don't match schema.\nSchema is:\nmessage spark_schema {\n    OPTIONAL group a (MAP) {\n        REPEATED group key_value {\n            REQUIRED byte_array key (UTF8);\n            OPTIONAL group value (MAP) {\n                REPEATED group key_value {\n                    REQUIRED int32 key (INT_32);\n                    REQUIRED boolean value;\n                }\n            }\n        }\n    }\n    REQUIRED int32 b (INT_32);\n    REQUIRED double c;\n}\nBut types require:\nmessage <name> {\n    REQUIRED int32 key (INT_32);\n    REQUIRED boolean value;\n}\nError: Parquet error: Struct \"SparkSchema\" has field \"key\" not in the schema".to_string())
    );
}

#[test]
fn test_row_group_rows_invalid_projection_derived() {
    #[derive(PartialEq, Record, Debug)]
    struct SparkSchema {
        key: i32,
        value: bool,
    }

    let res = test_row_group_rows::<SparkSchema>("nested_maps.snappy.parquet", None);

    assert_eq!(
        res.unwrap_err(),
        ParquetError::General("Types don't match schema.\nSchema is:\nmessage spark_schema {\n    OPTIONAL group a (MAP) {\n        REPEATED group key_value {\n            REQUIRED byte_array key (UTF8);\n            OPTIONAL group value (MAP) {\n                REPEATED group key_value {\n                    REQUIRED int32 key (INT_32);\n                    REQUIRED boolean value;\n                }\n            }\n        }\n    }\n    REQUIRED int32 b (INT_32);\n    REQUIRED double c;\n}\nBut types require:\nmessage <name> {\n    REQUIRED int32 key (INT_32);\n    REQUIRED boolean value;\n}\nError: Parquet error: Struct \"SparkSchema\" has field \"key\" not in the schema".to_string())
    );
}

// #[test]
// #[should_panic(expected = "Invalid map type")]
// fn test_file_reader_rows_invalid_map_type_derived() {
//     let schema = "
//     message spark_schema {
//       OPTIONAL group a (MAP) {
//         REPEATED group key_value {
//           REQUIRED BYTE_ARRAY key (UTF8);
//           OPTIONAL group value (MAP) {
//             REPEATED group key_value {
//               REQUIRED INT32 key;
//             }
//           }
//         }
//       }
//     }
//   ";
//     let schema = parse_message_type(&schema).unwrap();
//     test_file_reader_rows::<Row>("nested_maps.snappy.parquet", Some(schema)).unwrap();
// }

fn test_file_reader_rows<T>(
    file_name: &str,
    schema: Option<()>,
) -> Result<Vec<T>, ParquetError>
where
    T: Record,
{
    assert!(schema.is_none());
    let file = get_test_file(file_name);
    let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
    let iter = file_reader.get_row_iter(None)?;
    Ok(iter.collect())
}

fn test_row_group_rows<T>(
    file_name: &str,
    schema: Option<()>,
) -> Result<Vec<T>, ParquetError>
where
    T: Record,
{
    assert!(schema.is_none());
    let file = get_test_file(file_name);
    let file_reader: SerializedFileReader<_> = SerializedFileReader::new(file)?;
    // Check the first row group only, because files will contain only single row group
    let row_group_reader = file_reader.get_row_group(0).unwrap();
    let iter = row_group_reader.get_row_iter(None)?;
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

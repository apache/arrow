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

extern crate arrow;
extern crate criterion;

use criterion::*;

use arrow::datatypes::*;
use arrow::json::ReaderBuilder;
use std::io::Cursor;
use std::sync::Arc;

fn json_primitive_to_record_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Float64, true),
        Field::new("c3", DataType::UInt32, true),
        Field::new("c4", DataType::Boolean, true),
    ]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let json_content = r#"
        {"c1": "eleven", "c2": 6.2222222225, "c3": 5.0, "c4": false}
        {"c1": "twelve", "c2": -55555555555555.2, "c3": 3}
        {"c1": null, "c2": 3, "c3": 125, "c4": null}
        {"c2": -35, "c3": 100.0, "c4": true}
        {"c1": "fifteen", "c2": null, "c4": true}
        {"c1": "eleven", "c2": 6.2222222225, "c3": 5.0, "c4": false}
        {"c1": "twelve", "c2": -55555555555555.2, "c3": 3}
        {"c1": null, "c2": 3, "c3": 125, "c4": null}
        {"c2": -35, "c3": 100.0, "c4": true}
        {"c1": "fifteen", "c2": null, "c4": true}
        "#;
    let cursor = Cursor::new(json_content);
    let mut reader = builder.build(cursor).unwrap();
    #[allow(clippy::unit_arg)]
    criterion::black_box({
        reader.next().unwrap();
    });
}

fn json_list_primitive_to_record_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "c1",
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "c2",
            DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
            true,
        ),
        Field::new(
            "c3",
            DataType::List(Box::new(Field::new("item", DataType::UInt32, true))),
            true,
        ),
        Field::new(
            "c4",
            DataType::List(Box::new(Field::new("item", DataType::Boolean, true))),
            true,
        ),
    ]));
    let builder = ReaderBuilder::new().with_schema(schema).with_batch_size(64);
    let json_content = r#"
        {"c1": ["eleven"], "c2": [6.2222222225, -3.2, null], "c3": [5.0, 6], "c4": [false, true]}
        {"c1": ["twelve"], "c2": [-55555555555555.2, 12500000.0], "c3": [3, 4, 5]}
        {"c1": null, "c2": [3], "c3": [125, 127, 129], "c4": [null, false, true]}
        {"c2": [-35], "c3": [100.0, 200.0], "c4": null}
        {"c1": ["fifteen"], "c2": [null, 2.1, 1.5, -3], "c4": [true, false, null]}
        {"c1": ["fifteen"], "c2": [], "c4": [true, false, null]}
        {"c1": ["eleven"], "c2": [6.2222222225, -3.2, null], "c3": [5.0, 6], "c4": [false, true]}
        {"c1": ["twelve"], "c2": [-55555555555555.2, 12500000.0], "c3": [3, 4, 5]}
        {"c1": null, "c2": [3], "c3": [125, 127, 129], "c4": [null, false, true]}
        {"c2": [-35], "c3": [100.0, 200.0], "c4": null}
        {"c1": ["fifteen"], "c2": [null, 2.1, 1.5, -3], "c4": [true, false, null]}
        {"c1": ["fifteen"], "c2": [], "c4": [true, false, null]}
        "#;
    let cursor = Cursor::new(json_content);
    let mut reader = builder.build(cursor).unwrap();
    #[allow(clippy::unit_arg)]
    criterion::black_box({
        reader.next().unwrap();
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("json_primitive_to_record_batch", |b| {
        b.iter(json_primitive_to_record_batch)
    });
    c.bench_function("json_list_primitive_to_record_batch", |b| {
        b.iter(json_list_primitive_to_record_batch)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

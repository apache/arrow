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

///! This example demonstrates dealing with mixed types dynamically at runtime
use std::rc::Rc;

extern crate arrow;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::*;

fn main() {
    // define schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "nested",
            DataType::Struct(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Float64, false),
            ]),
            false,
        ),
    ]);

    // create some data
    let id = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);

    let nested = StructArray::from(vec![
        Rc::new(PrimitiveArray::from(vec!["a", "b", "c", "d", "e"])) as Rc<Array>,
        Rc::new(PrimitiveArray::from(vec![1.1, 2.2, 3.3, 4.4, 5.5])),
        Rc::new(PrimitiveArray::from(vec![2.2, 3.3, 4.4, 5.5, 6.6])),
    ]);

    // build a record batch
    let batch = RecordBatch::new(Rc::new(schema), vec![Rc::new(id), Rc::new(nested)]);

    process(&batch);
}

/// Create a new batch by performing a projection of id, (nested.b + nested.c) AS sum
fn process(batch: &RecordBatch) {
    let id = batch.column(0);
    let nested = batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let nested_b = nested
        .column(1)
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    let nested_c = nested
        .column(2)
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();

    let projected_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("sum", DataType::Float64, false),
    ]);

    let _ = RecordBatch::new(
        Rc::new(projected_schema),
        vec![
            id.clone(), //NOTE: this is cloning the Rc not the array data
            Rc::new(nested_b.add(nested_c)),
        ],
    );
}

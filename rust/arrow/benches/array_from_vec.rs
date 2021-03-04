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
extern crate criterion;
use criterion::Criterion;

extern crate arrow;

use arrow::array::*;
use arrow::buffer::Buffer;
use arrow::datatypes::*;
use std::{convert::TryFrom, sync::Arc};

fn array_from_vec(n: usize) {
    let mut v: Vec<u8> = Vec::with_capacity(n);
    for i in 0..n {
        v.push((i & 0xffff) as u8);
    }
    let arr_data = ArrayDataBuilder::new(DataType::Int32)
        .add_buffer(Buffer::from(v))
        .build();
    criterion::black_box(Int32Array::from(arr_data));
}

fn array_string_from_vec(n: usize) {
    let mut v: Vec<Option<&str>> = Vec::with_capacity(n);
    for i in 0..n {
        if i % 2 == 0 {
            v.push(Some("hello world"));
        } else {
            v.push(None);
        }
    }
    criterion::black_box(StringArray::from(v));
}

fn struct_array_values(
    n: usize,
) -> (
    &'static str,
    Vec<Option<&'static str>>,
    &'static str,
    Vec<Option<i32>>,
) {
    let mut strings: Vec<Option<&str>> = Vec::with_capacity(n);
    let mut ints: Vec<Option<i32>> = Vec::with_capacity(n);
    for _ in 0..n / 4 {
        strings.extend_from_slice(&[Some("joe"), None, None, Some("mark")]);
        ints.extend_from_slice(&[Some(1), Some(2), None, Some(4)]);
    }
    ("f1", strings, "f2", ints)
}

fn struct_array_from_vec(
    field1: &str,
    strings: &[Option<&str>],
    field2: &str,
    ints: &[Option<i32>],
) {
    let strings: ArrayRef = Arc::new(StringArray::from(strings.to_owned()));
    let ints: ArrayRef = Arc::new(Int32Array::from(ints.to_owned()));

    criterion::black_box(
        StructArray::try_from(vec![(field1, strings), (field2, ints)]).unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("array_from_vec 128", |b| b.iter(|| array_from_vec(128)));
    c.bench_function("array_from_vec 256", |b| b.iter(|| array_from_vec(256)));
    c.bench_function("array_from_vec 512", |b| b.iter(|| array_from_vec(512)));

    c.bench_function("array_string_from_vec 128", |b| {
        b.iter(|| array_string_from_vec(128))
    });
    c.bench_function("array_string_from_vec 256", |b| {
        b.iter(|| array_string_from_vec(256))
    });
    c.bench_function("array_string_from_vec 512", |b| {
        b.iter(|| array_string_from_vec(512))
    });

    let (field1, strings, field2, ints) = struct_array_values(128);
    c.bench_function("struct_array_from_vec 128", |b| {
        b.iter(|| struct_array_from_vec(&field1, &strings, &field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(256);
    c.bench_function("struct_array_from_vec 256", |b| {
        b.iter(|| struct_array_from_vec(&field1, &strings, &field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(512);
    c.bench_function("struct_array_from_vec 512", |b| {
        b.iter(|| struct_array_from_vec(&field1, &strings, &field2, &ints))
    });

    let (field1, strings, field2, ints) = struct_array_values(1024);
    c.bench_function("struct_array_from_vec 1024", |b| {
        b.iter(|| struct_array_from_vec(&field1, &strings, &field2, &ints))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

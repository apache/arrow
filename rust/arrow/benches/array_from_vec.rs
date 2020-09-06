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

fn struct_array_values(n: usize) -> (Field, Vec<Option<&'static str>>, Field, Vec<Option<i32>>) {
    let mut strings: Vec<Option<&str>> = Vec::with_capacity(n);
    let mut ints: Vec<Option<i32>> = Vec::with_capacity(n);
    for _ in 0..n / 4 {
        strings.extend_from_slice(&[Some("joe"), None, None, Some("mark")]);
        ints.extend_from_slice(&[Some(1), Some(2), None, Some(4)]);
    }
    (Field::new("f1", DataType::Utf8, false),
     strings, Field::new("f2", DataType::Int32, false), ints)
}

fn struct_array_from_vec(field1: &Field, strings: &Vec<Option<&str>>, field2: &Field, ints: &Vec<Option<i32>>) {

    criterion::black_box({
        let len = strings.len();
        // this cheats a bit, as the compiler knows the that they will be strings and i32, but well
        let string_builder = StringBuilder::new(len);
        let int_builder = Int32Builder::new(len);

        let mut fields = Vec::new();
        let mut field_builders = Vec::new();
        fields.push(field1.clone());
        field_builders.push(Box::new(string_builder) as Box<dyn ArrayBuilder>);
        fields.push(field2.clone());
        field_builders.push(Box::new(int_builder) as Box<dyn ArrayBuilder>);

        let mut builder = StructBuilder::new(fields, field_builders);
        assert_eq!(2, builder.num_fields());

        let string_builder = builder
            .field_builder::<StringBuilder>(0)
            .expect("builder at field 0 should be string builder");
        for string in strings {
            if string.is_some() {
                string_builder.append_value(string.unwrap()).unwrap();
            } else {
                string_builder.append_null().unwrap();
            }
        }

        let int_builder = builder
            .field_builder::<Int32Builder>(1)
            .expect("builder at field 1 should be int builder");
        for int in ints {
            if int.is_some() {
                int_builder.append_value(int.unwrap()).unwrap();
            } else {
                int_builder.append_null().unwrap();
            }
        }

        for _ in 0..len / 4 {
            builder.append(true).unwrap();
            builder.append(true).unwrap();
            builder.append_null().unwrap();
            builder.append(true).unwrap();
        }

        builder.finish();
    });
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

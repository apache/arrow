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

use arrow::buffer::Buffer;
use arrow::builder::{BufferBuilderTrait, UInt8BufferBuilder};

fn create_buffer(size: usize) -> Buffer {
    let mut builder = UInt8BufferBuilder::new(size);
    for _i in 0..size {
        builder.append(1_u8).unwrap();
    }
    builder.finish()
}

fn bitwise_default<F>(size: usize, op: F)
where
    F: Fn(&u8, &u8) -> u8,
{
    let buffer_a = create_buffer(size);
    let buffer_b = create_buffer(size);

    criterion::black_box({
        let mut builder = UInt8BufferBuilder::new(buffer_a.len());
        for i in 0..buffer_a.len() {
            unsafe {
                builder
                    .append(op(
                        buffer_a.data().get_unchecked(i),
                        buffer_b.data().get_unchecked(i),
                    ))
                    .unwrap();
            }
        }
        builder.finish()
    });
}

fn bitwise_simd<F>(size: usize, op: F)
where
    F: Fn(&Buffer, &Buffer) -> Buffer,
{
    let buffer_a = create_buffer(size);
    let buffer_b = create_buffer(size);
    criterion::black_box(op(&buffer_a, &buffer_b));
}

fn add_benchmark(c: &mut Criterion) {
    c.bench_function("add", |b| b.iter(|| bitwise_default(512, |a, b| a & b)));
    c.bench_function("add simd", |b| b.iter(|| bitwise_simd(512, |a, b| a & b)));
    c.bench_function("or", |b| b.iter(|| bitwise_default(512, |a, b| a | b)));
    c.bench_function("or simd", |b| b.iter(|| bitwise_simd(512, |a, b| a | b)));
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

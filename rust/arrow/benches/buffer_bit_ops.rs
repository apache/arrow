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

use arrow::buffer::{Buffer, MutableBuffer};

///  Helper function to create arrays
fn create_buffer(size: usize) -> Buffer {
    let mut result = MutableBuffer::new(size).with_bitset(size, false);

    for i in 0..size {
        result.data_mut()[i] = 0b01010101 << i << (i % 4);
    }

    result.freeze()
}

fn bench_buffer_and(left: &Buffer, right: &Buffer) {
    criterion::black_box((left & right).unwrap());
}

fn bench_buffer_or(left: &Buffer, right: &Buffer) {
    criterion::black_box((left | right).unwrap());
}

fn bit_ops_benchmark(c: &mut Criterion) {
    let left = create_buffer(512 * 10);
    let right = create_buffer(512 * 10);

    c.bench_function("buffer_bit_ops and", |b| {
        b.iter(|| bench_buffer_and(&left, &right))
    });

    c.bench_function("buffer_bit_ops or", |b| {
        b.iter(|| bench_buffer_or(&left, &right))
    });
}

criterion_group!(benches, bit_ops_benchmark);
criterion_main!(benches);

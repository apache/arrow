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

use arrow::{
    buffer::{Buffer, MutableBuffer},
    datatypes::ToByteSlice,
};

fn mutable_buffer(bytes: &[u32], size: usize, capacity: usize) -> Buffer {
    criterion::black_box({
        let mut result = MutableBuffer::new(capacity);

        for _ in 0..size {
            result.extend_from_slice(bytes.to_byte_slice())
        }

        result.freeze()
    })
}

fn from_slice(bytes: &[u32], size: usize, capacity: usize) -> Buffer {
    criterion::black_box({
        let mut a = Vec::<u32>::with_capacity(capacity);
        
        for _ in 0..size {
            a.extend_from_slice(bytes)
        }

        Buffer::from(a.to_byte_slice())
    })
}

fn benchmark(c: &mut Criterion) {
    let bytes = &[128u32; 1025];
    let size = 2usize.pow(10);

    c.bench_function("mutable", |b| b.iter(|| mutable_buffer(bytes, size, 0)));

    c.bench_function("mutable prepared", |b| {
        b.iter(|| mutable_buffer(bytes, size, size * bytes.len() * std::mem::size_of::<u32>()))
    });

    c.bench_function("from_slice", |b| b.iter(|| from_slice(bytes, size, 0)));

    c.bench_function("from_slice prepared", |b| {
        b.iter(|| from_slice(bytes, size, size * bytes.len()))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

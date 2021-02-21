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
use arrow::util::test_util::seedable_rng;
use criterion::Criterion;
use rand::distributions::Uniform;
use rand::Rng;

extern crate arrow;

use arrow::{
    buffer::{Buffer, MutableBuffer},
    datatypes::ToByteSlice,
};

fn mutable_buffer_from_iter(data: &[Vec<bool>]) -> Vec<Buffer> {
    criterion::black_box(
        data.iter()
            .map(|vec| vec.iter().copied().collect::<MutableBuffer>().into())
            .collect::<Vec<_>>(),
    )
}

fn buffer_from_iter(data: &[Vec<bool>]) -> Vec<Buffer> {
    criterion::black_box(
        data.iter()
            .map(|vec| vec.iter().copied().collect::<Buffer>())
            .collect::<Vec<_>>(),
    )
}

fn mutable_buffer_iter_bitset(data: &[Vec<bool>]) -> Vec<Buffer> {
    criterion::black_box({
        data.iter()
            .map(|datum| {
                let mut result = MutableBuffer::new((data.len() + 7) / 8)
                    .with_bitset(datum.len(), false);
                for (i, value) in datum.iter().enumerate() {
                    if *value {
                        unsafe {
                            arrow::util::bit_util::set_bit_raw(result.as_mut_ptr(), i);
                        }
                    }
                }
                result.into()
            })
            .collect::<Vec<_>>()
    })
}

fn mutable_iter_extend_from_slice(data: &[Vec<u32>], capacity: usize) -> Buffer {
    criterion::black_box({
        let mut result = MutableBuffer::new(capacity);

        data.iter().for_each(|vec| {
            vec.iter()
                .for_each(|elem| result.extend_from_slice(elem.to_byte_slice()))
        });

        result.into()
    })
}

fn mutable_buffer(data: &[Vec<u32>], capacity: usize) -> Buffer {
    criterion::black_box({
        let mut result = MutableBuffer::new(capacity);

        data.iter().for_each(|vec| result.extend_from_slice(vec));

        result.into()
    })
}

fn mutable_buffer_extend(data: &[Vec<u32>], capacity: usize) -> Buffer {
    criterion::black_box({
        let mut result = MutableBuffer::new(capacity);

        data.iter()
            .for_each(|vec| result.extend(vec.iter().copied()));

        result.into()
    })
}

fn from_slice(data: &[Vec<u32>], capacity: usize) -> Buffer {
    criterion::black_box({
        let mut a = Vec::<u32>::with_capacity(capacity);

        data.iter().for_each(|vec| a.extend(vec));

        Buffer::from(a.to_byte_slice())
    })
}

fn create_data(size: usize) -> Vec<Vec<u32>> {
    let rng = &mut seedable_rng();
    let range = Uniform::new(0, 33);

    (0..size)
        .map(|_| {
            let size = rng.sample(range);
            seedable_rng()
                .sample_iter(&range)
                .take(size as usize)
                .collect()
        })
        .collect()
}

fn create_data_bool(size: usize) -> Vec<Vec<bool>> {
    let rng = &mut seedable_rng();
    let range = Uniform::new(0, 33);

    (0..size)
        .map(|_| {
            let size = rng.sample(range);
            seedable_rng()
                .sample_iter(&range)
                .take(size as usize)
                .map(|x| x > 15)
                .collect()
        })
        .collect()
}
fn benchmark(c: &mut Criterion) {
    let size = 2usize.pow(15);
    let data = create_data(size);

    let bool_data = create_data_bool(size);
    let cap = data.iter().map(|i| i.len()).sum();
    let byte_cap = cap * std::mem::size_of::<u32>();

    c.bench_function("mutable iter extend_from_slice", |b| {
        b.iter(|| {
            mutable_iter_extend_from_slice(
                criterion::black_box(&data),
                criterion::black_box(0),
            )
        })
    });
    c.bench_function("mutable", |b| {
        b.iter(|| mutable_buffer(criterion::black_box(&data), criterion::black_box(0)))
    });

    c.bench_function("mutable extend", |b| {
        b.iter(|| mutable_buffer_extend(&data, 0))
    });

    c.bench_function("mutable prepared", |b| {
        b.iter(|| {
            mutable_buffer(criterion::black_box(&data), criterion::black_box(byte_cap))
        })
    });

    c.bench_function("from_slice", |b| {
        b.iter(|| from_slice(criterion::black_box(&data), criterion::black_box(0)))
    });
    c.bench_function("from_slice prepared", |b| {
        b.iter(|| from_slice(criterion::black_box(&data), criterion::black_box(cap)))
    });

    c.bench_function("MutableBuffer iter bitset", |b| {
        b.iter(|| mutable_buffer_iter_bitset(criterion::black_box(&bool_data)))
    });
    c.bench_function("MutableBuffer::from_iter bool", |b| {
        b.iter(|| mutable_buffer_from_iter(criterion::black_box(&bool_data)))
    });
    c.bench_function("Buffer::from_iter bool", |b| {
        b.iter(|| buffer_from_iter(criterion::black_box(&bool_data)))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);

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
use arrow::error::ArrowError;
use arrow::error::Result;
#[cfg(feature = "simd")]
use arrow::util::bit_util;
use std::borrow::BorrowMut;
#[cfg(feature = "simd")]
use std::slice::{from_raw_parts, from_raw_parts_mut};

///  Helper function to create arrays
fn create_buffer(size: usize) -> Buffer {
    let mut result = MutableBuffer::new(size).with_bitset(size, false);

    for i in 0..size {
        result.data_mut()[i] = 0b01010101;
    }

    result.freeze()
}

fn bench_and_current_impl(left: &Buffer, right: &Buffer) {
    criterion::black_box((left & right).unwrap());
}

#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
fn bench_and_packed_simd_chunked_exact(left: &Buffer, right: &Buffer) {
    criterion::black_box(
        bitwise_bin_op_simd_helper(&left, &right, |a, b| a & b).unwrap(),
    );
}

fn bench_and_chunked_exact(left: &Buffer, right: &Buffer) {
    criterion::black_box(
        bitwise_bin_op_autovec_chunked_helper(&left, &right, |a, b| a & b).unwrap(),
    );
}

fn bench_and_autovec(left: &Buffer, right: &Buffer) {
    criterion::black_box(
        bitwise_bin_op_autovec_helper(&left, &right, |a, b| a & b).unwrap(),
    );
}

const AUTOVEC_LANES: usize = 64;

fn bitwise_bin_op_autovec_chunked_helper<F>(
    left: &Buffer,
    right: &Buffer,
    op: F,
) -> Result<Buffer>
where
    F: Fn(u8, u8) -> u8,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Buffers must be the same size to apply Bitwise AND.".to_string(),
        ));
    }

    let mut result = MutableBuffer::new(left.len()).with_bitset(left.len(), false);

    let mut left_chunks = left.data().chunks_exact(AUTOVEC_LANES);
    let mut right_chunks = right.data().chunks_exact(AUTOVEC_LANES);
    let mut result_chunks = result.data_mut().chunks_exact_mut(AUTOVEC_LANES);

    result_chunks
        .borrow_mut()
        .zip(left_chunks.borrow_mut().zip(right_chunks.borrow_mut()))
        .for_each(|(res, (left, right))| {
            for i in 0..AUTOVEC_LANES {
                res[i] = op(left[i], right[i]);
            }
        });

    result_chunks
        .into_remainder()
        .iter_mut()
        .zip(
            left_chunks
                .remainder()
                .iter()
                .zip(right_chunks.remainder().iter()),
        )
        .for_each(|(res, (left, right))| {
            *res = op(*left, *right);
        });

    Ok(result.freeze())
}

fn bitwise_bin_op_autovec_helper<F>(
    left: &Buffer,
    right: &Buffer,
    op: F,
) -> Result<Buffer>
where
    F: Fn(u8, u8) -> u8,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Buffers must be the same size to apply Bitwise AND.".to_string(),
        ));
    }

    let mut result = MutableBuffer::new(left.len()).with_bitset(left.len(), false);

    result
        .data_mut()
        .iter_mut()
        .zip(left.data().iter().zip(right.data().iter()))
        .for_each(|(res, (left, right))| {
            *res = op(*left, *right);
        });

    Ok(result.freeze())
}

///  Helper function for SIMD `BitAnd` and `BitOr` implementations
#[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
fn bitwise_bin_op_simd_helper<F>(left: &Buffer, right: &Buffer, op: F) -> Result<Buffer>
where
    F: Fn(packed_simd::u8x64, packed_simd::u8x64) -> packed_simd::u8x64,
{
    if left.len() != right.len() {
        return Err(ArrowError::ComputeError(
            "Buffers must be the same size to apply Bitwise AND.".to_string(),
        ));
    }

    let mut result = MutableBuffer::new(left.len()).with_bitset(left.len(), false);
    let lanes = packed_simd::u8x64::lanes();
    for i in (0..left.len()).step_by(lanes) {
        let left_data = unsafe { from_raw_parts(left.raw_data().add(i), lanes) };
        let right_data = unsafe { from_raw_parts(right.raw_data().add(i), lanes) };
        let result_slice: &mut [u8] = unsafe {
            from_raw_parts_mut((result.data_mut().as_mut_ptr() as *mut u8).add(i), lanes)
        };
        unsafe {
            bit_util::bitwise_bin_op_simd(&left_data, &right_data, result_slice, &op)
        };
    }

    Ok(result.freeze())
}

fn bit_ops_benchmark(c: &mut Criterion) {
    let left = create_buffer(512);
    let right = create_buffer(512);
    c.bench_function("buffer_bit_ops and current impl", |b| {
        b.iter(|| bench_and_current_impl(&left, &right))
    });
    #[cfg(all(any(target_arch = "x86", target_arch = "x86_64"), feature = "simd"))]
    c.bench_function("buffer_bit_ops and packed simd", |b| {
        b.iter(|| bench_and_packed_simd_chunked_exact(&left, &right))
    });
    c.bench_function("buffer_bit_ops and chunked autovec", |b| {
        b.iter(|| bench_and_chunked_exact(&left, &right))
    });
    c.bench_function("buffer_bit_ops and autovec", |b| {
        b.iter(|| bench_and_autovec(&left, &right))
    });
}

criterion_group!(benches, bit_ops_benchmark);
criterion_main!(benches);

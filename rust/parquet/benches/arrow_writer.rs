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
use criterion::{Criterion, Throughput};

extern crate arrow;
extern crate parquet;

use std::sync::Arc;

use arrow::datatypes::*;
use arrow::{record_batch::RecordBatch, util::data_gen::*};
use parquet::{
    arrow::ArrowWriter, errors::Result, file::writer::InMemoryWriteableCursor,
};

fn create_primitive_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("_1", DataType::Int8, true),
        Field::new("_2", DataType::Int16, true),
        Field::new("_3", DataType::Int32, true),
        Field::new("_4", DataType::Int64, true),
        Field::new("_5", DataType::UInt8, true),
        Field::new("_6", DataType::UInt16, true),
        Field::new("_7", DataType::UInt32, true),
        Field::new("_8", DataType::UInt64, true),
        Field::new("_9", DataType::Float32, true),
        Field::new("_10", DataType::Float64, true),
        Field::new("_11", DataType::Date32, true),
        Field::new("_12", DataType::Date64, true),
        Field::new("_13", DataType::Time32(TimeUnit::Second), true),
        Field::new("_14", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("_15", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new("_16", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("_17", DataType::Utf8, true),
        Field::new("_18", DataType::LargeUtf8, true),
        Field::new("_19", DataType::Boolean, true),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

fn _create_nested_bench_batch(
    size: usize,
    null_density: f32,
    true_density: f32,
) -> Result<RecordBatch> {
    let fields = vec![
        Field::new(
            "_1",
            DataType::Struct(vec![
                Field::new("_1", DataType::Int8, true),
                Field::new(
                    "_2",
                    DataType::Struct(vec![
                        Field::new("_1", DataType::Int8, true),
                        Field::new(
                            "_1",
                            DataType::Struct(vec![
                                Field::new("_1", DataType::Int8, true),
                                Field::new("_2", DataType::Utf8, true),
                            ]),
                            true,
                        ),
                        Field::new("_2", DataType::UInt8, true),
                    ]),
                    true,
                ),
            ]),
            true,
        ),
        Field::new(
            "_2",
            DataType::LargeList(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(vec![
                        Field::new(
                            "_1",
                            DataType::Struct(vec![
                                Field::new("_1", DataType::Int8, true),
                                Field::new("_2", DataType::Int16, true),
                                Field::new("_3", DataType::Int32, true),
                            ]),
                            true,
                        ),
                        Field::new(
                            "_2",
                            DataType::List(Box::new(Field::new(
                                "",
                                DataType::FixedSizeBinary(2),
                                true,
                            ))),
                            true,
                        ),
                    ]),
                    true,
                ))),
                true,
            ))),
            true,
        ),
    ];
    let schema = Schema::new(fields);
    Ok(create_random_batch(
        Arc::new(schema),
        size,
        null_density,
        true_density,
    )?)
}

#[inline]
fn write_batch(batch: &RecordBatch) -> Result<()> {
    // Write batch to an in-memory writer
    let cursor = InMemoryWriteableCursor::default();
    let mut writer = ArrowWriter::try_new(cursor, batch.schema(), None)?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn bench_primitive_writer(c: &mut Criterion) {
    let batch = create_primitive_bench_batch(1024, 0.25, 0.75).unwrap();
    let mut group = c.benchmark_group("write_batch primitive");
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("1024 values", |b| b.iter(|| write_batch(&batch).unwrap()));

    let batch = create_primitive_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values", |b| b.iter(|| write_batch(&batch).unwrap()));

    group.finish();
}

// This bench triggers a write error, it is ignored for now
fn _bench_nested_writer(c: &mut Criterion) {
    let batch = _create_nested_bench_batch(1024, 0.25, 0.75).unwrap();
    let mut group = c.benchmark_group("write_batch nested");
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("1024 values", |b| b.iter(|| write_batch(&batch).unwrap()));

    let batch = create_primitive_bench_batch(4096, 0.25, 0.75).unwrap();
    group.throughput(Throughput::Bytes(
        batch
            .columns()
            .iter()
            .map(|f| f.get_array_memory_size() as u64)
            .sum(),
    ));
    group.bench_function("4096 values", |b| b.iter(|| write_batch(&batch).unwrap()));

    group.finish();
}

criterion_group!(benches, bench_primitive_writer);
criterion_main!(benches);

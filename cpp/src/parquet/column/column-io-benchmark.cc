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

#include "benchmark/benchmark.h"

#include "parquet/file/reader-internal.h"
#include "parquet/file/writer-internal.h"
#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/util/input.h"

namespace parquet {

using format::ColumnChunk;
using schema::PrimitiveNode;

namespace benchmark {

std::unique_ptr<Int64Writer> BuildWriter(int64_t output_size, OutputStream* dst,
    ColumnChunk* metadata, ColumnDescriptor* schema) {
  std::unique_ptr<SerializedPageWriter> pager(
      new SerializedPageWriter(dst, Compression::UNCOMPRESSED, metadata));
  return std::unique_ptr<Int64Writer>(
      new Int64Writer(schema, std::move(pager), output_size, Encoding::PLAIN));
}

std::shared_ptr<ColumnDescriptor> Int64Schema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, Type::INT64);
  return std::make_shared<ColumnDescriptor>(
      node, repetition != Repetition::REQUIRED, repetition == Repetition::REPEATED);
}

void SetBytesProcessed(::benchmark::State& state, Repetition::type repetition) {
  int64_t bytes_processed = state.iterations() * state.range_x() * sizeof(int64_t);
  if (repetition != Repetition::REQUIRED) {
    bytes_processed += state.iterations() * state.range_x() * sizeof(int16_t);
  }
  if (repetition == Repetition::REPEATED) {
    bytes_processed += state.iterations() * state.range_x() * sizeof(int16_t);
  }
  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(int16_t));
}

template <Repetition::type repetition>
static void BM_WriteInt64Column(::benchmark::State& state) {
  format::ColumnChunk metadata;
  std::vector<int64_t> values(state.range_x(), 128);
  std::vector<int16_t> definition_levels(state.range_x(), 1);
  std::vector<int16_t> repetition_levels(state.range_x(), 0);
  std::shared_ptr<ColumnDescriptor> schema = Int64Schema(repetition);

  while (state.KeepRunning()) {
    InMemoryOutputStream dst;
    std::unique_ptr<Int64Writer> writer =
        BuildWriter(state.range_x(), &dst, &metadata, schema.get());
    writer->WriteBatch(
        values.size(), definition_levels.data(), repetition_levels.data(), values.data());
    writer->Close();
  }
  SetBytesProcessed(state, repetition);
}

BENCHMARK_TEMPLATE(BM_WriteInt64Column, Repetition::REQUIRED)->Range(1024, 65536);

BENCHMARK_TEMPLATE(BM_WriteInt64Column, Repetition::OPTIONAL)->Range(1024, 65536);

BENCHMARK_TEMPLATE(BM_WriteInt64Column, Repetition::REPEATED)->Range(1024, 65536);

std::unique_ptr<Int64Reader> BuildReader(
    std::shared_ptr<Buffer>& buffer, ColumnDescriptor* schema) {
  std::unique_ptr<InMemoryInputStream> source(new InMemoryInputStream(buffer));
  std::unique_ptr<SerializedPageReader> page_reader(
      new SerializedPageReader(std::move(source), Compression::UNCOMPRESSED));
  return std::unique_ptr<Int64Reader>(new Int64Reader(schema, std::move(page_reader)));
}

template <Repetition::type repetition>
static void BM_ReadInt64Column(::benchmark::State& state) {
  format::ColumnChunk metadata;
  std::vector<int64_t> values(state.range_x(), 128);
  std::vector<int16_t> definition_levels(state.range_x(), 1);
  std::vector<int16_t> repetition_levels(state.range_x(), 0);
  std::shared_ptr<ColumnDescriptor> schema = Int64Schema(repetition);

  InMemoryOutputStream dst;
  std::unique_ptr<Int64Writer> writer =
      BuildWriter(state.range_x(), &dst, &metadata, schema.get());
  writer->WriteBatch(
      values.size(), definition_levels.data(), repetition_levels.data(), values.data());
  writer->Close();

  std::shared_ptr<Buffer> src = dst.GetBuffer();
  std::vector<int64_t> values_out(state.range_y());
  std::vector<int16_t> definition_levels_out(state.range_y());
  std::vector<int16_t> repetition_levels_out(state.range_y());
  while (state.KeepRunning()) {
    std::unique_ptr<Int64Reader> reader = BuildReader(src, schema.get());
    int64_t values_read = 0;
    for (size_t i = 0; i < values.size(); i += values_read) {
      reader->ReadBatch(values_out.size(), definition_levels_out.data(),
          repetition_levels_out.data(), values_out.data(), &values_read);
    }
  }
  SetBytesProcessed(state, repetition);
}

BENCHMARK_TEMPLATE(BM_ReadInt64Column, Repetition::REQUIRED)
    ->RangePair(1024, 65536, 1, 1024);

BENCHMARK_TEMPLATE(BM_ReadInt64Column, Repetition::OPTIONAL)
    ->RangePair(1024, 65536, 1, 1024);

BENCHMARK_TEMPLATE(BM_ReadInt64Column, Repetition::REPEATED)
    ->RangePair(1024, 65536, 1, 1024);

}  // namespace benchmark

}  // namespace parquet

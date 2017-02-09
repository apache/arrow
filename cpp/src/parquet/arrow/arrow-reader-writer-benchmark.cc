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

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/file/reader-internal.h"
#include "parquet/file/writer-internal.h"
#include "parquet/util/memory.h"

#include "arrow/api.h"

using arrow::NumericBuilder;

namespace parquet {

using arrow::FileReader;
using arrow::WriteTable;
using schema::PrimitiveNode;

namespace benchmark {

// This should result in multiple pages for most primitive types
constexpr int64_t BENCHMARK_SIZE = 10 * 1024 * 1024;

template <typename ParquetType>
struct benchmark_traits {};

template <>
struct benchmark_traits<Int32Type> {
  using arrow_type = ::arrow::Int32Type;
};

template <>
struct benchmark_traits<Int64Type> {
  using arrow_type = ::arrow::Int64Type;
};

template <>
struct benchmark_traits<DoubleType> {
  using arrow_type = ::arrow::DoubleType;
};

template <typename ParquetType>
using ArrowType = typename benchmark_traits<ParquetType>::arrow_type;

template <typename ParquetType>
std::shared_ptr<ColumnDescriptor> MakeSchema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, ParquetType::type_num);
  return std::make_shared<ColumnDescriptor>(
      node, repetition != Repetition::REQUIRED, repetition == Repetition::REPEATED);
}

template <bool nullable, typename ParquetType>
void SetBytesProcessed(::benchmark::State& state) {
  int64_t bytes_processed =
      state.iterations() * BENCHMARK_SIZE * sizeof(typename ParquetType::c_type);
  if (nullable) {
    bytes_processed += state.iterations() * BENCHMARK_SIZE * sizeof(int16_t);
  }
  state.SetBytesProcessed(bytes_processed);
}

template <bool nullable, typename ParquetType>
std::shared_ptr<::arrow::Table> TableFromVector(
    const std::vector<typename ParquetType::c_type>& vec) {
  ::arrow::TypePtr type = std::make_shared<ArrowType<ParquetType>>();
  NumericBuilder<ArrowType<ParquetType>> builder(::arrow::default_memory_pool(), type);
  if (nullable) {
    std::vector<uint8_t> valid_bytes(BENCHMARK_SIZE, 0);
    int n = {0};
    std::generate(valid_bytes.begin(), valid_bytes.end(), [&n] { return n++ % 2; });
    builder.Append(vec.data(), vec.size(), valid_bytes.data());
  } else {
    builder.Append(vec.data(), vec.size(), nullptr);
  }
  std::shared_ptr<::arrow::Array> array;
  builder.Finish(&array);
  auto field = std::make_shared<::arrow::Field>("column", type, nullable);
  auto schema = std::make_shared<::arrow::Schema>(
      std::vector<std::shared_ptr<::arrow::Field>>({field}));
  auto column = std::make_shared<::arrow::Column>(field, array);
  return std::make_shared<::arrow::Table>(
      "table", schema, std::vector<std::shared_ptr<::arrow::Column>>({column}));
}

template <bool nullable, typename ParquetType>
static void BM_WriteColumn(::benchmark::State& state) {
  format::ColumnChunk thrift_metadata;
  std::vector<typename ParquetType::c_type> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<nullable, ParquetType>(values);

  while (state.KeepRunning()) {
    auto output = std::make_shared<InMemoryOutputStream>();
    WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE);
  }
  SetBytesProcessed<nullable, ParquetType>(state);
}

BENCHMARK_TEMPLATE(BM_WriteColumn, false, Int32Type);
BENCHMARK_TEMPLATE(BM_WriteColumn, true, Int32Type);

BENCHMARK_TEMPLATE(BM_WriteColumn, false, Int64Type);
BENCHMARK_TEMPLATE(BM_WriteColumn, true, Int64Type);

BENCHMARK_TEMPLATE(BM_WriteColumn, false, DoubleType);
BENCHMARK_TEMPLATE(BM_WriteColumn, true, DoubleType);

template <bool nullable, typename ParquetType>
static void BM_ReadColumn(::benchmark::State& state) {
  std::vector<typename ParquetType::c_type> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<nullable, ParquetType>(values);
  auto output = std::make_shared<InMemoryOutputStream>();
  WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE);
  std::shared_ptr<Buffer> buffer = output->GetBuffer();

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    FileReader filereader(::arrow::default_memory_pool(), std::move(reader));
    std::shared_ptr<::arrow::Table> table;
    filereader.ReadTable(&table);
  }
  SetBytesProcessed<nullable, ParquetType>(state);
}

BENCHMARK_TEMPLATE(BM_ReadColumn, false, Int32Type);
BENCHMARK_TEMPLATE(BM_ReadColumn, true, Int32Type);

BENCHMARK_TEMPLATE(BM_ReadColumn, false, Int64Type);
BENCHMARK_TEMPLATE(BM_ReadColumn, true, Int64Type);

BENCHMARK_TEMPLATE(BM_ReadColumn, false, DoubleType);
BENCHMARK_TEMPLATE(BM_ReadColumn, true, DoubleType);

}  // namespace benchmark

}  // namespace parquet

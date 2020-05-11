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

#include <iostream>
#include <random>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"

#include "arrow/api.h"
#include "arrow/util/logging.h"

using arrow::BooleanBuilder;
using arrow::NumericBuilder;

#define EXIT_NOT_OK(s)                                        \
  do {                                                        \
    ::arrow::Status _s = (s);                                 \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                      \
      std::cout << "Exiting: " << _s.ToString() << std::endl; \
      exit(EXIT_FAILURE);                                     \
    }                                                         \
  } while (0)

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

template <>
struct benchmark_traits<BooleanType> {
  using arrow_type = ::arrow::BooleanType;
};

template <typename ParquetType>
using ArrowType = typename benchmark_traits<ParquetType>::arrow_type;

template <typename ParquetType>
std::shared_ptr<ColumnDescriptor> MakeSchema(Repetition::type repetition) {
  auto node = PrimitiveNode::Make("int64", repetition, ParquetType::type_num);
  return std::make_shared<ColumnDescriptor>(node, repetition != Repetition::REQUIRED,
                                            repetition == Repetition::REPEATED);
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

constexpr int64_t kAlternatingOrNa = -1;

template <typename ParquetType>
std::shared_ptr<::arrow::Table> TableFromVector(
    const std::vector<typename ParquetType::c_type>& vec, bool nullable,
    int null_percentage = kAlternatingOrNa) {
  if (!nullable) {
    DCHECK(null_percentage = kAlternatingOrNa);
  }
  std::shared_ptr<::arrow::DataType> type = std::make_shared<ArrowType<ParquetType>>();
  NumericBuilder<ArrowType<ParquetType>> builder;
  if (nullable) {
    std::vector<uint8_t> valid_bytes(BENCHMARK_SIZE, 0);
    if (null_percentage == -1) {
      int n = {0};
      std::generate(valid_bytes.begin(), valid_bytes.end(), [&n] { return n++ % 2; });
    } else {
      std::default_random_engine rng(500);
      double valid_probability = 1.0 - (static_cast<double>(null_percentage) / 100);
      std::bernoulli_distribution dist(valid_probability);
      std::generate(valid_bytes.begin(), valid_bytes.end(),
                    [&] { return static_cast<uint8_t>(dist(rng)); });
    }
    EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), valid_bytes.data()));
  } else {
    EXIT_NOT_OK(builder.AppendValues(vec.data(), vec.size(), nullptr));
  }
  std::shared_ptr<::arrow::Array> array;
  EXIT_NOT_OK(builder.Finish(&array));

  auto field = ::arrow::field("column", type, nullable);
  auto schema = ::arrow::schema({field});
  return ::arrow::Table::Make(schema, {array});
}

template <>
std::shared_ptr<::arrow::Table> TableFromVector<BooleanType>(const std::vector<bool>& vec,
                                                             bool nullable,
                                                             int null_percentage) {
  BooleanBuilder builder;
  if (nullable) {
    std::vector<bool> valid_bytes(BENCHMARK_SIZE, 0);
    if (null_percentage == -1) {
      int n = {0};
      std::generate(valid_bytes.begin(), valid_bytes.end(),
                    [&n] { return n++ % 2 != 0; });
    } else {
      std::default_random_engine rng(500);
      double valid_probability = 1.0 - (static_cast<double>(null_percentage) / 100);
      std::bernoulli_distribution dist(valid_probability);
      std::generate(valid_bytes.begin(), valid_bytes.end(),
                    [&] { return static_cast<uint8_t>(dist(rng)); });
    }
    EXIT_NOT_OK(builder.AppendValues(vec, valid_bytes));
  } else {
    EXIT_NOT_OK(builder.AppendValues(vec));
  }
  std::shared_ptr<::arrow::Array> array;
  EXIT_NOT_OK(builder.Finish(&array));

  auto field = ::arrow::field("column", ::arrow::boolean(), nullable);
  auto schema = std::make_shared<::arrow::Schema>(
      std::vector<std::shared_ptr<::arrow::Field>>({field}));
  return ::arrow::Table::Make(schema, {array});
}

template <bool nullable, typename ParquetType>
static void BM_WriteColumn(::benchmark::State& state) {
  using T = typename ParquetType::c_type;
  std::vector<T> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<ParquetType>(values, nullable);

  while (state.KeepRunning()) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(
        WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));
  }
  SetBytesProcessed<nullable, ParquetType>(state);
}

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, Int32Type);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, Int32Type);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, Int64Type);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, Int64Type);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, DoubleType);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, DoubleType);

BENCHMARK_TEMPLATE2(BM_WriteColumn, false, BooleanType);
BENCHMARK_TEMPLATE2(BM_WriteColumn, true, BooleanType);

template <bool nullable, typename ParquetType>
static void BM_ReadColumn(::benchmark::State& state) {
  using T = typename ParquetType::c_type;

  std::vector<T> values(BENCHMARK_SIZE, static_cast<T>(128));
  double value_probability = static_cast<double>(state.range(1)) / 100.0;
  std::default_random_engine rng(500);
  std::bernoulli_distribution dist(value_probability);
  std::generate(values.begin(), values.end(),
                [&] { return static_cast<T>(dist(rng) * 128); });

  std::shared_ptr<::arrow::Table> table =
      TableFromVector<ParquetType>(values, nullable, state.range(0));
  auto output = CreateOutputStream();
  EXIT_NOT_OK(WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));

  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    std::unique_ptr<FileReader> arrow_reader;
    EXIT_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                 &arrow_reader));
    std::shared_ptr<::arrow::Table> table;
    EXIT_NOT_OK(arrow_reader->ReadTable(&table));
  }
  SetBytesProcessed<nullable, ParquetType>(state);
}

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});

BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/25})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/100})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/100});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/25})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/100})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/100});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, DoubleType)
    ->Args({kAlternatingOrNa, 0})
    ->Args({1, 20});
// Less coverage because int64_t should be pretty good representation for nullability and
// repeating values.
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, DoubleType)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/25});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, BooleanType)
    ->Args({kAlternatingOrNa, 0})
    ->Args({1, 20});
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, BooleanType)
    ->Ranges({{kAlternatingOrNa, 0}, {0, 10}});

static void BM_ReadIndividualRowGroups(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));

  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    std::unique_ptr<FileReader> arrow_reader;
    EXIT_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                 &arrow_reader));

    std::vector<std::shared_ptr<::arrow::Table>> tables;
    for (int i = 0; i < arrow_reader->num_row_groups(); i++) {
      // Only read the even numbered RowGroups
      if ((i % 2) == 0) {
        std::shared_ptr<::arrow::Table> table;
        EXIT_NOT_OK(arrow_reader->RowGroup(i)->ReadTable(&table));
        tables.push_back(table);
      }
    }

    std::shared_ptr<::arrow::Table> final_table;
    PARQUET_ASSIGN_OR_THROW(final_table, ConcatenateTables(tables));
  }
  SetBytesProcessed<true, Int64Type>(state);
}

BENCHMARK(BM_ReadIndividualRowGroups);

static void BM_ReadMultipleRowGroups(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));
  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    std::unique_ptr<FileReader> arrow_reader;
    EXIT_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                 &arrow_reader));

    std::vector<std::shared_ptr<::arrow::Table>> tables;
    std::vector<int> rgs;
    for (int i = 0; i < arrow_reader->num_row_groups(); i++) {
      // Only read the even numbered RowGroups
      if ((i % 2) == 0) {
        rgs.push_back(i);
      }
    }

    std::shared_ptr<::arrow::Table> table;
    EXIT_NOT_OK(arrow_reader->ReadRowGroups(rgs, &table));
  }
  SetBytesProcessed<true, Int64Type>(state);
}

BENCHMARK(BM_ReadMultipleRowGroups);

}  // namespace benchmark

}  // namespace parquet

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

#include <array>
#include <iostream>
#include <random>

#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/io/memory.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"

using arrow::Array;
using arrow::ArrayVector;
using arrow::BooleanBuilder;
using arrow::FieldVector;
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
void SetBytesProcessed(::benchmark::State& state, int64_t num_values = BENCHMARK_SIZE) {
  const int64_t items_processed = state.iterations() * num_values;
  const int64_t bytes_processed = items_processed * sizeof(typename ParquetType::c_type);

  state.SetItemsProcessed(bytes_processed);
  state.SetBytesProcessed(bytes_processed);
}

constexpr int64_t kAlternatingOrNa = -1;

template <typename T>
std::vector<T> RandomVector(int64_t true_percentage, int64_t vector_size,
                            const std::array<T, 2>& sample_values, int seed = 500) {
  std::vector<T> values(vector_size, {});
  if (true_percentage == kAlternatingOrNa) {
    int n = {0};
    std::generate(values.begin(), values.end(), [&n] { return n++ % 2; });
  } else {
    std::default_random_engine rng(seed);
    double true_probability = static_cast<double>(true_percentage) / 100.0;
    std::bernoulli_distribution dist(true_probability);
    std::generate(values.begin(), values.end(), [&] { return sample_values[dist(rng)]; });
  }
  return values;
}

template <typename ParquetType>
std::shared_ptr<::arrow::Table> TableFromVector(
    const std::vector<typename ParquetType::c_type>& vec, bool nullable,
    int64_t null_percentage = kAlternatingOrNa) {
  if (!nullable) {
    ARROW_CHECK_EQ(null_percentage, kAlternatingOrNa);
  }
  std::shared_ptr<::arrow::DataType> type = std::make_shared<ArrowType<ParquetType>>();
  NumericBuilder<ArrowType<ParquetType>> builder;
  if (nullable) {
    // Note true values select index 1 of sample_values
    auto valid_bytes = RandomVector<uint8_t>(/*true_percentage=*/null_percentage,
                                             vec.size(), /*sample_values=*/{1, 0});
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
                                                             int64_t null_percentage) {
  BooleanBuilder builder;
  if (nullable) {
    auto valid_bytes = RandomVector<bool>(/*true_percentage=*/null_percentage, vec.size(),
                                          {true, false});
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
  std::vector<T> values(BENCHMARK_SIZE, static_cast<T>(128));
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

int32_t kInfiniteUniqueValues = -1;

std::shared_ptr<::arrow::Table> RandomStringTable(int64_t length, int64_t unique_values,
                                                  int64_t null_percentage) {
  std::shared_ptr<::arrow::DataType> type = ::arrow::utf8();
  std::shared_ptr<::arrow::Array> arr;
  ::arrow::random::RandomArrayGenerator generator(/*seed=*/500);
  double null_probability = static_cast<double>(null_percentage) / 100.0;
  if (unique_values == kInfiniteUniqueValues) {
    arr = generator.String(length, /*min_length=*/3, /*max_length=*/32,
                           /*null_probability=*/null_probability);
  } else {
    arr = generator.StringWithRepeats(length, /*unique=*/unique_values,
                                      /*min_length=*/3, /*max_length=*/32,
                                      /*null_probability=*/null_probability);
  }
  return ::arrow::Table::Make(
      ::arrow::schema({::arrow::field("column", type, null_percentage > 0)}), {arr});
}

static void BM_WriteBinaryColumn(::benchmark::State& state) {
  std::shared_ptr<::arrow::Table> table =
      RandomStringTable(BENCHMARK_SIZE, state.range(1), state.range(0));

  while (state.KeepRunning()) {
    auto output = CreateOutputStream();
    EXIT_NOT_OK(
        WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE));
  }

  // Offsets + data
  int64_t total_bytes = table->column(0)->chunk(0)->data()->buffers[1]->size() +
                        table->column(0)->chunk(0)->data()->buffers[2]->size();
  state.SetItemsProcessed(BENCHMARK_SIZE * state.iterations());
  state.SetBytesProcessed(total_bytes * state.iterations());
}

BENCHMARK(BM_WriteBinaryColumn)
    ->ArgNames({"null_probability", "unique_values"})
    // We vary unique values to trigger the dictionary-encoded (for low-cardinality)
    // and plain (for high-cardinality) code paths.
    ->Args({0, 32})
    ->Args({0, kInfiniteUniqueValues})
    ->Args({1, 32})
    ->Args({50, 32})
    ->Args({99, 32})
    ->Args({1, kInfiniteUniqueValues})
    ->Args({50, kInfiniteUniqueValues})
    ->Args({99, kInfiniteUniqueValues});

template <typename T>
struct Examples {
  static constexpr std::array<T, 2> values() { return {127, 128}; }
};

template <>
struct Examples<bool> {
  static constexpr std::array<bool, 2> values() { return {false, true}; }
};

static void BenchmarkReadTable(::benchmark::State& state, const ::arrow::Table& table,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  auto output = CreateOutputStream();
  EXIT_NOT_OK(
      WriteTable(table, ::arrow::default_memory_pool(), output, table.num_rows()));
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

  if (num_values == -1) {
    num_values = table.num_rows();
  }
  state.SetItemsProcessed(num_values * state.iterations());
  if (total_bytes != -1) {
    state.SetBytesProcessed(total_bytes * state.iterations());
  }
}

static void BenchmarkReadArray(::benchmark::State& state,
                               const std::shared_ptr<Array>& array, bool nullable,
                               int64_t num_values = -1, int64_t total_bytes = -1) {
  auto schema = ::arrow::schema({field("s", array->type(), nullable)});
  auto table = ::arrow::Table::Make(schema, {array}, array->length());

  EXIT_NOT_OK(table->Validate());

  BenchmarkReadTable(state, *table, num_values, total_bytes);
}

//
// Benchmark reading a primitive column
//

template <bool nullable, typename ParquetType>
static void BM_ReadColumn(::benchmark::State& state) {
  using T = typename ParquetType::c_type;

  auto values = RandomVector<T>(/*percentage=*/state.range(1), BENCHMARK_SIZE,
                                Examples<T>::values());

  std::shared_ptr<::arrow::Table> table =
      TableFromVector<ParquetType>(values, nullable, state.range(0));

  BenchmarkReadTable(state, *table, table->num_rows(),
                     sizeof(typename ParquetType::c_type) * table->num_rows());
}

// There are two parameters here that cover different data distributions.
// null_percentage governs distribution and therefore runs of null values.
// first_value_percentage governs distribution of values (we select from 1 of 2)
// so when 0 or 100 RLE is triggered all the time.  When a value in the range (0, 100)
// there will be some percentage of RLE encoded values and some percentage of literal
// encoded values (RLE is much less likely with percentages close to 50).
BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});

BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int32Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/0});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, 1})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 10})
    ->Args({/*null_percentage=*/kAlternatingOrNa, 50});
BENCHMARK_TEMPLATE2(BM_ReadColumn, true, Int64Type)
    ->Args({/*null_percentage=*/kAlternatingOrNa, /*first_value_percentage=*/0})
    ->Args({/*null_percentage=*/1, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/5, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/10, /*first_value_percentage=*/5})
    ->Args({/*null_percentage=*/25, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/30, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/35, /*first_value_percentage=*/10})
    ->Args({/*null_percentage=*/45, /*first_value_percentage=*/25})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/50, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/75, /*first_value_percentage=*/1})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/50})
    ->Args({/*null_percentage=*/99, /*first_value_percentage=*/0});

BENCHMARK_TEMPLATE2(BM_ReadColumn, false, DoubleType)
    ->Args({kAlternatingOrNa, 0})
    ->Args({kAlternatingOrNa, 20});
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
    ->Args({kAlternatingOrNa, 1})
    ->Args({5, 10});

//
// Benchmark reading binary column
//

static void BM_ReadBinaryColumn(::benchmark::State& state) {
  std::shared_ptr<::arrow::Table> table =
      RandomStringTable(BENCHMARK_SIZE, state.range(1), state.range(0));

  // Offsets + data
  int64_t total_bytes = table->column(0)->chunk(0)->data()->buffers[1]->size() +
                        table->column(0)->chunk(0)->data()->buffers[2]->size();
  BenchmarkReadTable(state, *table, table->num_rows(), total_bytes);
}

BENCHMARK(BM_ReadBinaryColumn)
    ->ArgNames({"null_probability", "unique_values"})
    // We vary unique values to trigger the dictionary-encoded (for low-cardinality)
    // and plain (for high-cardinality) code paths.
    ->Args({0, 32})
    ->Args({0, kInfiniteUniqueValues})
    ->Args({1, 32})
    ->Args({50, 32})
    ->Args({99, 32})
    ->Args({1, kInfiniteUniqueValues})
    ->Args({50, kInfiniteUniqueValues})
    ->Args({99, kInfiniteUniqueValues});

//
// Benchmark reading a nested column
//

const std::vector<int64_t> kNestedNullPercents = {0, 1, 50, 99};

// XXX We can use ArgsProduct() starting from Benchmark 1.5.2
static void NestedReadArguments(::benchmark::internal::Benchmark* b) {
  for (const auto null_percentage : kNestedNullPercents) {
    b->Arg(null_percentage);
  }
}

static std::shared_ptr<Array> MakeStructArray(::arrow::random::RandomArrayGenerator* rng,
                                              const ArrayVector& children,
                                              double null_probability,
                                              bool propagate_validity = false) {
  ARROW_CHECK_GT(children.size(), 0);
  const int64_t length = children[0]->length();

  std::shared_ptr<::arrow::Buffer> null_bitmap;
  if (null_probability > 0.0) {
    null_bitmap = rng->NullBitmap(length, null_probability);
    if (propagate_validity) {
      // HACK: the Parquet writer currently doesn't allow non-empty list
      // entries where a parent node is null (for instance, a struct-of-list
      // where the outer struct is marked null but the inner list value is
      // non-empty).
      for (const auto& child : children) {
        null_bitmap = *::arrow::internal::BitmapOr(
            ::arrow::default_memory_pool(), null_bitmap->data(), 0,
            child->null_bitmap_data(), 0, length, 0);
      }
    }
  }
  FieldVector fields(children.size());
  char field_name = 'a';
  for (size_t i = 0; i < children.size(); ++i) {
    fields[i] = field(std::string{field_name++}, children[i]->type(),
                      /*nullable=*/null_probability > 0.0);
  }
  return *::arrow::StructArray::Make(children, std::move(fields), null_bitmap);
}

// Make a (int32, int64) struct array
static std::shared_ptr<Array> MakeStructArray(::arrow::random::RandomArrayGenerator* rng,
                                              int64_t size, double null_probability) {
  auto values1 = rng->Int32(size, -5, 5, null_probability);
  auto values2 = rng->Int64(size, -12345678912345LL, 12345678912345LL, null_probability);
  return MakeStructArray(rng, {values1, values2}, null_probability);
}

static void BM_ReadStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  ::arrow::random::RandomArrayGenerator rng(42);
  auto array = MakeStructArray(&rng, kNumValues, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructColumn)->Apply(NestedReadArguments);

static void BM_ReadStructOfStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  const int64_t kBytesPerValue = 2 * (sizeof(int32_t) + sizeof(int64_t));

  ::arrow::random::RandomArrayGenerator rng(42);
  auto values1 = MakeStructArray(&rng, kNumValues, null_probability);
  auto values2 = MakeStructArray(&rng, kNumValues, null_probability);
  auto array = MakeStructArray(&rng, {values1, values2}, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructOfStructColumn)->Apply(NestedReadArguments);

static void BM_ReadStructOfListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  auto values1 = rng.Int32(kNumValues, -5, 5, null_probability);
  auto values2 =
      rng.Int64(kNumValues, -12345678912345LL, 12345678912345LL, null_probability);
  auto list1 = rng.List(*values1, kNumValues / 10, null_probability);
  auto list2 = rng.List(*values2, kNumValues / 10, null_probability);
  auto array = MakeStructArray(&rng, {list1, list2}, null_probability,
                               /*propagate_validity =*/true);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadStructOfListColumn)->Apply(NestedReadArguments);

static void BM_ReadListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = rng.Int64(kNumValues, /*min=*/-5, /*max=*/5, null_probability);
  const int64_t kBytesPerValue = sizeof(int64_t);

  auto array = rng.List(*values, kNumValues / 10, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListColumn)->Apply(NestedReadArguments);

static void BM_ReadListOfStructColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = MakeStructArray(&rng, kNumValues, null_probability);
  const int64_t kBytesPerValue = sizeof(int32_t) + sizeof(int64_t);

  auto array = rng.List(*values, kNumValues / 10, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListOfStructColumn)->Apply(NestedReadArguments);

static void BM_ReadListOfListColumn(::benchmark::State& state) {
  constexpr int64_t kNumValues = BENCHMARK_SIZE / 10;
  const double null_probability = static_cast<double>(state.range(0)) / 100.0;
  const bool nullable = (null_probability != 0.0);

  ARROW_CHECK_GE(null_probability, 0.0);

  ::arrow::random::RandomArrayGenerator rng(42);

  auto values = rng.Int64(kNumValues, /*min=*/-5, /*max=*/5, null_probability);
  const int64_t kBytesPerValue = sizeof(int64_t);

  auto inner = rng.List(*values, kNumValues / 10, null_probability);
  auto array = rng.List(*inner, kNumValues / 100, null_probability);

  BenchmarkReadArray(state, array, nullable, kNumValues, kBytesPerValue * kNumValues);
}

BENCHMARK(BM_ReadListOfListColumn)->Apply(NestedReadArguments);

//
// Benchmark different ways of reading select row groups
//

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
  std::vector<int> rgs{0, 2, 4, 6, 8};

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    std::unique_ptr<FileReader> arrow_reader;
    EXIT_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                 &arrow_reader));
    std::shared_ptr<::arrow::Table> table;
    EXIT_NOT_OK(arrow_reader->ReadRowGroups(rgs, &table));
  }
  SetBytesProcessed<true, Int64Type>(state);
}

BENCHMARK(BM_ReadMultipleRowGroups);

static void BM_ReadMultipleRowGroupsGenerator(::benchmark::State& state) {
  std::vector<int64_t> values(BENCHMARK_SIZE, 128);
  std::shared_ptr<::arrow::Table> table = TableFromVector<Int64Type>(values, true);
  auto output = CreateOutputStream();
  // This writes 10 RowGroups
  EXIT_NOT_OK(
      WriteTable(*table, ::arrow::default_memory_pool(), output, BENCHMARK_SIZE / 10));
  PARQUET_ASSIGN_OR_THROW(auto buffer, output->Finish());
  std::vector<int> rgs{0, 2, 4, 6, 8};

  while (state.KeepRunning()) {
    auto reader =
        ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    std::unique_ptr<FileReader> unique_reader;
    EXIT_NOT_OK(FileReader::Make(::arrow::default_memory_pool(), std::move(reader),
                                 &unique_reader));
    std::shared_ptr<FileReader> arrow_reader = std::move(unique_reader);
    ASSIGN_OR_ABORT(auto generator,
                    arrow_reader->GetRecordBatchGenerator(arrow_reader, rgs, {0}));
    auto fut = ::arrow::CollectAsyncGenerator(generator);
    ASSIGN_OR_ABORT(auto batches, fut.result());
    ASSIGN_OR_ABORT(auto actual, ::arrow::Table::FromRecordBatches(std::move(batches)));
  }
  SetBytesProcessed<true, Int64Type>(state);
}

BENCHMARK(BM_ReadMultipleRowGroupsGenerator);

}  // namespace benchmark

}  // namespace parquet

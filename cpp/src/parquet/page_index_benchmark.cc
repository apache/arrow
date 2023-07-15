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

#include <cstdint>
#include <vector>

#include "benchmark/benchmark.h"

#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"

namespace parquet {

void BM_ReadOffsetIndex(::benchmark::State& state) {
  auto builder = OffsetIndexBuilder::Make();
  const size_t num_pages = state.range(0);
  constexpr size_t page_size = 1024;
  constexpr size_t first_row_index = 10000;
  for (size_t i = 0; i < num_pages; ++i) {
    builder->AddPage(page_size * i, page_size, first_row_index * i);
  }
  const int64_t final_position = 4096;
  builder->Finish(final_position);
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  auto buffer = sink->Finish().ValueOrDie();
  ReaderProperties properties;
  for (auto _ : state) {
    auto offset_index = OffsetIndex::Make(buffer->data() + 0, buffer->size(), properties);
    ::benchmark::DoNotOptimize(offset_index);
  }
  state.counters["num_pages"] = num_pages;
}

BENCHMARK(BM_ReadOffsetIndex)->Range(8, 1024);

// The sample string length for FLBA and ByteArray benchmarks
constexpr static uint32_t kDataStringLength = 8;

void GenerateRandomString(uint32_t length, uint32_t seed, std::vector<uint8_t>* heap) {
  // Character set used to generate random string
  const std::string charset =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine gen(seed);
  std::uniform_int_distribution<uint32_t> dist(0, static_cast<int>(charset.size() - 1));

  for (uint32_t i = 0; i < length; i++) {
    heap->push_back(charset[dist(gen)]);
  }
}

template <typename T>
void GenerateBenchmarkData(uint32_t size, uint32_t seed, T* data,
                           [[maybe_unused]] std::vector<uint8_t>* heap = nullptr) {
  if constexpr (std::is_integral_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<T> d(std::numeric_limits<T>::min(),
                                       std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_floating_point_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_real_distribution<T> d(std::numeric_limits<T>::lowest(),
                                        std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_same_v<FLBA, T>) {
    GenerateRandomString(kDataStringLength * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * kDataStringLength;
    }
  } else if constexpr (std::is_same_v<ByteArray, T>) {
    GenerateRandomString(kDataStringLength * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * kDataStringLength;
      data[i].len = kDataStringLength;
    }
  } else if constexpr (std::is_same_v<Int96, T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<int> d(std::numeric_limits<int>::min(),
                                         std::numeric_limits<int>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i].value[0] = d(gen);
      data[i].value[1] = d(gen);
      data[i].value[2] = d(gen);
    }
  }
}

template <typename DType>
void BM_ReadColumnIndex(::benchmark::State& state) {
  schema::NodePtr type = ::parquet::schema::PrimitiveNode::Make(
      "b", Repetition::OPTIONAL, DType::type_num, ConvertedType::NONE, 8);
  auto descr_ptr =
      std::make_unique<ColumnDescriptor>(type, /*def_level=*/1, /*rep_level=*/0);
  auto descr = descr_ptr.get();

  const size_t num_pages = state.range(0);

  auto builder = ColumnIndexBuilder::Make(descr);

  const size_t values_per_page = 100;
  for (int i = 0; i < num_pages; ++i) {
    auto stats = MakeStatistics<DType>(descr);
    std::vector<uint8_t> heap;
    std::vector<typename DType::c_type> values;
    values.resize(values_per_page);
    GenerateBenchmarkData(values_per_page, /*seed=*/0, values.data(), &heap);
    stats->Update(values.data(), values_per_page, 0);
    builder->AddPage(stats->Encode());
  }

  builder->Finish();
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  auto buffer = sink->Finish().ValueOrDie();
  ReaderProperties properties;
  for (auto _ : state) {
    auto column_index =
        ColumnIndex::Make(*descr, buffer->data() + 0, buffer->size(), properties);
    ::benchmark::DoNotOptimize(column_index);
  }
  state.counters["num_pages"] = num_pages;
}

BENCHMARK_TEMPLATE(BM_ReadColumnIndex, Int32Type)->Range(8, 1024);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, Int64Type)->Range(8, 1024);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, FloatType)->Range(8, 1024);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, DoubleType)->Range(8, 1024);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, FLBAType)->Range(8, 1024);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, ByteArrayType)->Range(8, 1024);

}  // namespace parquet

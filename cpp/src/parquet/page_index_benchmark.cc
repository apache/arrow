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

#include "parquet/benchmark_util.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"

namespace parquet::benchmark {

void PageIndexSetArgs(::benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"num_pages"});
  bench->Range(8, 1024);
}

void BM_ReadOffsetIndex(::benchmark::State& state) {
  auto builder = OffsetIndexBuilder::Make();
  const int num_pages = static_cast<int>(state.range(0));
  constexpr int64_t page_size = 1024;
  constexpr int64_t first_row_index = 10000;
  for (int i = 0; i < num_pages; ++i) {
    builder->AddPage(page_size * i, page_size, first_row_index * i);
  }
  constexpr int64_t final_position = 4096;
  builder->Finish(final_position);
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  auto buffer = sink->Finish().ValueOrDie();
  ReaderProperties properties;
  for (auto _ : state) {
    auto offset_index = OffsetIndex::Make(
        buffer->data() + 0, static_cast<uint32_t>(buffer->size()), properties);
    ::benchmark::DoNotOptimize(offset_index);
  }
  state.SetBytesProcessed(state.iterations() * buffer->size());
  state.SetItemsProcessed(state.iterations() * num_pages);
}

BENCHMARK(BM_ReadOffsetIndex)->Apply(PageIndexSetArgs);

// The sample string length for FLBA and ByteArray benchmarks
constexpr static uint32_t kDataStringLength = 8;

template <typename DType>
void BM_ReadColumnIndex(::benchmark::State& state) {
  schema::NodePtr type = ::parquet::schema::PrimitiveNode::Make(
      "b", Repetition::OPTIONAL, DType::type_num, ConvertedType::NONE, 8);
  auto descr_ptr =
      std::make_unique<ColumnDescriptor>(type, /*def_level=*/1, /*rep_level=*/0);
  auto descr = descr_ptr.get();

  const int num_pages = static_cast<int>(state.range(0));
  auto builder = ColumnIndexBuilder::Make(descr);

  const size_t values_per_page = 100;
  for (int i = 0; i < num_pages; ++i) {
    auto stats = MakeStatistics<DType>(descr);
    std::vector<uint8_t> heap;
    std::vector<typename DType::c_type> values;
    values.resize(values_per_page);
    GenerateBenchmarkData(values_per_page, /*seed=*/0, values.data(), &heap,
                          kDataStringLength);
    stats->Update(values.data(), values_per_page, /*null_count=*/0);
    builder->AddPage(stats->Encode());
  }

  builder->Finish();
  auto sink = CreateOutputStream();
  builder->WriteTo(sink.get());
  auto buffer = sink->Finish().ValueOrDie();
  ReaderProperties properties;
  for (auto _ : state) {
    auto column_index = ColumnIndex::Make(*descr, buffer->data() + 0,
                                          static_cast<int>(buffer->size()), properties);
    ::benchmark::DoNotOptimize(column_index);
  }
  state.SetBytesProcessed(state.iterations() * buffer->size());
  state.SetItemsProcessed(state.iterations() * num_pages);
}

BENCHMARK_TEMPLATE(BM_ReadColumnIndex, Int64Type)->Apply(PageIndexSetArgs);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, DoubleType)->Apply(PageIndexSetArgs);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, FLBAType)->Apply(PageIndexSetArgs);
BENCHMARK_TEMPLATE(BM_ReadColumnIndex, ByteArrayType)->Apply(PageIndexSetArgs);

}  // namespace parquet::benchmark

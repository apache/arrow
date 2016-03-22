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

#include "arrow/test-util.h"
#include "arrow/table/test-common.h"
#include "arrow/types/integer.h"
#include "arrow/util/memory-pool.h"

namespace arrow {
namespace {
  template <typename ArrayType>
  std::shared_ptr<Array> MakePrimitive(int32_t length, int32_t null_count = 0) {
    auto pool = GetDefaultMemoryPool();
    auto data = std::make_shared<PoolBuffer>(pool);
    auto nulls = std::make_shared<PoolBuffer>(pool);
    data->Resize(length * sizeof(typename ArrayType::value_type));
    nulls->Resize(util::bytes_for_bits(length));
    return std::make_shared<ArrayType>(length, data, 10, nulls);
  }
}  // anonymous namespace


static void BM_BuildInt32ColumnByChunk(benchmark::State& state) { //NOLINT non-const reference
  ArrayVector arrays;
  for (int chunk_n = 0; chunk_n < state.range_x(); ++chunk_n) {
    arrays.push_back(MakePrimitive<Int32Array>(100, 10));
  }
  const auto INT32 = std::make_shared<Int32Type>();
  const auto field = std::make_shared<Field>("c0", INT32);
  std::unique_ptr<Column> column;
  while (state.KeepRunning()) {
    column.reset(new Column(field, arrays));
  }
}

BENCHMARK(BM_BuildInt32ColumnByChunk)->Range(5, 50000);

}  // namespace arrow

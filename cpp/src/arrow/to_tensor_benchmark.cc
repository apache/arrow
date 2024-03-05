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

#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {

static void RecordBatchUniformTypesSimple(
    benchmark::State& state) {  // NOLINT non-const reference
  const int length = 9;
  auto ty = int32();

  auto f0 = field("f0", ty);
  auto f1 = field("f1", ty);
  auto f2 = field("f2", ty);

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema(fields);

  auto a0 = ArrayFromJSON(ty, "[1, 2, 3, 4, 5, 6, 7, 8, 9]");
  auto a1 = ArrayFromJSON(ty, "[10, 20, 30, 40, 50, 60, 70, 80, 90]");
  auto a2 = ArrayFromJSON(ty, "[100, 100, 100, 100, 100, 100, 100, 100, 100]");

  auto batch = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());
  }
  benchmark::DoNotOptimize(tensor);
  state.SetItemsProcessed(state.iterations() * batch->num_rows() * batch->num_columns());
  state.SetBytesProcessed(state.iterations() * ty->bit_width() * batch->num_columns() *
                          batch->num_rows());
}

BENCHMARK(RecordBatchUniformTypesSimple);

}  // namespace arrow

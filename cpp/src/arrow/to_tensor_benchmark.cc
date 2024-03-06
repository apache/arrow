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
#include "arrow/testing/random.h"
#include "arrow/type.h"

namespace arrow {

template <typename ValueType>
static void BatchToTensorSimple(benchmark::State& state) {
  std::shared_ptr<DataType> ty = TypeTraits<ValueType>::type_singleton();
  auto f0 = field("f0", ty);
  auto f1 = field("f1", ty);
  auto f2 = field("f2", ty);

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema(fields);

  constexpr int kNumRows = 500;
  arrow::random::RandomArrayGenerator gen_{42};
  auto a0 = gen_.ArrayOf(ty, kNumRows);
  auto a1 = gen_.ArrayOf(ty, kNumRows);
  auto a2 = gen_.ArrayOf(ty, kNumRows);

  auto batch = RecordBatch::Make(schema, kNumRows, {a0, a1, a2});

  ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());
  }
  benchmark::DoNotOptimize(tensor);
  state.SetItemsProcessed(state.iterations() * batch->num_rows() * batch->num_columns());
  state.SetBytesProcessed(state.iterations() * ty->bit_width() * batch->num_columns() *
                          batch->num_rows());
}

BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt8Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt16Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt32Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt64Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int8Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int16Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int32Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int64Type);
BENCHMARK_TEMPLATE(BatchToTensorSimple, HalfFloatType);
BENCHMARK_TEMPLATE(BatchToTensorSimple, FloatType);
BENCHMARK_TEMPLATE(BatchToTensorSimple, DoubleType);

}  // namespace arrow

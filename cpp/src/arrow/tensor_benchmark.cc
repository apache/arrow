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
#include "arrow/util/benchmark_util.h"

namespace arrow {

template <typename ValueType>
static void BatchToTensorSimple(benchmark::State& state) {
  RegressionArgs args(state);
  std::shared_ptr<DataType> ty = TypeTraits<ValueType>::type_singleton();

  const int64_t kNumRows = args.size;
  arrow::random::RandomArrayGenerator gen_{42};

  std::vector<std::shared_ptr<Field>> fields = {};
  std::vector<std::shared_ptr<Array>> columns = {};

  for (int i = 0; i < 100; ++i) {
    fields.push_back(field("f" + std::to_string(i), ty));
    columns.push_back(gen_.ArrayOf(ty, kNumRows));
  }
  auto schema = std::make_shared<Schema>(std::move(fields));
  auto batch = RecordBatch::Make(schema, kNumRows, columns);

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto tensor, batch->ToTensor());
  }
  state.SetItemsProcessed(state.iterations() * kNumRows * batch->num_columns());
  state.SetBytesProcessed(state.iterations() * ty->bit_width() * kNumRows *
                          batch->num_columns());
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (int64_t size : {kL1Size, kL2Size}) {
    bench->Arg(size);
  }
}

BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, UInt64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int8Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int16Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int32Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, Int64Type)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, HalfFloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, FloatType)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BatchToTensorSimple, DoubleType)->Apply(SetArgs);

}  // namespace arrow

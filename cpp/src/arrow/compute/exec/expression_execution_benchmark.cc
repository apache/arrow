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

#include "arrow/compute/cast.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

auto expression =
    and_(less(field_ref("x"), literal(20)), greater(field_ref("x"), literal(0)));

static void Execution(benchmark::State& state) {
  const auto array_batches = static_cast<int32_t>(state.range(0));
  const auto array_size = 10000000 / array_batches;

  ExecContext ctx;
  auto dataset_schema = schema({
      field("x", int64()),
  });
  ExecBatch input({Datum(ConstantArrayGenerator::Int64(array_size, 5))},
                  /*length=*/1);

  ASSIGN_OR_ABORT(auto bound, expression.Bind(*dataset_schema));
  for (auto _ : state) {
    for (int it = 0; it < array_batches; ++it)
      ABORT_NOT_OK(ExecuteScalarExpression(bound, input, &ctx).status());
  }
  state.SetBytesProcessed(state.iterations() * 8 * array_size * array_batches);
}

BENCHMARK(Execution)->RangeMultiplier(10)->Range(1, 10000000);

}  // namespace compute
}  // namespace arrow

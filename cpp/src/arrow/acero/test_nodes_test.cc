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

#include "arrow/acero/test_nodes.h"

#include <gtest/gtest.h>

#include "arrow/acero/exec_plan.h"
#include "arrow/table.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace acero {

TEST(JitterNode, Basic) {
  static constexpr random::SeedType kTestSeed = 42;
  static constexpr int kMaxJitterMod = 4;
  static constexpr int kNumBatches = 256;
  RegisterTestNodes();
  std::shared_ptr<Table> input =
      gen::Gen({gen::Constant(std::make_shared<Int32Scalar>(0))})
          ->FailOnError()
          ->Table(1, kNumBatches);
  Declaration plan =
      Declaration::Sequence({{"table_source", TableSourceNodeOptions(input)},
                             {"jitter", JitterNodeOptions(kTestSeed, kMaxJitterMod)}});
  QueryOptions query_options;
  query_options.sequence_output = false;
  ASSERT_OK_AND_ASSIGN(BatchesWithCommonSchema batches_and_schema,
                       DeclarationToExecBatches(std::move(plan), query_options));

  ASSERT_EQ(kNumBatches, static_cast<int>(batches_and_schema.batches.size()));
  int numOutOfPlace = 0;
  for (int idx = 0; idx < kNumBatches; idx++) {
    const ExecBatch& batch = batches_and_schema.batches[idx];
    int jitter = std::abs(idx - static_cast<int>(batch.index));
    if (jitter > 0) {
      numOutOfPlace++;
    }
  }
  ASSERT_GT(numOutOfPlace, 0);
}

}  // namespace acero
}  // namespace arrow

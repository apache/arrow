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

#pragma once

#include <arrow/testing/gtest_util.h>
#include <arrow/util/vector.h>

#include <functional>
#include <string>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/testing/visibility.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace compute {

using StartProducingFunc = std::function<Status(ExecNode*)>;
using StopProducingFunc = std::function<void(ExecNode*)>;

// Make a dummy node that has no execution behaviour
ARROW_TESTING_EXPORT
ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, std::vector<ExecNode*> inputs,
                        int num_outputs, StartProducingFunc = {}, StopProducingFunc = {});

ARROW_TESTING_EXPORT
ExecBatch ExecBatchFromJSON(const std::vector<ValueDescr>& descrs,
                            util::string_view json);

struct BatchesWithSchema {
  std::vector<ExecBatch> batches;
  std::shared_ptr<Schema> schema;

  AsyncGenerator<util::optional<ExecBatch>> gen(bool parallel, bool slow) const {
    auto opt_batches = ::arrow::internal::MapVector(
        [](ExecBatch batch) { return util::make_optional(std::move(batch)); }, batches);

    AsyncGenerator<util::optional<ExecBatch>> gen;

    if (parallel) {
      // emulate batches completing initial decode-after-scan on a cpu thread
      gen = MakeBackgroundGenerator(MakeVectorIterator(std::move(opt_batches)),
                                    ::arrow::internal::GetCpuThreadPool())
                .ValueOrDie();

      // ensure that callbacks are not executed immediately on a background thread
      gen =
          MakeTransferredGenerator(std::move(gen), ::arrow::internal::GetCpuThreadPool());
    } else {
      gen = MakeVectorGenerator(std::move(opt_batches));
    }

    if (slow) {
      gen =
          MakeMappedGenerator(std::move(gen), [](const util::optional<ExecBatch>& batch) {
            SleepABit();
            return batch;
          });
    }

    return gen;
  }
};

ARROW_TESTING_EXPORT
Future<std::vector<ExecBatch>> StartAndCollect(
    ExecPlan* plan, AsyncGenerator<util::optional<ExecBatch>> gen);

ARROW_TESTING_EXPORT
BatchesWithSchema MakeBasicBatches();

ARROW_TESTING_EXPORT
BatchesWithSchema MakeNestedBatches();

ARROW_TESTING_EXPORT
BatchesWithSchema MakeRandomBatches(const std::shared_ptr<Schema>& schema,
                                    int num_batches = 10, int batch_size = 4);

ARROW_TESTING_EXPORT
Result<std::shared_ptr<Table>> SortTableOnAllFields(const std::shared_ptr<Table>& tab);

ARROW_TESTING_EXPORT
void AssertTablesEqual(const std::shared_ptr<Table>& exp,
                       const std::shared_ptr<Table>& act);

ARROW_TESTING_EXPORT
void AssertExecBatchesEqual(const std::shared_ptr<Schema>& schema,
                            const std::vector<ExecBatch>& exp,
                            const std::vector<ExecBatch>& act);

ARROW_TESTING_EXPORT
bool operator==(const Declaration&, const Declaration&);

ARROW_TESTING_EXPORT
void PrintTo(const Declaration& decl, std::ostream* os);

}  // namespace compute
}  // namespace arrow

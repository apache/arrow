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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/record_batch.h"
#include "arrow/testing/visibility.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/type_fwd.h"

namespace arrow {
namespace compute {

using StartProducingFunc = std::function<Status(ExecNode*)>;
using StopProducingFunc = std::function<void(ExecNode*)>;

// Make a dummy node that has no execution behaviour
ARROW_TESTING_EXPORT
ExecNode* MakeDummyNode(ExecPlan* plan, std::string label, int num_inputs,
                        int num_outputs, StartProducingFunc = {}, StopProducingFunc = {});

using RecordBatchGenerator = AsyncGenerator<std::shared_ptr<RecordBatch>>;

// Make a source node (no inputs) that produces record batches by reading in the
// background from a RecordBatchReader.
ARROW_TESTING_EXPORT
ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    std::shared_ptr<RecordBatchReader> reader,
                                    ::arrow::internal::Executor* io_executor);

ARROW_TESTING_EXPORT
ExecNode* MakeRecordBatchReaderNode(ExecPlan* plan, std::string label,
                                    std::shared_ptr<Schema> schema,
                                    RecordBatchGenerator generator,
                                    ::arrow::internal::Executor* io_executor);

class RecordBatchCollectNode : public ExecNode {
 public:
  virtual RecordBatchGenerator generator() = 0;

 protected:
  using ExecNode::ExecNode;
};

ARROW_TESTING_EXPORT
RecordBatchCollectNode* MakeRecordBatchCollectNode(ExecPlan* plan, std::string label,
                                                   const std::shared_ptr<Schema>& schema);

}  // namespace compute
}  // namespace arrow

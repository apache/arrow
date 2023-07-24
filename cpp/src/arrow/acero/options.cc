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

#include "arrow/acero/options.h"
#include "arrow/acero/exec_plan.h"
#include "arrow/io/util_internal.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace acero {

constexpr int64_t TableSourceNodeOptions::kDefaultMaxBatchSize;

std::string ToString(JoinType t) {
  switch (t) {
    case JoinType::LEFT_SEMI:
      return "LEFT_SEMI";
    case JoinType::RIGHT_SEMI:
      return "RIGHT_SEMI";
    case JoinType::LEFT_ANTI:
      return "LEFT_ANTI";
    case JoinType::RIGHT_ANTI:
      return "RIGHT_ANTI";
    case JoinType::INNER:
      return "INNER";
    case JoinType::LEFT_OUTER:
      return "LEFT_OUTER";
    case JoinType::RIGHT_OUTER:
      return "RIGHT_OUTER";
    case JoinType::FULL_OUTER:
      return "FULL_OUTER";
  }
  ARROW_LOG(FATAL) << "Invalid variant of arrow::compute::JoinType";
  std::abort();
}

namespace {
ExecBatchIteratorMaker VecToItMaker(std::vector<ExecBatch> batches) {
  auto batches_ptr = std::make_shared<std::vector<std::shared_ptr<ExecBatch>>>();
  batches_ptr->reserve(batches.size());
  for (auto batch : batches) {
    batches_ptr->push_back(std::make_shared<ExecBatch>(std::move(batch)));
  }
  return
      [batches_ptr = std::move(batches_ptr)] { return MakeVectorIterator(*batches_ptr); };
}
}  // namespace

ExecBatchSourceNodeOptions::ExecBatchSourceNodeOptions(
    std::shared_ptr<Schema> schema, std::vector<ExecBatch> batches,
    ::arrow::internal::Executor* io_executor)
    : SchemaSourceNodeOptions(std::move(schema), VecToItMaker(std::move(batches)),
                              io_executor) {}

ExecBatchSourceNodeOptions::ExecBatchSourceNodeOptions(std::shared_ptr<Schema> schema,
                                                       std::vector<ExecBatch> batches,
                                                       bool requires_io)
    : SchemaSourceNodeOptions(std::move(schema), VecToItMaker(std::move(batches)),
                              requires_io) {}

}  // namespace acero
}  // namespace arrow

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

#include "arrow/compute/exec/groupby.h"

#include <mutex>
#include <thread>
#include <unordered_map>

#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/row/grouper.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"

namespace arrow {

namespace compute {

Result<std::shared_ptr<Table>> TableGroupBy(std::shared_ptr<Table> table,
                                            std::vector<Aggregate> aggregates,
                                            std::vector<FieldRef> keys, bool use_threads,
                                            MemoryPool* memory_pool) {
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(table))},
       {"aggregate", AggregateNodeOptions(std::move(aggregates), std::move(keys))}});
  return DeclarationToTable(std::move(plan), use_threads, memory_pool);
}

Result<std::shared_ptr<Table>> BatchGroupBy(std::shared_ptr<RecordBatch> batch,
                                            std::vector<Aggregate> aggregates,
                                            std::vector<FieldRef> keys, bool use_threads,
                                            MemoryPool* memory_pool) {
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> table,
                        Table::FromRecordBatches({std::move(batch)}));
  Declaration plan = Declaration::Sequence(
      {{"table_source", TableSourceNodeOptions(std::move(table))},
       {"aggregate", AggregateNodeOptions(std::move(aggregates), std::move(keys))}});
  return DeclarationToTable(std::move(plan), use_threads, memory_pool);
}

}  // namespace compute
}  // namespace arrow

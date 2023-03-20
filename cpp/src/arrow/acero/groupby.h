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

#include <memory>
#include <vector>

#include "arrow/acero/exec_plan.h"
#include "arrow/acero/options.h"
#include "arrow/acero/visibility.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace acero {

/// Convenience function to perform a group-by on a table
///
/// The result will be calculated using an exec plan with an aggregate node.
///
/// If there are no arguments/aggregates then the returned table will have one row
/// for each unique combination of keys
///
/// Note: If there are many groups the output table may have multiple chunks.
///
/// If there are no keys then the aggregates will be applied to the full table.
/// The output table in this scenario is guaranteed to have exactly 1 row.
///
/// \return a table that will have one column for each aggregate, named after they
/// aggregate function, and one column for each key
ARROW_ACERO_EXPORT
Result<std::shared_ptr<Table>> TableGroupBy(
    std::shared_ptr<Table> table, std::vector<Aggregate> aggregates,
    std::vector<FieldRef> keys, bool use_threads = false,
    MemoryPool* memory_pool = default_memory_pool());

/// Convenience function to perform a group-by on a record batch
///
/// \see GroupByTable
ARROW_ACERO_EXPORT
Result<std::shared_ptr<Table>> BatchGroupBy(
    std::shared_ptr<RecordBatch> record_batch, std::vector<Aggregate> aggregates,
    std::vector<FieldRef> keys, bool use_threads = false,
    MemoryPool* memory_pool = default_memory_pool());

}  // namespace acero
}  // namespace arrow

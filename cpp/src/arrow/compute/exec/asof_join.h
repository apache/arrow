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
#include <vector>

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/counting_semaphore.h>  // so we don't need to require C++20
#include <arrow/util/thread_pool.h>
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/tracing_internal.h"

#include "concurrent_bounded_queue.h"

namespace arrow {
namespace compute {

typedef int32_t KeyType;

// Maximum number of tables that can be joined
#define MAX_JOIN_TABLES 64

// Capacity of the input queues (for flow control)
// Why 2?
// It needs to be at least 1 to enable progress (otherwise queues have no capacity)
// It needs to be at least 2 to enable addition of a new queue entry while processing
// is being done for another input.
// There's no clear performance benefit to greater than 2.
#define QUEUE_CAPACITY 2

// The max rows per batch is dictated by the data type for row index
#define MAX_ROWS_PER_BATCH 0xFFFFFFFF
typedef uint32_t row_index_t;
typedef int col_index_t;

class AsofJoinSchema {
 public:
  std::shared_ptr<Schema> MakeOutputSchema(const std::vector<ExecNode*>& inputs,
                                           const AsofJoinNodeOptions& options);
};

class AsofJoinImpl {
 public:
  static Result<std::unique_ptr<AsofJoinImpl>> MakeBasic();
};

}  // namespace compute
}  // namespace arrow

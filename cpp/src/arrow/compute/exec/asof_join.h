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

#include <arrow/compute/api.h>
#include <arrow/compute/exec/exec_plan.h>
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/schema_util.h"

namespace arrow {
namespace compute {

typedef int32_t KeyType;

// Maximum number of tables that can be joined
constexpr int kMaxJoinTables = 64
typedef uint64_t row_index_t;
typedef int col_index_t;

class AsofJoinSchema {
 public:
  std::shared_ptr<Schema> MakeOutputSchema(const std::vector<ExecNode*>& inputs,
                                           const AsofJoinNodeOptions& options);
};

}  // namespace compute
}  // namespace arrow

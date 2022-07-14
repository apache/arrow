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

// These utilities are for internal / unit test use only.
// They allow for the construction of simple Substrait plans
// programmatically without first requiring the construction
// of an ExecPlan

// These utilities have to be here, and not in a test_util.cc
// file (or in a unit test) because only one .so is allowed
// to include each .pb.h file or else protobuf will encounter
// global namespace conflicts.

#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/type.h"

namespace arrow {
namespace engine {
namespace internal {

Result<std::shared_ptr<Buffer>> CreateScanProjectSubstrait(
    Id function_id, const std::shared_ptr<Table>& input_table,
    const std::vector<std::string>& arguments,
    const std::vector<std::shared_ptr<DataType>>& data_types,
    const DataType& output_type);

}  // namespace internal
}  // namespace engine
}  // namespace arrow

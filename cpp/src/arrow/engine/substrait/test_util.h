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

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/util.h"
#include "arrow/dataset/file_base.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

namespace engine {

const auto kNullConsumer = std::make_shared<compute::NullSinkNodeConsumer>();

ARROW_ENGINE_EXPORT std::shared_ptr<DataType> StripFieldNames(
    std::shared_ptr<DataType> type);

ARROW_ENGINE_EXPORT void WriteIpcData(const std::string& path,
                                      const std::shared_ptr<fs::FileSystem> file_system,
                                      const std::shared_ptr<Table> input);

ARROW_ENGINE_EXPORT Result<std::shared_ptr<Table>> GetTableFromPlan(
    compute::Declaration& other_declrs, compute::ExecContext& exec_context,
    const std::shared_ptr<Schema>& output_schema);

ARROW_ENGINE_EXPORT void CheckRoundTripResult(
    const std::shared_ptr<Table> expected_table, std::shared_ptr<Buffer>& buf,
    const std::vector<int>& include_columns = {},
    const ConversionOptions& conversion_options = {},
    const compute::SortOptions* sort_options = NULLPTR);

}  // namespace engine
}  // namespace arrow

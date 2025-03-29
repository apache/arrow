
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
#include <string>
#include <variant>
#include <vector>

#include "arrow/array/statistics.h"
#include "arrow/testing/visibility.h"

#include "arrow/type_fwd.h"

namespace arrow {
struct StatisticsArrayTOptions;
namespace test {

ARROW_TESTING_EXPORT Result<std::shared_ptr<Array>> MakeMockStatisticsArray(
    const std::string& columns_json,
    const std::vector<std::vector<std::string>>& nested_statistics_keys,
    const std::vector<std::vector<ArrayStatistics::ValueType>>& nested_statistics_values);

/// \brief Create a nested struct array with a specified depth.
///
/// \param[in] depth The depth of the nested structs. Must be greater than 1.
///
/// \return A parent_struct which has depth -1 child struct
ARROW_TESTING_EXPORT
Result<std::shared_ptr<Array>> MakeNestedStruct(int32_t depth);

/// \brief Check whether it's possible to create a statistics array with the given
/// options.
///
/// \param[in] value object for calling its Make Statistics Array
/// \param[in] options options for creating statistics array
///
/// \return Whether \ref Status::OK or \ref Status::Invalid("Max recursion depth reached")
ARROW_TESTING_EXPORT
Status CheckDepth(
    std::variant<std::shared_ptr<RecordBatch>, std::shared_ptr<Array>> value,
    const StatisticsArrayTOptions& options);
}  // namespace test
}  // namespace arrow

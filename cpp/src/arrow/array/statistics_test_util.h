
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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/statistics.h"
#include "arrow/array/statistics_option.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {

namespace detail {
ARROW_TESTING_EXPORT Result<std::shared_ptr<Array>> MakeMockStatisticsArray(
    const std::string& columns_json,
    const std::vector<std::vector<std::string>>& nested_statistics_keys,
    const std::vector<std::vector<ArrayStatistics::ValueType>>& nested_statistics_values,
    const std::vector<std::shared_ptr<DataType>>& string_type = {});
}  // namespace detail

/// \brief Create a nested struct array with a specified depth.
///
/// \param[in] depth The depth of the nested structs. Must be greater than 1.
///
/// \return A parent_struct which has depth -1 child struct
ARROW_TESTING_EXPORT
Result<std::shared_ptr<Array>> MakeNestedStruct(int32_t depth);

template <typename Type>
class TestStatisticsArray {
 public:
  void CheckStatStatisticsArray(
      const std::shared_ptr<Type>& value, const std::string& columns_json,
      const std::vector<std::vector<std::string>>& nested_statistics_keys,
      const std::vector<std::vector<ArrayStatistics::ValueType>>&
          nested_statistics_values,
      const std::vector<std::shared_ptr<DataType>>& string_type = {}) const {
    ASSERT_OK_AND_ASSIGN(auto statistics_array, value->MakeStatisticsArray(options_));
    ASSERT_OK_AND_ASSIGN(
        auto expected_statistics_array,
        detail::MakeMockStatisticsArray(columns_json, nested_statistics_keys,
                                        nested_statistics_values, string_type));
    AssertArraysEqual(*expected_statistics_array, *statistics_array, true);
  }

  void CheckDepth(const std::shared_ptr<Type>& value, bool is_error) const {
    if (!is_error) {
      ASSERT_OK(value->MakeStatisticsArray(options_).status());
    } else {
      ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Max recursion depth reached",
                                 value->MakeStatisticsArray(options_).status());
    }
  }

  StatisticsArrayTOptions options_;
};
}  // namespace arrow

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

#include <vector>

#include "arrow/builder.h"

namespace arrow::util::internal {

class NestedListGenerator {
 public:
  /// \brief Create a nested FixedSizeListType.
  ///
  /// \return `fixed_size_list(fixed_size_list(..., sizes[1]), sizes[0])`
  static std::shared_ptr<DataType> NestedFSLType(
      const std::shared_ptr<DataType>& inner_type, const std::vector<int>& sizes);

  /// \brief Create a nested FixedListType.
  ///
  /// \return `list(list(...))`
  static std::shared_ptr<DataType> NestedListType(
      const std::shared_ptr<DataType>& inner_type, size_t depth);

  static Result<std::shared_ptr<Array>> NestedFSLArray(
      const std::shared_ptr<DataType>& inner_type, const std::vector<int>& list_sizes,
      int64_t length);

  static Result<std::shared_ptr<Array>> NestedListArray(
      const std::shared_ptr<DataType>& inner_type, const std::vector<int>& list_sizes,
      int64_t length);

  /// \brief Generate all possible nested list configurations of depth 1 to max_depth.
  ///
  /// Each configuration consists of a single inner value type and a list of sizes.
  /// Both can be used with NestedFSLArray and NestedListArray to generate test data.
  ///
  /// The product of the list sizes and the size of the inner value type is always a power
  /// of 2 no greater than max_power_of_2_size. For max_depth=3 and
  /// max_power_of_2_size=32, this generates 108 configurations.
  ///
  /// \tparam Visit a function type with signature
  ///     void(const std::shared_ptr<DataType>& inner_type,
  ///          const std::vector<int>& list_sizes)
  template <class Visit>
  static void VisitAllNestedListConfigurations(
      const std::vector<std::shared_ptr<DataType>>& inner_value_types, Visit&& visit,
      int max_depth = 3, int max_power_of_2_size = 32) {
    for (int depth = 1; depth <= max_depth; depth++) {
      for (auto& type : inner_value_types) {
        assert(is_fixed_width(*type));
        int value_width = type->byte_width();

        std::vector<int> list_sizes;  // stack of list sizes
        auto pop = [&]() {            // pop the list_sizes stack
          assert(!list_sizes.empty());
          value_width /= list_sizes.back();
          list_sizes.pop_back();
        };
        auto next = [&]() {  // double the top of the stack
          assert(!list_sizes.empty());
          value_width *= 2;
          list_sizes.back() *= 2;
          return value_width;
        };
        auto push_1s = [&]() {  // fill the stack with 1s
          while (list_sizes.size() < static_cast<size_t>(depth)) {
            list_sizes.push_back(1);
          }
        };

        // Loop invariants:
        //   value_width == product(list_sizes) * type->byte_width()
        //   value_width is a power-of-2 (1, 2, 4, 8, 16, max_power_of_2_size=32)
        push_1s();
        do {
          // for (auto x : list_sizes) printf("%d * ", x);
          // printf("(%s) %d = %2d\n", type->name().c_str(), type->byte_width(),
          // value_width);
          visit(type, list_sizes);
          while (!list_sizes.empty()) {
            if (next() <= max_power_of_2_size) {
              push_1s();
              break;
            }
            pop();
          }
        } while (!list_sizes.empty());
      }
    }
  }

 private:
  // Append([...[[*next_inner_value++, *next_inner_value++, ...]]...])
  static Status AppendNestedList(ArrayBuilder* nested_builder, const int* list_sizes,
                                 int64_t* next_inner_value);

  static Result<std::shared_ptr<Array>> NestedListArray(
      ArrayBuilder* nested_builder, const std::vector<int>& list_sizes, int64_t length);
};

}  // namespace arrow::util::internal

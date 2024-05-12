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

#include "fixed_width_test_util.h"

namespace arrow::util::internal {

std::shared_ptr<DataType> NestedListGenerator::NestedFSLType(
    const std::shared_ptr<DataType>& inner_type, const std::vector<int>& sizes) {
  auto type = inner_type;
  for (auto it = sizes.rbegin(); it != sizes.rend(); it++) {
    type = fixed_size_list(type, *it);
  }
  return type;
}

std::shared_ptr<DataType> NestedListGenerator::NestedListType(
    const std::shared_ptr<DataType>& inner_type, size_t depth) {
  auto list_type = list(inner_type);
  for (size_t i = 1; i < depth; i++) {
    list_type = list(std::move(list_type));
  }
  return list_type;
}

Result<std::shared_ptr<Array>> NestedListGenerator::NestedFSLArray(
    const std::shared_ptr<DataType>& inner_type, const std::vector<int>& list_sizes,
    int64_t length) {
  auto nested_type = NestedFSLType(inner_type, list_sizes);
  ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(nested_type));
  return NestedListArray(builder.get(), list_sizes, length);
}

Result<std::shared_ptr<Array>> NestedListGenerator::NestedListArray(
    const std::shared_ptr<DataType>& inner_type, const std::vector<int>& list_sizes,
    int64_t length) {
  auto nested_type = NestedListType(inner_type, list_sizes.size());
  ARROW_ASSIGN_OR_RAISE(auto builder, MakeBuilder(nested_type));
  return NestedListArray(builder.get(), list_sizes, length);
}

}  // namespace arrow::util::internal

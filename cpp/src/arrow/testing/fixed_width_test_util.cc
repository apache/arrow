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

#include <cstdint>
#include <functional>
#include <limits>
#include <vector>

#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/testing/fixed_width_test_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow::util::internal {

namespace {
template <typename ArrowType>
inline Status AppendNumeric(ArrayBuilder* builder, int64_t* next_value) {
  using NumericBuilder = ::arrow::NumericBuilder<ArrowType>;
  using value_type = typename NumericBuilder::value_type;
  auto* numeric_builder = ::arrow::internal::checked_cast<NumericBuilder*>(builder);
  auto cast_next_value =
      static_cast<value_type>(*next_value % std::numeric_limits<value_type>::max());
  RETURN_NOT_OK(numeric_builder->Append(cast_next_value));
  *next_value += 1;
  return Status::OK();
}
}  // namespace

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

void NestedListGenerator::VisitAllNestedListConfigurations(
    const std::vector<std::shared_ptr<DataType>>& inner_value_types,
    const std::function<void(const std::shared_ptr<DataType>&, const std::vector<int>&)>&
        visit,
    int max_depth, int max_power_of_2_size) {
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

Status NestedListGenerator::AppendNestedList(ArrayBuilder* nested_builder,
                                             const int* list_sizes,
                                             int64_t* next_inner_value) {
  using ::arrow::internal::checked_cast;
  ArrayBuilder* builder = nested_builder;
  auto type = builder->type();
  if (type->id() == Type::FIXED_SIZE_LIST || type->id() == Type::LIST) {
    const int list_size = *list_sizes;
    if (type->id() == Type::FIXED_SIZE_LIST) {
      auto* fsl_builder = checked_cast<FixedSizeListBuilder*>(builder);
      assert(list_size == checked_cast<FixedSizeListType&>(*type).list_size());
      RETURN_NOT_OK(fsl_builder->Append());
      builder = fsl_builder->value_builder();
    } else {  // type->id() == Type::LIST)
      auto* list_builder = checked_cast<ListBuilder*>(builder);
      RETURN_NOT_OK(list_builder->Append(/*is_valid=*/true, list_size));
      builder = list_builder->value_builder();
    }
    list_sizes++;
    for (int i = 0; i < list_size; i++) {
      RETURN_NOT_OK(AppendNestedList(builder, list_sizes, next_inner_value));
    }
  } else {
    switch (type->id()) {
      case Type::INT8:
        RETURN_NOT_OK(AppendNumeric<Int8Type>(builder, next_inner_value));
        break;
      case Type::INT16:
        RETURN_NOT_OK(AppendNumeric<Int16Type>(builder, next_inner_value));
        break;
      case Type::INT32:
        RETURN_NOT_OK(AppendNumeric<Int32Type>(builder, next_inner_value));
        break;
      case Type::INT64:
        RETURN_NOT_OK(AppendNumeric<Int64Type>(builder, next_inner_value));
        break;
      default:
        return Status::NotImplemented("Unsupported type: ", *type);
    }
  }
  return Status::OK();
}

Result<std::shared_ptr<Array>> NestedListGenerator::NestedListArray(
    ArrayBuilder* nested_builder, const std::vector<int>& list_sizes, int64_t length) {
  int64_t next_inner_value = 0;
  for (int64_t i = 0; i < length; i++) {
    RETURN_NOT_OK(AppendNestedList(nested_builder, list_sizes.data(), &next_inner_value));
  }
  return nested_builder->Finish();
}

}  // namespace arrow::util::internal

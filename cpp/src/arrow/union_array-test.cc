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

#include <string>

#include <gtest/gtest.h>

#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

using arrow::internal::checked_pointer_cast;
using arrow::internal::checked_cast;

namespace arrow {

class TestUnionArray : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    auto type_ids = compute::_MakeArray<Int8Type, int8_t>(int8(), { 0, 1, 2, 0, 1, 3, 2, 0, 2, 1 }, {});
    type_ids_ = checked_pointer_cast<Int8Array>(type_ids);
  }

  void CheckFieldNames(const UnionArray& array, const std::vector<std::string>& names) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    ASSERT_EQ(type.num_children(), names.size());
    for (int i = 0; i < type.num_children(); ++i) {
      ASSERT_EQ(type.child(i)->name(), names[i]);
    }
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<Int8Array> type_ids_;
};

TEST_F(TestUnionArray, MakeDense) {
  using compute::_MakeArray;

  auto value_offsets = _MakeArray<Int32Type, int32_t>(int32(), { 0, 0, 0, 1, 1, 0, 1, 2, 1, 2 }, {});

  auto string_array = _MakeArray<StringType, std::string>(utf8(), { "abc", "def", "xyz" }, {});
  auto uint8_array = _MakeArray<UInt8Type, uint8_t>(uint8(), { 10, 20, 30 }, {});
  auto double_array = _MakeArray<DoubleType, double>(float64(), { 1.618, 2.718, 3.142 }, {});
  auto int8_array = _MakeArray<Int8Type, int8_t>(int8(), { -12 }, {});

  std::vector<std::shared_ptr<Array>> children = { string_array, uint8_array, double_array, int8_array };

  std::shared_ptr<Array> result;
  ASSERT_OK(UnionArray::MakeDense(*type_ids_, *value_offsets, children, &result));

  UnionArray& union_array = checked_cast<UnionArray&>(*result);
  ASSERT_EQ(UnionMode::DENSE, union_array.mode());
  CheckFieldNames(union_array, { "0", "1", "2", "3" });
}

TEST_F(TestUnionArray, MakeSparse) {
  using compute::_MakeArray;

  auto string_array = _MakeArray<StringType, std::string>(utf8(), { "abc", "", "", "def", "", "", "", "xyz", "", "" }, {});
  auto uint8_array = _MakeArray<UInt8Type, uint8_t>(uint8(), { 0, 10, 0, 0, 20, 0, 0, 0, 0, 30 }, {});
  auto double_array = _MakeArray<DoubleType, double>(float64(), { 0.0, 0.0, 1.618, 0.0, 0.0, 0.0, 2.718, 0.0, 3.142, 0.0 }, {});
  auto int8_array = _MakeArray<Int8Type, int8_t>(int8(), { 0, 0, 0, 0, 0, -12, 0, 0, 0, 0 }, {});

  std::vector<std::shared_ptr<Array>> children = { string_array, uint8_array, double_array, int8_array };

  std::shared_ptr<Array> result;
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, &result));

  UnionArray& union_array = checked_cast<UnionArray&>(*result);
  ASSERT_EQ(UnionMode::SPARSE, union_array.mode());
  CheckFieldNames(union_array, { "0", "1", "2", "3" });
}

}

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

#include "arrow/array.h"
// TODO ipc shouldn't be included here
#include "arrow/ipc/test-common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

TEST(TestUnionArray, TestSliceEquals) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(ipc::test::MakeUnion(&batch));

  auto CheckUnion = [](std::shared_ptr<Array> array) {
    const int64_t size = array->length();
    std::shared_ptr<Array> slice, slice2;
    slice = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    slice2 = array->Slice(2);
    ASSERT_EQ(size - 2, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(2, array->length(), 0, slice));

    // Chained slices
    slice2 = array->Slice(1)->Slice(1);
    ASSERT_TRUE(slice->Equals(slice2));

    slice = array->Slice(1, 5);
    slice2 = array->Slice(1, 5);
    ASSERT_EQ(5, slice->length());

    ASSERT_TRUE(slice->Equals(slice2));
    ASSERT_TRUE(array->RangeEquals(1, 6, 0, slice));

    AssertZeroPadded(*array);
    TestInitialized(*array);
  };

  CheckUnion(batch->column(1));
  CheckUnion(batch->column(2));
}

// -------------------------------------------------------------------------
// Tests for MakeDense and MakeSparse

class TestUnionArrayFactories : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    ArrayFromVector<Int8Type>({0, 1, 2, 0, 1, 3, 2, 0, 2, 1}, &type_ids_);
  }

  void CheckUnionArray(const UnionArray& array, UnionMode::type mode,
                       const std::vector<std::string>& field_names,
                       const std::vector<uint8_t>& type_codes) {
    ASSERT_EQ(mode, array.mode());
    CheckFieldNames(array, field_names);
    CheckTypeCodes(array, type_codes);
  }

  void CheckFieldNames(const UnionArray& array, const std::vector<std::string>& names) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    ASSERT_EQ(type.num_children(), names.size());
    for (int i = 0; i < type.num_children(); ++i) {
      ASSERT_EQ(type.child(i)->name(), names[i]);
    }
  }

  void CheckTypeCodes(const UnionArray& array, const std::vector<uint8_t>& codes) {
    const auto& type = checked_cast<const UnionType&>(*array.type());
    ASSERT_EQ(codes, type.type_codes());
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<Array> type_ids_;
};

TEST_F(TestUnionArrayFactories, TestMakeDense) {
  std::shared_ptr<Array> value_offsets;
  ArrayFromVector<Int32Type, int32_t>({0, 0, 0, 1, 1, 0, 1, 2, 1, 2}, &value_offsets);

  auto children = std::vector<std::shared_ptr<Array>>(4);
  ArrayFromVector<StringType, std::string>({"abc", "def", "xyz"}, &children[0]);
  ArrayFromVector<UInt8Type>({10, 20, 30}, &children[1]);
  ArrayFromVector<DoubleType>({1.618, 2.718, 3.142}, &children[2]);
  ArrayFromVector<Int8Type>({-12}, &children[3]);

  std::vector<std::string> field_names = {"str", "int1", "real", "int2"};
  std::vector<uint8_t> type_codes = {1, 2, 4, 8};

  std::shared_ptr<Array> result;

  // without field names and type codes
  ASSERT_OK(UnionArray::MakeDense(*type_ids_, *value_offsets, children, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE,
                  {"0", "1", "2", "3"}, {0, 1, 2, 3});

  // with field name
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               {"one"}, &result));
  ASSERT_OK(
      UnionArray::MakeDense(*type_ids_, *value_offsets, children, field_names, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE, field_names,
                  {0, 1, 2, 3});

  // with type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               std::vector<uint8_t>{0}, &result));
  ASSERT_OK(
      UnionArray::MakeDense(*type_ids_, *value_offsets, children, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE,
                  {"0", "1", "2", "3"}, type_codes);

  // with field names and type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeDense(*type_ids_, *value_offsets, children,
                                               {"one"}, type_codes, &result));
  ASSERT_OK(UnionArray::MakeDense(*type_ids_, *value_offsets, children, field_names,
                                  type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::DENSE, field_names,
                  type_codes);
}

TEST_F(TestUnionArrayFactories, TestMakeSparse) {
  auto children = std::vector<std::shared_ptr<Array>>(4);
  ArrayFromVector<StringType, std::string>(
      {"abc", "", "", "def", "", "", "", "xyz", "", ""}, &children[0]);
  ArrayFromVector<UInt8Type>({0, 10, 0, 0, 20, 0, 0, 0, 0, 30}, &children[1]);
  ArrayFromVector<DoubleType>({0.0, 0.0, 1.618, 0.0, 0.0, 0.0, 2.718, 0.0, 3.142, 0.0},
                              &children[2]);
  ArrayFromVector<Int8Type>({0, 0, 0, 0, 0, -12, 0, 0, 0, 0}, &children[3]);

  std::vector<std::string> field_names = {"str", "int1", "real", "int2"};
  std::vector<uint8_t> type_codes = {1, 2, 4, 8};

  std::shared_ptr<Array> result;

  // without field names and type codes
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE,
                  {"0", "1", "2", "3"}, {0, 1, 2, 3});

  // with field names
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children, {"one"}, &result));
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, field_names, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE, field_names,
                  {0, 1, 2, 3});

  // with type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children,
                                                std::vector<uint8_t>{0}, &result));
  ASSERT_OK(UnionArray::MakeSparse(*type_ids_, children, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE,
                  {"0", "1", "2", "3"}, type_codes);

  // with field names and type codes
  ASSERT_RAISES(Invalid, UnionArray::MakeSparse(*type_ids_, children, {"one"}, type_codes,
                                                &result));
  ASSERT_OK(
      UnionArray::MakeSparse(*type_ids_, children, field_names, type_codes, &result));
  CheckUnionArray(checked_cast<UnionArray&>(*result), UnionMode::SPARSE, field_names,
                  type_codes);
}

}  // namespace arrow

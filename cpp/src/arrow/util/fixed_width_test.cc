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

// #include <cmath>
// #include <string>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/fixed_width_internal.h"

namespace arrow::util {

namespace {
bool NotBool(const DataType& type) { return type.id() != Type::BOOL; }
bool NotInt32(const DataType& type) { return type.id() != Type::INT32; }
}  // namespace

class TestFixedWidth : public ::testing::Test {
 protected:
  std::shared_ptr<Array> bool_array_array_;
  std::shared_ptr<Array> int_array_array_;
  std::shared_ptr<Array> fsl_bool_array_;
  std::shared_ptr<Array> fsl_int_array_;
  std::shared_ptr<Array> fsl_int_nulls_array_;
  std::shared_ptr<Array> fsl_int_inner_nulls_array_;
  std::shared_ptr<Array> dict_string_array_;

  std::shared_ptr<DataType> fsl(int32_t list_size,
                                const std::shared_ptr<DataType>& value_type) {
    return fixed_size_list(value_type, list_size);
  }

 public:
  void SetUp() override {
    bool_array_array_ = ArrayFromJSON(boolean(), "[true, false, null]");
    int_array_array_ = ArrayFromJSON(int32(), "[1, 0, null]");
    fsl_bool_array_ = ArrayFromJSON(fsl(2, boolean()), "[[true, false]]");
    fsl_int_array_ = ArrayFromJSON(fsl(2, int32()), "[[1, 0], [2, 3]]");
    fsl_int_nulls_array_ = ArrayFromJSON(fsl(2, int32()), "[[1, 0], null, [1, 2]]");
    fsl_int_inner_nulls_array_ =
        ArrayFromJSON(fsl(2, int32()), "[[1, 0], [2, 3], [null, 2]]");
    dict_string_array_ =
        ArrayFromJSON(dictionary(int32(), utf8()), R"(["Alice", "Bob", "Alice"])");
  }
};

TEST_F(TestFixedWidth, IsFixedWidth) {
  auto arr = ArraySpan{*bool_array_array_->data()};
  // force_null_count doesn't matter because nulls at the top-level
  // of the array are allowed by IsFixedWidthLike.
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/true));

  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false, NotInt32));
  ASSERT_FALSE(IsFixedWidthLike(arr, /*force_null_count=*/false, NotBool));

  arr = ArraySpan{*int_array_array_->data()};
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/true));
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false, NotBool));
}

TEST_F(TestFixedWidth, IsFixedWidthLike) {
  auto arr = ArraySpan{*fsl_bool_array_->data()};
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));

  arr = ArraySpan{*fsl_int_array_->data()};
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));
  arr.null_count = kUnknownNullCount;
  // force_null_count=true isn't necessary because nulls at the top-level
  // of the array are allowed by IsFixedWidthLike.
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));

  arr.child_data[0].null_count = kUnknownNullCount;
  // inner nulls are not allowed by IsFixedWidthLike...
  ASSERT_FALSE(IsFixedWidthLike(arr, /*force_null_count=*/false));
  // ...but forcing null counting at on every internal array increases
  // the chances of IsFixedWidthLike returning true.
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/true));
  // Excluding INT32 from the internal array checks.
  ASSERT_FALSE(IsFixedWidthLike(arr, /*force_null_count=*/true, NotInt32));

  arr = ArraySpan{*fsl_int_nulls_array_->data()};
  // Nulls at the top-level of the array are allowed by IsFixedWidthLike.
  //
  // TODO(GH-10157): ArrayFromJSON uses FixedSizeListBuilder which currently
  // produces nulls on the child data if one of the list-typed elements is null.
  // ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false));

  arr = ArraySpan{*fsl_int_inner_nulls_array_->data()};
  // Inner nulls are not allowed by IsFixedWidthLike.
  ASSERT_FALSE(IsFixedWidthLike(arr, /*force_null_count=*/true));

  arr = ArraySpan{*dict_string_array_->data()};
  // Dictionaries are considered fixed-width by is_fixed_width(), but excluded
  // by IsFixedWidthLike if exclude_bool_and_dictionary=true.
  ASSERT_TRUE(IsFixedWidthLike(arr));
  ASSERT_TRUE(IsFixedWidthLike(arr, /*force_null_count=*/false,
                               /*exclude_bool_and_dictionary=*/false));
  ASSERT_FALSE(IsFixedWidthLike(arr, /*force_null_count=*/false,
                                /*exclude_bool_and_dictionary=*/true));
}

TEST_F(TestFixedWidth, MeasureWidthInBytes) {
  auto b = boolean();
  auto i8 = int8();
  auto i32 = int32();
  auto fsb = fixed_size_binary(3);
  auto dict = dictionary(int32(), utf8());
  auto varlen = utf8();
  ASSERT_EQ(FixedWidthInBytes(*b), -1);
  ASSERT_EQ(FixedWidthInBytes(*i8), 1);
  ASSERT_EQ(FixedWidthInBytes(*i32), 4);
  ASSERT_EQ(FixedWidthInBytes(*fsb), 3);
  ASSERT_EQ(FixedWidthInBytes(*dict), 4);

  ASSERT_EQ(FixedWidthInBytes(*varlen), -1);
  ASSERT_EQ(FixedWidthInBytes(*varlen), -1);

  ASSERT_EQ(FixedWidthInBytes(*fsl(0, b)), -1);
  ASSERT_EQ(FixedWidthInBytes(*fsl(3, b)), -1);
  ASSERT_EQ(FixedWidthInBytes(*fsl(5, b)), -1);

  ASSERT_EQ(FixedWidthInBytes(*fsl(0, i8)), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(3, i8)), 3);
  ASSERT_EQ(FixedWidthInBytes(*fsl(5, i8)), 5);
  ASSERT_EQ(FixedWidthInBytes(*fsl(0, i32)), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(3, i32)), 3 * 4);
  ASSERT_EQ(FixedWidthInBytes(*fsl(5, i32)), 5 * 4);
  ASSERT_EQ(FixedWidthInBytes(*fsl(5, fsb)), 5 * 3);
  ASSERT_EQ(FixedWidthInBytes(*fsl(5, dict)), 5 * 4);

  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(0, i8))), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(3, i8))), 2 * 3);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(5, i8))), 2 * 5);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(0, i32))), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(3, i32))), 2 * 3 * 4);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(5, i32))), 2 * 5 * 4);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(0, fsb))), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(3, fsb))), 2 * 3 * 3);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(5, fsb))), 2 * 5 * 3);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(0, dict))), 0);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(3, dict))), 2 * 3 * 4);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, fsl(5, dict))), 2 * 5 * 4);

  ASSERT_EQ(FixedWidthInBytes(*fsl(0, varlen)), -1);
  ASSERT_EQ(FixedWidthInBytes(*fsl(2, varlen)), -1);
}

TEST_F(TestFixedWidth, MeasureWidthInBits) {
  auto b = boolean();
  auto i8 = int8();
  auto i32 = int32();
  auto fsb = fixed_size_binary(3);
  auto dict = dictionary(int32(), utf8());
  auto varlen = utf8();
  ASSERT_EQ(FixedWidthInBits(*b), 1);
  ASSERT_EQ(FixedWidthInBits(*i8), 8);
  ASSERT_EQ(FixedWidthInBits(*i32), 4 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsb), 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*dict), 4 * 8);

  ASSERT_EQ(FixedWidthInBits(*varlen), -1);
  ASSERT_EQ(FixedWidthInBits(*varlen), -1);

  ASSERT_EQ(FixedWidthInBits(*fsl(0, b)), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(3, b)), 3);
  ASSERT_EQ(FixedWidthInBits(*fsl(5, b)), 5);

  ASSERT_EQ(FixedWidthInBits(*fsl(0, i8)), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(3, i8)), 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(5, i8)), 5 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(0, i32)), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(3, i32)), 4 * 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(5, i32)), 4 * 5 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(5, fsb)), 5 * 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(5, dict)), 5 * 4 * 8);

  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(0, i8))), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(3, i8))), 2 * 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(5, i8))), 2 * 5 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(0, i32))), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(3, i32))), 2 * 3 * 4 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(5, i32))), 2 * 5 * 4 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(0, fsb))), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(3, fsb))), 2 * 3 * 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(5, fsb))), 2 * 5 * 3 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(0, dict))), 0);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(3, dict))), 2 * 3 * 4 * 8);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, fsl(5, dict))), 2 * 5 * 4 * 8);

  ASSERT_EQ(FixedWidthInBits(*fsl(0, varlen)), -1);
  ASSERT_EQ(FixedWidthInBits(*fsl(2, varlen)), -1);
}

}  // namespace arrow::util

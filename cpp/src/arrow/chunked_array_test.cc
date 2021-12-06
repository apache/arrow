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

#include "arrow/chunked_array.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/endian.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestChunkedArray : public TestBase {
 protected:
  virtual void Construct() {
    one_ = std::make_shared<ChunkedArray>(arrays_one_);
    if (!arrays_another_.empty()) {
      another_ = std::make_shared<ChunkedArray>(arrays_another_);
    }
  }

  ArrayVector arrays_one_;
  ArrayVector arrays_another_;

  std::shared_ptr<ChunkedArray> one_;
  std::shared_ptr<ChunkedArray> another_;
};

TEST_F(TestChunkedArray, Make) {
  ASSERT_RAISES(Invalid, ChunkedArray::Make({}));

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> result,
                       ChunkedArray::Make({}, int64()));
  AssertTypeEqual(*int64(), *result->type());
  ASSERT_EQ(result->num_chunks(), 0);

  auto chunk0 = ArrayFromJSON(int8(), "[0, 1, 2]");
  auto chunk1 = ArrayFromJSON(int16(), "[3, 4, 5]");

  ASSERT_OK_AND_ASSIGN(result, ChunkedArray::Make({chunk0, chunk0}));
  ASSERT_OK_AND_ASSIGN(auto result2, ChunkedArray::Make({chunk0, chunk0}, int8()));
  AssertChunkedEqual(*result, *result2);

  ASSERT_RAISES(Invalid, ChunkedArray::Make({chunk0, chunk1}));
  ASSERT_RAISES(Invalid, ChunkedArray::Make({chunk0}, int16()));
}

TEST_F(TestChunkedArray, MakeEmpty) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ChunkedArray> empty,
                       ChunkedArray::MakeEmpty(int64()));
  AssertTypeEqual(*int64(), *empty->type());
  ASSERT_OK(empty->ValidateFull());
  ASSERT_EQ(empty->length(), 0);
}

TEST_F(TestChunkedArray, BasicEquals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(one_));
  ASSERT_FALSE(one_->Equals(nullptr));
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingTypes) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data32(100, 1);
  std::vector<int64_t> data64(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data32, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int64Type, int64_t>(null_bitmap, data64, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingLengths) {
  std::vector<bool> null_bitmap100(100, true);
  std::vector<bool> null_bitmap101(101, true);
  std::vector<int32_t> data100(100, 1);
  std::vector<int32_t> data101(101, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap100, data100, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap101, data101, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));

  std::vector<bool> null_bitmap1(1, true);
  std::vector<int32_t> data1(1, 1);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap1, data1, &array);
  arrays_one_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingMetadata) {
  auto left_ty = list(field("item", int32()));

  auto metadata = key_value_metadata({"foo"}, {"bar"});
  auto right_ty = list(field("item", int32(), true, metadata));

  std::vector<std::shared_ptr<Array>> left_chunks = {ArrayFromJSON(left_ty, "[[]]")};
  std::vector<std::shared_ptr<Array>> right_chunks = {ArrayFromJSON(right_ty, "[[]]")};

  ChunkedArray left(left_chunks);
  ChunkedArray right(right_chunks);
  ASSERT_TRUE(left.Equals(right));
}

TEST_F(TestChunkedArray, SliceEquals) {
  arrays_one_.push_back(MakeRandomArray<Int32Array>(100));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  Construct();

  std::shared_ptr<ChunkedArray> slice = one_->Slice(125, 50);
  ASSERT_EQ(slice->length(), 50);
  AssertChunkedEqual(*one_->Slice(125, 50), *slice);

  std::shared_ptr<ChunkedArray> slice2 = one_->Slice(75)->Slice(25)->Slice(25, 50);
  ASSERT_EQ(slice2->length(), 50);
  AssertChunkedEqual(*slice, *slice2);

  // Making empty slices of a ChunkedArray
  std::shared_ptr<ChunkedArray> slice3 = one_->Slice(one_->length(), 99);
  ASSERT_EQ(slice3->length(), 0);
  ASSERT_EQ(slice3->num_chunks(), 1);
  ASSERT_TRUE(slice3->type()->Equals(one_->type()));

  std::shared_ptr<ChunkedArray> slice4 = one_->Slice(10, 0);
  ASSERT_EQ(slice4->length(), 0);
  ASSERT_EQ(slice4->num_chunks(), 1);
  ASSERT_TRUE(slice4->type()->Equals(one_->type()));

  // Slicing an empty ChunkedArray
  std::shared_ptr<ChunkedArray> slice5 = slice4->Slice(0, 10);
  ASSERT_EQ(slice5->length(), 0);
  ASSERT_EQ(slice5->num_chunks(), 1);
  ASSERT_TRUE(slice5->type()->Equals(one_->type()));
}

TEST_F(TestChunkedArray, ZeroChunksIssues) {
  ArrayVector empty = {};
  auto no_chunks = std::make_shared<ChunkedArray>(empty, int8());

  // ARROW-8911, assert that slicing is a no-op when there are zero-chunks
  auto sliced = no_chunks->Slice(0, 0);
  auto sliced2 = no_chunks->Slice(0, 5);
  AssertChunkedEqual(*no_chunks, *sliced);
  AssertChunkedEqual(*no_chunks, *sliced2);
}

TEST_F(TestChunkedArray, Validate) {
  // Valid if empty
  ArrayVector empty = {};
  auto no_chunks = std::make_shared<ChunkedArray>(empty, utf8());
  ASSERT_OK(no_chunks->ValidateFull());

  random::RandomArrayGenerator gen(0);
  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  arrays_one_.push_back(gen.String(50, 0, 10, 0.1));
  Construct();
  ASSERT_RAISES(Invalid, one_->ValidateFull());
}

TEST_F(TestChunkedArray, PrintDiff) {
  random::RandomArrayGenerator gen(0);
  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();

  auto other = one_->Slice(25);
  ASSERT_OK_AND_ASSIGN(auto diff, PrintArrayDiff(*one_, *other));
  ASSERT_EQ(*diff, "Expected length 50 but was actually 25");

  ASSERT_OK_AND_ASSIGN(diff, PrintArrayDiff(*other, *one_));
  ASSERT_EQ(*diff, "Expected length 25 but was actually 50");
}

TEST_F(TestChunkedArray, View) {
  auto in_ty = int32();
  auto out_ty = fixed_size_binary(4);
#if ARROW_LITTLE_ENDIAN
  auto arr = ArrayFromJSON(in_ty, "[2020568934, 2054316386, null]");
  auto arr2 = ArrayFromJSON(in_ty, "[2020568934, 2054316386]");
#else
  auto arr = ArrayFromJSON(in_ty, "[1718579064, 1650553466, null]");
  auto arr2 = ArrayFromJSON(in_ty, "[1718579064, 1650553466]");
#endif
  auto ex = ArrayFromJSON(out_ty, R"(["foox", "barz", null])");
  auto ex2 = ArrayFromJSON(out_ty, R"(["foox", "barz"])");

  ArrayVector chunks = {arr, arr2};
  ArrayVector ex_chunks = {ex, ex2};
  auto carr = std::make_shared<ChunkedArray>(chunks);
  auto expected = std::make_shared<ChunkedArray>(ex_chunks);

  ASSERT_OK_AND_ASSIGN(auto result, carr->View(out_ty));
  AssertChunkedEqual(*expected, *result);

  // Zero length
  ArrayVector empty = {};
  carr = std::make_shared<ChunkedArray>(empty, in_ty);
  expected = std::make_shared<ChunkedArray>(empty, out_ty);
  ASSERT_OK_AND_ASSIGN(result, carr->View(out_ty));
  AssertChunkedEqual(*expected, *result);
}

TEST_F(TestChunkedArray, GetScalar) {
  auto ty = int32();
  ArrayVector chunks{ArrayFromJSON(ty, "[6, 7, null]"), ArrayFromJSON(ty, "[]"),
                     ArrayFromJSON(ty, "[null]"), ArrayFromJSON(ty, "[3, 4, 5]")};
  ChunkedArray carr(chunks);

  auto check_scalar = [](const ChunkedArray& array, int64_t index,
                         const Scalar& expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, array.GetScalar(index));
    AssertScalarsEqual(expected, *actual, /*verbose=*/true);
  };

  check_scalar(carr, 0, **MakeScalar(ty, 6));
  check_scalar(carr, 2, *MakeNullScalar(ty));
  check_scalar(carr, 3, *MakeNullScalar(ty));
  check_scalar(carr, 4, **MakeScalar(ty, 3));
  check_scalar(carr, 6, **MakeScalar(ty, 5));

  ASSERT_RAISES(Invalid, carr.GetScalar(7));
}

}  // namespace arrow

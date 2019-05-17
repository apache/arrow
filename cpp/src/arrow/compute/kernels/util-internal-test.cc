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
#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/compute/test-util.h"

namespace arrow {
namespace compute {
namespace detail {

using ::testing::_;
using ::testing::AllOf;
using ::testing::Each;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::IsNull;
using ::testing::Ne;
using ::testing::NotNull;
using ::testing::Return;

TEST(PropagateNulls, UnknownNullCountWithNullsZeroCopies) {
  ArrayData input(boolean(), /*length=*/16, kUnknownNullCount);
  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> nulls = std::make_shared<Buffer>(validity_bitmap, 8);
  input.buffers.push_back(nulls);
  FunctionContext ctx(default_memory_pool());
  ArrayData output;

  ASSERT_OK(PropagateNulls(&ctx, input, &output));

  ASSERT_THAT(output.buffers, ElementsAre(Eq(nulls)));
  ASSERT_THAT(output.null_count, 9);
}

TEST(PropagateNulls, UnknownNullCountWithoutNullsLeavesNullptr) {
  ArrayData input(boolean(), /*length=*/16, kUnknownNullCount);
  constexpr uint8_t validity_bitmap[8] = {255, 255, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> nulls = std::make_shared<Buffer>(validity_bitmap, 8);
  input.buffers.push_back(nulls);
  FunctionContext ctx(default_memory_pool());
  ArrayData output;

  ASSERT_OK(PropagateNulls(&ctx, input, &output));

  EXPECT_THAT(output.null_count, Eq(0));
  EXPECT_THAT(output.buffers, ElementsAre(IsNull())) << output.buffers[0]->data()[0];
}

TEST(PropagateNulls, OffsetAndHasNulls) {
  ArrayData input(boolean(), /*length=*/16, kUnknownNullCount,  // slice off the first 8
                  /*offset=*/7);
  constexpr uint8_t validity_bitmap[8] = {0, 1, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> nulls = std::make_shared<Buffer>(validity_bitmap, 8);
  input.buffers.push_back(nulls);
  FunctionContext ctx(default_memory_pool());
  ArrayData output;

  ASSERT_OK(PropagateNulls(&ctx, input, &output));

  // Copy is made.
  EXPECT_THAT(output.null_count, Eq(15));
  ASSERT_THAT(output.buffers, ElementsAre(AllOf(Ne(nulls), NotNull())));
  const auto& output_buffer = *output.buffers[0];
  // the slice shifts the bit one over
  ASSERT_THAT(std::vector<uint8_t>(output_buffer.data(),
                                   output_buffer.data() + output_buffer.size()),
              ElementsAreArray({2, 0}));
  ASSERT_THAT(std::vector<uint8_t>(output_buffer.data() + output_buffer.size(),
                                   output_buffer.data() + output_buffer.capacity()),
              Each(0));
}

TEST(AssignNullIntersection, ZeroCopyWhenZeroNullsOnOneInput) {
  ArrayData some_nulls(boolean(), /* length= */ 16, kUnknownNullCount);
  constexpr uint8_t validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> nulls = std::make_shared<Buffer>(validity_bitmap, 8);
  some_nulls.buffers.push_back(nulls);

  ArrayData no_nulls(boolean(), /* length= */ 16, /*null_count=*/0);

  FunctionContext ctx(default_memory_pool());
  ArrayData output;
  output.length = 16;

  ASSERT_OK(AssignNullIntersection(&ctx, some_nulls, no_nulls, &output));
  ASSERT_THAT(output.buffers, ElementsAre(Eq(nulls)));
  ASSERT_THAT(output.null_count, 9);

  output.buffers[0] = nullptr;
  ASSERT_OK(AssignNullIntersection(&ctx, no_nulls, some_nulls, &output));
  ASSERT_THAT(output.buffers, ElementsAre(Eq(nulls)));
  ASSERT_THAT(output.null_count, 9);
}

TEST(AssignNullIntersection, IntersectsNullsWhenSomeOnBoth) {
  ArrayData left(boolean(), /* length= */ 16, kUnknownNullCount);
  constexpr uint8_t left_validity_bitmap[8] = {254, 0, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> left_nulls = std::make_shared<Buffer>(left_validity_bitmap, 8);
  left.buffers.push_back(left_nulls);

  ArrayData right(boolean(), /* length= */ 16, kUnknownNullCount);
  constexpr uint8_t right_validity_bitmap[8] = {127, 0, 0, 0, 0, 0, 0, 0};
  std::shared_ptr<Buffer> right_nulls =
      std::make_shared<Buffer>(right_validity_bitmap, 8);
  right.buffers.push_back(right_nulls);

  FunctionContext ctx(default_memory_pool());
  ArrayData output;
  output.length = 16;

  ASSERT_OK(AssignNullIntersection(&ctx, left, right, &output));

  EXPECT_THAT(output.null_count, 10);
  ASSERT_THAT(output.buffers, ElementsAre(NotNull()));
  const auto& output_buffer = *output.buffers[0];
  EXPECT_THAT(std::vector<uint8_t>(output_buffer.data(),
                                   output_buffer.data() + output_buffer.size()),
              ElementsAreArray({126, 0}));
  EXPECT_THAT(std::vector<uint8_t>(output_buffer.data() + output_buffer.size(),
                                   output_buffer.data() + output_buffer.capacity()),
              Each(0));
}

TEST(PrimitiveAllocatingUnaryKernel, BooleanFunction) {
  MockUnaryKernel mock;
  EXPECT_CALL(mock, out_type()).WillRepeatedly(Return(boolean()));
  EXPECT_CALL(mock, Call(_, _, _)).WillOnce(Return(Status::OK()));
  PrimitiveAllocatingUnaryKernel kernel(&mock);

  auto input =
      std::make_shared<ArrayData>(boolean(), /* length= */ 16, kUnknownNullCount);
  FunctionContext ctx(default_memory_pool());
  Datum output;
  output.value = ArrayData::Make(kernel.out_type(), input->length);
  ASSERT_OK(kernel.Call(&ctx, input, &output));

  ASSERT_THAT(output.array()->buffers, ElementsAre(IsNull(), NotNull()));
  auto value_buffer = output.array()->buffers[1];
  EXPECT_THAT(value_buffer->size(), Eq(2));
  EXPECT_THAT(value_buffer->capacity(), Ge(2));
  // Booleans should have this always zeroed out.
  EXPECT_THAT(*(value_buffer->data() + value_buffer->size() - 1), Eq(0));
}

TEST(PrimitiveAllocatingUnaryKernel, NonBoolean) {
  MockUnaryKernel mock;
  EXPECT_CALL(mock, out_type()).WillRepeatedly(Return(int32()));
  EXPECT_CALL(mock, Call(_, _, _)).WillOnce(Return(Status::OK()));
  PrimitiveAllocatingUnaryKernel kernel(&mock);

  auto input =
      std::make_shared<ArrayData>(boolean(), /* length= */ 16, kUnknownNullCount);
  FunctionContext ctx(default_memory_pool());
  Datum output;
  output.value = ArrayData::Make(kernel.out_type(), input->length);
  ASSERT_OK(kernel.Call(&ctx, input, &output));

  ASSERT_THAT(output.array()->buffers, ElementsAre(IsNull(), NotNull()));
  auto value_buffer = output.array()->buffers[1];
  EXPECT_THAT(value_buffer->size(), Eq(64));
  EXPECT_THAT(value_buffer->capacity(), Ge(64));
}

TEST(PrimitiveAllocatingBinaryKernel, BooleanFunction) {
  MockBinaryKernel mock;
  EXPECT_CALL(mock, out_type()).WillRepeatedly(Return(boolean()));
  EXPECT_CALL(mock, Call(_, _, _, _)).WillOnce(Return(Status::OK()));
  PrimitiveAllocatingBinaryKernel kernel(&mock);

  auto input =
      std::make_shared<ArrayData>(boolean(), /* length= */ 16, kUnknownNullCount);
  FunctionContext ctx(default_memory_pool());
  Datum output;
  output.value = ArrayData::Make(kernel.out_type(), input->length);
  ASSERT_OK(kernel.Call(&ctx, input, input, &output));

  ASSERT_THAT(output.array()->buffers, ElementsAre(IsNull(), NotNull()));
  auto value_buffer = output.array()->buffers[1];
  EXPECT_THAT(value_buffer->size(), Eq(2));
  EXPECT_THAT(value_buffer->capacity(), Ge(2));
  // Booleans should have this always zeroed out.
  EXPECT_THAT(*(value_buffer->data() + value_buffer->size() - 1), Eq(0));
}

TEST(PrimitiveAllocatingBinaryKernel, NonBoolean) {
  MockBinaryKernel mock;
  EXPECT_CALL(mock, out_type()).WillRepeatedly(Return(int32()));
  EXPECT_CALL(mock, Call(_, _, _, _)).WillOnce(Return(Status::OK()));
  PrimitiveAllocatingBinaryKernel kernel(&mock);

  auto input =
      std::make_shared<ArrayData>(boolean(), /* length= */ 16, kUnknownNullCount);
  FunctionContext ctx(default_memory_pool());
  Datum output;
  output.value = ArrayData::Make(kernel.out_type(), input->length);
  ASSERT_OK(kernel.Call(&ctx, input, input, &output));

  ASSERT_THAT(output.array()->buffers, ElementsAre(IsNull(), NotNull()));
  auto value_buffer = output.array()->buffers[1];
  EXPECT_THAT(value_buffer->size(), Eq(64));
  EXPECT_THAT(value_buffer->capacity(), Ge(64));
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow

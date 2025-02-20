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

#include <gtest/gtest.h>

#include "arrow/testing/generator.h"

namespace arrow::gen {

template <typename CType>
void CheckStep(const Array& result, CType start, CType step, int64_t length) {
  using ArrowType = typename CTypeTraits<CType>::ArrowType;

  ASSERT_OK(result.ValidateFull());
  ASSERT_EQ(result.type_id(), TypeTraits<ArrowType>::type_singleton()->id());
  ASSERT_EQ(result.length(), length);
  ASSERT_EQ(result.null_bitmap(), nullptr);
  auto data = result.data()->GetValues<CType>(1);
  CType current = start;
  for (int64_t i = 0; i < length; ++i) {
    ASSERT_EQ(data[i], current);
    current += step;
  }
}

TEST(StepTest, Default) {
  for (auto length : {0, 1, 1024}) {
    ARROW_SCOPED_TRACE("length=" + std::to_string(length));
    ASSERT_OK_AND_ASSIGN(auto array, Step()->Generate(length));
    CheckStep<uint32_t>(*array, 0, 1, length);
  }
}

using NumericCTypes = ::testing::Types<int8_t, uint8_t, int16_t, uint16_t, int32_t,
                                       uint32_t, int64_t, uint64_t, float, double>;

template <typename CType>
class TypedStepTest : public ::testing::Test {};

TYPED_TEST_SUITE(TypedStepTest, NumericCTypes);

TYPED_TEST(TypedStepTest, Basic) {
  for (auto length : {0, 1, 1024}) {
    ARROW_SCOPED_TRACE("length=" + std::to_string(length));
    for (TypeParam start :
         {std::numeric_limits<TypeParam>::min(), static_cast<TypeParam>(0)}) {
      ARROW_SCOPED_TRACE("start=" + std::to_string(start));
      for (TypeParam step :
           {static_cast<TypeParam>(0), std::numeric_limits<TypeParam>::epsilon(),
            static_cast<TypeParam>(std::numeric_limits<TypeParam>::max() /
                                   (length + 1))}) {
        ARROW_SCOPED_TRACE("step=" + std::to_string(step));
        ASSERT_OK_AND_ASSIGN(auto array, Step(start, step)->Generate(length));
        CheckStep(*array, start, step, length);
      }
    }
  }
}

}  // namespace arrow::gen

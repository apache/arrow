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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"

namespace arrow {

TEST(TestNullScalar, Basics) {
  NullScalar scalar;
  ASSERT_FALSE(scalar.is_valid);
  ASSERT_TRUE(scalar.type->Equals(*null()));
}

template <typename T>
class TestNumericScalar : public ::testing::Test {
 public:
  TestNumericScalar() {}
};

TYPED_TEST_CASE(TestNumericScalar, NumericArrowTypes);

TYPED_TEST(TestNumericScalar, Basics) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;

  T value = static_cast<T>(1);

  auto scalar_val = std::make_shared<ScalarType>(value);
  ASSERT_EQ(value, scalar_val->value);
  ASSERT_TRUE(scalar_val->is_valid);

  auto expected_type = TypeTraits<TypeParam>::type_singleton();
  ASSERT_TRUE(scalar_val->type->Equals(*expected_type));

  T other_value = static_cast<T>(2);
  scalar_val->value = other_value;
  ASSERT_EQ(other_value, scalar_val->value);

  ScalarType stack_val = ScalarType(0, false);
  ASSERT_FALSE(stack_val.is_valid);
}

TEST(TestBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  BinaryScalar value(buf);
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*binary()));

  BinaryScalar null_value(nullptr, false);
  ASSERT_FALSE(null_value.is_valid);

  StringScalar value2(buf);
  ASSERT_TRUE(value2.value->Equals(*buf));
  ASSERT_TRUE(value2.is_valid);
  ASSERT_TRUE(value2.type->Equals(*utf8()));

  StringScalar null_value2(nullptr, false);
  ASSERT_FALSE(null_value2.is_valid);
}

TEST(TestFixedSizeBinaryScalar, Basics) {
  std::string data = "test data";
  auto buf = std::make_shared<Buffer>(data);

  auto ex_type = fixed_size_binary(9);

  FixedSizeBinaryScalar value(buf, ex_type);
  ASSERT_TRUE(value.value->Equals(*buf));
  ASSERT_TRUE(value.is_valid);
  ASSERT_TRUE(value.type->Equals(*ex_type));
}

// TODO test HalfFloatScalar

}  // namespace arrow

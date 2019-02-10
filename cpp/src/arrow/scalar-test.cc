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
#include "arrow/test-util.h"
#include "arrow/type_traits.h"

namespace arrow {

TEST(TestNullScalar, Basics) {
  NullScalar
}

using NumericTypes = ::testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type,
                                      Int8Type, Int16Type, Int32Type, Int64Type,
                                      FloatScalar, DoubleScalar>;

TYPED_TEST_CASE(TestNumericScalar, NumericTypes);

TYPED_TEST(TestNumericScalar, Basics) {
  using TemplateType = typename TestFixture::T;
  using T = typename TemplateType::c_type;
  using ScalarType = typename TypeTraits<TemplateType>::ScalarType;

  T value = static_cast<T>(1);

  auto scalar_val = std::make_shared<ScalarType>(value);
  ASSERT_EQ(value, scalar_val->value);
  ASSERT_TRUE(scalar_val->is_valid);

  auto expected_type = TypeTraits<TemplateType>::type_singleton();
  ASSERT_TRUE(scalar_val->type->Equals(*expected_type));

  T other_value = static_cast<T>(2);
  scalar_val->value = other_value;
  ASSERT_EQ(other_value, scalar_val->value);

  ScalarType stack_val = ScalarType(0, false);
  ASSERT_FALSE(stack_val.is_valid);
}

// TODO test HalfFloatScalar

}  // namespace arrow

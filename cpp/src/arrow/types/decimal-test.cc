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

#include "gtest/gtest.h"

#include "arrow/types/decimal.h"

namespace arrow {

TEST(TypesTest, TestDecimalType) {
  DecimalType t1(8, 4);

  ASSERT_EQ(t1.type, Type::DECIMAL);
  ASSERT_EQ(t1.precision, 8);
  ASSERT_EQ(t1.scale, 4);

  ASSERT_EQ(t1.ToString(), std::string("decimal(8, 4)"));

  // Test copy constructor
  DecimalType t2 = t1;
  ASSERT_EQ(t2.type, Type::DECIMAL);
  ASSERT_EQ(t2.precision, 8);
  ASSERT_EQ(t2.scale, 4);
}

}  // namespace arrow

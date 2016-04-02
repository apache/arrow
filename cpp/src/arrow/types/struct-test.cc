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
#include <vector>

#include "gtest/gtest.h"

#include "arrow/type.h"

using std::shared_ptr;
using std::string;
using std::vector;

namespace arrow {

TEST(TestStructType, Basics) {
  TypePtr f0_type = TypePtr(new Int32Type());
  auto f0 = std::make_shared<Field>("f0", f0_type);

  TypePtr f1_type = TypePtr(new StringType());
  auto f1 = std::make_shared<Field>("f1", f1_type);

  TypePtr f2_type = TypePtr(new UInt8Type());
  auto f2 = std::make_shared<Field>("f2", f2_type);

  vector<shared_ptr<Field>> fields = {f0, f1, f2};

  StructType struct_type(fields);

  ASSERT_TRUE(struct_type.child(0)->Equals(f0));
  ASSERT_TRUE(struct_type.child(1)->Equals(f1));
  ASSERT_TRUE(struct_type.child(2)->Equals(f2));

  ASSERT_EQ(struct_type.ToString(), "struct<f0: int32, f1: string, f2: uint8>");

  // TODO(wesm): out of bounds for field(...)
}

}  // namespace arrow

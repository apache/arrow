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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/column.h"
#include "arrow/schema.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

const auto INT32 = std::make_shared<Int32Type>();

class TestColumn : public TestBase {
 protected:
  std::shared_ptr<ChunkedArray> data_;
  std::unique_ptr<Column> column_;
};

TEST_F(TestColumn, BasicAPI) {
  ArrayVector arrays;
  arrays.push_back(MakePrimitive<Int32Array>(100));
  arrays.push_back(MakePrimitive<Int32Array>(100, 10));
  arrays.push_back(MakePrimitive<Int32Array>(100, 20));

  auto field = std::make_shared<Field>("c0", INT32);
  column_.reset(new Column(field, arrays));

  ASSERT_EQ("c0", column_->name());
  ASSERT_TRUE(column_->type()->Equals(INT32));
  ASSERT_EQ(300, column_->length());
  ASSERT_EQ(30, column_->null_count());
  ASSERT_EQ(3, column_->data()->num_chunks());
}

TEST_F(TestColumn, ChunksInhomogeneous) {
  ArrayVector arrays;
  arrays.push_back(MakePrimitive<Int32Array>(100));
  arrays.push_back(MakePrimitive<Int32Array>(100, 10));

  auto field = std::make_shared<Field>("c0", INT32);
  column_.reset(new Column(field, arrays));

  ASSERT_OK(column_->ValidateData());

  arrays.push_back(MakePrimitive<Int16Array>(100, 10));
  column_.reset(new Column(field, arrays));
  ASSERT_RAISES(Invalid, column_->ValidateData());
}

}  // namespace arrow

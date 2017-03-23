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
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

class TestChunkedArray : public TestBase {
 protected:
  virtual void Construct() {
    one_ = std::make_shared<ChunkedArray>(arrays_one_);
    another_ = std::make_shared<ChunkedArray>(arrays_another_);
  }

  ArrayVector arrays_one_;
  ArrayVector arrays_another_;

  std::shared_ptr<ChunkedArray> one_;
  std::shared_ptr<ChunkedArray> another_;
};

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

class TestColumn : public TestChunkedArray {
 protected:
  void Construct() override {
    TestChunkedArray::Construct();

    one_col_ = std::make_shared<Column>(one_field_, one_);
    another_col_ = std::make_shared<Column>(another_field_, another_);
  }

  std::shared_ptr<ChunkedArray> data_;
  std::unique_ptr<Column> column_;

  std::shared_ptr<Field> one_field_;
  std::shared_ptr<Field> another_field_;

  std::shared_ptr<Column> one_col_;
  std::shared_ptr<Column> another_col_;
};

TEST_F(TestColumn, BasicAPI) {
  ArrayVector arrays;
  arrays.push_back(MakePrimitive<Int32Array>(100));
  arrays.push_back(MakePrimitive<Int32Array>(100, 10));
  arrays.push_back(MakePrimitive<Int32Array>(100, 20));

  auto field = std::make_shared<Field>("c0", int32());
  column_.reset(new Column(field, arrays));

  ASSERT_EQ("c0", column_->name());
  ASSERT_TRUE(column_->type()->Equals(int32()));
  ASSERT_EQ(300, column_->length());
  ASSERT_EQ(30, column_->null_count());
  ASSERT_EQ(3, column_->data()->num_chunks());

  // nullptr array should not break
  column_.reset(new Column(field, std::shared_ptr<Array>(nullptr)));
  ASSERT_NE(column_.get(), nullptr);
}

TEST_F(TestColumn, ChunksInhomogeneous) {
  ArrayVector arrays;
  arrays.push_back(MakePrimitive<Int32Array>(100));
  arrays.push_back(MakePrimitive<Int32Array>(100, 10));

  auto field = std::make_shared<Field>("c0", int32());
  column_.reset(new Column(field, arrays));

  ASSERT_OK(column_->ValidateData());

  arrays.push_back(MakePrimitive<Int16Array>(100, 10));
  column_.reset(new Column(field, arrays));
  ASSERT_RAISES(Invalid, column_->ValidateData());
}

TEST_F(TestColumn, Equals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  one_field_ = std::make_shared<Field>("column", int32());
  another_field_ = std::make_shared<Field>("column", int32());

  Construct();
  ASSERT_TRUE(one_col_->Equals(one_col_));
  ASSERT_FALSE(one_col_->Equals(nullptr));
  ASSERT_TRUE(one_col_->Equals(another_col_));
  ASSERT_TRUE(one_col_->Equals(*another_col_.get()));

  // Field is different
  another_field_ = std::make_shared<Field>("two", int32());
  Construct();
  ASSERT_FALSE(one_col_->Equals(another_col_));
  ASSERT_FALSE(one_col_->Equals(*another_col_.get()));

  // ChunkedArray is different
  another_field_ = std::make_shared<Field>("column", int32());
  arrays_another_.push_back(array);
  Construct();
  ASSERT_FALSE(one_col_->Equals(another_col_));
  ASSERT_FALSE(one_col_->Equals(*another_col_.get()));
}

}  // namespace arrow

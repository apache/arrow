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
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/field.h"
#include "arrow/schema.h"
#include "arrow/table/column.h"
#include "arrow/type.h"

using std::shared_ptr;
using std::vector;

namespace arrow {

class TestColumn : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = GetDefaultMemoryPool();
  }

  std::shared_ptr<Array> MakeInt32Array(int32_t length, int32_t null_count = 0) {

  }

 protected:
  MemoryPool* pool_;

  std::shared_ptr<ChunkedArray> data_;
  std::unique_ptr<Column> column_;
};

TEST_F(TestColumn, BasicAPI) {
  ArrayVector arrays;

  arrays.push_back(MakeInt32Array(100));
  arrays.push_back(MakeInt32Array(100, 10));
  arrays.push_back(MakeInt32Array(100, 20));

  auto field = std::make_shared<Field>("c0",

  column_.reset(new Column(arrays)
}

TEST_F(TestColumn, ChunksInhomogeneous) {

}

} // namespace arrow

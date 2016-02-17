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

#ifndef ARROW_TYPES_TEST_COMMON_H
#define ARROW_TYPES_TEST_COMMON_H

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "arrow/test-util.h"
#include "arrow/type.h"

using std::unique_ptr;

namespace arrow {

class TestBuilder : public ::testing::Test {
 public:
  void SetUp() {
    type_ = TypePtr(new UInt8Type());
    type_nn_ = TypePtr(new UInt8Type(false));
    builder_.reset(new UInt8Builder(type_));
    builder_nn_.reset(new UInt8Builder(type_nn_));
  }
 protected:
  TypePtr type_;
  TypePtr type_nn_;
  unique_ptr<ArrayBuilder> builder_;
  unique_ptr<ArrayBuilder> builder_nn_;
};

} // namespace arrow

#endif // ARROW_TYPES_TEST_COMMON_H

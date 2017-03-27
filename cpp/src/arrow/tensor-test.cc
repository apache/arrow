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

// Unit tests for DataType (and subclasses), Field, and Schema

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/buffer.h"
#include "arrow/tensor.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

namespace arrow {

TEST(TestTensor, ZeroDim) {
  const int64_t values = 1;
  std::vector<int64_t> shape = {};

  using T = int64_t;

  std::shared_ptr<MutableBuffer> buffer;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), values * sizeof(T), &buffer));

  Int64Tensor t0(buffer, shape);

  ASSERT_EQ(1, t0.size());
}

TEST(TestTensor, BasicCtors) {
  const int64_t values = 24;
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};

  using T = int64_t;

  std::shared_ptr<MutableBuffer> buffer;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), values * sizeof(T), &buffer));

  Int64Tensor t1(buffer, shape);
  Int64Tensor t2(buffer, shape, strides);
  Int64Tensor t3(buffer, shape, strides, dim_names);

  ASSERT_EQ(24, t1.size());
  ASSERT_TRUE(t1.is_mutable());
  ASSERT_FALSE(t1.has_dim_names());

  ASSERT_EQ(strides, t1.strides());
  ASSERT_EQ(strides, t2.strides());

  ASSERT_EQ("foo", t3.dim_name(0));
  ASSERT_EQ("bar", t3.dim_name(1));
}

}  // namespace arrow

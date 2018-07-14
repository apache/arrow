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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/tensor.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

namespace arrow {

TEST(TestTensor, ZeroDim) {
  const int64_t values = 1;
  std::vector<int64_t> shape = {};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  Tensor t0(int64(), buffer, shape);

  ASSERT_EQ(1, t0.size());
}

TEST(TestTensor, BasicCtors) {
  const int64_t values = 24;
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};
  std::vector<std::string> dim_names = {"foo", "bar"};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  Tensor t1(int64(), buffer, shape);
  Tensor t2(int64(), buffer, shape, strides);
  Tensor t3(int64(), buffer, shape, strides, dim_names);

  ASSERT_EQ(24, t1.size());
  ASSERT_TRUE(t1.is_mutable());

  ASSERT_EQ(strides, t1.strides());
  ASSERT_EQ(strides, t2.strides());

  ASSERT_EQ("foo", t3.dim_name(0));
  ASSERT_EQ("bar", t3.dim_name(1));
  ASSERT_EQ("", t1.dim_name(0));
  ASSERT_EQ("", t1.dim_name(1));
}

TEST(TestTensor, IsContiguous) {
  const int64_t values = 24;
  std::vector<int64_t> shape = {4, 6};
  std::vector<int64_t> strides = {48, 8};

  using T = int64_t;

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(values * sizeof(T), &buffer));

  std::vector<int64_t> c_strides = {48, 8};
  std::vector<int64_t> f_strides = {8, 32};
  std::vector<int64_t> noncontig_strides = {8, 8};
  Tensor t1(int64(), buffer, shape, c_strides);
  Tensor t2(int64(), buffer, shape, f_strides);
  Tensor t3(int64(), buffer, shape, noncontig_strides);

  ASSERT_TRUE(t1.is_contiguous());
  ASSERT_TRUE(t2.is_contiguous());
  ASSERT_FALSE(t3.is_contiguous());
}

TEST(TestTensor, ZeroDimensionalTensor) {
  std::vector<int64_t> shape = {0};

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(0, &buffer));

  Tensor t(int64(), buffer, shape);
  ASSERT_EQ(t.strides().size(), 1);
}

}  // namespace arrow

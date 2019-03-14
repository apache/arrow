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
#include <functional>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/boolean.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/compute/test-util.h"

using std::shared_ptr;
using std::vector;

namespace arrow {
namespace compute {

using BinaryKernelFunc =
    std::function<Status(FunctionContext*, const Datum&, const Datum&, Datum* out)>;

class TestBooleanKernel : public ComputeFixture, public TestBase {
 public:
  void TestArrayBinary(const BinaryKernelFunc& kernel, const std::shared_ptr<Array>& left,
                       const std::shared_ptr<Array>& right,
                       const std::shared_ptr<Array>& expected) {
    Datum result;
    ASSERT_OK(kernel(&this->ctx_, left, right, &result));
    ASSERT_EQ(Datum::ARRAY, result.kind());
    std::shared_ptr<Array> result_array = result.make_array();
    ASSERT_ARRAYS_EQUAL(*expected, *result_array);
  }

  void TestChunkedArrayBinary(const BinaryKernelFunc& kernel,
                              const std::shared_ptr<ChunkedArray>& left,
                              const std::shared_ptr<ChunkedArray>& right,
                              const std::shared_ptr<ChunkedArray>& expected) {
    Datum result;
    std::shared_ptr<Array> result_array;
    ASSERT_OK(kernel(&this->ctx_, left, right, &result));
    ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
    std::shared_ptr<ChunkedArray> result_ca = result.chunked_array();
    ASSERT_TRUE(result_ca->Equals(expected));
  }

  void TestBinaryKernel(const BinaryKernelFunc& kernel, const std::vector<bool>& values1,
                        const std::vector<bool>& values2,
                        const std::vector<bool>& values3,
                        const std::vector<bool>& values3_nulls) {
    auto type = boolean();
    auto a1 = _MakeArray<BooleanType, bool>(type, values1, {});
    auto a2 = _MakeArray<BooleanType, bool>(type, values2, {});
    auto a3 = _MakeArray<BooleanType, bool>(type, values3, {});
    auto a1_nulls = _MakeArray<BooleanType, bool>(type, values1, values1);
    auto a2_nulls = _MakeArray<BooleanType, bool>(type, values2, values2);
    auto a3_nulls = _MakeArray<BooleanType, bool>(type, values3, values3_nulls);

    TestArrayBinary(kernel, a1, a2, a3);
    TestArrayBinary(kernel, a1_nulls, a2_nulls, a3_nulls);
    TestArrayBinary(kernel, a1->Slice(1), a2->Slice(1), a3->Slice(1));
    TestArrayBinary(kernel, a1_nulls->Slice(1), a2_nulls->Slice(1), a3_nulls->Slice(1));

    // ChunkedArray
    std::vector<std::shared_ptr<Array>> ca1_arrs = {a1, a1->Slice(1)};
    auto ca1 = std::make_shared<ChunkedArray>(ca1_arrs);
    std::vector<std::shared_ptr<Array>> ca2_arrs = {a2, a2->Slice(1)};
    auto ca2 = std::make_shared<ChunkedArray>(ca2_arrs);
    std::vector<std::shared_ptr<Array>> ca3_arrs = {a3, a3->Slice(1)};
    auto ca3 = std::make_shared<ChunkedArray>(ca3_arrs);
    TestChunkedArrayBinary(kernel, ca1, ca2, ca3);

    // ChunkedArray with different chunks
    std::vector<std::shared_ptr<Array>> ca4_arrs = {a1->Slice(0, 1), a1->Slice(1),
                                                    a1->Slice(1, 1), a1->Slice(2)};
    auto ca4 = std::make_shared<ChunkedArray>(ca4_arrs);
    TestChunkedArrayBinary(kernel, ca4, ca2, ca3);
  }
};

TEST_F(TestBooleanKernel, Invert) {
  vector<bool> values1 = {true, false, true, false};
  vector<bool> values2 = {false, true, false, true};

  auto type = boolean();
  auto a1 = _MakeArray<BooleanType, bool>(type, values1, {true, true, true, false});
  auto a2 = _MakeArray<BooleanType, bool>(type, values2, {true, true, true, false});

  // Plain array
  Datum result;
  ASSERT_OK(Invert(&this->ctx_, a1, &result));
  ASSERT_EQ(Datum::ARRAY, result.kind());
  ASSERT_ARRAYS_EQUAL(*a2, *result.make_array());

  // Array with offset
  ASSERT_OK(Invert(&this->ctx_, a1->Slice(1), &result));
  ASSERT_EQ(Datum::ARRAY, result.kind());
  ASSERT_ARRAYS_EQUAL(*a2->Slice(1), *result.make_array());

  // ChunkedArray
  std::vector<std::shared_ptr<Array>> ca1_arrs = {a1, a1->Slice(1)};
  auto ca1 = std::make_shared<ChunkedArray>(ca1_arrs);
  std::vector<std::shared_ptr<Array>> ca2_arrs = {a2, a2->Slice(1)};
  auto ca2 = std::make_shared<ChunkedArray>(ca2_arrs);
  ASSERT_OK(Invert(&this->ctx_, ca1, &result));
  ASSERT_EQ(Datum::CHUNKED_ARRAY, result.kind());
  std::shared_ptr<ChunkedArray> result_ca = result.chunked_array();
  ASSERT_ARRAYS_EQUAL(*ca2, *result_ca);
}

TEST_F(TestBooleanKernel, InvertEmptyArray) {
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value = ArrayData::Make(boolean(), 0 /* length */, std::move(data_buffers),
                                0 /* null_count */);

  Datum result;
  ASSERT_OK(Invert(&this->ctx_, input, &result));
  ASSERT_ARRAYS_EQUAL(*input.make_array(), *result.make_array());
}

TEST_F(TestBooleanKernel, BinaryOpOnEmptyArray) {
  auto type = boolean();
  std::vector<std::shared_ptr<Buffer>> data_buffers(2);
  Datum input;
  input.value = ArrayData::Make(boolean(), 0 /* length */, std::move(data_buffers),
                                0 /* null_count */);

  Datum result;
  ASSERT_OK(And(&this->ctx_, input, input, &result));
  // Result should be empty as well.
  ASSERT_ARRAYS_EQUAL(*input.make_array(), *result.make_array());
}

TEST_F(TestBooleanKernel, And) {
  vector<bool> values1 = {true, false, true, false, true, true};
  vector<bool> values2 = {true, true, false, false, true, false};
  vector<bool> values3 = {true, false, false, false, true, false};
  TestBinaryKernel(And, values1, values2, values3, values3);
}

TEST_F(TestBooleanKernel, Or) {
  vector<bool> values1 = {true, false, true, false, true, true};
  vector<bool> values2 = {true, true, false, false, true, false};
  vector<bool> values3 = {true, true, true, false, true, true};
  vector<bool> values3_nulls = {true, false, false, false, true, false};
  TestBinaryKernel(Or, values1, values2, values3, values3_nulls);
}

TEST_F(TestBooleanKernel, Xor) {
  vector<bool> values1 = {true, false, true, false, true, true};
  vector<bool> values2 = {true, true, false, false, true, false};
  vector<bool> values3 = {false, true, true, false, false, true};
  vector<bool> values3_nulls = {true, false, false, false, true, false};
  TestBinaryKernel(Xor, values1, values2, values3, values3_nulls);
}

}  // namespace compute
}  // namespace arrow

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
#include <cstdio>
#include <functional>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/compute/test-util.h"

namespace arrow {
namespace compute {

// ----------------------------------------------------------------------
// Datum

template <typename T>
void CheckImplicitConstructor(enum Datum::type expected_kind) {
  std::shared_ptr<T> value;
  Datum datum = value;
  ASSERT_EQ(expected_kind, datum.kind());
}

TEST(TestDatum, ImplicitConstructors) {
  CheckImplicitConstructor<Scalar>(Datum::SCALAR);

  CheckImplicitConstructor<Array>(Datum::ARRAY);

  // Instantiate from array subclass
  CheckImplicitConstructor<BinaryArray>(Datum::ARRAY);

  CheckImplicitConstructor<ChunkedArray>(Datum::CHUNKED_ARRAY);
  CheckImplicitConstructor<RecordBatch>(Datum::RECORD_BATCH);

  CheckImplicitConstructor<Table>(Datum::TABLE);
}

class TestInvokeBinaryKernel : public ComputeFixture, public TestBase {};

TEST_F(TestInvokeBinaryKernel, Exceptions) {
  MockBinaryKernel kernel;
  std::vector<Datum> outputs;
  std::shared_ptr<Table> table;
  std::vector<bool> values1 = {true, false, true};
  std::vector<bool> values2 = {false, true, false};

  auto type = boolean();
  auto a1 = _MakeArray<BooleanType, bool>(type, values1, {});
  auto a2 = _MakeArray<BooleanType, bool>(type, values2, {});

  // Left is not an array-like
  ASSERT_RAISES(Invalid, detail::InvokeBinaryArrayKernel(&this->ctx_, &kernel, table, a2,
                                                         &outputs));
  // Right is not an array-like
  ASSERT_RAISES(Invalid, detail::InvokeBinaryArrayKernel(&this->ctx_, &kernel, a1, table,
                                                         &outputs));
  // Different sized inputs
  ASSERT_RAISES(Invalid, detail::InvokeBinaryArrayKernel(&this->ctx_, &kernel, a1,
                                                         a1->Slice(1), &outputs));
}

}  // namespace compute
}  // namespace arrow

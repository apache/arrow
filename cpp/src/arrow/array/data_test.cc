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

#include "arrow/array.h"
#include "arrow/array/data.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {

TEST(ArraySpan, SetSlice) {
  auto arr = ArrayFromJSON(int32(), "[0, 1, 2, 3, 4, 5, 6, null, 7, 8, 9]");
  ArraySpan span(*arr->data());
  ASSERT_EQ(span.length, arr->length());
  ASSERT_EQ(span.null_count, 1);
  ASSERT_EQ(span.offset, 0);

  span.SetSlice(0, 7);
  ASSERT_EQ(span.length, 7);
  ASSERT_EQ(span.null_count, kUnknownNullCount);
  ASSERT_EQ(span.offset, 0);
  ASSERT_EQ(span.GetNullCount(), 0);

  span.SetSlice(7, 4);
  ASSERT_EQ(span.length, 4);
  ASSERT_EQ(span.null_count, kUnknownNullCount);
  ASSERT_EQ(span.offset, 7);
  ASSERT_EQ(span.GetNullCount(), 1);
}

}  // namespace arrow

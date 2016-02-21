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
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "parquet/exception.h"
#include "parquet/util/buffer.h"

using std::string;

namespace parquet_cpp {

class TestBuffer : public ::testing::Test {
};

TEST_F(TestBuffer, Resize) {
  OwnedMutableBuffer buf;

  ASSERT_EQ(0, buf.size());
  ASSERT_NO_THROW(buf.Resize(100));
  ASSERT_EQ(100, buf.size());
  ASSERT_NO_THROW(buf.Resize(200));
  ASSERT_EQ(200, buf.size());

  // Make it smaller, too
  ASSERT_NO_THROW(buf.Resize(50));
  ASSERT_EQ(50, buf.size());
}

TEST_F(TestBuffer, ResizeOOM) {
  // realloc fails, even though there may be no explicit limit
  OwnedMutableBuffer buf;
  ASSERT_NO_THROW(buf.Resize(100));
  int64_t to_alloc = std::numeric_limits<int64_t>::max();
  ASSERT_THROW(buf.Resize(to_alloc), ParquetException);
}

} // namespace parquet_cpp

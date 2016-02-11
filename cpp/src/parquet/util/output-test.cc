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

#include <memory>

#include <gtest/gtest.h>

#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

namespace parquet_cpp {

TEST(TestInMemoryOutputStream, Basics) {
  std::unique_ptr<InMemoryOutputStream> stream(new InMemoryOutputStream(8));

  std::vector<uint8_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

  stream->Write(&data[0], 4);
  ASSERT_EQ(4, stream->Tell());
  stream->Write(&data[4], data.size() - 4);

  std::vector<uint8_t> out;
  stream->Transfer(&out);

  test::assert_vector_equal(data, out);

  ASSERT_EQ(0, stream->Tell());
}

} // namespace parquet_cpp

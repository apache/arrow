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

#ifndef ARROW_TEST_UTIL_H_
#define ARROW_TEST_UTIL_H_

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/bit-util.h"
#include "arrow/util/random.h"
#include "arrow/util/status.h"

#define ASSERT_RAISES(ENUM, expr)               \
  do {                                          \
    Status s = (expr);                          \
    ASSERT_TRUE(s.Is##ENUM());                  \
  } while (0)


#define ASSERT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    ASSERT_TRUE(s.ok());                        \
  } while (0)


#define EXPECT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    EXPECT_TRUE(s.ok());                        \
  } while (0)


namespace arrow {

template <typename T>
void randint(int64_t N, T lower, T upper, std::vector<T>* out) {
  Random rng(random_seed());
  uint64_t draw;
  uint64_t span = upper - lower;
  T val;
  for (int64_t i = 0; i < N; ++i) {
    draw = rng.Uniform64(span);
    val = lower + static_cast<T>(draw);
    out->push_back(val);
  }
}


template <typename T>
std::shared_ptr<Buffer> to_buffer(const std::vector<T>& values) {
  return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(values.data()),
      values.size() * sizeof(T));
}

void random_nulls(int64_t n, double pct_null, std::vector<uint8_t>* nulls) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    nulls->push_back(static_cast<uint8_t>(rng.NextDoubleFraction() > pct_null));
  }
}

void random_nulls(int64_t n, double pct_null, std::vector<bool>* nulls) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    nulls->push_back(rng.NextDoubleFraction() > pct_null);
  }
}

static inline int null_count(const std::vector<uint8_t>& nulls) {
  int result = 0;
  for (size_t i = 0; i < nulls.size(); ++i) {
    if (nulls[i] > 0) {
      ++result;
    }
  }
  return result;
}

std::shared_ptr<Buffer> bytes_to_null_buffer(uint8_t* bytes, int length) {
  std::shared_ptr<Buffer> out;

  // TODO(wesm): error checking
  util::bytes_to_bits(bytes, length, &out);
  return out;
}

} // namespace arrow

#endif // ARROW_TEST_UTIL_H_

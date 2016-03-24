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

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/type.h"
#include "arrow/column.h"
#include "arrow/schema.h"
#include "arrow/table.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/random.h"
#include "arrow/util/status.h"

#define ASSERT_RAISES(ENUM, expr)               \
  do {                                          \
    Status s = (expr);                          \
    if (!s.Is##ENUM()) {                        \
      FAIL() << s.ToString();                   \
    }                                           \
  } while (0)


#define ASSERT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    if (!s.ok()) {                              \
        FAIL() << s.ToString();                 \
    }                                           \
  } while (0)


#define EXPECT_OK(expr)                         \
  do {                                          \
    Status s = (expr);                          \
    EXPECT_TRUE(s.ok());                        \
  } while (0)


namespace arrow {

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
  }

  template <typename ArrayType>
  std::shared_ptr<Array> MakePrimitive(int32_t length, int32_t null_count = 0) {
    auto data = std::make_shared<PoolBuffer>(pool_);
    auto null_bitmap = std::make_shared<PoolBuffer>(pool_);
    EXPECT_OK(data->Resize(length * sizeof(typename ArrayType::value_type)));
    EXPECT_OK(null_bitmap->Resize(util::bytes_for_bits(length)));
    return std::make_shared<ArrayType>(length, data, 10, null_bitmap);
  }

 protected:
  MemoryPool* pool_;
};

namespace test {

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

void random_null_bitmap(int64_t n, double pct_null, std::vector<uint8_t>* null_bitmap) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    if (rng.NextDoubleFraction() > pct_null) {
      null_bitmap->push_back(1);
    } else {
      // null
      null_bitmap->push_back(0);
    }
  }
}

void random_null_bitmap(int64_t n, double pct_null, std::vector<bool>* null_bitmap) {
  Random rng(random_seed());
  for (int i = 0; i < n; ++i) {
    null_bitmap->push_back(rng.NextDoubleFraction() > pct_null);
  }
}

static inline void random_bytes(int n, uint32_t seed, uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d(0, 255);

  for (int i = 0; i < n; ++i) {
    out[i] = d(gen) & 0xFF;
  }
}

template <typename T>
void rand_uniform_int(int n, uint32_t seed, T min_value, T max_value, T* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  for (int i = 0; i < n; ++i) {
    out[i] = d(gen);
  }
}

static inline int bitmap_popcount(const uint8_t* data, int length) {
  int count = 0;
  for (int i = 0; i < length; ++i) {
    // TODO: accelerate this
    if (util::get_bit(data, i)) ++count;
  }
  return count;
}

static inline int null_count(const std::vector<uint8_t>& valid_bytes) {
  int result = 0;
  for (size_t i = 0; i < valid_bytes.size(); ++i) {
    if (valid_bytes[i] == 0) {
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

} // namespace test
} // namespace arrow

#endif // ARROW_TEST_UTIL_H_

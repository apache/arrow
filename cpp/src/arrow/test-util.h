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
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/column.h"
#include "arrow/memory_pool.h"
#include "arrow/schema.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/random.h"

#define ASSERT_RAISES(ENUM, expr)                  \
  do {                                             \
    ::arrow::Status s = (expr);                    \
    if (!s.Is##ENUM()) { FAIL() << s.ToString(); } \
  } while (0)

#define ASSERT_OK(expr)                      \
  do {                                       \
    ::arrow::Status s = (expr);              \
    if (!s.ok()) { FAIL() << s.ToString(); } \
  } while (0)

#define ASSERT_OK_NO_THROW(expr) ASSERT_NO_THROW(ASSERT_OK(expr))

#define EXPECT_OK(expr)         \
  do {                          \
    ::arrow::Status s = (expr); \
    EXPECT_TRUE(s.ok());        \
  } while (0)

namespace arrow {

namespace test {

template <typename T>
void randint(int64_t N, T lower, T upper, std::vector<T>* out) {
  Random rng(random_seed());
  uint64_t draw;
  uint64_t span = upper - lower;
  T val;
  for (int64_t i = 0; i < N; ++i) {
    draw = rng.Uniform64(span);
    val = static_cast<T>(draw + lower);
    out->push_back(val);
  }
}

template <typename T>
void random_real(
    int64_t n, uint32_t seed, T min_value, T max_value, std::vector<T>* out) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<T> d(min_value, max_value);
  for (int64_t i = 0; i < n; ++i) {
    out->push_back(d(gen));
  }
}

template <typename T>
std::shared_ptr<Buffer> GetBufferFromVector(const std::vector<T>& values) {
  return std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(values.data()), values.size() * sizeof(T));
}

template <typename T>
inline Status CopyBufferFromVector(
    const std::vector<T>& values, std::shared_ptr<Buffer>* result) {
  int64_t nbytes = static_cast<int>(values.size()) * sizeof(T);

  auto buffer = std::make_shared<PoolBuffer>(default_memory_pool());
  RETURN_NOT_OK(buffer->Resize(nbytes));
  memcpy(buffer->mutable_data(), values.data(), nbytes);

  *result = buffer;
  return Status::OK();
}

static inline Status GetBitmapFromBoolVector(
    const std::vector<bool>& is_valid, std::shared_ptr<Buffer>* result) {
  size_t length = is_valid.size();

  std::shared_ptr<MutableBuffer> buffer;
  RETURN_NOT_OK(GetEmptyBitmap(default_memory_pool(), length, &buffer));

  uint8_t* bitmap = buffer->mutable_data();
  for (size_t i = 0; i < static_cast<size_t>(length); ++i) {
    if (is_valid[i]) { BitUtil::SetBit(bitmap, i); }
  }

  *result = buffer;
  return Status::OK();
}

// Sets approximately pct_null of the first n bytes in null_bytes to zero
// and the rest to non-zero (true) values.
static inline void random_null_bytes(int64_t n, double pct_null, uint8_t* null_bytes) {
  Random rng(random_seed());
  for (int64_t i = 0; i < n; ++i) {
    null_bytes[i] = rng.NextDoubleFraction() > pct_null;
  }
}

static inline void random_is_valid(
    int64_t n, double pct_null, std::vector<bool>* is_valid) {
  Random rng(random_seed());
  for (int64_t i = 0; i < n; ++i) {
    is_valid->push_back(rng.NextDoubleFraction() > pct_null);
  }
}

static inline void random_bytes(int64_t n, uint32_t seed, uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d(0, 255);

  for (int64_t i = 0; i < n; ++i) {
    out[i] = static_cast<uint8_t>(d(gen) & 0xFF);
  }
}

static inline void random_ascii(int64_t n, uint32_t seed, uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d(65, 122);

  for (int64_t i = 0; i < n; ++i) {
    out[i] = static_cast<uint8_t>(d(gen) & 0xFF);
  }
}

template <typename T>
void rand_uniform_int(int64_t n, uint32_t seed, T min_value, T max_value, T* out) {
  DCHECK(out || (n == 0));
  std::mt19937 gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  for (int64_t i = 0; i < n; ++i) {
    out[i] = static_cast<T>(d(gen));
  }
}

static inline int64_t null_count(const std::vector<uint8_t>& valid_bytes) {
  int64_t result = 0;
  for (size_t i = 0; i < valid_bytes.size(); ++i) {
    if (valid_bytes[i] == 0) { ++result; }
  }
  return result;
}

std::shared_ptr<Buffer> bytes_to_null_buffer(const std::vector<uint8_t>& bytes) {
  std::shared_ptr<Buffer> out;

  // TODO(wesm): error checking
  BitUtil::BytesToBits(bytes, &out);
  return out;
}

Status MakeRandomInt32PoolBuffer(int64_t length, MemoryPool* pool,
    std::shared_ptr<PoolBuffer>* pool_buffer, uint32_t seed = 0) {
  DCHECK(pool);
  auto data = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(data->Resize(length * sizeof(int32_t)));
  test::rand_uniform_int(length, seed, 0, std::numeric_limits<int32_t>::max(),
      reinterpret_cast<int32_t*>(data->mutable_data()));
  *pool_buffer = data;
  return Status::OK();
}

Status MakeRandomBytePoolBuffer(int64_t length, MemoryPool* pool,
    std::shared_ptr<PoolBuffer>* pool_buffer, uint32_t seed = 0) {
  auto bytes = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(bytes->Resize(length));
  test::random_bytes(length, seed, bytes->mutable_data());
  *pool_buffer = bytes;
  return Status::OK();
}

}  // namespace test

class TestBase : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    random_seed_ = 0;
  }

  template <typename ArrayType>
  std::shared_ptr<Array> MakePrimitive(int64_t length, int64_t null_count = 0) {
    auto data = std::make_shared<PoolBuffer>(pool_);
    const int64_t data_nbytes = length * sizeof(typename ArrayType::value_type);
    EXPECT_OK(data->Resize(data_nbytes));

    // Fill with random data
    test::random_bytes(data_nbytes, random_seed_++, data->mutable_data());

    auto null_bitmap = std::make_shared<PoolBuffer>(pool_);
    EXPECT_OK(null_bitmap->Resize(BitUtil::BytesForBits(length)));
    return std::make_shared<ArrayType>(length, data, null_bitmap, null_count);
  }

 protected:
  uint32_t random_seed_;
  MemoryPool* pool_;
};

template <typename TYPE, typename C_TYPE>
void ArrayFromVector(const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
    std::shared_ptr<Array>* out) {
  MemoryPool* pool = default_memory_pool();
  typename TypeTraits<TYPE>::BuilderType builder(pool);
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }
  ASSERT_OK(builder.Finish(out));
}

template <typename TYPE, typename C_TYPE>
void ArrayFromVector(const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  MemoryPool* pool = default_memory_pool();
  typename TypeTraits<TYPE>::BuilderType builder(pool);
  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_OK(builder.Append(values[i]));
  }
  ASSERT_OK(builder.Finish(out));
}

class TestBuilder : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = default_memory_pool();
    type_ = TypePtr(new UInt8Type());
    builder_.reset(new UInt8Builder(pool_));
    builder_nn_.reset(new UInt8Builder(pool_));
  }

 protected:
  MemoryPool* pool_;

  TypePtr type_;
  std::unique_ptr<ArrayBuilder> builder_;
  std::unique_ptr<ArrayBuilder> builder_nn_;
};

template <class T, class Builder>
Status MakeArray(const std::vector<uint8_t>& valid_bytes, const std::vector<T>& values,
    int64_t size, Builder* builder, std::shared_ptr<Array>* out) {
  // Append the first 1000
  for (int64_t i = 0; i < size; ++i) {
    if (valid_bytes[i] > 0) {
      RETURN_NOT_OK(builder->Append(values[i]));
    } else {
      RETURN_NOT_OK(builder->AppendNull());
    }
  }
  return builder->Finish(out);
}

}  // namespace arrow

#endif  // ARROW_TEST_UTIL_H_

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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#define ASSERT_RAISES(ENUM, expr) \
  do {                            \
    ::arrow::Status s = (expr);   \
    if (!s.Is##ENUM()) {          \
      FAIL() << s.ToString();     \
    }                             \
  } while (false)

#define ASSERT_OK(expr)         \
  do {                          \
    ::arrow::Status s = (expr); \
    if (!s.ok()) {              \
      FAIL() << s.ToString();   \
    }                           \
  } while (false)

#define ASSERT_OK_NO_THROW(expr) ASSERT_NO_THROW(ASSERT_OK(expr))

#define EXPECT_OK(expr)         \
  do {                          \
    ::arrow::Status s = (expr); \
    EXPECT_TRUE(s.ok());        \
  } while (false)

#define ABORT_NOT_OK(s)                  \
  do {                                   \
    ::arrow::Status _s = (s);            \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      exit(EXIT_FAILURE);                \
    }                                    \
  } while (false);

namespace arrow {

using ArrayVector = std::vector<std::shared_ptr<Array>>;

namespace test {

template <typename T, typename U>
void randint(int64_t N, T lower, T upper, std::vector<U>* out) {
  const int random_seed = 0;
  std::mt19937 gen(random_seed);
  std::uniform_int_distribution<T> d(lower, upper);
  out->resize(N, static_cast<T>(0));
  std::generate(out->begin(), out->end(), [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T, typename U>
void random_real(int64_t n, uint32_t seed, T min_value, T max_value,
                 std::vector<U>* out) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<T> d(min_value, max_value);
  out->resize(n, static_cast<T>(0));
  std::generate(out->begin(), out->end(), [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T>
std::shared_ptr<Buffer> GetBufferFromVector(const std::vector<T>& values) {
  return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(values.data()),
                                  values.size() * sizeof(T));
}

template <typename T>
inline Status CopyBufferFromVector(const std::vector<T>& values, MemoryPool* pool,
                                   std::shared_ptr<Buffer>* result) {
  int64_t nbytes = static_cast<int>(values.size()) * sizeof(T);

  auto buffer = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(buffer->Resize(nbytes));
  auto immutable_data = reinterpret_cast<const uint8_t*>(values.data());
  std::copy(immutable_data, immutable_data + nbytes, buffer->mutable_data());

  *result = buffer;
  return Status::OK();
}

template <typename T>
static inline Status GetBitmapFromVector(const std::vector<T>& is_valid,
                                         std::shared_ptr<Buffer>* result) {
  size_t length = is_valid.size();

  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(GetEmptyBitmap(default_memory_pool(), length, &buffer));

  uint8_t* bitmap = buffer->mutable_data();
  for (size_t i = 0; i < static_cast<size_t>(length); ++i) {
    if (is_valid[i]) {
      BitUtil::SetBit(bitmap, i);
    }
  }

  *result = buffer;
  return Status::OK();
}

// Sets approximately pct_null of the first n bytes in null_bytes to zero
// and the rest to non-zero (true) values.
static inline void random_null_bytes(int64_t n, double pct_null, uint8_t* null_bytes) {
  const int random_seed = 0;
  std::mt19937 gen(random_seed);
  std::uniform_real_distribution<double> d(0.0, 1.0);
  std::generate(null_bytes, null_bytes + n,
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

static inline void random_is_valid(int64_t n, double pct_null,
                                   std::vector<bool>* is_valid) {
  const int random_seed = 0;
  std::mt19937 gen(random_seed);
  std::uniform_real_distribution<double> d(0.0, 1.0);
  is_valid->resize(n, false);
  std::generate(is_valid->begin(), is_valid->end(),
                [&d, &gen, &pct_null] { return d(gen) > pct_null; });
}

static inline void random_bytes(int64_t n, uint32_t seed, uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
}

static int32_t DecimalSize(int32_t precision) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;
  DCHECK_LE(precision, 38) << "decimal precision must be less than or equal to 38, got "
                           << precision;

  switch (precision) {
    case 1:
    case 2:
      return 1;  // 127
    case 3:
    case 4:
      return 2;  // 32,767
    case 5:
    case 6:
      return 3;  // 8,388,607
    case 7:
    case 8:
    case 9:
      return 4;  // 2,147,483,427
    case 10:
    case 11:
      return 5;  // 549,755,813,887
    case 12:
    case 13:
    case 14:
      return 6;  // 140,737,488,355,327
    case 15:
    case 16:
      return 7;  // 36,028,797,018,963,967
    case 17:
    case 18:
      return 8;  // 9,223,372,036,854,775,807
    case 19:
    case 20:
    case 21:
      return 9;  // 2,361,183,241,434,822,606,847
    case 22:
    case 23:
      return 10;  // 604,462,909,807,314,587,353,087
    case 24:
    case 25:
    case 26:
      return 11;  // 154,742,504,910,672,534,362,390,527
    case 27:
    case 28:
      return 12;  // 39,614,081,257,132,168,796,771,975,167
    case 29:
    case 30:
    case 31:
      return 13;  // 10,141,204,801,825,835,211,973,625,643,007
    case 32:
    case 33:
      return 14;  // 2,596,148,429,267,413,814,265,248,164,610,047
    case 34:
    case 35:
      return 15;  // 664,613,997,892,457,936,451,903,530,140,172,287
    case 36:
    case 37:
    case 38:
      return 16;  // 170,141,183,460,469,231,731,687,303,715,884,105,727
    default:
      DCHECK(false);
      break;
  }
  return -1;
}

static inline void random_decimals(int64_t n, uint32_t seed, int32_t precision,
                                   uint8_t* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
  const int32_t required_bytes = DecimalSize(precision);
  constexpr int32_t byte_width = 16;
  std::fill(out, out + byte_width * n, '\0');

  for (int64_t i = 0; i < n; ++i, out += byte_width) {
    std::generate(out, out + required_bytes,
                  [&d, &gen] { return static_cast<uint8_t>(d(gen)); });

    // sign extend if the sign bit is set for the last byte generated
    // 0b10000000 == 0x80 == 128
    if ((out[required_bytes - 1] & '\x80') != 0) {
      std::fill(out + required_bytes, out + byte_width, '\xFF');
    }
  }
}

template <typename T, typename U>
void rand_uniform_int(int64_t n, uint32_t seed, T min_value, T max_value, U* out) {
  DCHECK(out || (n == 0));
  std::mt19937 gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  std::generate(out, out + n, [&d, &gen] { return static_cast<U>(d(gen)); });
}

static inline void random_ascii(int64_t n, uint32_t seed, uint8_t* out) {
  rand_uniform_int(n, seed, static_cast<int32_t>('A'), static_cast<int32_t>('z'), out);
}

static inline int64_t null_count(const std::vector<uint8_t>& valid_bytes) {
  return static_cast<int64_t>(std::count(valid_bytes.cbegin(), valid_bytes.cend(), '\0'));
}

Status MakeRandomInt32PoolBuffer(int64_t length, MemoryPool* pool,
                                 std::shared_ptr<PoolBuffer>* pool_buffer,
                                 uint32_t seed = 0) {
  DCHECK(pool);
  auto data = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(data->Resize(length * sizeof(int32_t)));
  test::rand_uniform_int(length, seed, 0, std::numeric_limits<int32_t>::max(),
                         reinterpret_cast<int32_t*>(data->mutable_data()));
  *pool_buffer = data;
  return Status::OK();
}

Status MakeRandomBytePoolBuffer(int64_t length, MemoryPool* pool,
                                std::shared_ptr<PoolBuffer>* pool_buffer,
                                uint32_t seed = 0) {
  auto bytes = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(bytes->Resize(length));
  test::random_bytes(length, seed, bytes->mutable_data());
  *pool_buffer = bytes;
  return Status::OK();
}

}  // namespace test

template <typename TYPE, typename C_TYPE>
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  typename TypeTraits<TYPE>::BuilderType builder(type, default_memory_pool());
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
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  typename TypeTraits<TYPE>::BuilderType builder(type, default_memory_pool());
  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_OK(builder.Append(values[i]));
  }
  ASSERT_OK(builder.Finish(out));
}

template <typename TYPE, typename C_TYPE>
void ArrayFromVector(const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  typename TypeTraits<TYPE>::BuilderType builder;
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
  typename TypeTraits<TYPE>::BuilderType builder;
  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_OK(builder.Append(values[i]));
  }
  ASSERT_OK(builder.Finish(out));
}

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

#define ASSERT_ARRAYS_EQUAL(LEFT, RIGHT)                                               \
  do {                                                                                 \
    if (!(LEFT).Equals((RIGHT))) {                                                     \
      std::stringstream pp_result;                                                     \
      std::stringstream pp_expected;                                                   \
                                                                                       \
      EXPECT_OK(PrettyPrint(RIGHT, 0, &pp_result));                                    \
      EXPECT_OK(PrettyPrint(LEFT, 0, &pp_expected));                                   \
      FAIL() << "Got: \n" << pp_result.str() << "\nExpected: \n" << pp_expected.str(); \
    }                                                                                  \
  } while (false)

void AssertArraysEqual(const Array& expected, const Array& actual) {
  ASSERT_ARRAYS_EQUAL(expected, actual);
}

#define ASSERT_BATCHES_EQUAL(LEFT, RIGHT)    \
  do {                                       \
    if (!LEFT.ApproxEquals(RIGHT)) {         \
      std::stringstream ss;                  \
      ss << "Left:\n";                       \
      ASSERT_OK(PrettyPrint(LEFT, 0, &ss));  \
                                             \
      ss << "\nRight:\n";                    \
      ASSERT_OK(PrettyPrint(RIGHT, 0, &ss)); \
      FAIL() << ss.str();                    \
    }                                        \
  } while (false)

}  // namespace arrow

#endif  // ARROW_TEST_UTIL_H_

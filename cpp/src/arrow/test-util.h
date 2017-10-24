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
  std::uniform_int_distribution<int> d(0, std::numeric_limits<uint8_t>::max());
  std::generate(out, out + n, [&d, &gen] { return static_cast<uint8_t>(d(gen) & 0xFF); });
}

static void DecimalRange(int32_t precision, Decimal128* min_decimal,
                         Decimal128* max_decimal) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;
  DCHECK_LE(precision, 38) << "decimal precision must be less than or equal to 38, got "
                           << precision;

  switch (precision) {
    case 1:
    case 2:
      *max_decimal = std::numeric_limits<int8_t>::max();
      break;
    case 3:
    case 4:
      *max_decimal = std::numeric_limits<int16_t>::max();
      break;
    case 5:
    case 6:
      *max_decimal = 8388607;
      break;
    case 7:
    case 8:
    case 9:
      *max_decimal = std::numeric_limits<int32_t>::max();
      break;
    case 10:
    case 11:
      *max_decimal = 549755813887;
      break;
    case 12:
    case 13:
    case 14:
      *max_decimal = 140737488355327;
      break;
    case 15:
    case 16:
      *max_decimal = 36028797018963967;
      break;
    case 17:
    case 18:
      *max_decimal = std::numeric_limits<int64_t>::max();
      break;
    case 19:
    case 20:
    case 21:
      *max_decimal = Decimal128("2361183241434822606847");
      break;
    case 22:
    case 23:
      *max_decimal = Decimal128("604462909807314587353087");
      break;
    case 24:
    case 25:
    case 26:
      *max_decimal = Decimal128("154742504910672534362390527");
      break;
    case 27:
    case 28:
      *max_decimal = Decimal128("39614081257132168796771975167");
      break;
    case 29:
    case 30:
    case 31:
      *max_decimal = Decimal128("10141204801825835211973625643007");
      break;
    case 32:
    case 33:
      *max_decimal = Decimal128("2596148429267413814265248164610047");
      break;
    case 34:
    case 35:
      *max_decimal = Decimal128("664613997892457936451903530140172287");
      break;
    case 36:
    case 37:
    case 38:
      *max_decimal = Decimal128("170141183460469231731687303715884105727");
      break;
    default:
      DCHECK(false);
      break;
  }

  *min_decimal = ~(*max_decimal);
}

class UniformDecimalDistribution {
 public:
  explicit UniformDecimalDistribution(int32_t precision) {
    Decimal128 max_decimal;
    Decimal128 min_decimal;
    DecimalRange(precision, &min_decimal, &max_decimal);

    const auto min_low = static_cast<int64_t>(min_decimal.low_bits());
    const auto max_low = static_cast<int64_t>(max_decimal.low_bits());

    const int64_t min_high = min_decimal.high_bits();
    const int64_t max_high = max_decimal.high_bits();

    using param_type = std::uniform_int_distribution<int64_t>::param_type;

    lower_dist_.param(param_type(min_low, max_low));
    upper_dist_.param(param_type(min_high, max_high));
  }

  template <typename Generator>
  Decimal128 operator()(Generator& gen) {
    return Decimal128(upper_dist_(gen), static_cast<uint64_t>(lower_dist_(gen)));
  }

 private:
  // The lower bits distribution is intentionally int64_t.
  // If it were uint64_t then the size of the interval [min_high, max_high] would be 0
  // because min_high > max_high due to 2's complement.
  // So, we generate the same range of bits using int64_t and then cast to uint64_t.
  std::uniform_int_distribution<int64_t> lower_dist_;
  std::uniform_int_distribution<int64_t> upper_dist_;
};

static inline void random_decimals(int64_t n, uint32_t seed, int32_t precision,
                                   uint8_t* out) {
  std::mt19937 gen(seed);
  UniformDecimalDistribution dist(precision);

  for (int64_t i = 0; i < n; ++i, out += 16) {
    const Decimal128 value(dist(gen));
    value.ToBytes(out);
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

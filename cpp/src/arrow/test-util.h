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

#ifndef _WIN32
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
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
#include "arrow/util/visibility.h"

#define STRINGIFY(x) #x

#define ASSERT_RAISES(ENUM, expr)                                         \
  do {                                                                    \
    ::arrow::Status s = (expr);                                           \
    if (!s.Is##ENUM()) {                                                  \
      FAIL() << "Expected '" STRINGIFY(expr) "' to fail with " STRINGIFY( \
                    ENUM) ", but got "                                    \
             << s.ToString();                                             \
    }                                                                     \
  } while (false)

#define ASSERT_OK(expr)                                               \
  do {                                                                \
    ::arrow::Status s = (expr);                                       \
    if (!s.ok()) {                                                    \
      FAIL() << "'" STRINGIFY(expr) "' failed with " << s.ToString(); \
    }                                                                 \
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
      std::cerr << s.ToString() << "\n"; \
      std::abort();                      \
    }                                    \
  } while (false);

namespace arrow {

using ArrayVector = std::vector<std::shared_ptr<Array>>;

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
inline Status CopyBufferFromVector(const std::vector<T>& values, MemoryPool* pool,
                                   std::shared_ptr<Buffer>* result) {
  int64_t nbytes = static_cast<int>(values.size()) * sizeof(T);

  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(AllocateBuffer(pool, nbytes, &buffer));
  auto immutable_data = reinterpret_cast<const uint8_t*>(values.data());
  std::copy(immutable_data, immutable_data + nbytes, buffer->mutable_data());
  memset(buffer->mutable_data() + nbytes, 0,
         static_cast<size_t>(buffer->capacity() - nbytes));

  *result = buffer;
  return Status::OK();
}

template <typename T>
static inline Status GetBitmapFromVector(const std::vector<T>& is_valid,
                                         std::shared_ptr<Buffer>* result) {
  size_t length = is_valid.size();

  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(AllocateEmptyBitmap(length, &buffer));

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
ARROW_EXPORT void random_null_bytes(int64_t n, double pct_null, uint8_t* null_bytes);
ARROW_EXPORT void random_is_valid(int64_t n, double pct_null,
                                  std::vector<bool>* is_valid);
ARROW_EXPORT void random_bytes(int64_t n, uint32_t seed, uint8_t* out);
ARROW_EXPORT int32_t DecimalSize(int32_t precision);
ARROW_EXPORT void random_decimals(int64_t n, uint32_t seed, int32_t precision,
                                  uint8_t* out);
ARROW_EXPORT void random_ascii(int64_t n, uint32_t seed, uint8_t* out);
ARROW_EXPORT int64_t CountNulls(const std::vector<uint8_t>& valid_bytes);

ARROW_EXPORT Status MakeRandomByteBuffer(int64_t length, MemoryPool* pool,
                                         std::shared_ptr<ResizableBuffer>* out,
                                         uint32_t seed = 0);

ARROW_EXPORT void AssertArraysEqual(const Array& expected, const Array& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& expected,
                                     const ChunkedArray& actual);
ARROW_EXPORT void AssertChunkedEqual(const ChunkedArray& actual,
                                     const ArrayVector& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer,
                                    const std::vector<uint8_t>& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const std::string& expected);
ARROW_EXPORT void AssertBufferEqual(const Buffer& buffer, const Buffer& expected);
ARROW_EXPORT void AssertSchemaEqual(const Schema& lhs, const Schema& rhs);

ARROW_EXPORT void PrintColumn(const Column& col, std::stringstream* ss);
ARROW_EXPORT void AssertTablesEqual(const Table& expected, const Table& actual,
                                    bool same_chunk_layout = true);

ARROW_EXPORT void CompareBatch(const RecordBatch& left, const RecordBatch& right);

// Check if the padding of the buffers of the array is zero.
// Also cause valgrind warnings if the padding bytes are uninitialized.
ARROW_EXPORT void AssertZeroPadded(const Array& array);

// Check if the valid buffer bytes are initialized
// and cause valgrind warnings otherwise.
ARROW_EXPORT void TestInitialized(const Array& array);

template <typename BuilderType>
void FinishAndCheckPadding(BuilderType* builder, std::shared_ptr<Array>* out) {
  ASSERT_OK(builder->Finish(out));
  AssertZeroPadded(**out);
  TestInitialized(**out);
}

template <typename T, typename U>
void rand_uniform_int(int64_t n, uint32_t seed, T min_value, T max_value, U* out) {
  DCHECK(out || (n == 0));
  std::mt19937 gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  std::generate(out, out + n, [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T, typename Enable = void>
struct GenerateRandom {};

template <typename T>
struct GenerateRandom<T, typename std::enable_if<std::is_integral<T>::value>::type> {
  static void Gen(int64_t length, uint32_t seed, void* out) {
    rand_uniform_int(length, seed, std::numeric_limits<T>::min(),
                     std::numeric_limits<T>::max(), reinterpret_cast<T*>(out));
  }
};

template <typename T>
Status MakeRandomBuffer(int64_t length, MemoryPool* pool,
                        std::shared_ptr<ResizableBuffer>* out, uint32_t seed = 0) {
  DCHECK(pool);
  std::shared_ptr<ResizableBuffer> result;
  RETURN_NOT_OK(AllocateResizableBuffer(pool, sizeof(T) * length, &result));
  GenerateRandom<T>::Gen(length, seed, result->mutable_data());
  *out = result;
  return Status::OK();
}

// ArrayFromVector: construct an Array from vectors of C values

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  DCHECK_EQ(TYPE::type_id, type->id())
      << "template parameter and concrete DataType instance don't agree";

  std::unique_ptr<ArrayBuilder> builder_ptr;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder_ptr));
  // Get the concrete builder class to access its Append() specializations
  auto& builder = dynamic_cast<typename TypeTraits<TYPE>::BuilderType&>(*builder_ptr);

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }
  ASSERT_OK(builder.Finish(out));
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::shared_ptr<DataType>& type,
                     const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  DCHECK_EQ(TYPE::type_id, type->id())
      << "template parameter and concrete DataType instance don't agree";

  std::unique_ptr<ArrayBuilder> builder_ptr;
  ASSERT_OK(MakeBuilder(default_memory_pool(), type, &builder_ptr));
  // Get the concrete builder class to access its Append() specializations
  auto& builder = dynamic_cast<typename TypeTraits<TYPE>::BuilderType&>(*builder_ptr);

  for (size_t i = 0; i < values.size(); ++i) {
    ASSERT_OK(builder.Append(values[i]));
  }
  ASSERT_OK(builder.Finish(out));
}

// Overloads without a DataType argument, for parameterless types

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::vector<bool>& is_valid, const std::vector<C_TYPE>& values,
                     std::shared_ptr<Array>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, out);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ArrayFromVector(const std::vector<C_TYPE>& values, std::shared_ptr<Array>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ArrayFromVector<TYPE, C_TYPE>(type, values, out);
}

// ChunkedArrayFromVector: construct a ChunkedArray from vectors of C values

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::shared_ptr<DataType>& type,
                            const std::vector<std::vector<bool>>& is_valid,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  ArrayVector chunks;
  DCHECK_EQ(is_valid.size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    std::shared_ptr<Array> array;
    ArrayFromVector<TYPE, C_TYPE>(type, is_valid[i], values[i], &array);
    chunks.push_back(array);
  }
  *out = std::make_shared<ChunkedArray>(chunks);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::shared_ptr<DataType>& type,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  ArrayVector chunks;
  for (size_t i = 0; i < values.size(); ++i) {
    std::shared_ptr<Array> array;
    ArrayFromVector<TYPE, C_TYPE>(type, values[i], &array);
    chunks.push_back(array);
  }
  *out = std::make_shared<ChunkedArray>(chunks);
}

// Overloads without a DataType argument, for parameterless types

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::vector<std::vector<bool>>& is_valid,
                            const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ChunkedArrayFromVector<TYPE, C_TYPE>(type, is_valid, values, out);
}

template <typename TYPE, typename C_TYPE = typename TYPE::c_type>
void ChunkedArrayFromVector(const std::vector<std::vector<C_TYPE>>& values,
                            std::shared_ptr<ChunkedArray>* out) {
  auto type = TypeTraits<TYPE>::type_singleton();
  ChunkedArrayFromVector<TYPE, C_TYPE>(type, values, out);
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

#define DECL_T() typedef typename TestFixture::T T;

#define DECL_TYPE() typedef typename TestFixture::Type Type;

#define ASSERT_BATCHES_EQUAL(LEFT, RIGHT)    \
  do {                                       \
    if (!(LEFT).ApproxEquals(RIGHT)) {       \
      std::stringstream ss;                  \
      ss << "Left:\n";                       \
      ASSERT_OK(PrettyPrint(LEFT, 0, &ss));  \
                                             \
      ss << "\nRight:\n";                    \
      ASSERT_OK(PrettyPrint(RIGHT, 0, &ss)); \
      FAIL() << ss.str();                    \
    }                                        \
  } while (false)

// ----------------------------------------------------------------------
// A RecordBatchReader for serving a sequence of in-memory record batches

class BatchIterator : public RecordBatchReader {
 public:
  BatchIterator(const std::shared_ptr<Schema>& schema,
                const std::vector<std::shared_ptr<RecordBatch>>& batches)
      : schema_(schema), batches_(batches), position_(0) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    if (position_ >= batches_.size()) {
      *out = nullptr;
    } else {
      *out = batches_[position_++];
    }
    return Status::OK();
  }

 private:
  std::shared_ptr<Schema> schema_;
  std::vector<std::shared_ptr<RecordBatch>> batches_;
  size_t position_;
};

}  // namespace arrow

#endif  // ARROW_TEST_UTIL_H_

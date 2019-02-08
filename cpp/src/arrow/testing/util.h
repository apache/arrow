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

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ChunkedArray;
class Column;
class Table;

using ArrayVector = std::vector<std::shared_ptr<Array>>;

template <typename T, typename U>
void randint(int64_t N, T lower, T upper, std::vector<U>* out) {
  const int random_seed = 0;
  std::default_random_engine gen(random_seed);
  std::uniform_int_distribution<T> d(lower, upper);
  out->resize(N, static_cast<T>(0));
  std::generate(out->begin(), out->end(), [&d, &gen] { return static_cast<U>(d(gen)); });
}

template <typename T, typename U>
void random_real(int64_t n, uint32_t seed, T min_value, T max_value,
                 std::vector<U>* out) {
  std::default_random_engine gen(seed);
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

template <typename T, typename U>
void rand_uniform_int(int64_t n, uint32_t seed, T min_value, T max_value, U* out) {
  DCHECK(out || (n == 0));
  std::default_random_engine gen(seed);
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

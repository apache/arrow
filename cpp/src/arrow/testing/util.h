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
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/visibility.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/visitor_inline.h"

namespace arrow {

template <typename T>
Status CopyBufferFromVector(const std::vector<T>& values, MemoryPool* pool,
                            std::shared_ptr<Buffer>* result) {
  int64_t nbytes = static_cast<int>(values.size()) * sizeof(T);

  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(nbytes, pool));
  auto immutable_data = reinterpret_cast<const uint8_t*>(values.data());
  std::copy(immutable_data, immutable_data + nbytes, buffer->mutable_data());
  memset(buffer->mutable_data() + nbytes, 0,
         static_cast<size_t>(buffer->capacity() - nbytes));

  *result = std::move(buffer);
  return Status::OK();
}

// Sets approximately pct_null of the first n bytes in null_bytes to zero
// and the rest to non-zero (true) values.
ARROW_TESTING_EXPORT void random_null_bytes(int64_t n, double pct_null,
                                            uint8_t* null_bytes);
ARROW_TESTING_EXPORT void random_is_valid(int64_t n, double pct_null,
                                          std::vector<bool>* is_valid,
                                          int random_seed = 0);
ARROW_TESTING_EXPORT void random_bytes(int64_t n, uint32_t seed, uint8_t* out);
ARROW_TESTING_EXPORT std::string random_string(int64_t n, uint32_t seed);
ARROW_TESTING_EXPORT int32_t DecimalSize(int32_t precision);
ARROW_TESTING_EXPORT void random_decimals(int64_t n, uint32_t seed, int32_t precision,
                                          uint8_t* out);
ARROW_TESTING_EXPORT void random_ascii(int64_t n, uint32_t seed, uint8_t* out);
ARROW_TESTING_EXPORT int64_t CountNulls(const std::vector<uint8_t>& valid_bytes);

ARROW_TESTING_EXPORT Status MakeRandomByteBuffer(int64_t length, MemoryPool* pool,
                                                 std::shared_ptr<ResizableBuffer>* out,
                                                 uint32_t seed = 0);

ARROW_TESTING_EXPORT uint64_t random_seed();

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

template <typename Fn>
struct VisitBuilderImpl {
  template <typename T, typename BuilderType = typename TypeTraits<T>::BuilderType,
            // need to let SFINAE drop this Visit when it would result in
            // [](NullBuilder*){}(double_builder)
            typename E = typename std::result_of<Fn(BuilderType*)>::type>
  Status Visit(const T&) {
    fn_(internal::checked_cast<BuilderType*>(builder_));
    return Status::OK();
  }

  Status Visit(const DataType& t) {
    return Status::NotImplemented("visiting builders of type ", t);
  }

  Status Visit() { return VisitTypeInline(*builder_->type(), this); }

  ArrayBuilder* builder_;
  Fn fn_;
};

template <typename Fn>
Status VisitBuilder(ArrayBuilder* builder, Fn&& fn) {
  return VisitBuilderImpl<Fn>{builder, std::forward<Fn>(fn)}.Visit();
}

template <typename Fn>
Result<std::shared_ptr<Array>> ArrayFromBuilderVisitor(
    const std::shared_ptr<DataType>& type, int64_t initial_capacity,
    int64_t visitor_repetitions, Fn&& fn) {
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(default_memory_pool(), type, &builder));

  if (initial_capacity != 0) {
    RETURN_NOT_OK(builder->Resize(initial_capacity));
  }

  for (int64_t i = 0; i < visitor_repetitions; ++i) {
    RETURN_NOT_OK(VisitBuilder(builder.get(), std::forward<Fn>(fn)));
  }

  std::shared_ptr<Array> out;
  RETURN_NOT_OK(builder->Finish(&out));
  return std::move(out);
}

template <typename Fn>
Result<std::shared_ptr<Array>> ArrayFromBuilderVisitor(
    const std::shared_ptr<DataType>& type, int64_t length, Fn&& fn) {
  return ArrayFromBuilderVisitor(type, length, length, std::forward<Fn>(fn));
}

static inline std::vector<std::shared_ptr<DataType> (*)(FieldVector, std::vector<int8_t>)>
UnionTypeFactories() {
  return {sparse_union, dense_union};
}

// Return the value of the ARROW_TEST_DATA environment variable or return error
// Status
ARROW_TESTING_EXPORT Status GetTestResourceRoot(std::string*);

// Get a TCP port number to listen on.  This is a different number every time,
// as reusing the same port across tests can produce spurious bind errors on
// Windows.
ARROW_TESTING_EXPORT int GetListenPort();

ARROW_TESTING_EXPORT
const std::vector<std::shared_ptr<DataType>>& all_dictionary_index_types();

}  // namespace arrow

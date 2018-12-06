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

#include "arrow/compute/kernels/hash.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

using internal::checked_cast;
using internal::DictionaryTraits;
using internal::HashTraits;

namespace compute {

namespace {

#define CHECK_IMPLEMENTED(KERNEL, FUNCNAME, TYPE)                  \
  if (!KERNEL) {                                                   \
    std::stringstream ss;                                          \
    ss << FUNCNAME << " not implemented for " << type->ToString(); \
    return Status::NotImplemented(ss.str());                       \
  }

// ----------------------------------------------------------------------
// Unique implementation

class UniqueAction {
 public:
  UniqueAction(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  Status Reset() { return Status::OK(); }

  Status Reserve(const int64_t length) { return Status::OK(); }

  void ObserveNull() {}

  template <class Index>
  void ObserveFound(Index index) {}

  template <class Index>
  void ObserveNotFound(Index index) {}

  Status Flush(Datum* out) { return Status::OK(); }
};

// ----------------------------------------------------------------------
// Dictionary encode implementation

class DictEncodeAction {
 public:
  DictEncodeAction(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : indices_builder_(pool) {}

  Status Reset() {
    indices_builder_.Reset();
    return Status::OK();
  }

  Status Reserve(const int64_t length) { return indices_builder_.Reserve(length); }

  void ObserveNull() { indices_builder_.UnsafeAppendNull(); }

  template <class Index>
  void ObserveFound(Index index) {
    indices_builder_.UnsafeAppend(index);
  }

  template <class Index>
  void ObserveNotFound(Index index) {
    return ObserveFound(index);
  }

  Status Flush(Datum* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(indices_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

 private:
  Int32Builder indices_builder_;
};

// ----------------------------------------------------------------------
// Base class for all hash kernel implementations

class HashKernelImpl : public HashKernel {
 public:
  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());
    RETURN_NOT_OK(Append(ctx, *input.array()));
    return Flush(out);
  }

  Status Append(FunctionContext* ctx, const ArrayData& input) override {
    std::lock_guard<std::mutex> guard(lock_);
    return Append(input);
  }

  virtual Status Append(const ArrayData& arr) = 0;

 protected:
  std::mutex lock_;
};

// ----------------------------------------------------------------------
// Base class for all "regular" hash kernel implementations
// (NullType has a separate implementation)

template <typename Type, typename Scalar, typename Action>
class RegularHashKernelImpl : public HashKernelImpl {
 public:
  RegularHashKernelImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : pool_(pool), type_(type), action_(type, pool) {}

  Status Reset() override {
    memo_table_.reset(new MemoTable(0));
    return action_.Reset();
  }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    return ArrayDataVisitor<Type>::Visit(arr, this);
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return DictionaryTraits<Type>::GetDictionaryArrayData(pool_, type_, *memo_table_,
                                                          0 /* start_offset */, out);
  }

  Status VisitNull() {
    action_.ObserveNull();
    return Status::OK();
  }

  Status VisitValue(const Scalar& value) {
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this](int32_t memo_index) {
      action_.ObserveNotFound(memo_index);
    };
    memo_table_->GetOrInsert(value, on_found, on_not_found);
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<Type>::MemoTableType;

  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
  std::unique_ptr<MemoTable> memo_table_;
};

// ----------------------------------------------------------------------
// Hash kernel implementation for nulls

template <typename Action>
class NullHashKernelImpl : public HashKernelImpl {
 public:
  NullHashKernelImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : pool_(pool), type_(type), action_(type, pool) {}

  Status Reset() override { return action_.Reset(); }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    for (int64_t i = 0; i < arr.length; ++i) {
      action_.ObserveNull();
    }
    return Status::OK();
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being a valid dictionary value
    auto null_array = std::make_shared<NullArray>(0);
    *out = null_array->data();
    return Status::OK();
  }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Action, typename Enable = void>
struct HashKernelTraits {};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_null<Type>> {
  using HashKernelImpl = NullHashKernelImpl<Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_has_c_type<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, typename Type::c_type, Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_boolean<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, bool, Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_binary<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, util::string_view, Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_fixed_size_binary<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, util::string_view, Action>;
};

}  // namespace

Status GetUniqueKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                       std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;

#define UNIQUE_CASE(InType)                                                           \
  case InType::type_id:                                                               \
    kernel.reset(new typename HashKernelTraits<InType, UniqueAction>::HashKernelImpl( \
        type, ctx->memory_pool()));                                                   \
    break

  switch (type->id()) {
    UNIQUE_CASE(NullType);
    UNIQUE_CASE(BooleanType);
    UNIQUE_CASE(UInt8Type);
    UNIQUE_CASE(Int8Type);
    UNIQUE_CASE(UInt16Type);
    UNIQUE_CASE(Int16Type);
    UNIQUE_CASE(UInt32Type);
    UNIQUE_CASE(Int32Type);
    UNIQUE_CASE(UInt64Type);
    UNIQUE_CASE(Int64Type);
    UNIQUE_CASE(FloatType);
    UNIQUE_CASE(DoubleType);
    UNIQUE_CASE(Date32Type);
    UNIQUE_CASE(Date64Type);
    UNIQUE_CASE(Time32Type);
    UNIQUE_CASE(Time64Type);
    UNIQUE_CASE(TimestampType);
    UNIQUE_CASE(BinaryType);
    UNIQUE_CASE(StringType);
    UNIQUE_CASE(FixedSizeBinaryType);
    UNIQUE_CASE(Decimal128Type);
    default:
      break;
  }

#undef UNIQUE_CASE

  CHECK_IMPLEMENTED(kernel, "unique", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

Status GetDictionaryEncodeKernel(FunctionContext* ctx,
                                 const std::shared_ptr<DataType>& type,
                                 std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;

#define DICTIONARY_ENCODE_CASE(InType)                                                \
  case InType::type_id:                                                               \
    kernel.reset(new                                                                  \
                 typename HashKernelTraits<InType, DictEncodeAction>::HashKernelImpl( \
                     type, ctx->memory_pool()));                                      \
    break

  switch (type->id()) {
    DICTIONARY_ENCODE_CASE(NullType);
    DICTIONARY_ENCODE_CASE(BooleanType);
    DICTIONARY_ENCODE_CASE(UInt8Type);
    DICTIONARY_ENCODE_CASE(Int8Type);
    DICTIONARY_ENCODE_CASE(UInt16Type);
    DICTIONARY_ENCODE_CASE(Int16Type);
    DICTIONARY_ENCODE_CASE(UInt32Type);
    DICTIONARY_ENCODE_CASE(Int32Type);
    DICTIONARY_ENCODE_CASE(UInt64Type);
    DICTIONARY_ENCODE_CASE(Int64Type);
    DICTIONARY_ENCODE_CASE(FloatType);
    DICTIONARY_ENCODE_CASE(DoubleType);
    DICTIONARY_ENCODE_CASE(Date32Type);
    DICTIONARY_ENCODE_CASE(Date64Type);
    DICTIONARY_ENCODE_CASE(Time32Type);
    DICTIONARY_ENCODE_CASE(Time64Type);
    DICTIONARY_ENCODE_CASE(TimestampType);
    DICTIONARY_ENCODE_CASE(BinaryType);
    DICTIONARY_ENCODE_CASE(StringType);
    DICTIONARY_ENCODE_CASE(FixedSizeBinaryType);
    DICTIONARY_ENCODE_CASE(Decimal128Type);
    default:
      break;
  }

#undef DICTIONARY_ENCODE_CASE

  CHECK_IMPLEMENTED(kernel, "dictionary-encode", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

namespace {

Status InvokeHash(FunctionContext* ctx, HashKernel* func, const Datum& value,
                  std::vector<Datum>* kernel_outputs,
                  std::shared_ptr<Array>* dictionary) {
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, value, kernel_outputs));

  std::shared_ptr<ArrayData> dict_data;
  RETURN_NOT_OK(func->GetDictionary(&dict_data));
  *dictionary = MakeArray(dict_data);
  return Status::OK();
}

}  // namespace

Status Unique(FunctionContext* ctx, const Datum& value, std::shared_ptr<Array>* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetUniqueKernel(ctx, value.type(), &func));

  std::vector<Datum> dummy_outputs;
  return InvokeHash(ctx, func.get(), value, &dummy_outputs, out);
}

Status DictionaryEncode(FunctionContext* ctx, const Datum& value, Datum* out) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetDictionaryEncodeKernel(ctx, value.type(), &func));

  std::shared_ptr<Array> dictionary;
  std::vector<Datum> indices_outputs;
  RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &indices_outputs, &dictionary));

  // Create the dictionary type
  DCHECK_EQ(indices_outputs[0].kind(), Datum::ARRAY);
  std::shared_ptr<DataType> dict_type =
      ::arrow::dictionary(indices_outputs[0].array()->type, dictionary);

  // Create DictionaryArray for each piece yielded by the kernel invocations
  std::vector<std::shared_ptr<Array>> dict_chunks;
  for (const Datum& datum : indices_outputs) {
    dict_chunks.emplace_back(
        std::make_shared<DictionaryArray>(dict_type, MakeArray(datum.array())));
  }

  *out = detail::WrapArraysLike(value, dict_chunks);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

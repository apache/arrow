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
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
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

#define CHECK_IMPLEMENTED(KERNEL, FUNCNAME, TYPE)                                       \
  if (!KERNEL) {                                                                        \
    return Status::NotImplemented(FUNCNAME, " not implemented for ", type->ToString()); \
  }

// ----------------------------------------------------------------------
// Unique implementation

class ActionBase {
 public:
  ActionBase(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool) {}

 protected:
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
};

class UniqueAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  Status Reset() { return Status::OK(); }

  Status Reserve(const int64_t length) { return Status::OK(); }

  template <class Index>
  void ObserveNullFound(Index index) {}

  template <class Index>
  void ObserveNullNotFound(Index index) {}

  template <class Index>
  void ObserveFound(Index index) {}

  template <class Index>
  void ObserveNotFound(Index index) {}

  Status Flush(Datum* out) { return Status::OK(); }

  std::shared_ptr<DataType> out_type() const { return type_; }

  Status FlushFinal(Datum* out) { return Status::OK(); }
};

// ----------------------------------------------------------------------
// Count values implementation (see HashKernel for description of methods)

class ValueCountsAction final : ActionBase {
 public:
  using ActionBase::ActionBase;

  ValueCountsAction(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ActionBase(type, pool), count_builder_(pool) {}

  Status Reserve(const int64_t length) {
    // builder size is independent of input array size.
    return Status::OK();
  }

  Status Reset() {
    count_builder_.Reset();
    return Status::OK();
  }

  // Don't do anything on flush because we don't want to finalize the builder
  // or incur the cost of memory copies.
  Status Flush(Datum* out) { return Status::OK(); }

  std::shared_ptr<DataType> out_type() const { return type_; }

  // Return the counts corresponding the MemoTable keys.
  Status FlushFinal(Datum* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(count_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  template <class Index>
  void ObserveNullFound(Index index) {
    count_builder_[index]++;
  }

  template <class Index>
  void ObserveNullNotFound(Index index) {
    ARROW_LOG(FATAL) << "ObserveNullNotFound without err_status should not be called";
  }

  template <class Index>
  void ObserveNullNotFound(Index index, Status* status) {
    Status s = count_builder_.Append(1);
    if (ARROW_PREDICT_FALSE(!s.ok())) {
      *status = s;
    }
  }

  template <class Index>
  void ObserveFound(Index slot) {
    count_builder_[slot]++;
  }

  template <class Index>
  void ObserveNotFound(Index slot, Status* status) {
    Status s = count_builder_.Append(1);
    if (ARROW_PREDICT_FALSE(!s.ok())) {
      *status = s;
    }
  }

 private:
  Int64Builder count_builder_;
};

// ----------------------------------------------------------------------
// Dictionary encode implementation (see HashKernel for description of methods)

class DictEncodeAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  DictEncodeAction(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ActionBase(type, pool), indices_builder_(pool) {}

  Status Reset() {
    indices_builder_.Reset();
    return Status::OK();
  }

  Status Reserve(const int64_t length) { return indices_builder_.Reserve(length); }

  template <class Index>
  void ObserveNullFound(Index index) {
    indices_builder_.UnsafeAppendNull();
  }

  template <class Index>
  void ObserveNullNotFound(Index index) {
    indices_builder_.UnsafeAppendNull();
  }

  template <class Index>
  void ObserveFound(Index index) {
    indices_builder_.UnsafeAppend(index);
  }

  template <class Index>
  void ObserveNotFound(Index index) {
    ObserveFound(index);
  }

  Status Flush(Datum* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(indices_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return int32(); }
  Status FlushFinal(Datum* out) { return Status::OK(); }

 private:
  Int32Builder indices_builder_;
};

/// \brief Invoke hash table kernel on input array, returning any output
/// values. Implementations should be thread-safe
///
/// This interface is implemented below using visitor pattern on "Action"
/// implementations.  It is not consolidate to keep the contract clearer.
class HashKernel : public UnaryKernel {
 public:
  // Reset for another run.
  virtual Status Reset() = 0;
  // Prepare the Action for the given input (e.g. reserve appropriately sized
  // data structures) and visit the given input with Action.
  virtual Status Append(FunctionContext* ctx, const ArrayData& input) = 0;
  // Flush out accumulated results from the last invocation of Call.
  virtual Status Flush(Datum* out) = 0;
  // Flush out accumulated results across all invocations of Call. The kernel
  // should not be used until after Reset() is called.
  virtual Status FlushFinal(Datum* out) = 0;
  // Get the values (keys) acummulated in the dictionary so far.
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;
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

template <bool B, typename T = void>
using enable_if_t = typename std::enable_if<B, T>::type;

template <typename Type, typename Scalar, typename Action, bool with_error_status = false,
          bool with_memo_visit_null = true>
class RegularHashKernelImpl : public HashKernelImpl {
 public:
  RegularHashKernelImpl(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : pool_(pool), type_(type), action_(type, pool) {}

  Status Reset() override {
    memo_table_.reset(new MemoTable(pool_, 0));
    return action_.Reset();
  }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    return ArrayDataVisitor<Type>::Visit(arr, this);
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }

  Status FlushFinal(Datum* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return DictionaryTraits<Type>::GetDictionaryArrayData(pool_, type_, *memo_table_,
                                                          0 /* start_offset */, out);
  }

  template <typename Enable = Status>
  auto VisitNull() -> enable_if_t<!with_error_status, Enable> {
    auto on_found = [this](int32_t memo_index) { action_.ObserveNullFound(memo_index); };
    auto on_not_found = [this](int32_t memo_index) {
      action_.ObserveNullNotFound(memo_index);
    };

    if (with_memo_visit_null) {
      memo_table_->GetOrInsertNull(on_found, on_not_found);
    } else {
      action_.ObserveNullNotFound(-1);
    }
    return Status::OK();
  }

  template <typename Enable = Status>
  auto VisitNull() -> enable_if_t<with_error_status, Enable> {
    Status s = Status::OK();
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this, &s](int32_t memo_index) {
      action_.ObserveNotFound(memo_index, &s);
    };

    if (with_memo_visit_null) {
      memo_table_->GetOrInsertNull(on_found, on_not_found);
    } else {
      action_.ObserveNullNotFound(-1);
    }

    return s;
  }

  template <typename Enable = Status>
  auto VisitValue(const Scalar& value) ->
      typename std::enable_if<!with_error_status, Enable>::type {
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this](int32_t memo_index) {
      action_.ObserveNotFound(memo_index);
    };

    memo_table_->GetOrInsert(value, on_found, on_not_found);
    return Status::OK();
  }

  template <typename Enable = Status>
  auto VisitValue(const Scalar& value) ->
      typename std::enable_if<with_error_status, Enable>::type {
    Status s = Status::OK();
    auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
    auto on_not_found = [this, &s](int32_t memo_index) {
      action_.ObserveNotFound(memo_index, &s);
    };
    memo_table_->GetOrInsert(value, on_found, on_not_found);
    return s;
  }

  std::shared_ptr<DataType> out_type() const override { return action_.out_type(); }

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
      if (i == 0) {
        action_.ObserveNullNotFound(0);
      } else {
        action_.ObserveNullFound(0);
      }
    }
    return Status::OK();
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }
  Status FlushFinal(Datum* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    // TODO(wesm): handle null being a valid dictionary value
    auto null_array = std::make_shared<NullArray>(0);
    *out = null_array->data();
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return null(); }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null, typename Enable = void>
struct HashKernelTraits {};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        enable_if_null<Type>> {
  using HashKernelImpl = NullHashKernelImpl<Action>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        enable_if_has_c_type<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, typename Type::c_type, Action,
                                               with_error_status, with_memo_visit_null>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        enable_if_boolean<Type>> {
  using HashKernelImpl =
      RegularHashKernelImpl<Type, bool, Action, with_error_status, with_memo_visit_null>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        enable_if_binary<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, util::string_view, Action,
                                               with_error_status, with_memo_visit_null>;
};

template <typename Type, typename Action, bool with_error_status,
          bool with_memo_visit_null>
struct HashKernelTraits<Type, Action, with_error_status, with_memo_visit_null,
                        enable_if_fixed_size_binary<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, util::string_view, Action,
                                               with_error_status, with_memo_visit_null>;
};

}  // namespace

#define PROCESS_SUPPORTED_HASH_TYPES(PROCESS) \
  PROCESS(NullType)                           \
  PROCESS(BooleanType)                        \
  PROCESS(UInt8Type)                          \
  PROCESS(Int8Type)                           \
  PROCESS(UInt16Type)                         \
  PROCESS(Int16Type)                          \
  PROCESS(UInt32Type)                         \
  PROCESS(Int32Type)                          \
  PROCESS(UInt64Type)                         \
  PROCESS(Int64Type)                          \
  PROCESS(FloatType)                          \
  PROCESS(DoubleType)                         \
  PROCESS(Date32Type)                         \
  PROCESS(Date64Type)                         \
  PROCESS(Time32Type)                         \
  PROCESS(Time64Type)                         \
  PROCESS(TimestampType)                      \
  PROCESS(BinaryType)                         \
  PROCESS(StringType)                         \
  PROCESS(FixedSizeBinaryType)                \
  PROCESS(Decimal128Type)

Status GetUniqueKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                       std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;
  switch (type->id()) {
#define PROCESS(InType)                                                               \
  case InType::type_id:                                                               \
    kernel.reset(                                                                     \
        new                                                                           \
        typename HashKernelTraits<InType, UniqueAction, false, true>::HashKernelImpl( \
            type, ctx->memory_pool()));                                               \
    break;

    PROCESS_SUPPORTED_HASH_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }

  CHECK_IMPLEMENTED(kernel, "unique", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

Status GetDictionaryEncodeKernel(FunctionContext* ctx,
                                 const std::shared_ptr<DataType>& type,
                                 std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;

  switch (type->id()) {
#define PROCESS(InType)                                                                  \
  case InType::type_id:                                                                  \
    kernel.reset(                                                                        \
        new typename HashKernelTraits<InType, DictEncodeAction, false,                   \
                                      false>::HashKernelImpl(type, ctx->memory_pool())); \
    break;

    PROCESS_SUPPORTED_HASH_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }

#undef DICTIONARY_ENCODE_CASE

  CHECK_IMPLEMENTED(kernel, "dictionary-encode", type);
  RETURN_NOT_OK(kernel->Reset());
  *out = std::move(kernel);
  return Status::OK();
}

Status GetValueCountsKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                            std::unique_ptr<HashKernel>* out) {
  std::unique_ptr<HashKernel> kernel;

  switch (type->id()) {
#define PROCESS(InType)                                                                 \
  case InType::type_id:                                                                 \
    kernel.reset(                                                                       \
        new typename HashKernelTraits<InType, ValueCountsAction, true,                  \
                                      true>::HashKernelImpl(type, ctx->memory_pool())); \
    break;

    PROCESS_SUPPORTED_HASH_TYPES(PROCESS)
#undef PROCESS
    default:
      break;
  }

  CHECK_IMPLEMENTED(kernel, "count-values", type);
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
      ::arrow::dictionary(indices_outputs[0].array()->type, dictionary->type());

  // Create DictionaryArray for each piece yielded by the kernel invocations
  std::vector<std::shared_ptr<Array>> dict_chunks;
  for (const Datum& datum : indices_outputs) {
    dict_chunks.emplace_back(std::make_shared<DictionaryArray>(
        dict_type, MakeArray(datum.array()), dictionary));
  }

  *out = detail::WrapArraysLike(value, dict_chunks);
  return Status::OK();
}

const char kValuesFieldName[] = "values";
const char kCountsFieldName[] = "counts";
const int32_t kValuesFieldIndex = 0;
const int32_t kCountsFieldIndex = 1;
Status ValueCounts(FunctionContext* ctx, const Datum& value,
                   std::shared_ptr<Array>* counts) {
  std::unique_ptr<HashKernel> func;
  RETURN_NOT_OK(GetValueCountsKernel(ctx, value.type(), &func));

  // Calls return nothing for counts.
  std::vector<Datum> unused_output;
  std::shared_ptr<Array> uniques;
  RETURN_NOT_OK(InvokeHash(ctx, func.get(), value, &unused_output, &uniques));

  Datum value_counts;
  RETURN_NOT_OK(func->FlushFinal(&value_counts));

  auto data_type = std::make_shared<StructType>(std::vector<std::shared_ptr<Field>>{
      std::make_shared<Field>(kValuesFieldName, uniques->type()),
      std::make_shared<Field>(kCountsFieldName, int64())});
  *counts = std::make_shared<StructArray>(
      data_type, uniques->length(),
      std::vector<std::shared_ptr<Array>>{uniques, MakeArray(value_counts.array())});
  return Status::OK();
}
#undef PROCESS_SUPPORTED_HASH_TYPES
}  // namespace compute
}  // namespace arrow

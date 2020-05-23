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

#include "arrow/array/dict_internal.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/hashing.h"

namespace arrow {

class MemoryPool;

using internal::DictionaryTraits;
using internal::HashTraits;

namespace compute {

namespace {

class ActionBase {
 public:
  ActionBase(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool) {}

 protected:
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// Unique

class UniqueAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  static constexpr bool with_error_status = false;
  static constexpr bool with_memo_visit_null = true;

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

  Status FlushFinal(Datum* out) { return Status::OK(); }
};

// ----------------------------------------------------------------------
// Count values

class ValueCountsAction final : ActionBase {
 public:
  using ActionBase::ActionBase;

  static constexpr bool with_error_status = true;
  static constexpr bool with_memo_visit_null = true;

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
// Dictionary encode implementation

class DictEncodeAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  static constexpr bool with_error_status = false;
  static constexpr bool with_memo_visit_null = false;

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

  Status FlushFinal(Datum* out) { return Status::OK(); }

 private:
  Int32Builder indices_builder_;
};

class HashKernel : public KernelState {
 public:
  // Reset for another run.
  virtual Status Reset() = 0;
  // Prepare the Action for the given input (e.g. reserve appropriately sized
  // data structures) and visit the given input with Action.
  virtual Status Append(KernelContext* ctx, const ArrayData& input) = 0;
  // Flush out accumulated results from the last invocation of Call.
  virtual Status Flush(Datum* out) = 0;
  // Flush out accumulated results across all invocations of Call. The kernel
  // should not be used until after Reset() is called.
  virtual Status FlushFinal(Datum* out) = 0;
  // Get the values (keys) accumulated in the dictionary so far.
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;
};

// ----------------------------------------------------------------------
// Base class for all hash kernel implementations

class HashKernelImpl : public HashKernel {
 public:
  Status Append(KernelContext* ctx, const ArrayData& input) override {
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

template <typename Type, typename Scalar, typename Action,
          bool with_error_status = Action::with_error_status,
          bool with_memo_visit_null = Action::with_memo_visit_null>
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
    return DoAppend(arr);
  }

  Status Flush(Datum* out) override { return action_.Flush(out); }

  Status FlushFinal(Datum* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return DictionaryTraits<Type>::GetDictionaryArrayData(pool_, type_, *memo_table_,
                                                          0 /* start_offset */, out);
  }

  template <bool HasError = with_error_status>
  enable_if_t<!HasError, Status> DoAppend(const ArrayData& arr) {
    auto process_value = [this](util::optional<Scalar> v) {
      if (v.has_value()) {
        auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
        auto on_not_found = [this](int32_t memo_index) {
          action_.ObserveNotFound(memo_index);
        };

        int32_t unused_memo_index;
        return memo_table_->GetOrInsert(*v, std::move(on_found), std::move(on_not_found),
                                        &unused_memo_index);
      } else {
        // Null
        if (with_memo_visit_null) {
          auto on_found = [this](int32_t memo_index) {
            action_.ObserveNullFound(memo_index);
          };
          auto on_not_found = [this](int32_t memo_index) {
            action_.ObserveNullNotFound(memo_index);
          };
          memo_table_->GetOrInsertNull(std::move(on_found), std::move(on_not_found));
        } else {
          action_.ObserveNullNotFound(-1);
        }
        return Status::OK();
      }
    };
    return VisitArrayDataInline<Type>(arr, std::move(process_value));
  }

  template <bool HasError = with_error_status>
  enable_if_t<HasError, Status> DoAppend(const ArrayData& arr) {
    auto process_value = [this](util::optional<Scalar> v) {
      Status s = Status::OK();
      if (v.has_value()) {
        auto on_found = [this](int32_t memo_index) { action_.ObserveFound(memo_index); };
        auto on_not_found = [this, &s](int32_t memo_index) {
          action_.ObserveNotFound(memo_index, &s);
        };

        int32_t unused_memo_index;
        RETURN_NOT_OK(memo_table_->GetOrInsert(
            *v, std::move(on_found), std::move(on_not_found), &unused_memo_index));
      } else {
        // Null
        if (with_memo_visit_null) {
          auto on_found = [this](int32_t memo_index) {
            action_.ObserveNullFound(memo_index);
          };
          auto on_not_found = [this, &s](int32_t memo_index) {
            action_.ObserveNullNotFound(memo_index, &s);
          };
          memo_table_->GetOrInsertNull(std::move(on_found), std::move(on_not_found));
        } else {
          action_.ObserveNullNotFound(-1);
        }
      }
      return s;
    };
    return VisitArrayDataInline<Type>(arr, std::move(process_value));
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
struct HashKernelTraits<Type, Action, enable_if_has_string_view<Type>> {
  using HashKernelImpl = RegularHashKernelImpl<Type, util::string_view, Action>;
};

template <typename T, typename R = void>
using enable_if_can_hash =
    enable_if_t<is_null_type<T>::value || has_c_type<T>::value ||
                    is_base_binary_type<T>::value || is_fixed_size_binary_type<T>::value,
                R>;

template <typename Type, typename Action>
struct HashInitFunctor {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using HashKernelType = typename HashKernelTraits<Type, Action>::HashKernelImpl;

  static std::unique_ptr<KernelState> Init(KernelContext* ctx,
                                           const KernelInitArgs& args) {
    auto result = std::unique_ptr<HashKernel>(
        new HashKernelType(args.inputs[0].type, ctx->memory_pool()));
    ctx->SetStatus(result->Reset());
    return std::move(result);
  }
};

template <typename Action>
struct HashInitVisitor {
  VectorKernel* out;

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Hashing not available for ", type.ToString());
  }

  template <typename Type>
  enable_if_can_hash<Type, Status> Visit(const Type&) {
    out->init = HashInitFunctor<Type, Action>::Init;
    return Status::OK();
  }
};

void HashExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->Append(ctx, *batch[0].array()));
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->Flush(out));
}

void UniqueFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->GetDictionary(&uniques));
  *out = {Datum(uniques)};
}

void DictEncodeFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->GetDictionary(&uniques));
  auto dict_type = dictionary(int32(), uniques->type);
  auto dict = MakeArray(uniques);
  for (size_t i = 0; i < out->size(); ++i) {
    (*out)[i] =
        std::make_shared<DictionaryArray>(dict_type, (*out)[i].make_array(), dict);
  }
}

void ValueCountsFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->GetDictionary(&uniques));

  Datum value_counts;
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->FlushFinal(&value_counts));
  auto data_type =
      struct_({field(kValuesFieldName, uniques->type), field(kCountsFieldName, int64())});
  ArrayVector children = {MakeArray(uniques), value_counts.make_array()};
  auto result = std::make_shared<StructArray>(data_type, uniques->length, children);
  *out = {Datum(result)};
}

ValueDescr DictEncodeOutput(KernelContext*, const std::vector<ValueDescr>& descrs) {
  return ValueDescr::Array(dictionary(int32(), descrs[0].type));
}

ValueDescr ValueCountsOutput(KernelContext*, const std::vector<ValueDescr>& descrs) {
  return ValueDescr::Array(struct_(
      {field(kValuesFieldName, descrs[0].type), field(kCountsFieldName, int64())}));
}

template <typename Action>
void AddKernel(VectorFunction* func, VectorKernel kernel,
               const std::shared_ptr<DataType>& type) {
  HashInitVisitor<Action> visitor{&kernel};
  DCHECK_OK(VisitTypeInline(*type, &visitor));
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

template <typename Action>
void AddHashKernels(VectorFunction* func, VectorKernel base,
                    OutputType::Resolver out_resolver) {
  OutputType out_ty(out_resolver);
  for (const auto& ty : PrimitiveTypes()) {
    base.signature = KernelSignature::Make({InputType::Array(ty)}, out_ty);
    AddKernel<Action>(func, base, ty);
  }

  // Example parametric types that we want to match only on Type::type
  auto parametric_types = {time32(TimeUnit::SECOND), time64(TimeUnit::MICRO),
                           timestamp(TimeUnit::SECOND), fixed_size_binary(0),
                           decimal(12, 2)};
  for (const auto& ty : parametric_types) {
    base.signature = KernelSignature::Make({InputType::Array(ty->id())}, out_ty);
    AddKernel<Action>(func, base, /*dummy=*/ty);
  }
}

}  // namespace

namespace internal {

void RegisterVectorHash(FunctionRegistry* registry) {
  VectorKernel base;
  base.exec = HashExec;

  // Unique and ValueCounts output unchunked arrays

  base.finalize = UniqueFinalize;
  base.output_chunked = false;
  auto unique = std::make_shared<VectorFunction>("unique", Arity::Unary());
  AddHashKernels<UniqueAction>(unique.get(), base, /*output_type=*/FirstType);
  DCHECK_OK(registry->AddFunction(std::move(unique)));

  base.finalize = ValueCountsFinalize;
  auto value_counts = std::make_shared<VectorFunction>("value_counts", Arity::Unary());
  AddHashKernels<ValueCountsAction>(value_counts.get(), base, ValueCountsOutput);
  DCHECK_OK(registry->AddFunction(std::move(value_counts)));

  base.finalize = DictEncodeFinalize;
  base.output_chunked = true;
  auto dict_encode =
      std::make_shared<VectorFunction>("dictionary_encode", Arity::Unary());
  AddHashKernels<DictEncodeAction>(dict_encode.get(), base, DictEncodeOutput);
  DCHECK_OK(registry->AddFunction(std::move(dict_encode)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

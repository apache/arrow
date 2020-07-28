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

#include <cstring>
#include <mutex>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/dict_internal.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/util/hashing.h"
#include "arrow/util/make_unique.h"

namespace arrow {

using internal::DictionaryTraits;
using internal::HashTraits;

namespace compute {
namespace internal {

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

  // Flush out accumulated results from the last invocation of Call.
  virtual Status Flush(Datum* out) = 0;
  // Flush out accumulated results across all invocations of Call. The kernel
  // should not be used until after Reset() is called.
  virtual Status FlushFinal(Datum* out) = 0;
  // Get the values (keys) accumulated in the dictionary so far.
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;

  virtual std::shared_ptr<DataType> value_type() const = 0;

  Status Append(KernelContext* ctx, const ArrayData& input) {
    std::lock_guard<std::mutex> guard(lock_);
    return Append(input);
  }

  // Prepare the Action for the given input (e.g. reserve appropriately sized
  // data structures) and visit the given input with Action.
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
class RegularHashKernel : public HashKernel {
 public:
  RegularHashKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
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

  std::shared_ptr<DataType> value_type() const override { return type_; }

  template <bool HasError = with_error_status>
  enable_if_t<!HasError, Status> DoAppend(const ArrayData& arr) {
    return VisitArrayDataInline<Type>(
        arr,
        [this](Scalar v) {
          auto on_found = [this](int32_t memo_index) {
            action_.ObserveFound(memo_index);
          };
          auto on_not_found = [this](int32_t memo_index) {
            action_.ObserveNotFound(memo_index);
          };

          int32_t unused_memo_index;
          return memo_table_->GetOrInsert(v, std::move(on_found), std::move(on_not_found),
                                          &unused_memo_index);
        },
        [this]() {
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
        });
  }

  template <bool HasError = with_error_status>
  enable_if_t<HasError, Status> DoAppend(const ArrayData& arr) {
    return VisitArrayDataInline<Type>(
        arr,
        [this](Scalar v) {
          Status s = Status::OK();
          auto on_found = [this](int32_t memo_index) {
            action_.ObserveFound(memo_index);
          };
          auto on_not_found = [this, &s](int32_t memo_index) {
            action_.ObserveNotFound(memo_index, &s);
          };

          int32_t unused_memo_index;
          RETURN_NOT_OK(memo_table_->GetOrInsert(
              v, std::move(on_found), std::move(on_not_found), &unused_memo_index));
          return s;
        },
        [this]() {
          // Null
          Status s = Status::OK();
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
          return s;
        });
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
class NullHashKernel : public HashKernel {
 public:
  NullHashKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
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

  std::shared_ptr<DataType> value_type() const override { return type_; }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  Action action_;
};

// ----------------------------------------------------------------------
// Hashing for dictionary type

class DictionaryHashKernel : public HashKernel {
 public:
  explicit DictionaryHashKernel(std::unique_ptr<HashKernel> indices_kernel)
      : indices_kernel_(std::move(indices_kernel)) {}

  Status Reset() override { return indices_kernel_->Reset(); }

  Status HandleDictionary(const std::shared_ptr<ArrayData>& dict) {
    if (!dictionary_) {
      dictionary_ = dict;
    } else if (!MakeArray(dictionary_)->Equals(*MakeArray(dict))) {
      return Status::Invalid(
          "Only hashing for data with equal dictionaries "
          "currently supported");
    }
    return Status::OK();
  }

  Status Append(const ArrayData& arr) override {
    RETURN_NOT_OK(HandleDictionary(arr.dictionary));
    return indices_kernel_->Append(arr);
  }

  Status Flush(Datum* out) override { return indices_kernel_->Flush(out); }

  Status FlushFinal(Datum* out) override { return indices_kernel_->FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return indices_kernel_->GetDictionary(out);
  }

  std::shared_ptr<DataType> value_type() const override {
    return indices_kernel_->value_type();
  }

  std::shared_ptr<ArrayData> dictionary() const { return dictionary_; }

 private:
  std::unique_ptr<HashKernel> indices_kernel_;
  std::shared_ptr<ArrayData> dictionary_;
};

// ----------------------------------------------------------------------

template <typename Type, typename Action, typename Enable = void>
struct HashKernelTraits {};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_null<Type>> {
  using HashKernel = NullHashKernel<Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_has_c_type<Type>> {
  using HashKernel = RegularHashKernel<Type, typename Type::c_type, Action>;
};

template <typename Type, typename Action>
struct HashKernelTraits<Type, Action, enable_if_has_string_view<Type>> {
  using HashKernel = RegularHashKernel<Type, util::string_view, Action>;
};

template <typename Type, typename Action>
std::unique_ptr<HashKernel> HashInitImpl(KernelContext* ctx, const KernelInitArgs& args) {
  using HashKernelType = typename HashKernelTraits<Type, Action>::HashKernel;
  auto result = ::arrow::internal::make_unique<HashKernelType>(args.inputs[0].type,
                                                               ctx->memory_pool());
  ctx->SetStatus(result->Reset());
  return std::move(result);
}

template <typename Type, typename Action>
std::unique_ptr<KernelState> HashInit(KernelContext* ctx, const KernelInitArgs& args) {
  return std::move(HashInitImpl<Type, Action>(ctx, args));
}

template <typename Action>
KernelInit GetHashInit(Type::type type_id) {
  // ARROW-8933: Generate only a single hash kernel per physical data
  // representation
  switch (type_id) {
    case Type::NA:
      return HashInit<NullType, Action>;
    case Type::BOOL:
      return HashInit<BooleanType, Action>;
    case Type::INT8:
    case Type::UINT8:
      return HashInit<UInt8Type, Action>;
    case Type::INT16:
    case Type::UINT16:
      return HashInit<UInt16Type, Action>;
    case Type::INT32:
    case Type::UINT32:
    case Type::FLOAT:
    case Type::DATE32:
    case Type::TIME32:
      return HashInit<UInt32Type, Action>;
    case Type::INT64:
    case Type::UINT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
      return HashInit<UInt64Type, Action>;
    case Type::BINARY:
    case Type::STRING:
      return HashInit<BinaryType, Action>;
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return HashInit<LargeBinaryType, Action>;
    case Type::FIXED_SIZE_BINARY:
    case Type::DECIMAL:
      return HashInit<FixedSizeBinaryType, Action>;
    default:
      DCHECK(false);
      return nullptr;
  }
}

template <typename Action>
std::unique_ptr<KernelState> DictionaryHashInit(KernelContext* ctx,
                                                const KernelInitArgs& args) {
  const auto& dict_type = checked_cast<const DictionaryType&>(*args.inputs[0].type);
  std::unique_ptr<HashKernel> indices_hasher;
  switch (dict_type.index_type()->id()) {
    case Type::INT8:
      indices_hasher = HashInitImpl<UInt8Type, Action>(ctx, args);
      break;
    case Type::INT16:
      indices_hasher = HashInitImpl<UInt16Type, Action>(ctx, args);
      break;
    case Type::INT32:
      indices_hasher = HashInitImpl<UInt32Type, Action>(ctx, args);
      break;
    case Type::INT64:
      indices_hasher = HashInitImpl<UInt64Type, Action>(ctx, args);
      break;
    default:
      DCHECK(false) << "Unsupported dictionary index type";
      break;
  }
  return ::arrow::internal::make_unique<DictionaryHashKernel>(std::move(indices_hasher));
}

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

std::shared_ptr<ArrayData> BoxValueCounts(const std::shared_ptr<ArrayData>& uniques,
                                          const std::shared_ptr<ArrayData>& counts) {
  auto data_type =
      struct_({field(kValuesFieldName, uniques->type), field(kCountsFieldName, int64())});
  ArrayVector children = {MakeArray(uniques), MakeArray(counts)};
  return std::make_shared<StructArray>(data_type, uniques->length, children)->data();
}

void ValueCountsFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  Datum value_counts;

  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->GetDictionary(&uniques));
  KERNEL_RETURN_IF_ERROR(ctx, hash_impl->FlushFinal(&value_counts));
  *out = {Datum(BoxValueCounts(uniques, value_counts.array()))};
}

void UniqueFinalizeDictionary(KernelContext* ctx, std::vector<Datum>* out) {
  UniqueFinalize(ctx, out);
  if (ctx->HasError()) {
    return;
  }
  auto hash = checked_cast<DictionaryHashKernel*>(ctx->state());
  (*out)[0].mutable_array()->dictionary = hash->dictionary();
}

void ValueCountsFinalizeDictionary(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash = checked_cast<DictionaryHashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  Datum value_counts;
  KERNEL_RETURN_IF_ERROR(ctx, hash->GetDictionary(&uniques));
  KERNEL_RETURN_IF_ERROR(ctx, hash->FlushFinal(&value_counts));
  uniques->dictionary = hash->dictionary();
  *out = {Datum(BoxValueCounts(uniques, value_counts.array()))};
}

ValueDescr DictEncodeOutput(KernelContext*, const std::vector<ValueDescr>& descrs) {
  return ValueDescr::Array(dictionary(int32(), descrs[0].type));
}

ValueDescr ValueCountsOutput(KernelContext*, const std::vector<ValueDescr>& descrs) {
  return ValueDescr::Array(struct_(
      {field(kValuesFieldName, descrs[0].type), field(kCountsFieldName, int64())}));
}

template <typename Action>
void AddHashKernels(VectorFunction* func, VectorKernel base, OutputType out_ty) {
  for (const auto& ty : PrimitiveTypes()) {
    base.init = GetHashInit<Action>(ty->id());
    base.signature = KernelSignature::Make({InputType::Array(ty)}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }

  // Example parametric types that we want to match only on Type::type
  auto parametric_types = {time32(TimeUnit::SECOND), time64(TimeUnit::MICRO),
                           timestamp(TimeUnit::SECOND), fixed_size_binary(0)};
  for (const auto& ty : parametric_types) {
    base.init = GetHashInit<Action>(ty->id());
    base.signature = KernelSignature::Make({InputType::Array(ty->id())}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }

  base.init = GetHashInit<Action>(Type::DECIMAL);
  base.signature = KernelSignature::Make({InputType::Array(Type::DECIMAL)}, out_ty);
  DCHECK_OK(func->AddKernel(base));
}

}  // namespace

void RegisterVectorHash(FunctionRegistry* registry) {
  VectorKernel base;
  base.exec = HashExec;

  // ----------------------------------------------------------------------
  // unique

  base.finalize = UniqueFinalize;
  base.output_chunked = false;
  auto unique = std::make_shared<VectorFunction>("unique", Arity::Unary());
  AddHashKernels<UniqueAction>(unique.get(), base, OutputType(FirstType));

  // Dictionary unique
  base.init = DictionaryHashInit<UniqueAction>;
  base.finalize = UniqueFinalizeDictionary;
  base.signature =
      KernelSignature::Make({InputType::Array(Type::DICTIONARY)}, OutputType(FirstType));
  DCHECK_OK(unique->AddKernel(base));

  DCHECK_OK(registry->AddFunction(std::move(unique)));

  // ----------------------------------------------------------------------
  // value_counts

  base.finalize = ValueCountsFinalize;
  auto value_counts = std::make_shared<VectorFunction>("value_counts", Arity::Unary());
  AddHashKernels<ValueCountsAction>(value_counts.get(), base,
                                    OutputType(ValueCountsOutput));

  // Dictionary value counts
  base.init = DictionaryHashInit<ValueCountsAction>;
  base.finalize = ValueCountsFinalizeDictionary;
  base.signature = KernelSignature::Make({InputType::Array(Type::DICTIONARY)},
                                         OutputType(ValueCountsOutput));
  DCHECK_OK(value_counts->AddKernel(base));

  DCHECK_OK(registry->AddFunction(std::move(value_counts)));

  // ----------------------------------------------------------------------
  // dictionary_encode

  base.finalize = DictEncodeFinalize;
  // Unique and ValueCounts output unchunked arrays
  base.output_chunked = true;
  auto dict_encode =
      std::make_shared<VectorFunction>("dictionary_encode", Arity::Unary());
  AddHashKernels<DictEncodeAction>(dict_encode.get(), base, OutputType(DictEncodeOutput));

  // Calling dictionary_encode on dictionary input not supported, but if it
  // ends up being needed (or convenience), a kernel could be added to make it
  // a no-op

  DCHECK_OK(registry->AddFunction(std::move(dict_encode)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

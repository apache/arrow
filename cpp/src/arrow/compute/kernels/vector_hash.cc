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
#include <memory>
#include <mutex>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/dict_internal.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/result.h"
#include "arrow/util/hashing.h"

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

  UniqueAction(const std::shared_ptr<DataType>& type, const FunctionOptions* options,
               MemoryPool* pool)
      : ActionBase(type, pool) {}

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

  bool ShouldEncodeNulls() { return true; }

  Status Flush(ExecResult* out) { return Status::OK(); }

  Status FlushFinal(ExecResult* out) { return Status::OK(); }
};

// ----------------------------------------------------------------------
// Count values

class ValueCountsAction final : ActionBase {
 public:
  using ActionBase::ActionBase;

  static constexpr bool with_error_status = true;

  ValueCountsAction(const std::shared_ptr<DataType>& type, const FunctionOptions* options,
                    MemoryPool* pool)
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
  Status Flush(ExecResult* out) { return Status::OK(); }

  // Return the counts corresponding the MemoTable keys.
  Status FlushFinal(ExecResult* out) {
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

  bool ShouldEncodeNulls() const { return true; }

 private:
  Int64Builder count_builder_;
};

// ----------------------------------------------------------------------
// Dictionary encode implementation

class DictEncodeAction final : public ActionBase {
 public:
  using ActionBase::ActionBase;

  static constexpr bool with_error_status = false;

  DictEncodeAction(const std::shared_ptr<DataType>& type, const FunctionOptions* options,
                   MemoryPool* pool)
      : ActionBase(type, pool), indices_builder_(pool) {
    if (auto options_ptr = static_cast<const DictionaryEncodeOptions*>(options)) {
      encode_options_ = *options_ptr;
    }
  }

  Status Reset() {
    indices_builder_.Reset();
    return Status::OK();
  }

  Status Reserve(const int64_t length) { return indices_builder_.Reserve(length); }

  template <class Index>
  void ObserveNullFound(Index index) {
    if (encode_options_.null_encoding_behavior == DictionaryEncodeOptions::MASK) {
      indices_builder_.UnsafeAppendNull();
    } else {
      indices_builder_.UnsafeAppend(index);
    }
  }

  template <class Index>
  void ObserveNullNotFound(Index index) {
    ObserveNullFound(index);
  }

  template <class Index>
  void ObserveFound(Index index) {
    indices_builder_.UnsafeAppend(index);
  }

  template <class Index>
  void ObserveNotFound(Index index) {
    ObserveFound(index);
  }

  bool ShouldEncodeNulls() {
    return encode_options_.null_encoding_behavior == DictionaryEncodeOptions::ENCODE;
  }

  Status Flush(ExecResult* out) {
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(indices_builder_.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  Status FlushFinal(ExecResult* out) { return Status::OK(); }

 private:
  Int32Builder indices_builder_;
  DictionaryEncodeOptions encode_options_;
};

class HashKernel : public KernelState {
 public:
  HashKernel() : options_(nullptr) {}
  explicit HashKernel(const FunctionOptions* options) : options_(options) {}

  // Reset for another run.
  virtual Status Reset() = 0;

  // Flush out accumulated results from the last invocation of Call.
  virtual Status Flush(ExecResult* out) = 0;
  // Flush out accumulated results across all invocations of Call. The kernel
  // should not be used until after Reset() is called.
  virtual Status FlushFinal(ExecResult* out) = 0;
  // Get the values (keys) accumulated in the dictionary so far.
  virtual Status GetDictionary(std::shared_ptr<ArrayData>* out) = 0;

  virtual std::shared_ptr<DataType> value_type() const = 0;

  Status Append(KernelContext* ctx, const ArraySpan& input) {
    std::lock_guard<std::mutex> guard(lock_);
    return Append(input);
  }

  // Prepare the Action for the given input (e.g. reserve appropriately sized
  // data structures) and visit the given input with Action.
  virtual Status Append(const ArraySpan& arr) = 0;

 protected:
  const FunctionOptions* options_;
  std::mutex lock_;
};

// ----------------------------------------------------------------------
// Base class for all "regular" hash kernel implementations
// (NullType has a separate implementation)

template <typename Type, typename Scalar, typename Action,
          bool with_error_status = Action::with_error_status>
class RegularHashKernel : public HashKernel {
 public:
  RegularHashKernel(const std::shared_ptr<DataType>& type, const FunctionOptions* options,
                    MemoryPool* pool)
      : HashKernel(options), pool_(pool), type_(type), action_(type, options, pool) {}

  Status Reset() override {
    memo_table_.reset(new MemoTable(pool_, 0));
    return action_.Reset();
  }

  Status Append(const ArraySpan& arr) override {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    return DoAppend(arr);
  }

  Status Flush(ExecResult* out) override { return action_.Flush(out); }

  Status FlushFinal(ExecResult* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return DictionaryTraits<Type>::GetDictionaryArrayData(pool_, type_, *memo_table_,
                                                          0 /* start_offset */, out);
  }

  std::shared_ptr<DataType> value_type() const override { return type_; }

  template <bool HasError = with_error_status>
  enable_if_t<!HasError, Status> DoAppend(const ArraySpan& arr) {
    return VisitArraySpanInline<Type>(
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
          if (action_.ShouldEncodeNulls()) {
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
  enable_if_t<HasError, Status> DoAppend(const ArraySpan& arr) {
    return VisitArraySpanInline<Type>(
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
          auto on_found = [this](int32_t memo_index) {
            action_.ObserveNullFound(memo_index);
          };
          auto on_not_found = [this, &s](int32_t memo_index) {
            action_.ObserveNullNotFound(memo_index, &s);
          };
          if (action_.ShouldEncodeNulls()) {
            memo_table_->GetOrInsertNull(std::move(on_found), std::move(on_not_found));
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

template <typename Action, bool with_error_status = Action::with_error_status>
class NullHashKernel : public HashKernel {
 public:
  NullHashKernel(const std::shared_ptr<DataType>& type, const FunctionOptions* options,
                 MemoryPool* pool)
      : pool_(pool), type_(type), action_(type, options, pool) {}

  Status Reset() override { return action_.Reset(); }

  Status Append(const ArraySpan& arr) override { return DoAppend(arr); }

  template <bool HasError = with_error_status>
  enable_if_t<!HasError, Status> DoAppend(const ArraySpan& arr) {
    RETURN_NOT_OK(action_.Reserve(arr.length));
    for (int64_t i = 0; i < arr.length; ++i) {
      if (i == 0) {
        seen_null_ = true;
        action_.ObserveNullNotFound(0);
      } else {
        action_.ObserveNullFound(0);
      }
    }
    return Status::OK();
  }

  template <bool HasError = with_error_status>
  enable_if_t<HasError, Status> DoAppend(const ArraySpan& arr) {
    Status s = Status::OK();
    RETURN_NOT_OK(action_.Reserve(arr.length));
    for (int64_t i = 0; i < arr.length; ++i) {
      if (seen_null_ == false && i == 0) {
        seen_null_ = true;
        action_.ObserveNullNotFound(0, &s);
      } else {
        action_.ObserveNullFound(0);
      }
    }
    return s;
  }

  Status Flush(ExecResult* out) override { return action_.Flush(out); }
  Status FlushFinal(ExecResult* out) override { return action_.FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    std::shared_ptr<NullArray> null_array;
    if (seen_null_) {
      null_array = std::make_shared<NullArray>(1);
    } else {
      null_array = std::make_shared<NullArray>(0);
    }
    *out = null_array->data();
    return Status::OK();
  }

  std::shared_ptr<DataType> value_type() const override { return type_; }

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  bool seen_null_ = false;
  Action action_;
};

// ----------------------------------------------------------------------
// Hashing for dictionary type

class DictionaryHashKernel : public HashKernel {
 public:
  explicit DictionaryHashKernel(std::unique_ptr<HashKernel> indices_kernel,
                                std::shared_ptr<DataType> dictionary_value_type)
      : indices_kernel_(std::move(indices_kernel)),
        dictionary_value_type_(std::move(dictionary_value_type)) {}

  Status Reset() override { return indices_kernel_->Reset(); }

  Status Append(const ArraySpan& arr) override {
    auto arr_dict = arr.dictionary().ToArray();
    if (!dictionary_) {
      dictionary_ = arr_dict;
    } else if (!dictionary_->Equals(*arr_dict)) {
      // NOTE: This approach computes a new dictionary unification per chunk.
      // This is in effect O(n*k) where n is the total chunked array length and
      // k is the number of chunks (therefore O(n**2) if chunks have a fixed size).
      //
      // A better approach may be to run the kernel over each individual chunk,
      // and then hash-aggregate all results (for example sum-group-by for
      // the "value_counts" kernel).
      auto out_dict_type = dictionary_->type();
      std::shared_ptr<Buffer> transpose_map;
      std::shared_ptr<Array> out_dict;
      ARROW_ASSIGN_OR_RAISE(auto unifier, DictionaryUnifier::Make(out_dict_type));

      ARROW_CHECK_OK(unifier->Unify(*dictionary_));
      ARROW_CHECK_OK(unifier->Unify(*arr_dict, &transpose_map));
      ARROW_CHECK_OK(unifier->GetResult(&out_dict_type, &out_dict));

      dictionary_ = out_dict;
      auto transpose = reinterpret_cast<const int32_t*>(transpose_map->data());
      auto in_dict_array = arr.ToArray();
      ARROW_ASSIGN_OR_RAISE(
          auto tmp, arrow::internal::checked_cast<const DictionaryArray&>(*in_dict_array)
                        .Transpose(arr.type->GetSharedPtr(), out_dict, transpose));
      return indices_kernel_->Append(*tmp->data());
    }

    return indices_kernel_->Append(arr);
  }

  Status Flush(ExecResult* out) override { return indices_kernel_->Flush(out); }

  Status FlushFinal(ExecResult* out) override { return indices_kernel_->FlushFinal(out); }

  Status GetDictionary(std::shared_ptr<ArrayData>* out) override {
    return indices_kernel_->GetDictionary(out);
  }

  std::shared_ptr<DataType> value_type() const override {
    return indices_kernel_->value_type();
  }

  std::shared_ptr<DataType> dictionary_value_type() const {
    return dictionary_value_type_;
  }

  std::shared_ptr<Array> dictionary() const { return dictionary_; }

 private:
  std::unique_ptr<HashKernel> indices_kernel_;
  std::shared_ptr<Array> dictionary_;
  std::shared_ptr<DataType> dictionary_value_type_;
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
  using HashKernel = RegularHashKernel<Type, std::string_view, Action>;
};

template <typename Type, typename Action>
Result<std::unique_ptr<HashKernel>> HashInitImpl(KernelContext* ctx,
                                                 const KernelInitArgs& args) {
  using HashKernelType = typename HashKernelTraits<Type, Action>::HashKernel;
  auto result = std::make_unique<HashKernelType>(args.inputs[0].GetSharedPtr(),
                                                 args.options, ctx->memory_pool());
  RETURN_NOT_OK(result->Reset());
  return std::move(result);
}

template <typename Type, typename Action>
Result<std::unique_ptr<KernelState>> HashInit(KernelContext* ctx,
                                              const KernelInitArgs& args) {
  return HashInitImpl<Type, Action>(ctx, args);
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
    case Type::INTERVAL_MONTHS:
      return HashInit<UInt32Type, Action>;
    case Type::INT64:
    case Type::UINT64:
    case Type::DOUBLE:
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:
      return HashInit<UInt64Type, Action>;
    case Type::BINARY:
    case Type::STRING:
      return HashInit<BinaryType, Action>;
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return HashInit<LargeBinaryType, Action>;
    case Type::FIXED_SIZE_BINARY:
    case Type::DECIMAL128:
    case Type::DECIMAL256:
      return HashInit<FixedSizeBinaryType, Action>;
    case Type::INTERVAL_MONTH_DAY_NANO:
      return HashInit<MonthDayNanoIntervalType, Action>;
    default:
      DCHECK(false);
      return nullptr;
  }
}

using DictionaryEncodeState = OptionsWrapper<DictionaryEncodeOptions>;

template <typename Action>
Result<std::unique_ptr<KernelState>> DictionaryHashInit(KernelContext* ctx,
                                                        const KernelInitArgs& args) {
  const auto& dict_type = checked_cast<const DictionaryType&>(*args.inputs[0].type);
  Result<std::unique_ptr<HashKernel>> indices_hasher;
  switch (dict_type.index_type()->id()) {
    case Type::INT8:
    case Type::UINT8:
      indices_hasher = HashInitImpl<UInt8Type, Action>(ctx, args);
      break;
    case Type::INT16:
    case Type::UINT16:
      indices_hasher = HashInitImpl<UInt16Type, Action>(ctx, args);
      break;
    case Type::INT32:
    case Type::UINT32:
      indices_hasher = HashInitImpl<UInt32Type, Action>(ctx, args);
      break;
    case Type::INT64:
    case Type::UINT64:
      indices_hasher = HashInitImpl<UInt64Type, Action>(ctx, args);
      break;
    default:
      DCHECK(false) << "Unsupported dictionary index type";
      break;
  }
  RETURN_NOT_OK(indices_hasher);
  return std::make_unique<DictionaryHashKernel>(std::move(indices_hasher.ValueOrDie()),
                                                dict_type.value_type());
}

Status HashExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  RETURN_NOT_OK(hash_impl->Append(ctx, batch[0].array));
  RETURN_NOT_OK(hash_impl->Flush(out));
  return Status::OK();
}

Status UniqueFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  RETURN_NOT_OK(hash_impl->GetDictionary(&uniques));
  *out = {Datum(uniques)};
  return Status::OK();
}

Status DictEncodeFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  RETURN_NOT_OK(hash_impl->GetDictionary(&uniques));
  auto dict_type = dictionary(int32(), uniques->type);
  auto dict = MakeArray(uniques);
  for (size_t i = 0; i < out->size(); ++i) {
    (*out)[i] =
        std::make_shared<DictionaryArray>(dict_type, (*out)[i].make_array(), dict);
  }
  return Status::OK();
}

std::shared_ptr<ArrayData> BoxValueCounts(const std::shared_ptr<ArrayData>& uniques,
                                          const std::shared_ptr<ArrayData>& counts) {
  auto data_type =
      struct_({field(kValuesFieldName, uniques->type), field(kCountsFieldName, int64())});
  ArrayVector children = {MakeArray(uniques), MakeArray(counts)};
  return std::make_shared<StructArray>(data_type, uniques->length, children)->data();
}

Status ValueCountsFinalize(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash_impl = checked_cast<HashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;

  RETURN_NOT_OK(hash_impl->GetDictionary(&uniques));

  ExecResult result;
  RETURN_NOT_OK(hash_impl->FlushFinal(&result));
  *out = {Datum(BoxValueCounts(uniques, result.array_data()))};
  return Status::OK();
}

// Return the dictionary from the hash kernel or allocate an empty one.
// Required because on empty inputs, we don't ever see the input and
// hence have no dictionary.
Result<std::shared_ptr<ArrayData>> EnsureHashDictionary(KernelContext* ctx,
                                                        DictionaryHashKernel* hash) {
  if (hash->dictionary()) {
    return hash->dictionary()->data();
  }
  ARROW_ASSIGN_OR_RAISE(auto null, MakeArrayOfNull(hash->dictionary_value_type(),
                                                   /*length=*/0, ctx->memory_pool()));
  return null->data();
}

Status UniqueFinalizeDictionary(KernelContext* ctx, std::vector<Datum>* out) {
  RETURN_NOT_OK(UniqueFinalize(ctx, out));
  auto hash = checked_cast<DictionaryHashKernel*>(ctx->state());
  ARROW_ASSIGN_OR_RAISE((*out)[0].mutable_array()->dictionary,
                        EnsureHashDictionary(ctx, hash));
  return Status::OK();
}

Status ValueCountsFinalizeDictionary(KernelContext* ctx, std::vector<Datum>* out) {
  auto hash = checked_cast<DictionaryHashKernel*>(ctx->state());
  std::shared_ptr<ArrayData> uniques;
  ExecResult result;
  RETURN_NOT_OK(hash->GetDictionary(&uniques));
  RETURN_NOT_OK(hash->FlushFinal(&result));
  ARROW_ASSIGN_OR_RAISE(uniques->dictionary, EnsureHashDictionary(ctx, hash));
  *out = {Datum(BoxValueCounts(uniques, result.array_data()))};
  return Status::OK();
}

Result<TypeHolder> DictEncodeOutput(KernelContext*,
                                    const std::vector<TypeHolder>& types) {
  return dictionary(int32(), types[0].GetSharedPtr());
}

Result<TypeHolder> ValueCountsOutput(KernelContext*,
                                     const std::vector<TypeHolder>& types) {
  return struct_({field(kValuesFieldName, types[0].GetSharedPtr()),
                  field(kCountsFieldName, int64())});
}

template <typename Action>
void AddHashKernels(VectorFunction* func, VectorKernel base, OutputType out_ty) {
  for (const auto& ty : PrimitiveTypes()) {
    base.init = GetHashInit<Action>(ty->id());
    base.signature = KernelSignature::Make({ty}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }

  // Example parametric types that we want to match only on Type::type
  auto parametric_types = {time32(TimeUnit::SECOND), time64(TimeUnit::MICRO),
                           timestamp(TimeUnit::SECOND), duration(TimeUnit::SECOND),
                           fixed_size_binary(0)};
  for (const auto& ty : parametric_types) {
    base.init = GetHashInit<Action>(ty->id());
    base.signature = KernelSignature::Make({ty->id()}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }

  for (auto t : {Type::DECIMAL128, Type::DECIMAL256}) {
    base.init = GetHashInit<Action>(t);
    base.signature = KernelSignature::Make({t}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }

  for (const auto& ty : IntervalTypes()) {
    base.init = GetHashInit<Action>(ty->id());
    base.signature = KernelSignature::Make({ty}, out_ty);
    DCHECK_OK(func->AddKernel(base));
  }
}

const FunctionDoc unique_doc("Compute unique elements",
                             ("Return an array with distinct values.\n"
                              "Nulls are considered as a distinct value as well."),
                             {"array"});

const FunctionDoc value_counts_doc(
    "Compute counts of unique elements",
    ("For each distinct value, compute the number of times it occurs in the array.\n"
     "The result is returned as an array of `struct<input type, int64>`.\n"
     "Nulls in the input are counted and included in the output as well."),
    {"array"});

const DictionaryEncodeOptions* GetDefaultDictionaryEncodeOptions() {
  static const auto kDefaultDictionaryEncodeOptions = DictionaryEncodeOptions::Defaults();
  return &kDefaultDictionaryEncodeOptions;
}

const FunctionDoc dictionary_encode_doc(
    "Dictionary-encode array",
    ("Return a dictionary-encoded version of the input array."), {"array"},
    "DictionaryEncodeOptions");

}  // namespace

void RegisterVectorHash(FunctionRegistry* registry) {
  VectorKernel base;
  base.exec = HashExec;

  // ----------------------------------------------------------------------
  // unique

  base.finalize = UniqueFinalize;
  base.output_chunked = false;
  auto unique = std::make_shared<VectorFunction>("unique", Arity::Unary(), unique_doc);
  AddHashKernels<UniqueAction>(unique.get(), base, FirstType);

  // Dictionary unique
  base.init = DictionaryHashInit<UniqueAction>;
  base.finalize = UniqueFinalizeDictionary;
  base.signature = KernelSignature::Make({Type::DICTIONARY}, FirstType);
  DCHECK_OK(unique->AddKernel(base));

  DCHECK_OK(registry->AddFunction(std::move(unique)));

  // ----------------------------------------------------------------------
  // value_counts

  base.finalize = ValueCountsFinalize;
  auto value_counts =
      std::make_shared<VectorFunction>("value_counts", Arity::Unary(), value_counts_doc);
  AddHashKernels<ValueCountsAction>(value_counts.get(), base, ValueCountsOutput);

  // Dictionary value counts
  base.init = DictionaryHashInit<ValueCountsAction>;
  base.finalize = ValueCountsFinalizeDictionary;
  base.signature = KernelSignature::Make({Type::DICTIONARY}, ValueCountsOutput);
  DCHECK_OK(value_counts->AddKernel(base));

  DCHECK_OK(registry->AddFunction(std::move(value_counts)));

  // ----------------------------------------------------------------------
  // dictionary_encode

  base.finalize = DictEncodeFinalize;
  // Unique and ValueCounts output unchunked arrays
  base.output_chunked = true;

  auto dict_encode = std::make_shared<VectorFunction>(
      "dictionary_encode", Arity::Unary(), dictionary_encode_doc,
      GetDefaultDictionaryEncodeOptions());
  AddHashKernels<DictEncodeAction>(dict_encode.get(), base, DictEncodeOutput);

  // Calling dictionary_encode on dictionary input not supported, but if it
  // ends up being needed (or convenience), a kernel could be added to make it
  // a no-op

  DCHECK_OK(registry->AddFunction(std::move(dict_encode)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

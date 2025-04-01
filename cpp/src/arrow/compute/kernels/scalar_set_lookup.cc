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

#include "arrow/array/array_base.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/util/hashing.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::HashTraits;

namespace compute::internal {
namespace {

// This base class enables non-templated access to the value set type
struct SetLookupStateBase : public KernelState {
  std::shared_ptr<DataType> value_set_type;
};

template <typename Type>
struct SetLookupState : public SetLookupStateBase {
  explicit SetLookupState(MemoryPool* pool) : memory_pool(pool) {}

  Status Init(const SetLookupOptions& options) {
    this->null_matching_behavior = options.GetNullMatchingBehavior();
    if (options.value_set.is_array()) {
      const ArrayData& value_set = *options.value_set.array();
      memo_index_to_value_index.reserve(value_set.length);
      lookup_table =
          MemoTable(memory_pool, value_set.length);
      RETURN_NOT_OK(AddArrayValueSet(options, *options.value_set.array()));
    } else if (options.value_set.kind() == Datum::CHUNKED_ARRAY) {
      const ChunkedArray& value_set = *options.value_set.chunked_array();
      memo_index_to_value_index.reserve(value_set.length());
      lookup_table =
          MemoTable(memory_pool, value_set.length());

      int64_t offset = 0;
      for (const std::shared_ptr<Array>& chunk : value_set.chunks()) {
        RETURN_NOT_OK(AddArrayValueSet(options, *chunk->data(), offset));
        offset += chunk->length();
      }
    } else {
      return Status::Invalid("value_set should be an array or chunked array");
    }
    if (this->null_matching_behavior != SetLookupOptions::SKIP &&
        lookup_table->GetNull() >= 0) {
      null_index = memo_index_to_value_index[lookup_table->GetNull()];
    }
    value_set_type = options.value_set.type();
    return Status::OK();
  }

  Status AddArrayValueSet(const SetLookupOptions& options, const ArrayData& data,
                          int64_t start_index = 0) {
    using T = typename GetViewType<Type>::T;
    int32_t index = static_cast<int32_t>(start_index);
    auto visit_valid = [&](T v) {
      const auto memo_size = static_cast<int32_t>(memo_index_to_value_index.size());
      int32_t unused_memo_index;
      // (capture `memo_size` by value because of ARROW-17567)
      auto on_found = [&, memo_size](int32_t memo_index) {
        DCHECK_LT(memo_index, memo_size);
      };
      auto on_not_found = [&, memo_size](int32_t memo_index) {
        DCHECK_EQ(memo_index, memo_size);
        memo_index_to_value_index.push_back(index);
      };
      RETURN_NOT_OK(lookup_table->GetOrInsert(
          v, std::move(on_found), std::move(on_not_found), &unused_memo_index));
      ++index;
      return Status::OK();
    };
    auto visit_null = [&]() {
      const auto memo_size = static_cast<int32_t>(memo_index_to_value_index.size());
      auto on_found = [&, memo_size](int32_t memo_index) {
        DCHECK_LT(memo_index, memo_size);
      };
      auto on_not_found = [&, memo_size](int32_t memo_index) {
        DCHECK_EQ(memo_index, memo_size);
        memo_index_to_value_index.push_back(index);
      };
      lookup_table->GetOrInsertNull(std::move(on_found), std::move(on_not_found));
      ++index;
      return Status::OK();
    };

    return VisitArraySpanInline<Type>(data, visit_valid, visit_null);
  }

  using MemoTable = typename HashTraits<Type>::MemoTableType;
  std::optional<MemoTable> lookup_table;  // use optional for delayed initialization
  MemoryPool* memory_pool;
  // When there are duplicates in value_set, the MemoTable indices must
  // be mapped back to indices in the value_set.
  std::vector<int32_t> memo_index_to_value_index;
  int32_t null_index = -1;
  SetLookupOptions::NullMatchingBehavior null_matching_behavior;
};

template <>
struct SetLookupState<NullType> : public SetLookupStateBase {
  explicit SetLookupState(MemoryPool*) {}

  Status Init(SetLookupOptions& options) {
    null_matching_behavior = options.GetNullMatchingBehavior();
    value_set_has_null = (options.value_set.length() > 0) &&
                         this->null_matching_behavior != SetLookupOptions::SKIP;
    value_set_type = null();
    return Status::OK();
  }

  bool value_set_has_null;
  SetLookupOptions::NullMatchingBehavior null_matching_behavior;
};

// TODO: Put this concept somewhere reusable
template <int width>
struct UnsignedIntType;

template <>
struct UnsignedIntType<1> {
  using Type = UInt8Type;
};

template <>
struct UnsignedIntType<2> {
  using Type = UInt16Type;
};

template <>
struct UnsignedIntType<4> {
  using Type = UInt32Type;
};

template <>
struct UnsignedIntType<8> {
  using Type = UInt64Type;
};

// Constructing the type requires a type parameter
struct InitStateVisitor {
  KernelContext* ctx;
  SetLookupOptions options;
  TypeHolder arg_type;
  std::unique_ptr<KernelState> result;

  InitStateVisitor(KernelContext* ctx, const KernelInitArgs& args)
      : ctx(ctx),
        options(*checked_cast<const SetLookupOptions*>(args.options)),
        arg_type(args.inputs[0]) {}

  template <typename Type>
  Status Init() {
    using StateType = SetLookupState<Type>;
    result.reset(new StateType(ctx->exec_context()->memory_pool()));
    return static_cast<StateType*>(result.get())->Init(options);
  }

  Status Visit(const DataType&) { return Init<NullType>(); }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return Init<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value &&
                  !std::is_same<Type, MonthDayNanoIntervalType>::value,
              Status>
  Visit(const Type&) {
    return Init<typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return Init<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) { return Init<FixedSizeBinaryType>(); }

  Status Visit(const MonthDayNanoIntervalType& type) {
    return Init<MonthDayNanoIntervalType>();
  }

  Result<std::unique_ptr<KernelState>> GetResult() {
    if (arg_type.id() == Type::TIMESTAMP &&
        options.value_set.type()->id() == Type::TIMESTAMP) {
      // Other types will fail when casting, so no separate check is needed
      const auto& ty1 = checked_cast<const TimestampType&>(*arg_type);
      const auto& ty2 = checked_cast<const TimestampType&>(*options.value_set.type());
      if (ty1.timezone().empty() ^ ty2.timezone().empty()) {
        return Status::TypeError(
            "Cannot compare timestamp with timezone to timestamp without timezone, got: ",
            ty1, " and ", ty2);
      }
    } else if ((arg_type.id() == Type::STRING || arg_type.id() == Type::LARGE_STRING) &&
               !is_base_binary_like(options.value_set.type()->id())) {
      // This is a bit of a hack, but don't implicitly cast from a non-binary
      // type to string, since most types support casting to string and that
      // may lead to surprises. However, we do want most other implicit casts.
      return Status::TypeError("Array type doesn't match type of values set: ", *arg_type,
                               " vs ", *options.value_set.type());
    }

    if (!options.value_set.is_arraylike()) {
      return Status::Invalid("Set lookup value set must be Array or ChunkedArray");
    } else if (!options.value_set.type()->Equals(*arg_type)) {
      auto cast_result =
          Cast(options.value_set, CastOptions::Safe(arg_type.GetSharedPtr()),
               ctx->exec_context());
      if (cast_result.ok()) {
        options.value_set = *cast_result;
      } else if (CanCast(*arg_type.type, *options.value_set.type())) {
        // Avoid casting from non binary types to string like above
        // Otherwise, will try to cast input array to value set type during kernel exec
        if ((options.value_set.type()->id() == Type::STRING ||
             options.value_set.type()->id() == Type::LARGE_STRING) &&
            !is_base_binary_like(arg_type.id())) {
          return Status::TypeError("Array type doesn't match type of values set: ",
                                   *arg_type, " vs ", *options.value_set.type());
        }
      } else {
        return Status::TypeError("Array type doesn't match type of values set: ",
                                 *arg_type, " vs ", *options.value_set.type());
      }
    }

    RETURN_NOT_OK(VisitTypeInline(*options.value_set.type(), this));
    return std::move(result);
  }
};

Result<std::unique_ptr<KernelState>> InitSetLookup(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
  if (args.options == nullptr) {
    return Status::Invalid(
        "Attempted to call a set lookup function without SetLookupOptions");
  }

  return InitStateVisitor{ctx, args}.GetResult();
}

struct IndexInVisitor {
  KernelContext* ctx;
  const ArraySpan& data;
  ArraySpan* out;
  uint8_t* out_bitmap;

  IndexInVisitor(KernelContext* ctx, const ArraySpan& data, ArraySpan* out)
      : ctx(ctx), data(data), out(out), out_bitmap(out->buffers[0].data) {}

  Status Visit(const DataType& type) {
    DCHECK(false) << "IndexIn " << type;
    return Status::NotImplemented("IndexIn has no implementation with value type ", type);
  }

  Status Visit(const NullType&) {
    const auto& state = checked_cast<const SetLookupState<NullType>&>(*ctx->state());

    if (data.length != 0) {
      bit_util::SetBitsTo(out_bitmap, out->offset, out->length,
                          state.null_matching_behavior == SetLookupOptions::MATCH &&
                              state.value_set_has_null);

      // Set all values to 0, which will be unmasked only if null is in the value_set
      // and null_matching_behavior is equal to MATCH
      std::memset(out->GetValues<int32_t>(1), 0x00, out->length * sizeof(int32_t));
    }
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIndexIn(const SetLookupState<Type>& state, const ArraySpan& input) {
    using T = typename GetViewType<Type>::T;
    FirstTimeBitmapWriter bitmap_writer(out_bitmap, out->offset, out->length);
    int32_t* out_data = out->GetValues<int32_t>(1);
    VisitArraySpanInline<Type>(
        input,
        [&](T v) {
          int32_t index = state.lookup_table->Get(v);
          if (index != -1) {
            bitmap_writer.Set();

            // matching needle; output index from value_set
            *out_data++ = state.memo_index_to_value_index[index];
          } else {
            // no matching needle; output null
            bitmap_writer.Clear();
            *out_data++ = 0;
          }
          bitmap_writer.Next();
        },
        [&]() {
          if (state.null_index != -1 &&
              state.null_matching_behavior == SetLookupOptions::MATCH) {
            bitmap_writer.Set();

            // value_set included null
            *out_data++ = state.null_index;
          } else {
            // value_set does not include null; output null
            bitmap_writer.Clear();
            *out_data++ = 0;
          }
          bitmap_writer.Next();
        });
    bitmap_writer.Finish();
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIndexIn() {
    const auto& state = checked_cast<const SetLookupState<Type>&>(*ctx->state());
    if (!data.type->Equals(state.value_set_type)) {
      auto materialized_input = data.ToArrayData();
      auto cast_result = Cast(*materialized_input, state.value_set_type,
                              CastOptions::Safe(), ctx->exec_context());
      if (ARROW_PREDICT_FALSE(!cast_result.ok())) {
        if (cast_result.status().IsNotImplemented()) {
          return Status::TypeError("Array type doesn't match type of values set: ",
                                   *data.type, " vs ", *state.value_set_type);
        }
        return cast_result.status();
      }
      auto casted_input = *cast_result;
      return ProcessIndexIn(state, *casted_input.array());
    }
    return ProcessIndexIn(state, data);
  }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return ProcessIndexIn<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value &&
                  !std::is_same<Type, MonthDayNanoIntervalType>::value,
              Status>
  Visit(const Type&) {
    return ProcessIndexIn<
        typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return ProcessIndexIn<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) {
    return ProcessIndexIn<FixedSizeBinaryType>();
  }

  Status Visit(const MonthDayNanoIntervalType& type) {
    return ProcessIndexIn<MonthDayNanoIntervalType>();
  }

  Status Execute() {
    const auto& state = checked_cast<const SetLookupStateBase&>(*ctx->state());
    return VisitTypeInline(*state.value_set_type, this);
  }
};

Status ExecIndexIn(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return IndexInVisitor(ctx, batch[0].array, out->array_span_mutable()).Execute();
}

// IsIn writes the results into a preallocated boolean data bitmap
struct IsInVisitor {
  KernelContext* ctx;
  const ArraySpan& data;
  ArraySpan* out;
  uint8_t* out_boolean_bitmap;
  uint8_t* out_null_bitmap;

  IsInVisitor(KernelContext* ctx, const ArraySpan& data, ArraySpan* out)
      : ctx(ctx),
        data(data),
        out(out),
        out_boolean_bitmap(out->buffers[1].data),
        out_null_bitmap(out->buffers[0].data) {}

  Status Visit(const DataType& type) {
    DCHECK(false) << "IndexIn " << type;
    return Status::NotImplemented("IsIn has no implementation with value type ", type);
  }

  Status Visit(const NullType&) {
    const auto& state = checked_cast<const SetLookupState<NullType>&>(*ctx->state());

    if (state.null_matching_behavior == SetLookupOptions::MATCH &&
        state.value_set_has_null) {
      bit_util::SetBitsTo(out_boolean_bitmap, out->offset, out->length, true);
      bit_util::SetBitsTo(out_null_bitmap, out->offset, out->length, true);
    } else if (state.null_matching_behavior == SetLookupOptions::SKIP ||
               (!state.value_set_has_null &&
                state.null_matching_behavior == SetLookupOptions::MATCH)) {
      bit_util::SetBitsTo(out_boolean_bitmap, out->offset, out->length, false);
      bit_util::SetBitsTo(out_null_bitmap, out->offset, out->length, true);
    } else {
      bit_util::SetBitsTo(out_null_bitmap, out->offset, out->length, false);
    }
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIsIn(const SetLookupState<Type>& state, const ArraySpan& input) {
    using T = typename GetViewType<Type>::T;
    FirstTimeBitmapWriter writer_boolean(out_boolean_bitmap, out->offset, out->length);
    FirstTimeBitmapWriter writer_null(out_null_bitmap, out->offset, out->length);
    bool value_set_has_null = state.null_index != -1;
    VisitArraySpanInline<Type>(
        input,
        [&](T v) {
          if (state.lookup_table->Get(v) != -1) {  // true
            writer_boolean.Set();
            writer_null.Set();
          } else if (state.null_matching_behavior == SetLookupOptions::INCONCLUSIVE &&
                     value_set_has_null) {  // null
            writer_boolean.Clear();
            writer_null.Clear();
          } else {  // false
            writer_boolean.Clear();
            writer_null.Set();
          }
          writer_boolean.Next();
          writer_null.Next();
        },
        [&]() {
          if (state.null_matching_behavior == SetLookupOptions::MATCH &&
              value_set_has_null) {  // true
            writer_boolean.Set();
            writer_null.Set();
          } else if (state.null_matching_behavior == SetLookupOptions::SKIP ||
                     (!value_set_has_null && state.null_matching_behavior ==
                                                 SetLookupOptions::MATCH)) {  // false
            writer_boolean.Clear();
            writer_null.Set();
          } else {  // null
            writer_boolean.Clear();
            writer_null.Clear();
          }
          writer_boolean.Next();
          writer_null.Next();
        });
    writer_boolean.Finish();
    writer_null.Finish();
    return Status::OK();
  }

  template <typename Type>
  Status ProcessIsIn() {
    const auto& state = checked_cast<const SetLookupState<Type>&>(*ctx->state());

    if (!data.type->Equals(state.value_set_type)) {
      auto materialized_input = data.ToArrayData();
      auto cast_result = Cast(*materialized_input, state.value_set_type,
                              CastOptions::Safe(), ctx->exec_context());
      if (ARROW_PREDICT_FALSE(!cast_result.ok())) {
        if (cast_result.status().IsNotImplemented()) {
          return Status::TypeError("Array type doesn't match type of values set: ",
                                   *data.type, " vs ", *state.value_set_type);
        }
        return cast_result.status();
      }
      auto casted_input = *cast_result;
      return ProcessIsIn(state, *casted_input.array());
    }
    return ProcessIsIn(state, data);
  }

  template <typename Type>
  enable_if_boolean<Type, Status> Visit(const Type&) {
    return ProcessIsIn<BooleanType>();
  }

  template <typename Type>
  enable_if_t<has_c_type<Type>::value && !is_boolean_type<Type>::value &&
                  !std::is_same<Type, MonthDayNanoIntervalType>::value,
              Status>
  Visit(const Type&) {
    return ProcessIsIn<typename UnsignedIntType<sizeof(typename Type::c_type)>::Type>();
  }

  template <typename Type>
  enable_if_base_binary<Type, Status> Visit(const Type&) {
    return ProcessIsIn<typename Type::PhysicalType>();
  }

  // Handle Decimal128Type, FixedSizeBinaryType
  Status Visit(const FixedSizeBinaryType& type) {
    return ProcessIsIn<FixedSizeBinaryType>();
  }

  Status Visit(const MonthDayNanoIntervalType& type) {
    return ProcessIsIn<MonthDayNanoIntervalType>();
  }

  Status Execute() {
    const auto& state = checked_cast<const SetLookupStateBase&>(*ctx->state());
    return VisitTypeInline(*state.value_set_type, this);
  }
};

Status ExecIsIn(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return IsInVisitor(ctx, batch[0].array, out->array_span_mutable()).Execute();
}

// Unary set lookup kernels available for the following input types
//
// * Null type
// * Boolean
// * Numeric
// * Simple temporal types (date, time, timestamp)
// * Base binary types
// * Decimal

void AddBasicSetLookupKernels(ScalarKernel kernel,
                              const std::shared_ptr<DataType>& out_ty,
                              ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      kernel.signature = KernelSignature::Make({InputType(ty->id())}, out_ty);
      DCHECK_OK(func->AddKernel(kernel));
    }
  };

  AddKernels(BaseBinaryTypes());
  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());
  AddKernels(DurationTypes());
  AddKernels({month_day_nano_interval()});

  std::vector<Type::type> other_types = {Type::BOOL, Type::DECIMAL128, Type::DECIMAL256,
                                         Type::FIXED_SIZE_BINARY};
  for (auto ty : other_types) {
    kernel.signature = KernelSignature::Make({ty}, out_ty);
    DCHECK_OK(func->AddKernel(kernel));
  }
}

const FunctionDoc is_in_doc{
    "Find each element in a set of values",
    ("For each element in `values`, return true if it is found in a given\n"
     "set of values, false otherwise.\n"
     "The set of values to look for must be given in SetLookupOptions.\n"
     "By default, nulls are matched against the value set, this can be\n"
     "changed in SetLookupOptions."),
    {"values"},
    "SetLookupOptions",
    /*options_required=*/true};

const FunctionDoc is_in_meta_doc{
    "Find each element in a set of values",
    ("For each element in `values`, return true if it is found in `value_set`,\n"
     "false otherwise."),
    {"values", "value_set"}};

const FunctionDoc index_in_doc{
    "Return index of each element in a set of values",
    ("For each element in `values`, return its index in a given set of\n"
     "values, or null if it is not found there.\n"
     "The set of values to look for must be given in SetLookupOptions.\n"
     "By default, nulls are matched against the value set, this can be\n"
     "changed in SetLookupOptions."),
    {"values"},
    "SetLookupOptions",
    /*options_required=*/true};

const FunctionDoc index_in_meta_doc{
    "Return index of each element in a set of values",
    ("For each element in `values`, return its index in the `value_set`,\n"
     "or null if it is not found there."),
    {"values", "value_set"}};

// Enables calling is_in with CallFunction as though it were binary.
class IsInMetaBinary : public MetaFunction {
 public:
  IsInMetaBinary() : MetaFunction("is_in_meta_binary", Arity::Binary(), is_in_meta_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    if (options != nullptr) {
      return Status::Invalid("Unexpected options for 'is_in_meta_binary' function");
    }
    return IsIn(args[0], args[1], ctx);
  }
};

// Enables calling index_in with CallFunction as though it were binary.
class IndexInMetaBinary : public MetaFunction {
 public:
  IndexInMetaBinary()
      : MetaFunction("index_in_meta_binary", Arity::Binary(), index_in_meta_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    if (options != nullptr) {
      return Status::Invalid("Unexpected options for 'index_in_meta_binary' function");
    }
    return IndexIn(args[0], args[1], ctx);
  }
};

struct SetLookupFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* values) const override {
    EnsureDictionaryDecoded(values);
    return DispatchExact(*values);
  }
};

}  // namespace

void RegisterScalarSetLookup(FunctionRegistry* registry) {
  // IsIn writes its boolean output into preallocated memory
  {
    ScalarKernel isin_base;
    isin_base.init = InitSetLookup;
    isin_base.exec = ExecIsIn;
    isin_base.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    auto is_in = std::make_shared<SetLookupFunction>("is_in", Arity::Unary(), is_in_doc);

    AddBasicSetLookupKernels(isin_base, /*output_type=*/boolean(), is_in.get());

    isin_base.signature = KernelSignature::Make({null()}, boolean());
    DCHECK_OK(is_in->AddKernel(isin_base));
    DCHECK_OK(registry->AddFunction(is_in));

    DCHECK_OK(registry->AddFunction(std::make_shared<IsInMetaBinary>()));
  }

  // IndexIn writes its int32 output into preallocated memory
  {
    ScalarKernel index_in_base;
    index_in_base.init = InitSetLookup;
    index_in_base.exec = ExecIndexIn;
    index_in_base.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    auto index_in =
        std::make_shared<SetLookupFunction>("index_in", Arity::Unary(), index_in_doc);

    AddBasicSetLookupKernels(index_in_base, /*output_type=*/int32(), index_in.get());

    index_in_base.signature = KernelSignature::Make({null()}, int32());
    DCHECK_OK(index_in->AddKernel(index_in_base));
    DCHECK_OK(registry->AddFunction(index_in));

    DCHECK_OK(registry->AddFunction(std::make_shared<IndexInMetaBinary>()));
  }
}

}  // namespace compute::internal
}  // namespace arrow

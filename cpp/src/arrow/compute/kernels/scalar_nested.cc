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

// Vector kernels involving nested types

#include <cmath>
#include <limits>
#include "arrow/array/array_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"
#include "arrow/util/unreachable.h"

namespace arrow {

using internal::ToChars;

namespace compute {
namespace internal {
namespace {

template <typename Type, typename offset_type = typename Type::offset_type>
Status ListValueLength(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& arr = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  auto out_values = out_arr->GetValues<offset_type>(1);
  if (is_list_view(*arr.type)) {
    const auto* sizes = arr.GetValues<offset_type>(2);
    if (arr.length > 0) {
      memcpy(out_values, sizes, arr.length * sizeof(offset_type));
    }
  } else {
    const offset_type* offsets = arr.GetValues<offset_type>(1);
    // Offsets are always well-defined and monotonic, even for null values
    for (int64_t i = 0; i < arr.length; ++i) {
      *out_values++ = offsets[i + 1] - offsets[i];
    }
  }
  return Status::OK();
}

Status FixedSizeListValueLength(KernelContext* ctx, const ExecSpan& batch,
                                ExecResult* out) {
  auto width = checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();
  const ArraySpan& arr = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  int32_t* out_values = out_arr->GetValues<int32_t>(1);
  std::fill(out_values, out_values + arr.length, width);
  return Status::OK();
}

template <typename InListType>
void AddListValueLengthKernel(ScalarFunction* func,
                              const std::shared_ptr<DataType>& out_type) {
  auto in_type = {InputType(InListType::type_id)};
  ScalarKernel kernel(in_type, out_type, ListValueLength<InListType>);
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

template <>
void AddListValueLengthKernel<FixedSizeListType>(
    ScalarFunction* func, const std::shared_ptr<DataType>& out_type) {
  auto in_type = {InputType(Type::FIXED_SIZE_LIST)};
  ScalarKernel kernel(in_type, out_type, FixedSizeListValueLength);
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

void AddListValueLengthKernels(ScalarFunction* func) {
  AddListValueLengthKernel<ListType>(func, int32());
  AddListValueLengthKernel<LargeListType>(func, int64());
  AddListValueLengthKernel<ListViewType>(func, int32());
  AddListValueLengthKernel<LargeListViewType>(func, int64());
  AddListValueLengthKernel<FixedSizeListType>(func, int32());
}

const FunctionDoc list_value_length_doc{
    "Compute list lengths",
    ("`lists` must have a list-like type.\n"
     "For each non-null value in `lists`, its length is emitted.\n"
     "Null values emit a null in the output."),
    {"lists"}};

template <typename ScalarType, typename T = typename ScalarType::ValueType>
Status GetListElementIndex(const ExecValue& value, T* out) {
  if (value.is_scalar()) {
    const auto& index_scalar = value.scalar_as<ScalarType>();
    if (ARROW_PREDICT_FALSE(!index_scalar.is_valid)) {
      return Status::Invalid("Index must not be null");
    }
    *out = index_scalar.value;
  } else {
    const ArraySpan& index_array = value.array;
    if (index_array.length > 1) {
      return Status::NotImplemented(
          "list_element not yet implemented for arrays "
          "of list indices");
    }
    if (index_array.GetNullCount() > 0) {
      return Status::Invalid("Index must not contain nulls");
    }
    *out = index_array.GetValues<T>(1)[0];
  }
  if (ARROW_PREDICT_FALSE(*out < 0)) {
    return Status::Invalid("Index ", *out,
                           " is out of bounds: should be greater than or equal to 0");
  }
  return Status::OK();
}

template <typename T>
std::string ToString(const std::optional<T>& o) {
  return o.has_value() ? ToChars(*o) : "(nullopt)";
}

/// \param stop User-provided stop or the length of the input list
int64_t ListSliceLength(int64_t start, int64_t step, int64_t stop) {
  DCHECK_GE(step, 1);
  const auto size = std::max<int64_t>(stop - start, 0);
  return bit_util::CeilDiv(size, step);
}

std::optional<int64_t> EffectiveSliceStop(const ListSliceOptions& opts,
                                          const BaseListType& input_type) {
  if (!opts.stop.has_value() && input_type.id() == Type::FIXED_SIZE_LIST) {
    return checked_cast<const FixedSizeListType&>(input_type).list_size();
  }
  return opts.stop;
}

Result<TypeHolder> ListSliceOutputType(const ListSliceOptions& opts,
                                       const BaseListType& input_list_type) {
  const auto& value_type = input_list_type.field(0);
  const bool is_fixed_size_list = input_list_type.id() == Type::FIXED_SIZE_LIST;
  const auto return_fixed_size_list =
      opts.return_fixed_size_list.value_or(is_fixed_size_list);
  if (return_fixed_size_list) {
    auto stop = EffectiveSliceStop(opts, input_list_type);
    if (!stop.has_value()) {
      return Status::Invalid(
          "Unable to produce FixedSizeListArray from non-FixedSizeListArray without "
          "`stop` being set.");
    }
    if (opts.step < 1) {
      return Status::Invalid("`step` must be greater than or equal to 1, got: ",
                             opts.step);
    }
    const auto length = ListSliceLength(opts.start, opts.step, *stop);
    return fixed_size_list(value_type, static_cast<int32_t>(length));
  }
  if (is_fixed_size_list) {
    return list(value_type);
  }
  return TypeHolder{&input_list_type};
}

template <typename InListType>
struct ListSlice {
  using offset_type = typename InListType::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& opts = OptionsWrapper<ListSliceOptions>::Get(ctx);
    const ArraySpan& list_array = batch[0].array;
    const auto* list_type = checked_cast<const BaseListType*>(list_array.type);

    // Pre-conditions
    if (opts.start < 0 || (opts.stop.has_value() && opts.start > opts.stop.value())) {
      return Status::Invalid(
          "`start`(", opts.start,
          ") should be greater than or equal to 0 and not greater than `stop`(",
          ToString(opts.stop), ")");
    }
    if (opts.step < 1) {
      return Status::Invalid("`step` must be greater than or equal to 1, got: ",
                             opts.step);
    }

    auto* pool = ctx->memory_pool();
    ARROW_ASSIGN_OR_RAISE(auto output_type_holder, ListSliceOutputType(opts, *list_type));
    constexpr auto kInputTypeId = InListType::type_id;
    auto output_type = output_type_holder.GetSharedPtr();
    switch (output_type->id()) {
      // The various `if constexpr` guards below avoid generating
      // ListSlice<InListType>::BuildArray<ListBuilder> specializations
      // that will never be invoked at runtime.
      case Type::LIST:
        DCHECK(kInputTypeId == Type::LIST || kInputTypeId == Type::FIXED_SIZE_LIST);
        if constexpr (kInputTypeId == Type::LIST ||
                      kInputTypeId == Type::FIXED_SIZE_LIST) {
          return BuildArray<ListBuilder>(pool, opts, batch, output_type, out);
        }
        break;
      case Type::LARGE_LIST:
        DCHECK_EQ(kInputTypeId, Type::LARGE_LIST);
        if constexpr (kInputTypeId == Type::LARGE_LIST) {
          return BuildArray<LargeListBuilder>(pool, opts, batch, output_type, out);
        }
        break;
      case Type::FIXED_SIZE_LIST:
        // A fixed-size list can be produced from any list-like input
        // if ListSliceOptions::return_fixed_size_list is set to true
        return BuildArray<FixedSizeListBuilder>(pool, opts, batch, output_type, out);
      case Type::LIST_VIEW:
        DCHECK_EQ(kInputTypeId, Type::LIST_VIEW);
        if constexpr (kInputTypeId == Type::LIST_VIEW) {
          return BuildArray<ListViewBuilder>(pool, opts, batch, output_type, out);
        }
        break;
      case Type::LARGE_LIST_VIEW:
        DCHECK_EQ(kInputTypeId, Type::LARGE_LIST_VIEW);
        if constexpr (kInputTypeId == Type::LARGE_LIST_VIEW) {
          return BuildArray<LargeListViewBuilder>(pool, opts, batch, output_type, out);
        }
        break;
      default:
        break;
    }
    Unreachable();
    return Status::OK();
  }

  /// \brief Builds the array of list slices from the input list array
  template <typename BuilderType>
  static Status BuildArray(MemoryPool* pool, const ListSliceOptions& opts,
                           const ExecSpan& batch,
                           const std::shared_ptr<DataType>& output_type,
                           ExecResult* out) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(pool, output_type, &builder));
    auto* list_builder = checked_cast<BuilderType*>(builder.get());
    RETURN_NOT_OK(list_builder->Resize(batch[0].array.length));
    if constexpr (std::is_same_v<InListType, FixedSizeListType>) {
      RETURN_NOT_OK(BuildArrayFromFixedSizeListType(opts.start, opts.step, opts.stop,
                                                    batch, list_builder));
    } else {
      RETURN_NOT_OK(BuildArrayFromVarLenListLikeType(opts.start, opts.step, opts.stop,
                                                     batch, list_builder));
    }
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(list_builder->FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }

  template <typename BuilderType>
  static Status BuildArrayFromFixedSizeListType(int64_t start, int64_t step,
                                                std::optional<int64_t> stop,
                                                const ExecSpan& batch,
                                                BuilderType* out_list_builder) {
    static_assert(std::is_same_v<InListType, FixedSizeListType>);
    constexpr bool kIsFixedSizeOutput = std::is_same_v<BuilderType, FixedSizeListBuilder>;
    const auto& fsl_type = checked_cast<const FixedSizeListType&>(*batch[0].type());
    const ArraySpan& list_array = batch[0].array;
    const ArraySpan& values_array = list_array.child_data[0];
    ArrayBuilder* value_builder = out_list_builder->value_builder();

    auto* is_valid = list_array.GetValues<uint8_t>(0, 0);
    const auto list_size = static_cast<int64_t>(fsl_type.list_size());
    const int64_t effective_stop = stop.value_or(list_size);
    int64_t slice_length, value_count;
    int64_t null_padding = 0;
    if constexpr (kIsFixedSizeOutput) {
      if (list_size < effective_stop) {
        slice_length = ListSliceLength(start, step, effective_stop);
        value_count = ListSliceLength(start, step, list_size);
        DCHECK_LE(value_count, slice_length);
        null_padding = slice_length - value_count;
      } else {
        slice_length = ListSliceLength(start, step, effective_stop);
        value_count = slice_length;
      }
    } else {
      slice_length = ListSliceLength(start, step, std::min(list_size, effective_stop));
      value_count = slice_length;
    }
    int64_t offset = list_array.offset * list_size;
    for (int64_t i = 0; i < list_array.length; ++i) {
      if (is_valid && !bit_util::GetBit(is_valid, list_array.offset + i)) {
        RETURN_NOT_OK(out_list_builder->AppendNull());
      } else {
        int64_t start_offset = offset + start;
        RETURN_NOT_OK(AppendListSliceDimensions<kIsFixedSizeOutput>(slice_length,
                                                                    out_list_builder));
        RETURN_NOT_OK(AppendListSliceValues(start_offset, step, value_count, null_padding,
                                            values_array, value_builder));
      }
      offset += list_size;
    }
    return Status::OK();
  }

  template <typename BuilderType>
  static Status BuildArrayFromVarLenListLikeType(int64_t start, int64_t step,
                                                 std::optional<int64_t> stop,
                                                 const ExecSpan& batch,
                                                 BuilderType* out_list_builder) {
    constexpr bool kIsListViewInput = is_list_view(InListType::type_id);
    constexpr bool kIsFixedSizeOutput = std::is_same_v<BuilderType, FixedSizeListBuilder>;
    const ArraySpan& list_array = batch[0].array;
    const ArraySpan& values_array = list_array.child_data[0];
    ArrayBuilder* value_builder = out_list_builder->value_builder();

    const auto* is_valid = list_array.GetValues<uint8_t>(0, 0);
    const auto* offsets = list_array.GetValues<offset_type>(1);
    const offset_type* sizes = nullptr;
    if constexpr (kIsListViewInput) {
      sizes = list_array.GetValues<offset_type>(2);
    }
    for (int64_t i = 0; i < list_array.length; ++i) {
      const offset_type offset = offsets[i];
      const int64_t list_size = kIsListViewInput ? sizes[i] : offsets[i + 1] - offset;
      if (is_valid && !bit_util::GetBit(is_valid, list_array.offset + i)) {
        RETURN_NOT_OK(out_list_builder->AppendNull());
      } else {
        int64_t effective_stop = stop.value_or(list_size);
        int64_t slice_length, value_count;
        int64_t null_padding = 0;
        if constexpr (kIsFixedSizeOutput) {
          if (list_size < effective_stop) {
            slice_length = ListSliceLength(start, step, effective_stop);
            value_count = ListSliceLength(start, step, list_size);
            DCHECK_LE(value_count, slice_length);
            null_padding = slice_length - value_count;
          } else {
            slice_length = ListSliceLength(start, step, effective_stop);
            value_count = slice_length;
          }
        } else {
          slice_length =
              ListSliceLength(start, step, std::min(list_size, effective_stop));
          value_count = slice_length;
        }
        RETURN_NOT_OK(AppendListSliceDimensions<kIsFixedSizeOutput>(slice_length,
                                                                    out_list_builder));
        RETURN_NOT_OK(AppendListSliceValues(offset + start, step, value_count,
                                            null_padding, values_array, value_builder));
      }
    }
    return Status::OK();
  }

  template <bool kIsFixedSizeOutput, typename BuilderType>
  static Status AppendListSliceDimensions(int64_t slice_length,
                                          BuilderType* out_list_builder) {
    if constexpr (kIsFixedSizeOutput) {
      DCHECK_EQ(out_list_builder->type()->id(), Type::FIXED_SIZE_LIST);
      return out_list_builder->Append();
    } else {
      return out_list_builder->Append(/*is_valid=*/true, slice_length);
    }
  }

  /// \param value_count The pre-validated number of values to append starting
  ///                    from `start_offset` with a step of `step`
  /// \param null_padding The number of nulls to append after the values
  static Status AppendListSliceValues(int64_t start_offset, int64_t step,
                                      int64_t value_count, int64_t null_padding,
                                      const ArraySpan& values_array,
                                      ArrayBuilder* out_value_builder) {
    if (step == 1) {
      RETURN_NOT_OK(
          out_value_builder->AppendArraySlice(values_array, start_offset, value_count));
    } else {
      auto cursor_offset = start_offset;
      for (int64_t i = 0; i < value_count; i++) {
        RETURN_NOT_OK(
            out_value_builder->AppendArraySlice(values_array, cursor_offset, 1));
        cursor_offset += step;
      }
    }
    if (null_padding > 0) {
      RETURN_NOT_OK(out_value_builder->AppendNulls(null_padding));
    }
    return Status::OK();
  }
};

Result<TypeHolder> MakeListSliceResolve(KernelContext* ctx,
                                        const std::vector<TypeHolder>& types) {
  const auto& opts = OptionsWrapper<ListSliceOptions>::Get(ctx);
  const auto* list_type = checked_cast<const BaseListType*>(types[0].type);
  return ListSliceOutputType(opts, *list_type);
}

template <typename InListType>
void AddListSliceKernels(ScalarFunction* func) {
  auto inputs = {InputType(InListType::type_id)};
  auto output = OutputType{MakeListSliceResolve};
  ScalarKernel kernel(inputs, output, ListSlice<InListType>::Exec,
                      OptionsWrapper<ListSliceOptions>::Init);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

void AddListSliceKernels(ScalarFunction* func) {
  AddListSliceKernels<ListType>(func);
  AddListSliceKernels<LargeListType>(func);
  AddListSliceKernels<FixedSizeListType>(func);
  AddListSliceKernels<ListViewType>(func);
  AddListSliceKernels<LargeListViewType>(func);
}

const FunctionDoc list_slice_doc(
    "Compute slice of list-like array",
    ("`lists` must have a list-like type.\n"
     "For each list element, compute a slice, returning a new list array.\n"
     "A variable or fixed size list array is returned, depending on options."),
    {"lists"}, "ListSliceOptions",
    /*options_required=*/true);

template <typename Type, typename IndexType>
struct ListElement {
  using ListArrayType = typename TypeTraits<Type>::ArrayType;
  using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;
  using IndexValueType = typename IndexScalarType::ValueType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& list = batch[0].array;
    const ArraySpan& list_values = list.child_data[0];
    const offset_type* offsets = list.GetValues<offset_type>(1);

    IndexValueType index = 0;
    RETURN_NOT_OK(GetListElementIndex<IndexScalarType>(batch[1], &index));

    std::unique_ptr<ArrayBuilder> builder;

    const Type* list_type = checked_cast<const Type*>(list.type);
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), list_type->value_type(), &builder));
    RETURN_NOT_OK(builder->Reserve(list.length));
    for (int i = 0; i < list.length; ++i) {
      if (list.IsNull(i)) {
        RETURN_NOT_OK(builder->AppendNull());
        continue;
      }

      const offset_type value_offset = offsets[i];
      const offset_type value_length = offsets[i + 1] - offsets[i];
      if (ARROW_PREDICT_FALSE(index >=
                              static_cast<typename IndexType::c_type>(value_length))) {
        return Status::Invalid("Index ", index, " is out of bounds: should be in [0, ",
                               value_length, ")");
      }
      RETURN_NOT_OK(builder->AppendArraySlice(list_values, value_offset + index, 1));
    }
    ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
    out->value = result->data();
    return Status::OK();
  }
};

template <typename Type, typename IndexType>
struct FixedSizeListElement {
  using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;
  using IndexValueType = typename IndexScalarType::ValueType;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto item_size = checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();
    const ArraySpan& list = batch[0].array;
    const ArraySpan& list_values = list.child_data[0];

    IndexValueType index = 0;
    RETURN_NOT_OK(GetListElementIndex<IndexScalarType>(batch[1], &index));

    std::unique_ptr<ArrayBuilder> builder;

    const Type* list_type = checked_cast<const Type*>(list.type);
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), list_type->value_type(), &builder));
    RETURN_NOT_OK(builder->Reserve(list.length));
    for (int i = 0; i < list.length; ++i) {
      if (list.IsNull(i)) {
        RETURN_NOT_OK(builder->AppendNull());
        continue;
      }
      if (ARROW_PREDICT_FALSE(index >=
                              static_cast<typename IndexType::c_type>(item_size))) {
        return Status::Invalid("Index ", index, " is out of bounds: should be in [0, ",
                               item_size, ")");
      }
      RETURN_NOT_OK(builder->AppendArraySlice(list_values,
                                              (list.offset + i) * item_size + index, 1));
    }
    ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
    out->value = result->data();
    return Status::OK();
  }
};

template <typename InListType, template <typename...> class Functor>
void AddListElementKernels(ScalarFunction* func) {
  for (const auto& index_type : IntTypes()) {
    auto inputs = {InputType(InListType::type_id), InputType(index_type)};
    auto output = OutputType{ListValuesType};
    auto sig = KernelSignature::Make(std::move(inputs), std::move(output),
                                     /*is_varargs=*/false);
    auto scalar_exec = GenerateInteger<Functor, InListType>({index_type->id()});
    ScalarKernel kernel{std::move(sig), std::move(scalar_exec)};
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
}

void AddListElementKernels(ScalarFunction* func) {
  AddListElementKernels<ListType, ListElement>(func);
  AddListElementKernels<LargeListType, ListElement>(func);
  AddListElementKernels<ListViewType, ListElement>(func);
  AddListElementKernels<LargeListViewType, ListElement>(func);
  AddListElementKernels<FixedSizeListType, FixedSizeListElement>(func);
}

const FunctionDoc list_element_doc(
    "Compute elements using of nested list values using an index",
    ("`lists` must have a list-like type.\n"
     "For each value in each list of `lists`, the element at `index`\n"
     "is emitted. Null values emit a null in the output."),
    {"lists", "index"});

// The types "equal" compares `values` and `queries` as, raising the errors it would.
// float16, which "equal" lacks, is compared as float32, and binary views as large
// binary.
Result<std::vector<TypeHolder>> ComparableTypes(
    KernelContext* ctx, const std::shared_ptr<DataType>& values_type,
    const std::shared_ptr<DataType>& queries_type) {
  std::vector<TypeHolder> types;
  for (const auto& type : {values_type, queries_type}) {
    types.emplace_back(type->id() == Type::HALF_FLOAT ? float32() : type);
  }
  ARROW_ASSIGN_OR_RAISE(auto equal,
                        ctx->exec_context()->func_registry()->GetFunction("equal"));
  RETURN_NOT_OK(equal->DispatchBest(&types));
  RETURN_NOT_OK(ValidateComparison(*types[0], *types[1]));
  if (types[0].id() == Type::FIXED_SIZE_BINARY && *types[0] != *types[1]) {
    // Fixed-size binary values of different widths differ, as binary values do
    types = {binary(), binary()};
  }
  for (const auto& [from, to] :
       {std::pair(values_type, &types[0]), std::pair(queries_type, &types[1])}) {
    if (is_binary_view_like(to->id())) {
      *to = large_binary();
    }
    // Raise also where there is nothing to cast, e.g. for null queries
    if (!from->Equals(**to) && !CanCast(*from, **to)) {
      return Status::NotImplemented("Unsupported cast from ", *from, " to ", **to);
    }
  }
  return types;
}

struct ListContainsState : public KernelState {
  explicit ListContainsState(Result<std::vector<TypeHolder>> types)
      : types(std::move(types)) {}

  // The result of ComparableTypes, raised for non-null queries only
  Result<std::vector<TypeHolder>> types;
};

// Compare types once, raising even if there is nothing to search, like for null lists.
// Null queries are matched without "equal", but must be comparable too unless they
// are null-typed or of the values' type.
Result<std::unique_ptr<KernelState>> InitListContains(KernelContext* ctx,
                                                      const KernelInitArgs& args) {
  const auto& values_type =
      checked_cast<const BaseListType&>(*args.inputs[0]).value_type();
  const auto queries_type = args.inputs[1].GetSharedPtr();
  auto types = ComparableTypes(ctx, values_type, queries_type);
  if (queries_type->id() != Type::NA && !queries_type->Equals(*values_type)) {
    RETURN_NOT_OK(types.status());
  }
  return std::make_unique<ListContainsState>(std::move(types));
}

Status CastTo(KernelContext* ctx, const TypeHolder& type, Datum* datum) {
  if (!datum->type()->Equals(*type)) {
    ARROW_ASSIGN_OR_RAISE(*datum,
                          Cast(*datum, type, CastOptions::Safe(), ctx->exec_context()));
  }
  return Status::OK();
}

// Calls `visit` with a function telling whether value j equals query k, both of the
// same type. Unlike "equal", NaN equals NaN.
template <typename Visit>
Status VisitEqual(const ArraySpan& values, const ArraySpan& queries, Visit&& visit) {
  auto numbers = [&](auto zero) {
    using T = decltype(zero);
    const T* v = values.GetValues<T>(1);
    const T* q = queries.GetValues<T>(1);
    // `x != x` only holds for NaN
    return visit([v, q](int64_t j, int64_t k) {
      return v[j] == q[k] || (q[k] != q[k] && v[j] != v[j]);
    });
  };
  auto binary = [&](auto zero) {
    using T = decltype(zero);
    const T* vo = values.GetValues<T>(1);
    const T* qo = queries.GetValues<T>(1);
    const uint8_t* vd = values.buffers[2].data;
    const uint8_t* qd = queries.buffers[2].data;
    return visit([=](int64_t j, int64_t k) {
      const T length = vo[j + 1] - vo[j];
      return length == qo[k + 1] - qo[k] &&
             std::memcmp(vd + vo[j], qd + qo[k], length) == 0;
    });
  };
  switch (values.type->id()) {
    case Type::BOOL:
      return visit([&](int64_t j, int64_t k) {
        return bit_util::GetBit(values.buffers[1].data, values.offset + j) ==
               bit_util::GetBit(queries.buffers[1].data, queries.offset + k);
      });
    case Type::FLOAT:
      return numbers(float{});
    case Type::DOUBLE:
      return numbers(double{});
    case Type::BINARY:
    case Type::STRING:
      return binary(int32_t{});
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
      return binary(int64_t{});
    default:
      break;
  }
  // Integers, temporal types, decimals and fixed-size binary compare bitwise
  switch (const int width = values.type->bit_width() / 8) {
    case 0:
      // Only zero-width fixed-size binary, whose values all equal each other
      return visit([](int64_t, int64_t) { return true; });
    case 1:
      return numbers(uint8_t{});
    case 2:
      return numbers(uint16_t{});
    case 4:
      return numbers(uint32_t{});
    case 8:
      return numbers(uint64_t{});
    default: {
      if (width < 0) {
        return Status::NotImplemented("list_contains for ", *values.type);
      }
      const uint8_t* v = values.buffers[1].data + values.offset * width;
      const uint8_t* q = queries.buffers[1].data + queries.offset * width;
      return visit([=](int64_t j, int64_t k) {
        return std::memcmp(v + j * width, q + k * width, width) == 0;
      });
    }
  }
}

// Returns a function giving the (child offset, length) of list i, with no values for
// null list views
template <typename Type>
auto GetListRanges(const ArraySpan& list) {
  using Range = std::pair<int64_t, int64_t>;
  if constexpr (std::is_same_v<Type, FixedSizeListType>) {
    const int64_t width = checked_cast<const FixedSizeListType&>(*list.type).list_size();
    return [&list, width](int64_t i) { return Range((list.offset + i) * width, width); };
  } else {
    const auto* offsets = list.GetValues<typename Type::offset_type>(1);
    if constexpr (is_list_view_type<Type>::value) {
      const auto* sizes = list.GetValues<typename Type::offset_type>(2);
      // Reading the validity bit directly keeps this cheap enough to be inlined
      const uint8_t* validity = list.MayHaveNulls() ? list.buffers[0].data : nullptr;
      return [offsets, sizes, validity, offset = list.offset](int64_t i) {
        return Range(
            offsets[i],
            validity == nullptr || bit_util::GetBit(validity, offset + i) ? sizes[i] : 0);
      };
    } else {
      return
          [offsets](int64_t i) { return Range(offsets[i], offsets[i + 1] - offsets[i]); };
    }
  }
}

// Cast `values`, the child values from `values_start` on, to `type`. Values that no list
// references, like those of null lists, may not convert, so then they are cast as nulls.
template <typename Type>
Status CastListValues(KernelContext* ctx, const ArraySpan& list, int64_t values_start,
                      const TypeHolder& type, Datum* values) {
  if (CastTo(ctx, type, values).ok()) {
    return Status::OK();
  }
  auto data = values->array()->Copy();
  ARROW_ASSIGN_OR_RAISE(auto validity, AllocateEmptyBitmap(data->offset + data->length,
                                                           ctx->memory_pool()));
  const auto list_range = GetListRanges<Type>(list);
  for (int64_t i = 0; i < list.length; ++i) {
    if (list.IsValid(i)) {
      const auto [start, length] = list_range(i);
      bit_util::SetBitsTo(validity->mutable_data(), data->offset + start - values_start,
                          length, true);
    }
  }
  if (data->MayHaveNulls()) {
    arrow::internal::BitmapAnd(validity->data(), data->offset, data->buffers[0]->data(),
                               data->offset, data->length, data->offset,
                               validity->mutable_data());
  }
  data->buffers[0] = std::move(validity);
  data->null_count = kUnknownNullCount;
  *values = std::move(data);
  return CastTo(ctx, type, values);
}

// Whether lists are searched in full or in chunks is decided once per batch, from the
// mean number of values per list: below kShortListLength, scanning all values without
// branching on the data is fastest. Longer lists are searched kSearchChunkLength values
// at a time, which compile to vector compares, stopping after the first chunk holding a
// match. Both were tuned on int64 values, with lists of 3 to 1000 values and matches at
// their start, middle or end (see GH-51611).
constexpr int64_t kShortListLength = 32;
constexpr int64_t kSearchChunkLength = 16;

// Search each list for its query: `matches(j, i)` tells whether child value j matches
// the query of list i, relative to the first child value referenced. If given,
// `confirm` is the exact check, and `matches` a cheaper superset of it that long lists
// are searched with first.
template <typename ListRanges, typename Matches, typename Confirm = std::nullptr_t>
void SearchLists(ListRanges list_range, int64_t num_lists, int64_t values_start,
                 int64_t values_length, ArraySpan* out_arr, Matches&& matches,
                 Confirm&& confirm = nullptr) {
  constexpr bool kConfirm = !std::is_same_v<std::decay_t<Confirm>, std::nullptr_t>;
  auto emit = [&](auto&& found_in) {
    int64_t i = 0;
    arrow::internal::GenerateBitsUnrolled(out_arr->buffers[1].data, out_arr->offset,
                                          out_arr->length, [&] {
                                            auto [start, length] = list_range(i);
                                            start -= values_start;
                                            return found_in(start, start + length, i++);
                                          });
  };
  if (values_length < 32 * num_lists) {
    // Short lists are faster to check in full, without branching on the data
    auto exact = [&](int64_t j, int64_t i) {
      if constexpr (kConfirm) {
        return confirm(j, i);
      } else {
        return matches(j, i);
      }
    };
    emit([&](int64_t start, int64_t end, int64_t i) {
      bool found = false;
      for (int64_t j = start; j < end; ++j) found |= exact(j, i);
      return found;
    });
  } else {
    // Stop after the first chunk of 16 values holding a match
    auto any = [](auto&& predicate, int64_t start, int64_t end, int64_t i) {
      bool found = false;
      for (int64_t chunk = start; chunk < end && !found; chunk += 16) {
        for (int64_t j = chunk; j < std::min(chunk + 16, end); ++j) {
          found |= predicate(j, i);
        }
      }
      return found;
    };
    emit([&](int64_t start, int64_t end, int64_t i) {
      if constexpr (kConfirm) {
        return any(matches, start, end, i) && any(confirm, start, end, i);
      } else {
        return any(matches, start, end, i);
      }
    });
  }
}

// Look up each query in the values of a single list
Status ScalarListContains(KernelContext* ctx,
                          const Result<std::vector<TypeHolder>>& types,
                          const BaseListScalar& list, const ArraySpan& query_span,
                          ArraySpan* out_arr) {
  Datum queries = query_span.ToArrayData();
  const bool null_queries = queries.null_count() == queries.length();
  if (!null_queries) {
    // Raise even for a null list, like for list arrays
    RETURN_NOT_OK(types.status());
  }
  bit_util::SetBitsTo(out_arr->buffers[0].data, out_arr->offset, out_arr->length,
                      list.is_valid);
  out_arr->null_count = list.is_valid ? 0 : out_arr->length;
  if (!list.is_valid) {
    return Status::OK();
  }
  Datum values = list.value;
  if (null_queries) {
    bit_util::SetBitsTo(out_arr->buffers[1].data, out_arr->offset, out_arr->length,
                        values.array()->ComputeLogicalNullCount() > 0);
    return Status::OK();
  }
  RETURN_NOT_OK(CastTo(ctx, (*types)[0], &values));
  RETURN_NOT_OK(CastTo(ctx, (*types)[1], &queries));
  ExecContext* exec_ctx = ctx->exec_context();
  if (is_floating(values.type()->id())) {
    // "is_in" tells -0.0 from 0.0 and NaNs with different payloads apart, so turn
    // -0.0 into 0.0 and all NaNs into one
    ARROW_ASSIGN_OR_RAISE(auto zero, MakeScalar(values.type(), 0.0));
    ARROW_ASSIGN_OR_RAISE(auto nan, MakeScalar(values.type(), std::nan("")));
    for (Datum* datum : {&values, &queries}) {
      ARROW_ASSIGN_OR_RAISE(*datum, CallFunction("add", {*datum, zero}, exec_ctx));
      ARROW_ASSIGN_OR_RAISE(Datum is_nan, CallFunction("is_nan", {*datum}, exec_ctx));
      ARROW_ASSIGN_OR_RAISE(*datum,
                            CallFunction("if_else", {is_nan, nan, *datum}, exec_ctx));
    }
  }
  const SetLookupOptions options(values);
  ARROW_ASSIGN_OR_RAISE(Datum found,
                        CallFunction("is_in", {queries}, &options, exec_ctx));
  const ArrayData& found_data = *found.array();
  arrow::internal::CopyBitmap(found_data.buffers[1]->data(), found_data.offset,
                              found_data.length, out_arr->buffers[1].data,
                              out_arr->offset);
  return Status::OK();
}

template <typename Type>
Status ListContains(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* out_arr = out->array_span_mutable();
  if (batch.length == 0) {
    return Status::OK();
  }
  const auto& state_types = checked_cast<const ListContainsState&>(*ctx->state()).types;
  if (batch[0].is_scalar()) {
    // A scalar list is only paired with an array of queries here
    return ScalarListContains(ctx, state_types,
                              checked_cast<const BaseListScalar&>(*batch[0].scalar),
                              batch[1].array, out_arr);
  }
  const ArraySpan& list = batch[0].array;

  // Only null lists emit a null
  if (list.MayHaveNulls()) {
    arrow::internal::CopyBitmap(list.buffers[0].data, list.offset, list.length,
                                out_arr->buffers[0].data, out_arr->offset);
  } else {
    bit_util::SetBitsTo(out_arr->buffers[0].data, out_arr->offset, out_arr->length, true);
  }
  out_arr->null_count = list.null_count;

  // The child values referenced by the lists, in any order for list views
  auto list_range = GetListRanges<Type>(list);
  int64_t values_start = 0;
  int64_t values_end = list.child_data[0].length;
  if constexpr (!is_list_view_type<Type>::value) {
    values_start = list_range(0).first;
    values_end = list_range(list.length - 1).first + list_range(list.length - 1).second;
  }
  Datum values =
      list.child_data[0].ToArrayData()->Slice(values_start, values_end - values_start);
  auto search = [&](auto&&... predicates) {
    SearchLists(list_range, list.length, values_start, values.length(), out_arr,
                predicates...);
  };

  // A scalar query is a single query for all lists
  const bool scalar_query = batch[1].is_scalar();
  Datum queries;
  if (scalar_query) {
    ARROW_ASSIGN_OR_RAISE(queries,
                          MakeArrayFromScalar(*batch[1].scalar, 1, ctx->memory_pool()));
  } else {
    queries = batch[1].array.ToArrayData();
  }
  const int64_t query_step = scalar_query ? 0 : 1;

  if (queries.null_count() == queries.length()) {
    if (values.type()->id() == arrow::Type::DICTIONARY) {
      // Values are also null where their dictionary is
      RETURN_NOT_OK(
          CastTo(ctx, checked_cast<const DictionaryType&>(*values.type()).value_type(),
                 &values));
    }
    const ArraySpan v(*values.array());
    search([&](int64_t j, int64_t) { return v.IsNull(j); });
    return Status::OK();
  }
  RETURN_NOT_OK(state_types.status());
  const auto& types = *state_types;
  if (!values.type()->Equals(*types[0])) {
    if constexpr (is_list_view_type<Type>::value) {
      // Only cast the span of values that views reference
      values_start = std::numeric_limits<int64_t>::max();
      values_end = 0;
      for (int64_t i = 0; i < list.length; ++i) {
        const auto [start, length] = list_range(i);
        if (length > 0) {
          values_start = std::min(values_start, start);
          values_end = std::max(values_end, start + length);
        }
      }
      values_start = std::min(values_start, values_end);
      values = list.child_data[0].ToArrayData()->Slice(values_start,
                                                       values_end - values_start);
    }
    RETURN_NOT_OK(CastListValues<Type>(ctx, list, values_start, types[0], &values));
  }
  RETURN_NOT_OK(CastTo(ctx, types[1], &queries));
  const ArraySpan v(*values.array());
  const ArraySpan q(*queries.array());
  return VisitEqual(v, q, [&](auto&& equal) {
    // Null checks are only paid for where there are nulls
    if (q.MayHaveNulls()) {
      search([&](int64_t j, int64_t i) {
        const int64_t k = i * query_step;
        return q.IsNull(k) ? v.IsNull(j) : v.IsValid(j) && equal(j, k);
      });
    } else if (v.MayHaveNulls()) {
      // Null values may hold anything, so lists found are searched again for them
      const uint8_t* validity = v.buffers[0].data;
      search([&](int64_t j, int64_t i) { return equal(j, i * query_step); },
             [&, validity, offset = v.offset](int64_t j, int64_t i) {
               // `&` rather than `&&`, so short lists don't branch on the data
               return (static_cast<int>(bit_util::GetBit(validity, offset + j)) &
                       static_cast<int>(equal(j, i * query_step))) != 0;
             });
    } else {
      search([&](int64_t j, int64_t i) { return equal(j, i * query_step); });
    }
    return Status::OK();
  });
}

void AddListContainsKernels(ScalarFunction* func) {
  auto add_kernel = [&](Type::type list_type_id, ArrayKernelExec exec) {
    ScalarKernel kernel({InputType(list_type_id), InputType::Any()}, boolean(), exec,
                        InitListContains);
    // A null value is searched for rather than propagated
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  };
  add_kernel(Type::LIST, ListContains<ListType>);
  add_kernel(Type::LARGE_LIST, ListContains<LargeListType>);
  add_kernel(Type::LIST_VIEW, ListContains<ListViewType>);
  add_kernel(Type::LARGE_LIST_VIEW, ListContains<LargeListViewType>);
  add_kernel(Type::FIXED_SIZE_LIST, ListContains<FixedSizeListType>);
}

const FunctionDoc list_contains_doc(
    "Check whether lists contain a given value",
    ("`lists` must have a list-like type and `value` must be comparable\n"
     "with the list value type.\n"
     "For each list in `lists`, true is emitted if any of its values is equal\n"
     "to the corresponding `value`, false otherwise. A null `value` matches\n"
     "null list values, and a NaN `value` matches NaN list values; otherwise\n"
     "null list values never match. Null lists emit a null in the output."),
    {"lists", "value"});

struct StructFieldFunctor {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = OptionsWrapper<StructFieldOptions>::Get(ctx);
    std::shared_ptr<Array> current = MakeArray(batch[0].array.ToArrayData());

    FieldPath field_path;
    if (options.field_ref.IsNested() || options.field_ref.IsName()) {
      ARROW_ASSIGN_OR_RAISE(field_path, options.field_ref.FindOne(*current->type()));
    } else {
      DCHECK(options.field_ref.IsFieldPath());
      field_path = *options.field_ref.field_path();
    }

    for (const auto& index : field_path.indices()) {
      RETURN_NOT_OK(CheckIndex(index, *current->type()));
      switch (current->type()->id()) {
        case Type::STRUCT: {
          const auto& struct_array = checked_cast<const StructArray&>(*current);
          ARROW_ASSIGN_OR_RAISE(
              current, struct_array.GetFlattenedField(index, ctx->memory_pool()));
          break;
        }
        case Type::DENSE_UNION: {
          // We implement this here instead of in DenseUnionArray since it's
          // easiest to do via Take(), but DenseUnionArray can't rely on
          // arrow::compute. See ARROW-8891.
          const auto& union_array = checked_cast<const DenseUnionArray&>(*current);

          // Generate a bitmap for the offsets buffer based on the type codes buffer.
          ARROW_ASSIGN_OR_RAISE(
              std::shared_ptr<Buffer> take_bitmap,
              ctx->AllocateBitmap(union_array.length() + union_array.offset()));
          const int8_t* type_codes = union_array.raw_type_codes();
          const int8_t type_code = union_array.union_type()->type_codes()[index];
          int64_t offset = 0;
          arrow::internal::GenerateBitsUnrolled(
              take_bitmap->mutable_data(), union_array.offset(), union_array.length(),
              [&] { return type_codes[offset++] == type_code; });

          // Pass the combined buffer to Take().
          Datum take_indices(
              ArrayData(int32(), union_array.length(),
                        {std::move(take_bitmap), union_array.value_offsets()},
                        kUnknownNullCount, union_array.offset()));
          // Do not slice the child since the indices are relative to the unsliced
          // array.
          ARROW_ASSIGN_OR_RAISE(
              Datum result,
              CallFunction("take", {union_array.field(index), std::move(take_indices)}));
          current = result.make_array();
          break;
        }
        case Type::SPARSE_UNION: {
          const auto& union_array = checked_cast<const SparseUnionArray&>(*current);
          ARROW_ASSIGN_OR_RAISE(current,
                                union_array.GetFlattenedField(index, ctx->memory_pool()));
          break;
        }
        default:
          // Should have been checked in ResolveStructFieldType
          return Status::TypeError("struct_field: cannot reference child field of type ",
                                   *current->type());
      }
    }
    out->value = std::move(current->data());
    return Status::OK();
  }

  static Status CheckIndex(int index, const DataType& type) {
    if (!ValidParentType(type)) {
      return Status::TypeError("struct_field: cannot subscript field of type ", type);
    } else if (index < 0 || index >= type.num_fields()) {
      return Status::Invalid("struct_field: out-of-bounds field reference to field ",
                             index, " in type ", type, " with ", type.num_fields(),
                             " fields");
    }
    return Status::OK();
  }

  static bool ValidParentType(const DataType& type) {
    return type.id() == Type::STRUCT || type.id() == Type::DENSE_UNION ||
           type.id() == Type::SPARSE_UNION;
  }
};

Result<TypeHolder> ResolveStructFieldType(KernelContext* ctx,
                                          const std::vector<TypeHolder>& types) {
  const auto& field_ref = OptionsWrapper<StructFieldOptions>::Get(ctx).field_ref;
  const DataType* type = types.front().type;

  FieldPath field_path;
  if (field_ref.IsNested() || field_ref.IsName()) {
    ARROW_ASSIGN_OR_RAISE(field_path, field_ref.FindOne(*type));
  } else {
    field_path = *field_ref.field_path();
  }

  for (const auto& index : field_path.indices()) {
    RETURN_NOT_OK(StructFieldFunctor::CheckIndex(index, *type));
    type = type->field(index)->type().get();
  }
  return type;
}

void AddStructFieldKernels(ScalarFunction* func) {
  for (const auto in_type : {Type::STRUCT, Type::DENSE_UNION, Type::SPARSE_UNION}) {
    ScalarKernel kernel({in_type}, ResolveStructFieldType, StructFieldFunctor::Exec,
                        OptionsWrapper<StructFieldOptions>::Init);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
}

const FunctionDoc struct_field_doc(
    "Extract children of a struct or union by index",
    ("Given a list of indices (passed via StructFieldOptions), extract\n"
     "the child array or scalar with the given child index, recursively.\n"
     "\n"
     "For union inputs, nulls are emitted for union values that reference\n"
     "a different child than specified. Also, the indices are always\n"
     "in physical order, not logical type codes - for example, the first\n"
     "child is always index 0.\n"
     "\n"
     "An empty list of indices returns the argument unchanged."),
    {"values"}, "StructFieldOptions", /*options_required=*/true);

Result<TypeHolder> MakeStructResolve(KernelContext* ctx,
                                     const std::vector<TypeHolder>& types) {
  auto names = OptionsWrapper<MakeStructOptions>::Get(ctx).field_names;
  auto nullable = OptionsWrapper<MakeStructOptions>::Get(ctx).field_nullability;
  auto metadata = OptionsWrapper<MakeStructOptions>::Get(ctx).field_metadata;

  if (names.size() == 0) {
    names.resize(types.size());
    nullable.resize(types.size(), true);
    metadata.resize(types.size(), nullptr);
    int i = 0;
    for (auto& name : names) {
      name = ToChars(i++);
    }
  } else if (names.size() != types.size() || nullable.size() != types.size() ||
             metadata.size() != types.size()) {
    return Status::Invalid("make_struct() was passed ", types.size(), " arguments but ",
                           names.size(), " field names, ", nullable.size(),
                           " nullability bits, and ", metadata.size(),
                           " metadata dictionaries.");
  }

  size_t i = 0;
  FieldVector fields(types.size());

  for (const TypeHolder& type : types) {
    fields[i] = field(std::move(names[i]), type.GetSharedPtr(), nullable[i],
                      std::move(metadata[i]));
    ++i;
  }

  return TypeHolder(struct_(std::move(fields)));
}

Status MakeStructExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ARROW_ASSIGN_OR_RAISE(TypeHolder type, MakeStructResolve(ctx, batch.GetTypes()));

  for (int i = 0; i < batch.num_values(); ++i) {
    const auto& field = checked_cast<const StructType&>(*type.type).field(i);
    if (batch[i].null_count() > 0 && !field->nullable()) {
      return Status::Invalid("Output field ", field, " (#", i,
                             ") does not allow nulls but the corresponding "
                             "argument was not entirely valid.");
    }
  }

  ArrayData* out_data = out->array_data().get();
  out_data->length = batch.length;
  out_data->type = type.GetSharedPtr();
  out_data->child_data.resize(batch.num_values());
  for (int i = 0; i < batch.num_values(); ++i) {
    if (batch[i].is_array()) {
      out_data->child_data[i] = batch[i].array.ToArrayData();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          std::shared_ptr<Array> promoted,
          MakeArrayFromScalar(*batch[i].scalar, batch.length, ctx->memory_pool()));
      out_data->child_data[i] = promoted->data();
    }
  }
  return Status::OK();
}

const FunctionDoc make_struct_doc{"Wrap Arrays into a StructArray",
                                  ("Names of the StructArray's fields are\n"
                                   "specified through MakeStructOptions."),
                                  {"*args"},
                                  "MakeStructOptions"};
template <typename KeyType>
struct MapLookupFunctor {
  using UnboxedKey = typename UnboxScalar<KeyType>::T;
  static Result<int64_t> GetOneMatchingIndex(const ArraySpan& keys, UnboxedKey query_key,
                                             const bool use_last) {
    int64_t match_index = -1;
    RETURN_NOT_OK(FindMatchingIndices(keys, query_key, [&](int64_t index) -> Status {
      match_index = index;
      if (use_last) {
        return Status::OK();
      } else {
        // If use_last is false, then this will abort the loop
        return Status::Cancelled("Found match, short-circuiting");
      }
    }));
    return match_index;
  }

  template <typename FoundItem>
  static Status FindMatchingIndices(const ArraySpan& keys, UnboxedKey query_key,
                                    FoundItem callback) {
    int64_t index = 0;
    Status status = VisitArrayValuesInline<KeyType>(
        keys,
        [&](UnboxedKey key) -> Status {
          if (key == query_key) {
            return callback(index++);
          }
          ++index;
          return Status::OK();
        },
        [&]() -> Status {
          ++index;
          return Status::OK();
        });
    if (!status.ok() && !status.IsCancelled()) {
      return status;
    }
    return Status::OK();
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = OptionsWrapper<MapLookupOptions>::Get(ctx);
    const UnboxedKey query_key = UnboxScalar<KeyType>::Unbox(*options.query_key);

    const ArraySpan& map = batch[0].array;
    const int32_t* offsets = map.GetValues<int32_t>(1);

    // The struct holding the keys and values may have an offset
    int64_t kv_offset = map.child_data[0].offset;

    // We create a copy of the keys array because we will adjust the
    // offset and length for the map probes below
    ArraySpan map_keys = map.child_data[0].child_data[0];
    const ArraySpan& map_items = map.child_data[0].child_data[1];

    std::shared_ptr<DataType> item_type =
        checked_cast<const MapType*>(map.type)->item_type();

    std::unique_ptr<ArrayBuilder> builder;
    if (options.occurrence == MapLookupOptions::Occurrence::ALL) {
      RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), list(item_type), &builder));
      auto list_builder = checked_cast<ListBuilder*>(builder.get());
      auto value_builder = list_builder->value_builder();

      for (int64_t map_index = 0; map_index < map.length; ++map_index) {
        if (!map.IsValid(map_index)) {
          RETURN_NOT_OK(list_builder->AppendNull());
          continue;
        }

        const int64_t item_offset = offsets[map_index] + kv_offset;
        const int32_t item_size = offsets[map_index + 1] - offsets[map_index];

        // Adjust the keys view to just the map slot that we are about to search
        map_keys.SetSlice(item_offset, item_size);

        bool found_at_least_one_key = false;
        RETURN_NOT_OK(FindMatchingIndices(map_keys, query_key, [&](int64_t key_index) {
          if (!found_at_least_one_key) {
            RETURN_NOT_OK(list_builder->Append(true));
          }
          found_at_least_one_key = true;
          return value_builder->AppendArraySlice(map_items, item_offset + key_index, 1);
        }));
        if (!found_at_least_one_key) {
          // Key was not found in this map element, so we append a null list
          RETURN_NOT_OK(list_builder->AppendNull());
        }
      }
      ARROW_ASSIGN_OR_RAISE(auto result, list_builder->Finish());
      out->value = std::move(result->data());
    } else { /* occurrence == FIRST || LAST */
      RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), item_type, &builder));
      RETURN_NOT_OK(builder->Reserve(batch.length));
      for (int64_t map_index = 0; map_index < map.length; ++map_index) {
        if (!map.IsValid(map_index)) {
          RETURN_NOT_OK(builder->AppendNull());
          continue;
        }

        const int64_t item_offset = offsets[map_index] + kv_offset;
        const int32_t item_size = offsets[map_index + 1] - offsets[map_index];

        // Adjust the keys view to just the map slot that we are about to search
        map_keys.SetSlice(item_offset, item_size);

        ARROW_ASSIGN_OR_RAISE(
            int64_t item_index,
            GetOneMatchingIndex(map_keys, query_key,
                                options.occurrence == MapLookupOptions::LAST));

        if (item_index != -1) {
          RETURN_NOT_OK(
              builder->AppendArraySlice(map_items, item_offset + item_index, 1));
        } else {
          RETURN_NOT_OK(builder->AppendNull());
        }
      }
      ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
      out->value = std::move(result->data());
    }
    return Status::OK();
  }
};

Result<TypeHolder> ResolveMapLookupType(KernelContext* ctx,
                                        const std::vector<TypeHolder>& types) {
  const auto& options = OptionsWrapper<MapLookupOptions>::Get(ctx);
  const auto& type = checked_cast<const MapType&>(*types.front().type);
  std::shared_ptr<DataType> item_type = type.item_type();
  std::shared_ptr<DataType> key_type = type.key_type();

  if (!options.query_key) {
    return Status::Invalid("map_lookup: query_key can't be empty.");
  } else if (!options.query_key->is_valid) {
    return Status::Invalid("map_lookup: query_key can't be null.");
  } else if (!options.query_key->type->Equals(key_type)) {
    return Status::TypeError(
        "map_lookup: query_key type and Map key_type don't match. Expected "
        "type: ",
        *key_type, ", but got type: ", *options.query_key->type);
  }

  if (options.occurrence == MapLookupOptions::Occurrence::ALL) {
    return list(item_type);
  } else { /* occurrence == FIRST || LAST */
    return item_type;
  }
}

struct ResolveMapLookup {
  KernelContext* ctx;
  const ExecSpan& batch;
  ExecResult* out;

  template <typename KeyType>
  Status Execute() {
    return MapLookupFunctor<KeyType>::Exec(ctx, batch, out);
  }

  template <typename KeyType>
  enable_if_physical_integer<KeyType, Status> Visit(const KeyType& type) {
    return Execute<KeyType>();
  }

  template <typename KeyType>
  enable_if_decimal<KeyType, Status> Visit(const KeyType& type) {
    return Execute<KeyType>();
  }

  template <typename KeyType>
  enable_if_base_binary<KeyType, Status> Visit(const KeyType& type) {
    return Execute<KeyType>();
  }

  template <typename KeyType>
  enable_if_boolean<KeyType, Status> Visit(const KeyType& type) {
    return Execute<KeyType>();
  }

  Status Visit(const FixedSizeBinaryType& key) { return Execute<FixedSizeBinaryType>(); }

  Status Visit(const MonthDayNanoIntervalType& key) {
    return Execute<MonthDayNanoIntervalType>();
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("Got unsupported type: ", type.ToString());
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    ResolveMapLookup visitor{ctx, batch, out};
    return VisitTypeInline(*checked_cast<const MapType&>(*batch[0].type()).key_type(),
                           &visitor);
  }
};

void AddMapLookupKernels(ScalarFunction* func) {
  ScalarKernel kernel({InputType(Type::MAP)}, OutputType(ResolveMapLookupType),
                      ResolveMapLookup::Exec, OptionsWrapper<MapLookupOptions>::Init);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

const FunctionDoc map_lookup_doc{
    "Find the items corresponding to a given key in a Map",
    ("For a given query key (passed via MapLookupOptions), extract\n"
     "either the FIRST, LAST or ALL items from a Map that have\n"
     "matching keys."),
    {"container"},
    "MapLookupOptions",
    /*options_required=*/true};

}  // namespace

void RegisterScalarNested(FunctionRegistry* registry) {
  auto list_value_length = std::make_shared<ScalarFunction>(
      "list_value_length", Arity::Unary(), list_value_length_doc);
  AddListValueLengthKernels(list_value_length.get());
  DCHECK_OK(registry->AddFunction(std::move(list_value_length)));

  auto list_element =
      std::make_shared<ScalarFunction>("list_element", Arity::Binary(), list_element_doc);
  AddListElementKernels(list_element.get());
  DCHECK_OK(registry->AddFunction(std::move(list_element)));

  auto list_contains = std::make_shared<ScalarFunction>("list_contains", Arity::Binary(),
                                                        list_contains_doc);
  AddListContainsKernels(list_contains.get());
  DCHECK_OK(registry->AddFunction(std::move(list_contains)));

  auto list_slice =
      std::make_shared<ScalarFunction>("list_slice", Arity::Unary(), list_slice_doc);
  AddListSliceKernels(list_slice.get());
  DCHECK_OK(registry->AddFunction(std::move(list_slice)));

  auto struct_field =
      std::make_shared<ScalarFunction>("struct_field", Arity::Unary(), struct_field_doc);
  AddStructFieldKernels(struct_field.get());
  DCHECK_OK(registry->AddFunction(std::move(struct_field)));

  auto map_lookup =
      std::make_shared<ScalarFunction>("map_lookup", Arity::Unary(), map_lookup_doc);
  AddMapLookupKernels(map_lookup.get());
  DCHECK_OK(registry->AddFunction(std::move(map_lookup)));

  static MakeStructOptions kDefaultMakeStructOptions;
  auto make_struct_function = std::make_shared<ScalarFunction>(
      "make_struct", Arity::VarArgs(), make_struct_doc, &kDefaultMakeStructOptions);

  ScalarKernel kernel{KernelSignature::Make({InputType{}}, OutputType{MakeStructResolve},
                                            /*is_varargs=*/true),
                      MakeStructExec, OptionsWrapper<MakeStructOptions>::Init};
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(make_struct_function->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(make_struct_function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

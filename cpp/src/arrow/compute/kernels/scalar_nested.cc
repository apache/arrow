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
#include "arrow/array/array_base.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/string.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::ToChars;

namespace compute::internal {
namespace {

template <typename Type, typename offset_type = typename Type::offset_type>
Status ListValueLength(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& arr = batch[0].array;
  ArraySpan* out_arr = out->array_span_mutable();
  auto out_values = out_arr->GetValues<offset_type>(1);
  const offset_type* offsets = arr.GetValues<offset_type>(1);
  // Offsets are always well-defined and monotonic, even for null values
  for (int64_t i = 0; i < arr.length; ++i) {
    *out_values++ = offsets[i + 1] - offsets[i];
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

template <typename Type>
struct ListSlice {
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto opts = OptionsWrapper<ListSliceOptions>::Get(ctx);

    // Invariants
    if (opts.start < 0 || (opts.stop.has_value() && opts.start >= opts.stop.value())) {
      // TODO(ARROW-18281): support start == stop which should give empty lists
      return Status::Invalid("`start`(", opts.start,
                             ") should be greater than 0 and smaller than `stop`(",
                             ToString(opts.stop), ")");
    }
    if (opts.step < 1) {
      return Status::Invalid("`step` must be >= 1, got: ", opts.step);
    }

    const ArraySpan& list_array = batch[0].array;
    const Type* list_type = checked_cast<const Type*>(list_array.type);
    const auto value_type = list_type->field(0);
    const auto return_fixed_size_list = opts.return_fixed_size_list.value_or(
        list_type->id() == arrow::Type::FIXED_SIZE_LIST);
    std::unique_ptr<ArrayBuilder> builder;

    // should have been checked in resolver
    // if stop not set, then cannot return fixed size list without input being fixed size
    // list b/c we cannot determine the max list element in type resolving.
    DCHECK(opts.stop.has_value() ||
           (!opts.stop.has_value() && (!return_fixed_size_list ||
                                       list_type->id() == arrow::Type::FIXED_SIZE_LIST)));

    // construct array values
    if (return_fixed_size_list) {
      int32_t stop;
      if (opts.stop.has_value()) {
        stop = static_cast<int32_t>(opts.stop.value());
      } else {
        DCHECK_EQ(list_type->id(), arrow::Type::FIXED_SIZE_LIST);
        stop = reinterpret_cast<const FixedSizeListType*>(list_type)->list_size();
      }
      const auto size = std::max(stop - static_cast<int32_t>(opts.start), 0);
      const auto length = bit_util::CeilDiv(size, opts.step);
      RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(),
                                fixed_size_list(value_type, static_cast<int32_t>(length)),
                                &builder));
      RETURN_NOT_OK(BuildArray<FixedSizeListBuilder>(batch, opts, *builder));
    } else {
      if constexpr (std::is_same_v<Type, LargeListType>) {
        RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), large_list(value_type), &builder));
        RETURN_NOT_OK(BuildArray<LargeListBuilder>(batch, opts, *builder));
      } else {
        RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), list(value_type), &builder));
        RETURN_NOT_OK(BuildArray<ListBuilder>(batch, opts, *builder));
      }
    }

    // build output arrays and set result
    ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
    out->value = std::move(result->data());
    return Status::OK();
  }

  template <typename BuilderType>
  static Status BuildArray(const ExecSpan& batch, const ListSliceOptions& opts,
                           ArrayBuilder& builder) {
    if constexpr (std::is_same_v<Type, FixedSizeListType>) {
      RETURN_NOT_OK(BuildArrayFromFixedSizeListType<BuilderType>(batch, opts, builder));
    } else {
      RETURN_NOT_OK(BuildArrayFromListType<BuilderType>(batch, opts, builder));
    }
    return Status::OK();
  }

  template <typename BuilderType>
  static Status BuildArrayFromFixedSizeListType(const ExecSpan& batch,
                                                const ListSliceOptions& opts,
                                                ArrayBuilder& builder) {
    const auto list_size =
        checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();
    const ArraySpan& list_array = batch[0].array;
    const ArraySpan& list_values = list_array.child_data[0];

    auto list_builder = checked_cast<BuilderType*>(&builder);
    for (auto i = 0; i < list_array.length; ++i) {
      auto offset = (i + list_array.offset) * list_size;
      auto next_offset = offset + list_size;
      if (list_array.IsNull(i)) {
        RETURN_NOT_OK(list_builder->AppendNull());
      } else {
        RETURN_NOT_OK(SetValues<BuilderType>(list_builder, offset, next_offset, &opts,
                                             &list_values));
      }
    }
    return Status::OK();
  }

  template <typename BuilderType>
  static Status BuildArrayFromListType(const ExecSpan& batch,
                                       const ListSliceOptions& opts,
                                       ArrayBuilder& builder) {
    const ArraySpan& list_array = batch[0].array;
    const offset_type* offsets = list_array.GetValues<offset_type>(1);

    const ArraySpan& list_values = list_array.child_data[0];

    auto list_builder = checked_cast<BuilderType*>(&builder);
    for (auto i = 0; i < list_array.length; ++i) {
      const offset_type offset = offsets[i];
      const offset_type next_offset = offsets[i + 1];
      if (list_array.IsNull(i)) {
        RETURN_NOT_OK(list_builder->AppendNull());
      } else {
        RETURN_NOT_OK(SetValues<BuilderType>(list_builder, offset, next_offset, &opts,
                                             &list_values));
      }
    }
    return Status::OK();
  }
  template <typename BuilderType>
  static Status SetValues(BuilderType* list_builder, const offset_type offset,
                          const offset_type next_offset, const ListSliceOptions* opts,
                          const ArraySpan* list_values) {
    auto value_builder = list_builder->value_builder();
    auto cursor = offset;

    RETURN_NOT_OK(list_builder->Append());
    const auto size = opts->stop.has_value() ? (opts->stop.value() - opts->start)
                                             : ((next_offset - opts->start) - offset);
    while (cursor < offset + size) {
      if (cursor + opts->start >= next_offset) {
        if constexpr (!std::is_same_v<BuilderType, FixedSizeListBuilder>) {
          break;  // don't pad nulls for variable sized list output
        }
        RETURN_NOT_OK(value_builder->AppendNull());
      } else {
        RETURN_NOT_OK(
            value_builder->AppendArraySlice(*list_values, cursor + opts->start, 1));
      }
      cursor += static_cast<offset_type>(opts->step);
    }
    return Status::OK();
  }
};

Result<TypeHolder> MakeListSliceResolve(KernelContext* ctx,
                                        const std::vector<TypeHolder>& types) {
  const auto& opts = OptionsWrapper<ListSliceOptions>::Get(ctx);
  const auto list_type = checked_cast<const BaseListType*>(types[0].type);
  const auto value_type = list_type->field(0);
  const auto return_fixed_size_list =
      opts.return_fixed_size_list.value_or(list_type->id() == Type::FIXED_SIZE_LIST);
  if (return_fixed_size_list) {
    int32_t stop;
    if (!opts.stop.has_value()) {
      if (list_type->id() == Type::FIXED_SIZE_LIST) {
        stop = checked_cast<const FixedSizeListType*>(list_type)->list_size();
      } else {
        return Status::NotImplemented(
            "Unable to produce FixedSizeListArray from non-FixedSizeListArray without "
            "`stop` being set.");
      }
    } else {
      stop = static_cast<int32_t>(opts.stop.value());
    }
    const auto size = std::max(static_cast<int32_t>(stop - opts.start), 0);
    if (opts.step < 1) {
      return Status::Invalid("`step` must be >= 1, got: ", opts.step);
    }
    const auto length = bit_util::CeilDiv(size, opts.step);
    return fixed_size_list(value_type, static_cast<int32_t>(length));
  } else {
    // Returning large list if that's what we got in and didn't ask for fixed size
    if (list_type->id() == Type::LARGE_LIST) {
      return large_list(value_type);
    }
    return list(value_type);
  }
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
  AddListElementKernels<FixedSizeListType, FixedSizeListElement>(func);
}

const FunctionDoc list_element_doc(
    "Compute elements using of nested list values using an index",
    ("`lists` must have a list-like type.\n"
     "For each value in each list of `lists`, the element at `index`\n"
     "is emitted. Null values emit a null in the output."),
    {"lists", "index"});

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

struct AdjoinAsListState : public KernelState {
  explicit AdjoinAsListState(std::shared_ptr<DataType> list_type,
                             std::shared_ptr<DataType> input_type)
      : list_type(std::move(list_type)), input_type(std::move(input_type)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    auto options = static_cast<const AdjoinAsListOptions*>(args.options);
    if (!options) {
      return Status::Invalid(
          "Attempted to initialize KernelState from null FunctionOptions");
    }

    // Make sure input args have the same type
    if (args.inputs.empty()) {
      return Status::Invalid("AdjoinAsList requires at least one input argument");
    }

    auto input_type = args.inputs[0];
    if (std::any_of(args.inputs.begin() + 1, args.inputs.end(),
                    [&input_type](const auto& arg) { return arg != input_type; })) {
      return Status::TypeError(
          "AdjoinAsList requires all input arguments to have the same type");
    }

    switch (options->list_type) {
      case AdjoinAsListOptions::LIST:
        return std::make_unique<AdjoinAsListState>(list(input_type.GetSharedPtr()),
                                                   input_type.GetSharedPtr());
      case AdjoinAsListOptions::LARGE_LIST:
        return std::make_unique<AdjoinAsListState>(large_list(input_type.GetSharedPtr()),
                                                   input_type.GetSharedPtr());
      case AdjoinAsListOptions::FIXED_SIZE_LIST:
        return std::make_unique<AdjoinAsListState>(
            fixed_size_list(input_type.GetSharedPtr(),
                            static_cast<int32_t>(args.inputs.size())),
            input_type.GetSharedPtr());
      default:
        return Status::Invalid(
            "AdjoinAsList requires list_type to be LIST, "
            "LARGE_LIST or FIXED_SIZE_LIST");
    }
  }

  std::shared_ptr<DataType> list_type;
  std::shared_ptr<DataType> input_type;
};

Result<TypeHolder> ResolveAdjoinAsListOutput(KernelContext* ctx,
                                             const std::vector<TypeHolder>& types) {
  auto list_type = static_cast<const AdjoinAsListState*>(ctx->state())->list_type;
  return TypeHolder(list_type);
}

template <typename OutputType>
struct AdjoinAsListImpl {
  const std::shared_ptr<DataType>& list_type;
  const std::shared_ptr<DataType>& input_type;

  AdjoinAsListImpl(const std::shared_ptr<DataType>& list_type,
                   const std::shared_ptr<DataType>& input_type)
      : list_type(list_type), input_type(input_type) {}

  // ReserveData for binary builders
  template <typename InputType, typename Builder>
  Status ReserveBinaryData(const ExecSpan& batch, Builder* builder) {
    static_assert(is_base_binary_type<InputType>::value ||
                  is_fixed_size_binary_type<InputType>::value);
    int64_t total_bytes = 0;
    for (const auto& input : batch.values) {
      if (input.is_array()) {
        const auto& arr = input.array;
        if constexpr (std::is_same_v<InputType, FixedSizeBinaryType>) {
          total_bytes += arr.buffers[1].size;
        } else {
          total_bytes += arr.buffers[2].size;
        }
      } else {
        total_bytes +=
            checked_cast<const BaseBinaryScalar&>(*input.scalar).value->size() *
            batch.length;
      }
    }
    return builder->ReserveData(total_bytes);
  }

  // Construct offset buffer for variable-size list builders
  Result<std::shared_ptr<Buffer>> MakeOffsetsBuffer(const ExecSpan& batch) {
    TypedBufferBuilder<typename OutputType::offset_type> offset_builder;
    RETURN_NOT_OK(offset_builder.Reserve(batch.length + 1));
    typename OutputType::offset_type cur_offset = 0;
    offset_builder.UnsafeAppend(cur_offset);
    for (int i = 0; i < batch.length; ++i) {
      cur_offset += batch.num_values();
      offset_builder.UnsafeAppend(cur_offset);
    }
    return offset_builder.Finish(/*shrink_to_fit=*/false);
  }

  Status Visit(const NullType& null_type, KernelContext* ctx, const ExecSpan& batch,
               int64_t length, std::shared_ptr<ArrayData>* out_values) {
    *out_values = ArrayData::Make(null(), length, {nullptr}, length);
    return Status::OK();
  }

  template <typename InputType>
  std::enable_if_t<is_primitive_ctype<InputType>::value ||
                       is_base_binary_type<InputType>::value ||
                       is_fixed_size_binary_type<InputType>::value ||
                       is_temporal_type<InputType>::value,
                   Status>
  Visit(const InputType& type, KernelContext* ctx, const ExecSpan& batch, int64_t length,
        std::shared_ptr<ArrayData>* out_values) {
    using ArraySpanType = typename GetArraySpanType<InputType>::Type;
    using BuilderType = typename TypeTraits<InputType>::BuilderType;

    auto builder = std::make_shared<BuilderType>(type.GetSharedPtr(),
                                                 ctx->exec_context()->memory_pool());
    RETURN_NOT_OK(builder->Reserve(batch.num_values() * length));
    if constexpr (is_base_binary_type<InputType>::value ||
                  is_fixed_size_binary_type<InputType>::value) {
      RETURN_NOT_OK(ReserveBinaryData<InputType>(batch, builder.get()));
    }

    for (int i = 0; i < batch.length; ++i) {
      for (const auto& input : batch.values) {
        if (input.is_array()) {
          auto concrete_span = ArraySpanType(input.array);
          if (concrete_span.IsValid(i)) {
            builder->UnsafeAppend(concrete_span.GetView(i));
          } else {
            builder->UnsafeAppendNull();
          }
        } else {
          builder->UnsafeAppend(UnboxScalar<InputType>::Unbox(*input.scalar));
        }
      }
    }
    return builder->FinishInternal(out_values);
  }

  // Deal with nested/union types with a naive approach: First concatenate the inputs,
  // then shuffle it using Take
  // TODO(GH-37430): Better implementation for struct types
  Status Visit(const DataType& input_type, KernelContext* ctx, const ExecSpan& batch,
               int64_t length, std::shared_ptr<ArrayData>* out_values) {
    std::vector<std::shared_ptr<Array>> inputs;
    inputs.reserve(batch.num_values());
    // Starting index of each input in the concatenated array
    std::vector<int64_t> input_start_index;
    input_start_index.reserve(batch.num_values());
    int64_t cur_index = 0;
    for (const auto& input : batch.values) {
      input_start_index.push_back(cur_index);
      if (input.is_array()) {
        inputs.emplace_back(input.array.ToArray());
        cur_index += input.array.length;
      } else {
        ARROW_ASSIGN_OR_RAISE(auto arr_from_scalar,
                              MakeArrayFromScalar(*input.scalar, 1));
        inputs.emplace_back(std::move(arr_from_scalar));
        cur_index += 1;
      }
    }
    ARROW_ASSIGN_OR_RAISE(auto chunked_arr, ChunkedArray::Make(inputs));

    // Build child index for take
    Int64Builder child_indices_builder;
    RETURN_NOT_OK(child_indices_builder.Reserve(batch.num_values() * batch.length));
    for (int i = 0; i < batch.length; ++i) {
      for (int j = 0; j < batch.num_values(); ++j) {
        if (batch.values[j].is_array()) {
          child_indices_builder.UnsafeAppend(input_start_index[j] + i);
        } else {
          child_indices_builder.UnsafeAppend(input_start_index[j]);
        }
      }
    }
    std::shared_ptr<ArrayData> child_indices;
    RETURN_NOT_OK(child_indices_builder.FinishInternal(&child_indices));
    ARROW_ASSIGN_OR_RAISE(auto shuffled_data,
                          Take(chunked_arr, *child_indices, TakeOptions::NoBoundsCheck(),
                               ctx->exec_context()));
    ARROW_ASSIGN_OR_RAISE(auto concatenated_arr,
                          Concatenate(shuffled_data.chunked_array()->chunks()));
    *out_values = std::move(concatenated_arr->data());
    return Status::OK();
  }

  template <typename InputType>
  Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    auto length = batch.length * batch.num_values();
    auto out_data = *out->array_data_mutable();
    RETURN_NOT_OK(Visit(checked_cast<const InputType&>(*input_type), ctx, batch, length,
                        &out_data->child_data.emplace_back()));
    out_data->type = list_type;
    if constexpr (!is_fixed_size_list_type<OutputType>::value) {
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1], MakeOffsetsBuffer(batch));
    }
    return Status::OK();
  }
};

template <template <typename OutputType> typename AdjoinAsListImpl, typename InputType>
Status AdjoinAsListExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const auto& state = static_cast<const AdjoinAsListState*>(ctx->state());
  const auto& list_type = state->list_type;
  const auto& input_type = state->input_type;

  switch (list_type->id()) {
    case Type::LIST: {
      return AdjoinAsListImpl<ListType>(list_type, input_type)
          .template Exec<InputType>(ctx, batch, out);
    }
    case Type::LARGE_LIST: {
      return AdjoinAsListImpl<LargeListType>(list_type, input_type)
          .template Exec<InputType>(ctx, batch, out);
    }
    case Type::FIXED_SIZE_LIST: {
      return AdjoinAsListImpl<FixedSizeListType>(list_type, input_type)
          .template Exec<InputType>(ctx, batch, out);
    }
    default:
      return Status::Invalid(
          "AdjoinAsList requires list_type to be LIST, "
          "LARGE_LIST or FIXED_SIZE_LIST");
  }
}

// A visitor to dispatch type to its type-specific kernel at compile time
struct AdjoinAsListKernelGenerator {
  ScalarKernel kernel;

  AdjoinAsListKernelGenerator() {
    kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    kernel.init = AdjoinAsListState::Init;
  }

  template <typename ArrowType>
  Status Visit(const ArrowType* type) {
    kernel.signature = KernelSignature::Make({InputType(ArrowType::type_id)},
                                             OutputType(ResolveAdjoinAsListOutput), true);
    kernel.exec = AdjoinAsListExec<AdjoinAsListImpl, ArrowType>;
    return Status::OK();
  }
};

void AddAdjoinAsListKernels(ScalarFunction* func) {
  AdjoinAsListKernelGenerator generator;
  // non-parametric types
  for (const auto& tys :
       {PrimitiveTypes(), TemporalTypes(), DurationTypes(), IntervalTypes()}) {
    for (const auto& ty : tys) {
      DCHECK_OK(VisitTypeIdInline(ty->id(), &generator));
      DCHECK_OK(func->AddKernel(generator.kernel));
    }
  }

  // parametric types
  for (const auto& ty :
       {Type::FIXED_SIZE_BINARY, Type::DECIMAL128, Type::DECIMAL256, Type::LIST,
        Type::LARGE_LIST, Type::FIXED_SIZE_LIST, Type::SPARSE_UNION, Type::DENSE_UNION,
        Type::DICTIONARY, Type::STRUCT, Type::MAP}) {
    DCHECK_OK(VisitTypeIdInline(ty, &generator));
    DCHECK_OK(func->AddKernel(generator.kernel));
  }
}

FunctionDoc adjoin_as_list_doc(
    "Adjoin multiple arrays row-wise as a list array",
    "The input arrays must have the same type and length.\n"
    "For N arrays each with length M, the output list array will\n"
    "have length M and each list will have N elements.\n"
    "The output list type can be specified in AdjoinAsListOptions.",
    {"*args"}, "AdjoinAsListOptions", false);
}  // namespace

void RegisterScalarNested(FunctionRegistry* registry) {
  auto list_value_length = std::make_shared<ScalarFunction>(
      "list_value_length", Arity::Unary(), list_value_length_doc);
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LIST)}, int32(),
                                         ListValueLength<ListType>));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::FIXED_SIZE_LIST)}, int32(),
                                         FixedSizeListValueLength));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LARGE_LIST)}, int64(),
                                         ListValueLength<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(list_value_length)));

  auto list_element =
      std::make_shared<ScalarFunction>("list_element", Arity::Binary(), list_element_doc);
  AddListElementKernels(list_element.get());
  DCHECK_OK(registry->AddFunction(std::move(list_element)));

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

  static AdjoinAsListOptions kDefaultAdjoinAsListOptions;
  auto adjoin_as_list =
      std::make_shared<ScalarFunction>("adjoin_as_list", Arity::VarArgs(1),
                                       adjoin_as_list_doc, &kDefaultAdjoinAsListOptions);
  AddAdjoinAsListKernels(adjoin_as_list.get());
  DCHECK_OK(registry->AddFunction(std::move(adjoin_as_list)));
}

}  // namespace compute::internal
}  // namespace arrow

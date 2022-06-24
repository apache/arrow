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

#include "arrow/array/array_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_generate.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

template <typename Type, typename offset_type = typename Type::offset_type>
Status ListValueLength(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using OffsetScalarType = typename TypeTraits<Type>::OffsetScalarType;

  if (batch[0].is_array()) {
    const ArraySpan& arr = batch[0].array;
    ArraySpan* out_arr = out->array_span();
    auto out_values = out_arr->GetValues<offset_type>(1);
    const offset_type* offsets = arr.GetValues<offset_type>(1);
    // Offsets are always well-defined and monotonic, even for null values
    for (int64_t i = 0; i < arr.length; ++i) {
      *out_values++ = offsets[i + 1] - offsets[i];
    }
  } else {
    const auto& arg0 = batch[0].scalar_as<ScalarType>();
    if (arg0.is_valid) {
      checked_cast<OffsetScalarType*>(out->scalar().get())->value =
          static_cast<offset_type>(arg0.value->length());
    }
  }

  return Status::OK();
}

Status FixedSizeListValueLength(KernelContext* ctx, const ExecSpan& batch,
                                ExecResult* out) {
  auto width = checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();
  if (batch[0].is_array()) {
    const ArraySpan& arr = batch[0].array;
    ArraySpan* out_arr = out->array_span();
    int32_t* out_values = out_arr->GetValues<int32_t>(1);
    std::fill(out_values, out_values + arr.length, width);
  } else {
    const auto& arg0 = batch[0].scalar_as<FixedSizeListScalar>();
    if (arg0.is_valid) {
      checked_cast<Int32Scalar*>(out->scalar().get())->value = width;
    }
  }

  return Status::OK();
}

const FunctionDoc list_value_length_doc{
    "Compute list lengths",
    ("`lists` must have a list-like type.\n"
     "For each non-null value in `lists`, its length is emitted.\n"
     "Null values emit a null in the output."),
    {"lists"}};

template <typename Type, typename IndexType>
struct ListElementArray {
  using ListArrayType = typename TypeTraits<Type>::ArrayType;
  using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;
  using offset_type = typename Type::offset_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& index_scalar = batch[1].scalar_as<IndexScalarType>();
    if (ARROW_PREDICT_FALSE(!index_scalar.is_valid)) {
      return Status::Invalid("Index must not be null");
    }
    const ArraySpan& list = batch[0].array;
    const ArraySpan& list_values = list.child_data[0];
    const offset_type* offsets = list.GetValues<offset_type>(1);

    auto index = index_scalar.value;
    if (ARROW_PREDICT_FALSE(index < 0)) {
      return Status::Invalid("Index ", index,
                             " is out of bounds: should be greater than or equal to 0");
    }
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
struct FixedSizeListElementArray {
  using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& index_scalar = batch[1].scalar_as<IndexScalarType>();
    if (ARROW_PREDICT_FALSE(!index_scalar.is_valid)) {
      return Status::Invalid("Index must not be null");
    }

    auto item_size = checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();

    const ArraySpan& list = batch[0].array;
    const ArraySpan& list_values = list.child_data[0];

    auto index = index_scalar.value;
    if (ARROW_PREDICT_FALSE(index < 0)) {
      return Status::Invalid("Index ", index,
                             " is out of bounds: should be greater than or equal to 0");
    }
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

template <typename, typename IndexType>
struct ListElementScalar {
  static Status Exec(KernelContext* /*ctx*/, const ExecSpan& batch, ExecResult* out) {
    using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;
    const auto& index_scalar = batch[1].scalar_as<IndexScalarType>();
    if (ARROW_PREDICT_FALSE(!index_scalar.is_valid)) {
      return Status::Invalid("Index must not be null");
    }
    const auto& list_scalar = batch[0].scalar_as<BaseListScalar>();
    if (ARROW_PREDICT_FALSE(!list_scalar.is_valid)) {
      out->value = MakeNullScalar(
          checked_cast<const BaseListType&>(*batch[0].type()).value_type());
      return Status::OK();
    }
    auto list = list_scalar.value;
    auto index = index_scalar.value;
    auto len = list->length();
    if (ARROW_PREDICT_FALSE(index < 0 ||
                            index >= static_cast<typename IndexType::c_type>(len))) {
      return Status::Invalid("Index ", index, " is out of bounds: should be in [0, ", len,
                             ")");
    }
    ARROW_ASSIGN_OR_RAISE(out->value, list->GetScalar(index));
    return Status::OK();
  }
};

template <typename InListType, template <typename...> class Functor>
void AddListElementArrayKernels(ScalarFunction* func) {
  for (const auto& index_type : IntTypes()) {
    auto inputs = {InputType::Array(InListType::type_id), InputType::Scalar(index_type)};
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

void AddListElementArrayKernels(ScalarFunction* func) {
  AddListElementArrayKernels<ListType, ListElementArray>(func);
  AddListElementArrayKernels<LargeListType, ListElementArray>(func);
  AddListElementArrayKernels<FixedSizeListType, FixedSizeListElementArray>(func);
}

void AddListElementScalarKernels(ScalarFunction* func) {
  for (const auto list_type_id : {Type::LIST, Type::LARGE_LIST, Type::FIXED_SIZE_LIST}) {
    for (const auto& index_type : IntTypes()) {
      auto inputs = {InputType::Scalar(list_type_id), InputType::Scalar(index_type)};
      auto output = OutputType{ListValuesType};
      auto sig = KernelSignature::Make(std::move(inputs), std::move(output),
                                       /*is_varargs=*/false);
      auto scalar_exec = GenerateInteger<ListElementScalar, void>({index_type->id()});
      ScalarKernel kernel{std::move(sig), std::move(scalar_exec)};
      kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
  }
}

const FunctionDoc list_element_doc(
    "Compute elements using of nested list values using an index",
    ("`lists` must have a list-like type.\n"
     "For each value in each list of `lists`, the element at `index`\n"
     "is emitted. Null values emit a null in the output."),
    {"lists", "index"});

struct StructFieldFunctor {
  static Status ExecArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = OptionsWrapper<StructFieldOptions>::Get(ctx);

    std::shared_ptr<Array> current = MakeArray(batch[0].array.ToArrayData());
    for (const auto& index : options.indices) {
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
          // Do not slice the child since the indices are relative to the unsliced array.
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

  static Status ExecScalar(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = OptionsWrapper<StructFieldOptions>::Get(ctx);
    const Scalar* current = batch[0].scalar;
    for (const auto& index : options.indices) {
      RETURN_NOT_OK(CheckIndex(index, *current->type));
      if (!current->is_valid) {
        // out should already be a null scalar of the appropriate type
        return Status::OK();
      }

      switch (current->type->id()) {
        case Type::STRUCT: {
          current = checked_cast<const StructScalar&>(*current).value[index].get();
          break;
        }
        case Type::DENSE_UNION:
        case Type::SPARSE_UNION: {
          const auto& union_scalar = checked_cast<const UnionScalar&>(*current);
          const auto& union_ty = checked_cast<const UnionType&>(*current->type);
          if (union_scalar.type_code != union_ty.type_codes()[index]) {
            // out should already be a null scalar of the appropriate type
            return Status::OK();
          }
          current = union_scalar.value.get();
          break;
        }
        default:
          // Should have been checked in ResolveStructFieldType
          return Status::TypeError("struct_field: cannot reference child field of type ",
                                   *current->type);
      }
    }
    // XXX: Revisit the above to see if we can avoid shared_from_this
    out->value = current->Copy();
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

Result<ValueDescr> ResolveStructFieldType(KernelContext* ctx,
                                          const std::vector<ValueDescr>& descrs) {
  const auto& options = OptionsWrapper<StructFieldOptions>::Get(ctx);
  const std::shared_ptr<DataType>* type = &descrs.front().type;
  for (const auto& index : options.indices) {
    RETURN_NOT_OK(StructFieldFunctor::CheckIndex(index, **type));
    type = &(*type)->field(index)->type();
  }
  return ValueDescr(*type, descrs.front().shape);
}

void AddStructFieldKernels(ScalarFunction* func) {
  for (const auto shape : {ValueDescr::ARRAY, ValueDescr::SCALAR}) {
    for (const auto in_type : {Type::STRUCT, Type::DENSE_UNION, Type::SPARSE_UNION}) {
      ScalarKernel kernel({InputType(in_type, shape)}, OutputType(ResolveStructFieldType),
                          shape == ValueDescr::ARRAY ? StructFieldFunctor::ExecArray
                                                     : StructFieldFunctor::ExecScalar,
                          OptionsWrapper<StructFieldOptions>::Init);
      kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
      kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
      DCHECK_OK(func->AddKernel(std::move(kernel)));
    }
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

Result<ValueDescr> MakeStructResolve(KernelContext* ctx,
                                     const std::vector<ValueDescr>& descrs) {
  auto names = OptionsWrapper<MakeStructOptions>::Get(ctx).field_names;
  auto nullable = OptionsWrapper<MakeStructOptions>::Get(ctx).field_nullability;
  auto metadata = OptionsWrapper<MakeStructOptions>::Get(ctx).field_metadata;

  if (names.size() == 0) {
    names.resize(descrs.size());
    nullable.resize(descrs.size(), true);
    metadata.resize(descrs.size(), nullptr);
    int i = 0;
    for (auto& name : names) {
      name = std::to_string(i++);
    }
  } else if (names.size() != descrs.size() || nullable.size() != descrs.size() ||
             metadata.size() != descrs.size()) {
    return Status::Invalid("make_struct() was passed ", descrs.size(), " arguments but ",
                           names.size(), " field names, ", nullable.size(),
                           " nullability bits, and ", metadata.size(),
                           " metadata dictionaries.");
  }

  size_t i = 0;
  FieldVector fields(descrs.size());

  ValueDescr::Shape shape = ValueDescr::SCALAR;
  for (const ValueDescr& descr : descrs) {
    if (descr.shape != ValueDescr::SCALAR) {
      shape = ValueDescr::ARRAY;
    } else {
      switch (descr.type->id()) {
        case Type::EXTENSION:
        case Type::DENSE_UNION:
        case Type::SPARSE_UNION:
          return Status::NotImplemented("Broadcasting scalars of type ", *descr.type);
        default:
          break;
      }
    }

    fields[i] =
        field(std::move(names[i]), descr.type, nullable[i], std::move(metadata[i]));
    ++i;
  }

  return ValueDescr{struct_(std::move(fields)), shape};
}

Status MakeStructExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ARROW_ASSIGN_OR_RAISE(auto descr, MakeStructResolve(ctx, batch.GetDescriptors()));

  for (int i = 0; i < batch.num_values(); ++i) {
    const auto& field = checked_cast<const StructType&>(*descr.type).field(i);
    if (batch[i].null_count() > 0 && !field->nullable()) {
      return Status::Invalid("Output field ", field, " (#", i,
                             ") does not allow nulls but the corresponding "
                             "argument was not entirely valid.");
    }
  }

  /// TODO: remove this scalar output modality altogether
  if (descr.shape == ValueDescr::SCALAR) {
    ScalarVector scalars(batch.num_values());
    for (int i = 0; i < batch.num_values(); ++i) {
      scalars[i] = batch[i].scalar->Copy();
    }
    out->value =
        std::make_shared<StructScalar>(std::move(scalars), std::move(descr.type));
    return Status::OK();
  }

  ArrayData* out_data = out->array_data().get();
  out_data->length = batch.length;
  out_data->type = descr.type;
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

  static Status ExecMapArray(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = OptionsWrapper<MapLookupOptions>::Get(ctx);
    const UnboxedKey query_key = UnboxScalar<KeyType>::Unbox(*options.query_key);

    const ArraySpan& map = batch[0].array;
    const int32_t* offsets = map.GetValues<int32_t>(1);

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

        const int32_t item_offset = offsets[map_index];
        const int32_t item_size = offsets[map_index + 1] - offsets[map_index];

        // Adjust the keys view to just the map slot that we are about to search
        map_keys.SetOffset(item_offset);
        map_keys.length = item_size;

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

        const int32_t item_offset = offsets[map_index];
        const int32_t item_size = offsets[map_index + 1] - offsets[map_index];

        // Adjust the keys view to just the map slot that we are about to search
        map_keys.SetOffset(item_offset);
        map_keys.length = item_size;

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

  /// TODO(ARROW-16577): use array path for scalars to avoid having to
  /// maintain two code paths
  static Status ExecMapScalar(KernelContext* ctx, const ExecSpan& batch,
                              ExecResult* out) {
    const auto& options = OptionsWrapper<MapLookupOptions>::Get(ctx);
    UnboxedKey query_key = UnboxScalar<KeyType>::Unbox(*options.query_key);

    std::shared_ptr<DataType> item_type =
        checked_cast<const MapType&>(*batch[0].type()).item_type();
    const auto& map_scalar = batch[0].scalar_as<MapScalar>();

    if (ARROW_PREDICT_FALSE(!map_scalar.is_valid)) {
      if (options.occurrence == MapLookupOptions::Occurrence::ALL) {
        out->value = MakeNullScalar(list(item_type));
      } else {
        out->value = MakeNullScalar(item_type);
      }
      return Status::OK();
    }

    const auto& struct_array = checked_cast<const StructArray&>(*map_scalar.value);
    ArraySpan map_keys(*struct_array.data()->child_data[0]);

    // Keys offset and length must be adjusted to match its parent
    map_keys.length = struct_array.length();
    map_keys.offset = struct_array.offset();

    if (options.occurrence == MapLookupOptions::Occurrence::ALL) {
      ArraySpan map_items(*struct_array.data()->child_data[1]);
      // Keys offset and length must be adjusted to match its parent
      map_items.length = struct_array.length();
      map_items.offset = struct_array.offset();

      bool found_at_least_one_key = false;
      std::unique_ptr<ArrayBuilder> builder;
      RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), item_type, &builder));
      RETURN_NOT_OK(
          FindMatchingIndices(map_keys, query_key, [&](int64_t index) -> Status {
            found_at_least_one_key = true;
            RETURN_NOT_OK(builder->AppendArraySlice(map_items, index, 1));
            return Status::OK();
          }));
      if (!found_at_least_one_key) {
        out->value = MakeNullScalar(list(item_type));
      } else {
        ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
        ARROW_ASSIGN_OR_RAISE(out->value, MakeScalar(list(item_type), result));
      }
    } else { /* occurrence == FIRST || LAST */
      std::shared_ptr<Array> items = struct_array.field(1);
      ARROW_ASSIGN_OR_RAISE(
          int64_t item_index,
          GetOneMatchingIndex(map_keys, query_key,
                              options.occurrence == MapLookupOptions::LAST));
      if (item_index != -1) {
        ARROW_ASSIGN_OR_RAISE(out->value, items->GetScalar(item_index));
      } else {
        out->value = MakeNullScalar(item_type);
      }
    }
    return Status::OK();
  }
};

Result<ValueDescr> ResolveMapLookupType(KernelContext* ctx,
                                        const std::vector<ValueDescr>& descrs) {
  const auto& options = OptionsWrapper<MapLookupOptions>::Get(ctx);
  std::shared_ptr<DataType> type = descrs.front().type;
  std::shared_ptr<DataType> item_type = checked_cast<const MapType&>(*type).item_type();
  std::shared_ptr<DataType> key_type = checked_cast<const MapType&>(*type).key_type();

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
    return ValueDescr(list(item_type), descrs.front().shape);
  } else { /* occurrence == FIRST || LAST */
    return ValueDescr(item_type, descrs.front().shape);
  }
}

struct ResolveMapLookup {
  KernelContext* ctx;
  const ExecSpan& batch;
  ExecResult* out;

  template <typename KeyType>
  Status Execute() {
    if (batch[0].is_scalar()) {
      return MapLookupFunctor<KeyType>::ExecMapScalar(ctx, batch, out);
    }
    return MapLookupFunctor<KeyType>::ExecMapArray(ctx, batch, out);
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
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LIST)}, int32(),
                                         ListValueLength<ListType>));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::FIXED_SIZE_LIST)}, int32(),
                                         FixedSizeListValueLength));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LARGE_LIST)}, int64(),
                                         ListValueLength<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(list_value_length)));

  auto list_element =
      std::make_shared<ScalarFunction>("list_element", Arity::Binary(), list_element_doc);
  AddListElementArrayKernels(list_element.get());
  AddListElementScalarKernels(list_element.get());
  DCHECK_OK(registry->AddFunction(std::move(list_element)));

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

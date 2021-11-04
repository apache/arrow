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
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/util/bit_block_counter.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

template <typename Type, typename offset_type = typename Type::offset_type>
Status ListValueLength(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  using OffsetScalarType = typename TypeTraits<Type>::OffsetScalarType;

  if (batch[0].kind() == Datum::ARRAY) {
    typename TypeTraits<Type>::ArrayType list(batch[0].array());
    ArrayData* out_arr = out->mutable_array();
    auto out_values = out_arr->GetMutableValues<offset_type>(1);
    const offset_type* offsets = list.raw_value_offsets();
    ::arrow::internal::VisitBitBlocksVoid(
        list.data()->buffers[0], list.offset(), list.length(),
        [&](int64_t position) {
          *out_values++ = offsets[position + 1] - offsets[position];
        },
        [&]() { *out_values++ = 0; });
  } else {
    const auto& arg0 = batch[0].scalar_as<ScalarType>();
    if (arg0.is_valid) {
      checked_cast<OffsetScalarType*>(out->scalar().get())->value =
          static_cast<offset_type>(arg0.value->length());
    }
  }

  return Status::OK();
}

Status FixedSizeListValueLength(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  auto width = checked_cast<const FixedSizeListType&>(*batch[0].type()).list_size();
  if (batch[0].kind() == Datum::ARRAY) {
    const auto& arr = *batch[0].array();
    ArrayData* out_arr = out->mutable_array();
    auto* out_values = out_arr->GetMutableValues<int32_t>(1);
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
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using ListArrayType = typename TypeTraits<Type>::ArrayType;
    using IndexScalarType = typename TypeTraits<IndexType>::ScalarType;
    const auto& index_scalar = batch[1].scalar_as<IndexScalarType>();
    if (ARROW_PREDICT_FALSE(!index_scalar.is_valid)) {
      return Status::Invalid("Index must not be null");
    }
    ListArrayType list_array(batch[0].array());
    auto index = index_scalar.value;
    if (ARROW_PREDICT_FALSE(index < 0)) {
      return Status::Invalid("Index ", index,
                             " is out of bounds: should be greater than or equal to 0");
    }
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), list_array.value_type(), &builder));
    RETURN_NOT_OK(builder->Reserve(list_array.length()));
    for (int i = 0; i < list_array.length(); ++i) {
      if (list_array.IsNull(i)) {
        RETURN_NOT_OK(builder->AppendNull());
        continue;
      }
      std::shared_ptr<arrow::Array> value_array = list_array.value_slice(i);
      auto len = value_array->length();
      if (ARROW_PREDICT_FALSE(index >= static_cast<typename IndexType::c_type>(len))) {
        return Status::Invalid("Index ", index, " is out of bounds: should be in [0, ",
                               len, ")");
      }
      RETURN_NOT_OK(builder->AppendArraySlice(*value_array->data(), index, 1));
    }
    ARROW_ASSIGN_OR_RAISE(auto result, builder->Finish());
    out->value = result->data();
    return Status::OK();
  }
};

template <typename, typename IndexType>
struct ListElementScalar {
  static Status Exec(KernelContext* /*ctx*/, const ExecBatch& batch, Datum* out) {
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

template <typename InListType>
void AddListElementArrayKernels(ScalarFunction* func) {
  for (const auto& index_type : IntTypes()) {
    auto inputs = {InputType::Array(InListType::type_id), InputType::Scalar(index_type)};
    auto output = OutputType{ListValuesType};
    auto sig = KernelSignature::Make(std::move(inputs), std::move(output),
                                     /*is_varargs=*/false);
    auto scalar_exec = GenerateInteger<ListElementArray, InListType>({index_type->id()});
    ScalarKernel kernel{std::move(sig), std::move(scalar_exec)};
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }
}

void AddListElementArrayKernels(ScalarFunction* func) {
  AddListElementArrayKernels<ListType>(func);
  AddListElementArrayKernels<LargeListType>(func);
  AddListElementArrayKernels<FixedSizeListType>(func);
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

Status MakeStructExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ARROW_ASSIGN_OR_RAISE(auto descr, MakeStructResolve(ctx, batch.GetDescriptors()));

  for (int i = 0; i < batch.num_values(); ++i) {
    const auto& field = checked_cast<const StructType&>(*descr.type).field(i);
    if (batch[i].null_count() > 0 && !field->nullable()) {
      return Status::Invalid("Output field ", field, " (#", i,
                             ") does not allow nulls but the corresponding "
                             "argument was not entirely valid.");
    }
  }

  if (descr.shape == ValueDescr::SCALAR) {
    ScalarVector scalars(batch.num_values());
    for (int i = 0; i < batch.num_values(); ++i) {
      scalars[i] = batch[i].scalar();
    }

    *out =
        Datum(std::make_shared<StructScalar>(std::move(scalars), std::move(descr.type)));
    return Status::OK();
  }

  ArrayVector arrays(batch.num_values());
  for (int i = 0; i < batch.num_values(); ++i) {
    if (batch[i].is_array()) {
      arrays[i] = batch[i].make_array();
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(arrays[i], MakeArrayFromScalar(*batch[i].scalar(), batch.length,
                                                         ctx->memory_pool()));
  }

  *out = std::make_shared<StructArray>(descr.type, batch.length, std::move(arrays));
  return Status::OK();
}

const FunctionDoc make_struct_doc{"Wrap Arrays into a StructArray",
                                  ("Names of the StructArray's fields are\n"
                                   "specified through MakeStructOptions."),
                                  {"*args"},
                                  "MakeStructOptions"};

}  // namespace

void RegisterScalarNested(FunctionRegistry* registry) {
  auto list_value_length = std::make_shared<ScalarFunction>(
      "list_value_length", Arity::Unary(), &list_value_length_doc);
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LIST)}, int32(),
                                         ListValueLength<ListType>));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::FIXED_SIZE_LIST)}, int32(),
                                         FixedSizeListValueLength));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LARGE_LIST)}, int64(),
                                         ListValueLength<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(list_value_length)));

  auto list_element = std::make_shared<ScalarFunction>("list_element", Arity::Binary(),
                                                       &list_element_doc);
  AddListElementArrayKernels(list_element.get());
  AddListElementScalarKernels(list_element.get());
  DCHECK_OK(registry->AddFunction(std::move(list_element)));

  static MakeStructOptions kDefaultMakeStructOptions;
  auto make_struct_function = std::make_shared<ScalarFunction>(
      "make_struct", Arity::VarArgs(), &make_struct_doc, &kDefaultMakeStructOptions);

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

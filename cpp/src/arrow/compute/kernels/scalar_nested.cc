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
void ListValueLength(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
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
}

const FunctionDoc list_value_length_doc{
    "Compute list lengths",
    ("`lists` must have a list-like type.\n"
     "For each non-null value in `lists`, its length is emitted.\n"
     "Null values emit a null in the output."),
    {"lists"}};

Result<ValueDescr> ProjectResolve(KernelContext* ctx,
                                  const std::vector<ValueDescr>& descrs) {
  const auto& names = OptionsWrapper<ProjectOptions>::Get(ctx).field_names;
  if (names.size() != descrs.size()) {
    return Status::Invalid("project() was passed ", names.size(), " field ", "names but ",
                           descrs.size(), " arguments");
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

    fields[i] = field(names[i], descr.type);
    ++i;
  }

  return ValueDescr{struct_(std::move(fields)), shape};
}

void ProjectExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  KERNEL_ASSIGN_OR_RAISE(auto descr, ctx, ProjectResolve(ctx, batch.GetDescriptors()));

  if (descr.shape == ValueDescr::SCALAR) {
    ScalarVector scalars(batch.num_values());
    for (int i = 0; i < batch.num_values(); ++i) {
      scalars[i] = batch[i].scalar();
    }

    *out =
        Datum(std::make_shared<StructScalar>(std::move(scalars), std::move(descr.type)));
    return;
  }

  ArrayVector arrays(batch.num_values());
  for (int i = 0; i < batch.num_values(); ++i) {
    if (batch[i].is_array()) {
      arrays[i] = batch[i].make_array();
      continue;
    }

    KERNEL_ASSIGN_OR_RAISE(
        arrays[i], ctx,
        MakeArrayFromScalar(*batch[i].scalar(), batch.length, ctx->memory_pool()));
  }

  *out = std::make_shared<StructArray>(descr.type, batch.length, std::move(arrays));
}

const FunctionDoc project_doc{"Wrap Arrays into a StructArray",
                              ("Names of the StructArray's fields are\n"
                               "specified through ProjectOptions."),
                              {"*args"},
                              "ProjectOptions"};

}  // namespace

void RegisterScalarNested(FunctionRegistry* registry) {
  auto list_value_length = std::make_shared<ScalarFunction>(
      "list_value_length", Arity::Unary(), &list_value_length_doc);
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LIST)}, int32(),
                                         ListValueLength<ListType>));
  DCHECK_OK(list_value_length->AddKernel({InputType(Type::LARGE_LIST)}, int64(),
                                         ListValueLength<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(list_value_length)));

  auto project_function =
      std::make_shared<ScalarFunction>("project", Arity::VarArgs(), &project_doc);
  ScalarKernel kernel{KernelSignature::Make({InputType{}}, OutputType{ProjectResolve},
                                            /*is_varargs=*/true),
                      ProjectExec, OptionsWrapper<ProjectOptions>::Init};
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(project_function->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(project_function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

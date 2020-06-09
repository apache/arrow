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

#include "arrow/compute/kernels/test_util.h"

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  auto input = ArrayFromJSON(in_ty, json_input);
  auto expected = ArrayFromJSON(out_ty, json_expected);
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {input}, options));
  AssertArraysEqual(*expected, *out.make_array(), /*verbose=*/true);

  // Check all the scalars
  for (int64_t i = 0; i < input->length(); ++i) {
    ASSERT_OK_AND_ASSIGN(auto val, input->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(auto ex_val, expected->GetScalar(i));
    ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, {val}, options));
    AssertScalarsEqual(*ex_val, *out.scalar(), /*verbose=*/true);
  }
}

void ScalarFunctionPropertyMixin::Validate() {
  auto function = GetFunction();

  for (const auto& kernel : function->kernels()) {
    for (auto inputs : GenerateInputs(*kernel->signature)) {
      // FIXME(bkietz) get the output type alongside the inputs
      auto out_type = inputs[0].type();

      auto actual = function->Execute(inputs, GetParam().options);
      auto expected = ComputeExpected(inputs, out_type, GetParam().options);

      if (actual.ok()) {
        // TODO(bkietz) allow approximate equality for floats
        ASSERT_OK(expected.status());
        AssertDatumsEqual(*expected, *actual);
      } else {
        // don't require properties to get the error message right
        ASSERT_EQ(actual.status().code(), expected.status().code());
      }
    }
  }
}

Result<Datum> ScalarFunctionPropertyMixin::ComputeExpected(
    const std::vector<Datum>& args, const std::shared_ptr<DataType>& out_type,
    const FunctionOptions* options) {
  util::optional<int64_t> length;
  for (const Datum& arg : args) {
    if (arg.is_scalar()) continue;

    if (length && *length != arg.length()) {
      return Status::Invalid("mismatched array lengths in ScalarFunction application");
    }
    length = arg.length();
  }

  auto ApplyContractOnce = [&](int64_t i) {
    ScalarVector scalar_args;
    for (const Datum& arg : args) {
      if (arg.is_scalar()) {
        scalar_args.push_back(arg.scalar());
      } else {
        // TODO(bkietz) provide GetScalar(ArrayData)
        scalar_args.push_back(*arg.make_array()->GetScalar(i));
      }
    }
    return Contract(scalar_args, options);
  };

  if (!length) {
    // scalar output
    return ApplyContractOnce(0);
  }

  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(default_memory_pool(), out_type, &builder));
  RETURN_NOT_OK(builder->Resize(*length));

  for (int64_t i = 0; i < *length; ++i) {
    ARROW_ASSIGN_OR_RAISE(auto value, ApplyContractOnce(i));
    RETURN_NOT_OK(builder->Append(*value));
  }

  std::shared_ptr<ArrayData> array;
  RETURN_NOT_OK(builder->FinishInternal(&array));
  return array;
}

template <typename T>
std::vector<std::vector<T>> CartesianProduct(std::vector<std::vector<T>> axes) {
  if (axes.empty()) {
    return {};
  }

  size_t out_length = 1;
  std::vector<typename std::vector<T>::const_iterator> its(axes.size());
  for (size_t i = 0; i < axes.size(); ++i) {
    out_length *= axes[i].size();
    its[i] = axes[i].begin();
  }

  std::vector<std::vector<T>> out(out_length);
  for (auto& point : out) {
    for (auto it : its) {
      point.emplace_back(*it);
    }

    for (size_t i = 0; i < axes.size(); ++i) {
      if (++its[i] != axes[i].end()) {
        break;
      }
      its[i] = axes[i].begin();
    }
  }

  return out;
}

std::vector<std::vector<Datum>> ScalarFunctionPropertyMixin::GenerateInputs(
    const KernelSignature& signature) {
  std::vector<std::vector<Datum>> inputs;

  auto AppendInputsForType = [&](const std::shared_ptr<DataType>& type,
                                 std::vector<Datum>* vec) {
    auto array = rag_.Of(type, GetParam().length, GetParam().null_probability);
    vec->push_back(array);

    auto array_slice = array->Slice(GetParam().length / 3, 2 * GetParam().length / 3);
    vec->push_back(array_slice);

    auto scalar = *rag_.Of(type, 1, 0.0)->GetScalar(0);
    vec->push_back(scalar);

    auto null_scalar = MakeNullScalar(type);
    vec->push_back(null_scalar);
  };

  DCHECK(!signature.is_varargs());
  for (const InputType& in_type : signature.in_types()) {
    DCHECK_EQ(in_type.shape(), ValueDescr::ANY);

    inputs.emplace_back();

    if (in_type.kind() == InputType::EXACT_TYPE) {
      AppendInputsForType(in_type.type(), &inputs.back());
      continue;
    }

    if (in_type.kind() == InputType::ANY_TYPE) {
      // TODO(bkietz) use more input types
      AppendInputsForType(boolean(), &inputs.back());
      continue;
    }

    // TODO(bkietz) iterate over possible types based on a matcher
  }

  return CartesianProduct(std::move(inputs));
}

}  // namespace compute
}  // namespace arrow

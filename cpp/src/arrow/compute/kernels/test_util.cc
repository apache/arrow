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
#include "arrow/array/validate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace compute {

namespace {

template <typename T>
DatumVector GetDatums(const std::vector<T>& inputs) {
  DatumVector datums;
  for (const auto& input : inputs) {
    datums.emplace_back(input);
  }
  return datums;
}

template <typename... SliceArgs>
DatumVector SliceArrays(const DatumVector& inputs, SliceArgs... slice_args) {
  DatumVector sliced;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      sliced.push_back(*input.make_array()->Slice(slice_args...));
    } else {
      sliced.push_back(input);
    }
  }
  return sliced;
}

ScalarVector GetScalars(const DatumVector& inputs, int64_t index) {
  ScalarVector scalars;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      scalars.push_back(*input.make_array()->GetScalar(index));
    } else {
      scalars.push_back(input.scalar());
    }
  }
  return scalars;
}

}  // namespace

void CheckScalarNonRecursive(const std::string& func_name, const DatumVector& inputs,
                             const Datum& expected, const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, inputs, options));
  ValidateOutput(out);
  AssertDatumsEqual(expected, out, /*verbose=*/true);
}

void CheckScalar(std::string func_name, const ScalarVector& inputs,
                 std::shared_ptr<Scalar> expected, const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum out, CallFunction(func_name, GetDatums(inputs), options));
  ValidateOutput(out);
  if (!out.scalar()->Equals(*expected)) {
    std::string summary = func_name + "(";
    for (const auto& input : inputs) {
      summary += input->ToString() + ",";
    }
    summary.back() = ')';

    summary += " = " + out.scalar()->ToString() + " != " + expected->ToString();

    if (!out.type()->Equals(expected->type)) {
      summary += " (types differed: " + out.type()->ToString() + " vs " +
                 expected->type->ToString() + ")";
    }

    FAIL() << summary;
  }
}

void CheckScalar(std::string func_name, const DatumVector& inputs, Datum expected_datum,
                 const FunctionOptions* options) {
  CheckScalarNonRecursive(func_name, inputs, expected_datum, options);

  if (expected_datum.is_scalar()) return;
  ASSERT_TRUE(expected_datum.is_array())
      << "CheckScalar is only implemented for scalar/array expected values";
  auto expected = expected_datum.make_array();

  // check for at least 1 array, and make sure the others are of equal length
  bool has_array = false;
  for (const auto& input : inputs) {
    if (input.is_array()) {
      ASSERT_EQ(input.array()->length, expected->length());
      has_array = true;
    }
  }
  ASSERT_TRUE(has_array) << "Must have at least 1 array input to have an array output";

  // Check all the input scalars
  for (int64_t i = 0; i < expected->length(); ++i) {
    CheckScalar(func_name, GetScalars(inputs, i), *expected->GetScalar(i), options);
  }

  // Since it's a scalar function, calling it on sliced inputs should
  // result in the sliced expected output.
  const auto slice_length = expected->length() / 3;
  if (slice_length > 0) {
    CheckScalarNonRecursive(func_name, SliceArrays(inputs, 0, slice_length),
                            expected->Slice(0, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceArrays(inputs, slice_length, slice_length),
                            expected->Slice(slice_length, slice_length), options);

    CheckScalarNonRecursive(func_name, SliceArrays(inputs, 2 * slice_length),
                            expected->Slice(2 * slice_length), options);
  }

  // Should also work with an empty slice
  CheckScalarNonRecursive(func_name, SliceArrays(inputs, 0, 0), expected->Slice(0, 0),
                          options);

  // Ditto with ChunkedArray inputs
  if (slice_length > 0) {
    DatumVector chunked_inputs;
    chunked_inputs.reserve(inputs.size());
    for (const auto& input : inputs) {
      if (input.is_array()) {
        auto ar = input.make_array();
        auto ar_chunked = std::make_shared<ChunkedArray>(
            ArrayVector{ar->Slice(0, slice_length), ar->Slice(slice_length)});
        chunked_inputs.push_back(ar_chunked);
      } else {
        chunked_inputs.push_back(input.scalar());
      }
    }
    ArrayVector expected_chunks{expected->Slice(0, slice_length),
                                expected->Slice(slice_length)};

    ASSERT_OK_AND_ASSIGN(Datum out,
                         CallFunction(func_name, GetDatums(chunked_inputs), options));
    ValidateOutput(out);
    auto chunked = out.chunked_array();
    (void)chunked;
    AssertDatumsEqual(std::make_shared<ChunkedArray>(expected_chunks), out);
  }
}

Datum CheckDictionaryNonRecursive(const std::string& func_name, const DatumVector& args,
                                  bool result_is_encoded) {
  EXPECT_OK_AND_ASSIGN(Datum actual, CallFunction(func_name, args));
  ValidateOutput(actual);

  DatumVector decoded_args;
  decoded_args.reserve(args.size());
  for (const auto& arg : args) {
    if (arg.type()->id() == Type::DICTIONARY) {
      const auto& to_type = checked_cast<const DictionaryType&>(*arg.type()).value_type();
      EXPECT_OK_AND_ASSIGN(auto decoded, Cast(arg, to_type));
      decoded_args.push_back(decoded);
    } else {
      decoded_args.push_back(arg);
    }
  }
  EXPECT_OK_AND_ASSIGN(Datum expected, CallFunction(func_name, decoded_args));

  if (result_is_encoded) {
    EXPECT_EQ(Type::DICTIONARY, actual.type()->id())
        << "Result should have been dictionary-encoded";
    // Decode before comparison - we care about equivalent not identical results
    const auto& to_type =
        checked_cast<const DictionaryType&>(*actual.type()).value_type();
    EXPECT_OK_AND_ASSIGN(auto decoded, Cast(actual, to_type));
    AssertDatumsApproxEqual(expected, decoded, /*verbose=*/true);
  } else {
    AssertDatumsApproxEqual(expected, actual, /*verbose=*/true);
  }
  return actual;
}

void CheckDictionary(const std::string& func_name, const DatumVector& args,
                     bool result_is_encoded) {
  auto actual = CheckDictionaryNonRecursive(func_name, args, result_is_encoded);

  if (actual.is_scalar()) return;
  ASSERT_TRUE(actual.is_array());
  ASSERT_GE(actual.length(), 0);

  // Check all scalars
  for (int64_t i = 0; i < actual.length(); i++) {
    CheckDictionaryNonRecursive(func_name, GetDatums(GetScalars(args, i)),
                                result_is_encoded);
  }

  // Check slices of the input
  const auto slice_length = actual.length() / 3;
  if (slice_length > 0) {
    CheckDictionaryNonRecursive(func_name, SliceArrays(args, 0, slice_length),
                                result_is_encoded);
    CheckDictionaryNonRecursive(func_name, SliceArrays(args, slice_length, slice_length),
                                result_is_encoded);
    CheckDictionaryNonRecursive(func_name, SliceArrays(args, 2 * slice_length),
                                result_is_encoded);
  }

  // Check empty slice
  CheckDictionaryNonRecursive(func_name, SliceArrays(args, 0, 0), result_is_encoded);

  // Check chunked arrays
  if (slice_length > 0) {
    DatumVector chunked_args;
    chunked_args.reserve(args.size());
    for (const auto& arg : args) {
      if (arg.is_array()) {
        auto arr = arg.make_array();
        ArrayVector chunks{arr->Slice(0, slice_length), arr->Slice(slice_length)};
        chunked_args.push_back(std::make_shared<ChunkedArray>(std::move(chunks)));
      } else {
        chunked_args.push_back(arg);
      }
    }
    CheckDictionaryNonRecursive(func_name, chunked_args, result_is_encoded);
  }
}

void CheckScalarUnary(std::string func_name, Datum input, Datum expected,
                      const FunctionOptions* options) {
  DatumVector input_vector = {std::move(input)};
  CheckScalar(std::move(func_name), input_vector, expected, options);
}

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected, const FunctionOptions* options) {
  CheckScalarUnary(std::move(func_name), ArrayFromJSON(in_ty, json_input),
                   ArrayFromJSON(out_ty, json_expected), options);
}

void CheckVectorUnary(std::string func_name, Datum input, Datum expected,
                      const FunctionOptions* options) {
  ASSERT_OK_AND_ASSIGN(Datum actual, CallFunction(func_name, {input}, options));
  ValidateOutput(actual);
  AssertDatumsEqual(expected, actual, /*verbose=*/true);
}

void CheckScalarBinary(std::string func_name, Datum left_input, Datum right_input,
                       Datum expected, const FunctionOptions* options) {
  CheckScalar(std::move(func_name), {left_input, right_input}, expected, options);
}

void CheckScalarBinaryCommutative(std::string func_name, Datum left_input,
                                  Datum right_input, Datum expected,
                                  const FunctionOptions* options) {
  CheckScalar(func_name, {left_input, right_input}, expected, options);
  CheckScalar(func_name, {right_input, left_input}, expected, options);
}

namespace {

void ValidateOutputImpl(const ArrayData& output) {
  ASSERT_OK(::arrow::internal::ValidateArrayFull(output));
  TestInitialized(output);
}

void ValidateOutputImpl(const ChunkedArray& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& chunk : output.chunks()) {
    TestInitialized(*chunk);
  }
}

void ValidateOutputImpl(const RecordBatch& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& column : output.column_data()) {
    TestInitialized(*column);
  }
}

void ValidateOutputImpl(const Table& output) {
  ASSERT_OK(output.ValidateFull());
  for (const auto& column : output.columns()) {
    for (const auto& chunk : column->chunks()) {
      TestInitialized(*chunk);
    }
  }
}

void ValidateOutputImpl(const Scalar& output) { ASSERT_OK(output.ValidateFull()); }

}  // namespace

void ValidateOutput(const Datum& output) {
  switch (output.kind()) {
    case Datum::ARRAY:
      ValidateOutputImpl(*output.array());
      break;
    case Datum::CHUNKED_ARRAY:
      ValidateOutputImpl(*output.chunked_array());
      break;
    case Datum::RECORD_BATCH:
      ValidateOutputImpl(*output.record_batch());
      break;
    case Datum::TABLE:
      ValidateOutputImpl(*output.table());
      break;
    case Datum::SCALAR:
      ValidateOutputImpl(*output.scalar());
      break;
    default:
      break;
  }
}

void CheckDispatchBest(std::string func_name, std::vector<TypeHolder> original_values,
                       std::vector<TypeHolder> expected_equivalent_values) {
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction(func_name));

  auto values = original_values;
  ASSERT_OK_AND_ASSIGN(auto actual_kernel, function->DispatchBest(&values));

  ASSERT_OK_AND_ASSIGN(auto expected_kernel,
                       function->DispatchExact(expected_equivalent_values));

  EXPECT_EQ(actual_kernel, expected_kernel)
      << "  DispatchBest" << TypeHolder::ToString(original_values) << " => "
      << actual_kernel->signature->ToString() << "\n"
      << "  DispatchExact" << TypeHolder::ToString(expected_equivalent_values) << " => "
      << expected_kernel->signature->ToString();
  EXPECT_EQ(values.size(), expected_equivalent_values.size());
  for (size_t i = 0; i < values.size(); i++) {
    AssertTypeEqual(*values[i], *expected_equivalent_values[i]);
  }
}

void CheckDispatchFails(std::string func_name, std::vector<TypeHolder> types) {
  ASSERT_OK_AND_ASSIGN(auto function, GetFunctionRegistry()->GetFunction(func_name));
  ASSERT_NOT_OK(function->DispatchBest(&types));
  ASSERT_NOT_OK(function->DispatchExact(types));
}

}  // namespace compute
}  // namespace arrow

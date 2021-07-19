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

#pragma once

// IWYU pragma: begin_exports

#include <gmock/gmock.h>

#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernel.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"

// IWYU pragma: end_exports

namespace arrow {

using internal::checked_cast;

namespace compute {

using DatumVector = std::vector<Datum>;

template <typename Type, typename T>
std::shared_ptr<Array> _MakeArray(const std::shared_ptr<DataType>& type,
                                  const std::vector<T>& values,
                                  const std::vector<bool>& is_valid) {
  std::shared_ptr<Array> result;
  if (is_valid.size() > 0) {
    ArrayFromVector<Type, T>(type, is_valid, values, &result);
  } else {
    ArrayFromVector<Type, T>(type, values, &result);
  }
  return result;
}

inline std::string CompareOperatorToFunctionName(CompareOperator op) {
  static std::string function_names[] = {
      "equal", "not_equal", "greater", "greater_equal", "less", "less_equal",
  };
  return function_names[op];
}

void CheckScalar(std::string func_name, const ScalarVector& inputs,
                 std::shared_ptr<Scalar> expected,
                 const FunctionOptions* options = nullptr);

void CheckScalar(std::string func_name, const DatumVector& inputs, Datum expected,
                 const FunctionOptions* options = nullptr);

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected,
                      const FunctionOptions* options = nullptr);

void CheckScalarUnary(std::string func_name, std::shared_ptr<Array> input,
                      std::shared_ptr<Array> expected,
                      const FunctionOptions* options = nullptr);

void CheckScalarUnary(std::string func_name, std::shared_ptr<Scalar> input,
                      std::shared_ptr<Scalar> expected,
                      const FunctionOptions* options = nullptr);

void CheckScalarBinary(std::string func_name, std::shared_ptr<Scalar> left_input,
                       std::shared_ptr<Scalar> right_input,
                       std::shared_ptr<Scalar> expected,
                       const FunctionOptions* options = nullptr);

void CheckScalarBinary(std::string func_name, std::shared_ptr<Array> left_input,
                       std::shared_ptr<Array> right_input,
                       std::shared_ptr<Array> expected,
                       const FunctionOptions* options = nullptr);

void CheckScalarBinary(std::string func_name, std::shared_ptr<Array> left_input,
                       std::shared_ptr<Scalar> right_input,
                       std::shared_ptr<Array> expected,
                       const FunctionOptions* options = nullptr);

void CheckScalarBinary(std::string func_name, std::shared_ptr<Scalar> left_input,
                       std::shared_ptr<Array> right_input,
                       std::shared_ptr<Array> expected,
                       const FunctionOptions* options = nullptr);

void CheckVectorUnary(std::string func_name, Datum input, std::shared_ptr<Array> expected,
                      const FunctionOptions* options = nullptr);

void ValidateOutput(const Datum& output);

using BinaryTypes =
    ::testing::Types<BinaryType, LargeBinaryType, StringType, LargeStringType>;
using StringTypes = ::testing::Types<StringType, LargeStringType>;

static constexpr random::SeedType kRandomSeed = 0x0ff1ce;

template <template <typename> class DoTestFunctor>
void TestRandomPrimitiveCTypes() {
  DoTestFunctor<Int8Type>::Test(int8());
  DoTestFunctor<Int16Type>::Test(int16());
  DoTestFunctor<Int32Type>::Test(int32());
  DoTestFunctor<Int64Type>::Test(int64());
  DoTestFunctor<UInt8Type>::Test(uint8());
  DoTestFunctor<UInt16Type>::Test(uint16());
  DoTestFunctor<UInt32Type>::Test(uint32());
  DoTestFunctor<UInt64Type>::Test(uint64());
  DoTestFunctor<FloatType>::Test(float32());
  DoTestFunctor<DoubleType>::Test(float64());
  DoTestFunctor<Date32Type>::Test(date32());
  DoTestFunctor<Date64Type>::Test(date64());
  DoTestFunctor<Time32Type>::Test(time32(TimeUnit::SECOND));
  DoTestFunctor<Time64Type>::Test(time64(TimeUnit::MICRO));
  DoTestFunctor<TimestampType>::Test(timestamp(TimeUnit::SECOND));
  DoTestFunctor<TimestampType>::Test(timestamp(TimeUnit::MICRO));
  DoTestFunctor<DurationType>::Test(duration(TimeUnit::MILLI));
}

// Check that DispatchBest on a given function yields the same Kernel as
// produced by DispatchExact on another set of ValueDescrs.
void CheckDispatchBest(std::string func_name, std::vector<ValueDescr> descrs,
                       std::vector<ValueDescr> exact_descrs);

// Check that function fails to produce a Kernel for the set of ValueDescrs.
void CheckDispatchFails(std::string func_name, std::vector<ValueDescr> descrs);

// Helper to get a default instance of a type, including parameterized types
template <typename T>
enable_if_parameter_free<T, std::shared_ptr<DataType>> default_type_instance() {
  return TypeTraits<T>::type_singleton();
}
template <typename T>
enable_if_time<T, std::shared_ptr<DataType>> default_type_instance() {
  // Time32 requires second/milli, Time64 requires nano/micro
  if (bit_width(T::type_id) == 32) {
    return std::make_shared<T>(TimeUnit::type::SECOND);
  }
  return std::make_shared<T>(TimeUnit::type::NANO);
}
template <typename T>
enable_if_timestamp<T, std::shared_ptr<DataType>> default_type_instance() {
  return std::make_shared<T>(TimeUnit::type::SECOND);
}
template <typename T>
enable_if_decimal<T, std::shared_ptr<DataType>> default_type_instance() {
  return std::make_shared<T>(5, 2);
}

}  // namespace compute
}  // namespace arrow

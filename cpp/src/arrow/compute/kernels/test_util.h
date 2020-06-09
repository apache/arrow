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

#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include "arrow/array.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/pretty_print.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"

#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"

// IWYU pragma: end_exports

namespace arrow {
namespace compute {

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

template <typename Type, typename Enable = void>
struct DatumEqual {};

template <typename Type>
struct DatumEqual<Type, enable_if_floating_point<Type>> {
  static constexpr double kArbitraryDoubleErrorBound = 1.0;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = internal::checked_cast<const ScalarType*>(lhs.scalar().get());
      auto right = internal::checked_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(left->is_valid, right->is_valid);
      ASSERT_EQ(left->type->id(), right->type->id());
      ASSERT_NEAR(left->value, right->value, kArbitraryDoubleErrorBound);
    }
  }
};

template <typename Type>
struct DatumEqual<Type, enable_if_integer<Type>> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = internal::checked_cast<const ScalarType*>(lhs.scalar().get());
      auto right = internal::checked_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(*left, *right);
    }
  }
};

void CheckScalarUnary(std::string func_name, std::shared_ptr<DataType> in_ty,
                      std::string json_input, std::shared_ptr<DataType> out_ty,
                      std::string json_expected,
                      const FunctionOptions* options = nullptr);

using TestingStringTypes =
    ::testing::Types<StringType, LargeStringType, BinaryType, LargeBinaryType>;

static constexpr random::SeedType kRandomSeed = 0x0ff1ce;

struct ScalarFunctionPropertyTestParam {
  ScalarFunctionPropertyTestParam(int64_t length, double null_probability)
      : length(length), null_probability(null_probability) {}

  ScalarFunctionPropertyTestParam(int64_t length, double null_probability,
                                  const FunctionOptions* options)
      : length(length), null_probability(null_probability), options(options) {}

  static auto Values(std::initializer_list<ScalarFunctionPropertyTestParam> params)
      -> decltype(testing::ValuesIn(params)) {
    return testing::ValuesIn(params);
  }

  int64_t length;
  double null_probability;
  const FunctionOptions* options = nullptr;
};

class ScalarFunctionPropertyMixin
    : public testing::TestWithParam<ScalarFunctionPropertyTestParam> {
 protected:
  /// Return an instance of the ScalarFunction which should be tested.
  virtual std::shared_ptr<ScalarFunction> GetFunction() = 0;

  /// Contract() should contain a minimal implementation of the function's
  /// intended behavior. For example, a Property expressing a division function
  /// could unbox the Scalars and perform division on them.
  ///
  /// The arguments will be generated based on kernel signatures so validation of anything
  /// expressible in InputType is unnecessary (for example inputs will always have valid
  /// arity and type). Other errors should be emitted with the same StatusCode that a
  /// kernel would raise - in the example case of a division function a divide by zero
  /// error should be emitted by Contract().
  virtual Result<std::shared_ptr<Scalar>> Contract(const ScalarVector& args,
                                                   const FunctionOptions* options) = 0;

  /// Run randomized testing of all kernels in the function.
  void Validate();

  /// Helper for unboxing scalars efficiently in implementations of Contract()
  template <typename S>
  Result<std::shared_ptr<S>> Cast(const std::shared_ptr<Scalar>& scalar) {
    ARROW_ASSIGN_OR_RAISE(
        auto out, scalar->CastTo(TypeTraits<typename S::TypeClass>::type_singleton()));
    return internal::checked_pointer_cast<S>(std::move(out));
  }

  // apply Contract() to all inputs, building the expected output
  Result<Datum> ComputeExpected(const std::vector<Datum>& args,
                                const std::shared_ptr<DataType>& out_type,
                                const FunctionOptions* options);

  // Randomly generate valid function arguments from a KernelSignature.
  std::vector<std::vector<Datum>> GenerateInputs(const KernelSignature& signature);

  random::RandomArrayGenerator rag_{kRandomSeed};
};

}  // namespace compute
}  // namespace arrow

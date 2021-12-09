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

#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/optional.h"

#include <algorithm>
#include <iostream>

namespace arrow {
namespace compute {
namespace internal {

namespace {
// ----------------------------------------------------------------------
// IsMonotonic implementation

using IsMonotonicState = OptionsWrapper<IsMonotonicOptions>;

template <typename Type>
Status IsMonotonic(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // Not sure if this can fail.
  MonotonicityOrder order = IsMonotonicState::Get(ctx).order;

  // Check batch size
  if (batch.values.size() != 1) {
    return Status::Invalid("IsMonotonic expects a single datum (array) as input");
  }
  // Made sure there is at least one input datum.
  Datum input = batch[0];

  // Validate input datum type
  // - how much is handled by the type stuff in the registry?
  // I think this is unreachable (at least when invoked through the compute registry)
  if (!input.is_array()) {
    return Status::Invalid("IsMonotonic expects array datum as input");
  }

  // Made sure that the input datum is an array.
  const std::shared_ptr<ArrayData>& array_data = input.array();
  typename TypeTraits<Type>::ArrayType array(array_data);

  // Initial output for early-exists.
  *out = Datum(false);

  // Return early if there is just zero elements or one element in the array.
  if (array.length() <= 1) {
    *out = Datum(true);
    return Status::OK();
  }

  // Safety:
  // - Made sure that the length is at least 2 above.
  for (auto a = array.begin(), b = ++array.begin(); b != array.end(); ++a, ++b) {
    auto current = *a;
    auto next = *b;

    switch (order) {
      case MonotonicityOrder::Increasing: {
        if (!(next >= current)) {
          return Status::OK();
        }
        break;
      }
      case MonotonicityOrder::StrictlyIncreasing: {
        if (!(next > current)) {
          return Status::OK();
        }
        break;
      }
      case MonotonicityOrder::Decreasing: {
        if (!(next <= current)) {
          return Status::OK();
        }
        break;
      }
      case MonotonicityOrder::StrictlyDecreasing: {
        if (!(next < current)) {
          return Status::OK();
        }
        break;
      }
    }
  }

  // At this point the for loop did not early-exit.
  *out = Datum(true);

  return Status::OK();
}

}  // namespace

const FunctionDoc is_monotonic_doc{
    "Returns if the array contains monotonically increasing/decreasing"
    "values",
    ("Returns a BooleanScalar(true) only if all the elements of the input array \n"
     "are in ascending/descending order.\n"
     "The options define the order that is used to determine the monotonicity.\n"
     "Always returns BooleanScalar.\n"
     "Implemented for arrays with well-ordered element types."),
    {"array"},
    "IsMonotonicOptions"};

template <typename Type>
Status AddIsMonotonicKernel(VectorFunction* func) {
  static const ValueDescr output_type = ValueDescr::Scalar(boolean());
  VectorKernel is_monotonic_base;
  is_monotonic_base.init = IsMonotonicState::Init;
  is_monotonic_base.can_execute_chunkwise = false;
  is_monotonic_base.signature =
      KernelSignature::Make({TypeTraits<Type>::type_singleton()}, output_type);
  is_monotonic_base.exec = IsMonotonic<Type>;
  return func->AddKernel(is_monotonic_base);
}

void RegisterVectorIsMonotonic(FunctionRegistry* registry) {
  static const IsMonotonicOptions default_options;
  auto func = std::make_shared<VectorFunction>("is_monotonic", Arity::Unary(),
                                               &is_monotonic_doc, &default_options);

  DCHECK_OK(AddIsMonotonicKernel<Int8Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Int16Type>(func.get()));

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal

}  // namespace compute
}  // namespace arrow

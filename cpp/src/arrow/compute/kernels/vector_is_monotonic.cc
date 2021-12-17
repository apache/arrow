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

namespace arrow {
namespace compute {
namespace internal {

namespace {
// ----------------------------------------------------------------------
// IsMonotonic implementation

using IsMonotonicState = OptionsWrapper<IsMonotonicOptions>;

Status IsMonotonicOutput(bool increasing, bool strictly_increasing, bool decreasing,
                         bool strictly_decreasing, Datum* out) {
  ARROW_ASSIGN_OR_RAISE(
      *out, StructScalar::Make({std::make_shared<BooleanScalar>(increasing),
                                std::make_shared<BooleanScalar>(strictly_increasing),
                                std::make_shared<BooleanScalar>(decreasing),
                                std::make_shared<BooleanScalar>(strictly_decreasing)},
                               {"increasing", "strictly_increasing", "decreasing",
                                "strictly_decreasing"}));
  return Status::OK();
}

template <typename DataType>
enable_if_floating_point<DataType> IsMonotonicCheck(
    const typename DataType::c_type& current, const typename DataType::c_type& next,
    bool* increasing, bool* strictly_increasing, bool* decreasing,
    bool* strictly_decreasing, const IsMonotonicOptions& options) {
  // Short circuit for NaNs.
  // https://en.wikipedia.org/wiki/NaN#Comparison_with_NaN
  if (std::isnan(current) || std::isnan(next)) {
    *increasing = false;
    *strictly_increasing = false;
    *decreasing = false;
    *strictly_decreasing = false;
  } else {
    bool equal =
        // Approximately equal within some error bound (epsilon).
        (options.floating_approximate &&
         (fabs(current - next) <=
          static_cast<typename DataType::c_type>(options.epsilon))) ||
        // Or exactly equal.
        current == next;
    if (*increasing) {
      if (!(equal || next > current)) {
        *increasing = false;
        *strictly_increasing = false;
      }
    }
    if (*decreasing) {
      if (!(equal || next < current)) {
        *decreasing = false;
        *strictly_decreasing = false;
      }
    }
    if (*strictly_increasing) {
      if (equal || !(next > current)) {
        *strictly_increasing = false;
      }
    }
    if (*strictly_decreasing) {
      if (equal || !(next < current)) {
        *strictly_decreasing = false;
      }
    }
  }
}

template <typename DataType>
enable_if_not_floating_point<DataType> IsMonotonicCheck(
    const typename DataType::c_type& current, const typename DataType::c_type& next,
    bool* increasing, bool* strictly_increasing, bool* decreasing,
    bool* strictly_decreasing, const IsMonotonicOptions& options) {
  if (*increasing) {
    if (!(next >= current)) {
      *increasing = false;
      *strictly_increasing = false;
    }
  }
  if (*strictly_increasing) {
    if (!(next > current)) {
      *strictly_increasing = false;
    }
  }
  if (*decreasing) {
    if (!(next <= current)) {
      *decreasing = false;
      *strictly_decreasing = false;
    }
  }
  if (*strictly_decreasing) {
    if (!(next < current)) {
      *strictly_decreasing = false;
    }
  }
}

template <typename DataType>
enable_if_floating_point<DataType, bool> isnan(
    const util::optional<typename DataType::c_type>& opt) {
  return opt.has_value() && std::isnan(opt.value());
}

template <typename DataType>
enable_if_not_floating_point<DataType, bool> isnan(
    const util::optional<typename DataType::c_type>& opt) {
  return false;
}

template <typename DataType>
constexpr enable_if_floating_point<DataType, typename DataType::c_type> min() {
  return -std::numeric_limits<typename DataType::c_type>::infinity();
}

template <typename DataType>
constexpr enable_if_floating_point<DataType, typename DataType::c_type> max() {
  return std::numeric_limits<typename DataType::c_type>::infinity();
}

template <typename DataType>
constexpr enable_if_not_floating_point<DataType, typename DataType::c_type> min() {
  return std::numeric_limits<typename DataType::c_type>::min();
}

template <typename DataType>
constexpr enable_if_not_floating_point<DataType, typename DataType::c_type> max() {
  return std::numeric_limits<typename DataType::c_type>::max();
}

template <typename DataType>
Status IsMonotonic(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  using ArrayType = typename TypeTraits<DataType>::ArrayType;
  using CType = typename TypeTraits<DataType>::CType;

  auto options = IsMonotonicState::Get(ctx);

  // Check batch size
  if (batch.values.size() != 1) {
    return Status::Invalid("IsMonotonic expects a single datum (array) as input");
  }

  // Safety:
  // - Made sure there is at least one input datum.
  Datum input = batch[0];

  // Validate input datum type (useful for direct invocation only).
  if (!input.is_array()) {
    return Status::Invalid("IsMonotonic expects array datum as input");
  }

  // Safety:
  // - Made sure that the input datum is an array.
  const std::shared_ptr<ArrayData>& array_data = input.array();
  ArrayType array(array_data);

  // Return early if there are zero elements or one element in the array.
  // And return early if there are only nulls.
  if (array.length() <= 1 || array.null_count() == array.length()) {
    if (std::any_of(array.begin(), array.end(), isnan<DataType>)) {
      return IsMonotonicOutput(false, false, false, false, out);
    } else {
      // It is strictly increasing if there are zero or one elements or when nulls are
      // ignored.
      bool strictly =
          array.length() <= 1 ||
          options.null_handling == IsMonotonicOptions::NullHandling::IGNORE_NULLS;
      return IsMonotonicOutput(true, strictly, true, strictly, out);
    }
  }

  // Set null value based on option.
  const CType null_value =
      options.null_handling == IsMonotonicOptions::NullHandling::USE_MIN_VALUE
          ? min<DataType>()
          : max<DataType>();

  bool increasing = true, strictly_increasing = true, decreasing = true,
       strictly_decreasing = true;

  // Safety:
  // - Made sure that the length is at least 2 above.
  for (auto a = array.begin(), b = ++array.begin(); b != array.end();) {
    auto current = *a;
    auto next = *b;

    // Handle nulls.
    if (options.null_handling == IsMonotonicOptions::NullHandling::IGNORE_NULLS) {
      // Forward both iterators to search for a non-null value. The loop exit
      // condition prevents reading past the end.
      if (!current.has_value()) {
        ++a;
        ++b;
        continue;
      }
      // Once we have a value for current we should also make sure that next has a
      // value. The loop exit condition prevents reading past the end.
      if (!next.has_value()) {
        ++b;
        continue;
      }
    }

    IsMonotonicCheck<DataType>(current.value_or(null_value), next.value_or(null_value),
                               &increasing, &strictly_increasing, &decreasing,
                               &strictly_decreasing, options);

    // Early exit if all failed:
    if (!increasing && !strictly_increasing && !decreasing && !strictly_decreasing) {
      break;
    } else {
      ++a;
      ++b;
    }
  }

  // Output
  return IsMonotonicOutput(increasing, strictly_increasing, decreasing,
                           strictly_decreasing, out);
}

}  // namespace

const FunctionDoc is_monotonic_doc{
    "Returns whether the array contains monotonically (strictly)"
    "increasing/decreasing values",
    ("Returns a StructScalar indicating whether the values in the array are \n"
     "increasing, strictly increasing, decreasing and/or strictly decreasing.\n"
     "Output type is struct<increasing: boolean, strictly_increasing: boolean,\n"
     "decreasing: boolean, strictly_decreasing: boolean>.\n"
     "Null values are ignored by default.\n"
     "Implemented for arrays with well-ordered element types."),
    {"array"},
    "IsMonotonicOptions"};

template <typename Type>
Status AddIsMonotonicKernel(VectorFunction* func) {
  static const ValueDescr output_type = ValueDescr::Scalar(struct_({
      field("increasing", boolean()),
      field("strictly_increasing", boolean()),
      field("decreasing", boolean()),
      field("strictly_decreasing", boolean()),
  }));
  VectorKernel is_monotonic_base;
  is_monotonic_base.init = IsMonotonicState::Init;
  is_monotonic_base.can_execute_chunkwise = false;
  is_monotonic_base.signature =
      KernelSignature::Make({InputType::Array(Type::type_id)}, output_type);
  is_monotonic_base.exec = IsMonotonic<Type>;
  return func->AddKernel(is_monotonic_base);
}

void RegisterVectorIsMonotonic(FunctionRegistry* registry) {
  static const IsMonotonicOptions default_options;
  auto func = std::make_shared<VectorFunction>("is_monotonic", Arity::Unary(),
                                               &is_monotonic_doc, &default_options);

  DCHECK_OK(AddIsMonotonicKernel<BooleanType>(func.get()));

  // Signed and unsigned integer types
  DCHECK_OK(AddIsMonotonicKernel<Int8Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<UInt8Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Int16Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<UInt16Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Int32Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<UInt32Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Int64Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<UInt64Type>(func.get()));

  // Floating point types
  // DCHECK_OK(AddIsMonotonicKernel<HalfFloatType>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<FloatType>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<DoubleType>(func.get()));

  // Temporal types
  DCHECK_OK(AddIsMonotonicKernel<Time32Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Time64Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<TimestampType>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Date32Type>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<Date64Type>(func.get()));
  // DCHECK_OK(AddIsMonotonicKernel<DayTimeIntervalType>(func.get()));
  // DCHECK_OK(AddIsMonotonicKernel<MonthDayNanoIntervalType>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<MonthIntervalType>(func.get()));
  DCHECK_OK(AddIsMonotonicKernel<DurationType>(func.get()));

  // Decimal types
  // DCHECK_OK(AddIsMonotonicKernel<Decimal128Type>(func.get()));
  // DCHECK_OK(AddIsMonotonicKernel<Decimal256Type>(func.get()));

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal

}  // namespace compute
}  // namespace arrow

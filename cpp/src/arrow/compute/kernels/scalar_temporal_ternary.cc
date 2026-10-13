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

#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

struct MonthDayNanoIntervalFromPartsFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));
    EnsureDictionaryDecoded(types);
    for (const auto& type : *types) {
      if (!is_integer(type.id()) && type.id() != Type::NA) {
        return arrow::compute::detail::NoMatchingKernel(this, *types);
      }
    }
    // The executor safely casts integer components to their physical widths.
    (*types)[0] = int32();
    (*types)[1] = int32();
    (*types)[2] = int64();
    return ScalarFunction::DispatchExact(*types);
  }
};

template <typename Type>
struct IntervalPartValues {
  using CType = typename Type::c_type;

  explicit IntervalPartValues(const ExecValue& value) {
    if (value.is_array() && value.array.buffers[1].data != nullptr) {
      values = value.array.GetValues<CType>(1);
    } else if (value.is_scalar() && value.scalar->is_valid) {
      scalar = UnboxScalar<Type>::Unbox(*value.scalar);
    }
  }

  CType operator[](int64_t i) const { return values ? values[i] : scalar; }

  const CType* values = nullptr;
  CType scalar = 0;
};

Status ExecMonthDayNanoIntervalFromParts(KernelContext*, const ExecSpan& batch,
                                         ExecResult* out) {
  if (batch.length == 0) return Status::OK();

  const IntervalPartValues<Int32Type> months(batch[0]);
  const IntervalPartValues<Int32Type> days(batch[1]);
  const IntervalPartValues<Int64Type> nanoseconds(batch[2]);
  auto* output = out->array_span_mutable();
  auto* values = output->GetValues<MonthDayNanoIntervalType::MonthDayNanos>(1);
  // The executor has already intersected the three input validity bitmaps.
  // Visit only valid rows, including when an input is a null scalar.
  arrow::internal::VisitSetBitRunsVoid(
      output->buffers[0].data, output->offset, batch.length,
      [&](int64_t start, int64_t length) {
        for (int64_t i = start; i < start + length; ++i) {
          values[i] = {months[i], days[i], nanoseconds[i]};
        }
      });
  return Status::OK();
}

const FunctionDoc month_day_nano_interval_from_parts_doc{
    "Construct month-day-nanosecond intervals from separate components",
    ("Months, days, and nanoseconds must be integer arrays or scalars.\n"
     "Months and days are safely cast to int32, and nanoseconds to int64.\n"
     "Scalar inputs are broadcast. If any component is null, the interval is null.\n"
     "Components are preserved without normalization."),
    {"months", "days", "nanoseconds"}};

}  // namespace

void RegisterScalarTemporalTernary(FunctionRegistry* registry) {
  auto function = std::make_shared<MonthDayNanoIntervalFromPartsFunction>(
      "month_day_nano_interval_from_parts", Arity::Ternary(),
      month_day_nano_interval_from_parts_doc);
  DCHECK_OK(function->AddKernel({int32(), int32(), int64()}, month_day_nano_interval(),
                                ExecMonthDayNanoIntervalFromParts));
  DCHECK_OK(registry->AddFunction(std::move(function)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

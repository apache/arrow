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

#include <cstring>
#include <datasketches-cpp/hll/include/hll.hpp>

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/hll.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename ArrowType, typename VisitorArgType>
struct HllImpl : public ScalarAggregator {
  const HllOptions options;
  ::arrow::internal::HllImpl hll;

  using ThisType = HllImpl<ArrowType, VisitorArgType>;

  explicit HllImpl(const HllOptions& options)
      : options{options}, hll{options.lg_config_k} {}

  template <typename T>
  void Update(T value) {
    hll.Update(value);
  }

  Status Consume(KernelContext*, const ExecSpan& batch) override {
    if (batch[0].is_array()) {
      const ArraySpan& arr = batch[0].array;
      auto visit_null = []() {};
      auto visit_value = [&](VisitorArgType arg) { this->Update(arg); };
      VisitArraySpanInline<ArrowType>(arr, visit_value, visit_null);
    } else {
      const Scalar& input = *batch[0].scalar;
      if (input.is_valid) {
        this->Update(UnboxScalar<ArrowType>::Unbox(input));
      }
    }
    return Status::OK();
  }

  Status MergeFrom(KernelContext*, KernelState&& src) override {
    const auto& other = checked_cast<const ThisType&>(src);
    this->hll.Merge(other.hll);
    return Status::OK();
  }

  Status Finalize(KernelContext*, Datum* out) override {
    auto ndv = hll.Finalize();
    out->value = std::make_shared<DoubleScalar>(ndv);
    return Status::OK();
  }
};

template <typename Type, typename VisitorArgType>
Result<std::unique_ptr<KernelState>> HllInit(KernelContext*, const KernelInitArgs& args) {
  return std::make_unique<HllImpl<Type, VisitorArgType>>(
      static_cast<const HllOptions&>(*args.options));
}

const FunctionDoc hll_doc{
    "Calculate the approximate number of distinct (and non-NULL) values of an array",
    ("The precision can be controlled using HllOptions.\n"
     "Nulls are ignored. All NaNs of a given type are treated as equal."),
    {"array"},
    "HllOptions"};

template <typename Type, typename VisitorArgType = typename Type::c_type>
void AddHllKernel(InputType type, ScalarAggregateFunction* func) {
  AddAggKernel(KernelSignature::Make({type}, float64()), HllInit<Type, VisitorArgType>,
               func);
}

std::shared_ptr<ScalarAggregateFunction> AddHllAggKernels() {
  static auto default_hll_options = HllOptions::Defaults();
  auto func = std::make_shared<ScalarAggregateFunction>("hll", Arity::Unary(), hll_doc,
                                                        &default_hll_options);
  // Boolean
  AddHllKernel<BooleanType>(boolean(), func.get());
  // Numeric
  AddHllKernel<Int8Type>(int8(), func.get());
  AddHllKernel<Int16Type>(int16(), func.get());
  AddHllKernel<Int32Type>(int32(), func.get());
  AddHllKernel<Int64Type>(int64(), func.get());
  AddHllKernel<UInt8Type>(uint8(), func.get());
  AddHllKernel<UInt16Type>(uint16(), func.get());
  AddHllKernel<UInt32Type>(uint32(), func.get());
  AddHllKernel<UInt64Type>(uint64(), func.get());
  AddHllKernel<HalfFloatType>(float16(), func.get());
  AddHllKernel<FloatType>(float32(), func.get());
  AddHllKernel<DoubleType>(float64(), func.get());
  // Date
  AddHllKernel<Date32Type>(date32(), func.get());
  AddHllKernel<Date64Type>(date64(), func.get());
  // Time
  AddHllKernel<Time32Type>(match::SameTypeId(Type::TIME32), func.get());
  AddHllKernel<Time64Type>(match::SameTypeId(Type::TIME64), func.get());
  // Timestamp & Duration
  AddHllKernel<TimestampType>(match::SameTypeId(Type::TIMESTAMP), func.get());
  AddHllKernel<DurationType>(match::SameTypeId(Type::DURATION), func.get());
  // Interval
  AddHllKernel<MonthIntervalType>(month_interval(), func.get());
  AddHllKernel<DayTimeIntervalType>(day_time_interval(), func.get());
  AddHllKernel<MonthDayNanoIntervalType>(month_day_nano_interval(), func.get());
  // Binary & String
  AddHllKernel<BinaryType, std::string_view>(match::BinaryLike(), func.get());
  AddHllKernel<LargeBinaryType, std::string_view>(match::LargeBinaryLike(), func.get());
  // Fixed binary & Decimal
  AddHllKernel<FixedSizeBinaryType, std::string_view>(match::FixedSizeBinaryLike(),
                                                      func.get());
  return func;
}

void RegisterScalarAggregateHll(FunctionRegistry* registry) {
  auto hll = AddHllAggKernels();
  DCHECK_OK(registry->AddFunction(hll));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow

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

#include "arrow/compute/kernels/count.h"

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/aggregate.h"

namespace arrow {
namespace compute {

struct CountState {
  CountState() : non_nulls(0), nulls(0) {}
  CountState(int64_t non_nulls, int64_t nulls) : non_nulls(non_nulls), nulls(nulls) {}

  CountState operator+(const CountState& rhs) const {
    return CountState(this->non_nulls + rhs.non_nulls, this->nulls + rhs.nulls);
  }

  CountState& operator+=(const CountState& rhs) {
    this->non_nulls += rhs.non_nulls;
    this->nulls += rhs.nulls;
    return *this;
  }

  std::shared_ptr<Scalar> NonNullsAsScalar() const {
    using ScalarType = typename CTypeTraits<int64_t>::ScalarType;
    return std::make_shared<ScalarType>(non_nulls);
  }

  std::shared_ptr<Scalar> NullsAsScalar() const {
    using ScalarType = typename CTypeTraits<int64_t>::ScalarType;
    return std::make_shared<ScalarType>(nulls);
  }

  int64_t non_nulls = 0;
  int64_t nulls = 0;
};

class CountAggregateFunction final : public AggregateFunctionStaticState<CountState> {
 public:
  explicit CountAggregateFunction(const CountOptions& options) : options_(options) {}

  Status Consume(const Array& input, CountState* state) const override {
    const int64_t length = input.length();
    const int64_t nulls = input.null_count();

    state->nulls = nulls;
    state->non_nulls = length - nulls;

    return Status::OK();
  }

  Status Merge(const CountState& src, CountState* dst) const override {
    *dst += src;
    return Status::OK();
  }

  Status Finalize(const CountState& src, Datum* output) const override {
    switch (options_.count_mode) {
      case CountOptions::COUNT_ALL:
        *output = src.NonNullsAsScalar();
        break;
      case CountOptions::COUNT_NULL:
        *output = src.NullsAsScalar();
        break;
      default:
        return Status::Invalid("Unknown CountOptions encountered");
    }

    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return int64(); }

 private:
  CountOptions options_;
};

std::shared_ptr<AggregateFunction> MakeCountAggregateFunction(
    FunctionContext* context, const CountOptions& options) {
  return std::make_shared<CountAggregateFunction>(options);
}

Status Count(FunctionContext* context, const CountOptions& options, const Datum& value,
             Datum* out) {
  if (!value.is_array()) return Status::Invalid("Count is expecting an array datum.");

  auto aggregate = MakeCountAggregateFunction(context, options);
  auto kernel = std::make_shared<AggregateUnaryKernel>(aggregate);

  return kernel->Call(context, value, out);
}

Status Count(FunctionContext* context, const CountOptions& options, const Array& array,
             Datum* out) {
  return Count(context, options, array.data(), out);
}

}  // namespace compute
}  // namespace arrow

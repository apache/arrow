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

#include "arrow/compute/kernels/filter.h"

#include <limits>
#include <memory>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/take-internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

// IndexSequence which yields the indices of positions in a BooleanArray
// which are either null or true
class FilterIndexSequence {
 public:
  // constexpr so we'll never instantiate bounds checking
  constexpr bool never_out_of_bounds() const { return true; }
  void set_never_out_of_bounds() {}

  constexpr FilterIndexSequence() = default;

  FilterIndexSequence(const BooleanArray& filter, int64_t out_length)
      : filter_(&filter), out_length_(out_length) {}

  std::pair<int64_t, bool> Next() {
    // skip until an index is found at which the filter is either null or true
    while (filter_->IsValid(index_) && !filter_->Value(index_)) {
      ++index_;
    }
    bool is_valid = filter_->IsValid(index_);
    return std::make_pair(index_++, is_valid);
  }

  int64_t length() const { return out_length_; }

  int64_t null_count() const { return filter_->null_count(); }

 private:
  const BooleanArray* filter_ = nullptr;
  int64_t index_ = 0, out_length_ = -1;
};

// TODO(bkietz) this can be optimized
static int64_t OutputSize(const BooleanArray& filter) {
  auto offset = filter.offset();
  auto length = filter.length();
  int64_t size = 0;
  for (auto i = offset; i < offset + length; ++i) {
    if (filter.IsNull(i) || filter.Value(i)) {
      ++size;
    }
  }
  return size;
}

class FilterKernelImpl : public FilterKernel {
 public:
  FilterKernelImpl(const std::shared_ptr<DataType>& type,
                   std::unique_ptr<Taker<FilterIndexSequence>> taker)
      : FilterKernel(type), taker_(std::move(taker)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t length, std::shared_ptr<Array>* out) override {
    if (values.length() != filter.length()) {
      return Status::Invalid("filter and value array must have identical lengths");
    }
    RETURN_NOT_OK(taker_->Init(ctx->memory_pool()));
    RETURN_NOT_OK(taker_->Take(values, FilterIndexSequence(filter, length)));
    return taker_->Finish(out);
  }

  std::unique_ptr<Taker<FilterIndexSequence>> taker_;
};

Status FilterKernel::Make(const std::shared_ptr<DataType>& value_type,
                          std::unique_ptr<FilterKernel>* out) {
  std::unique_ptr<Taker<FilterIndexSequence>> taker;
  RETURN_NOT_OK(Taker<FilterIndexSequence>::Make(value_type, &taker));

  out->reset(new FilterKernelImpl(value_type, std::move(taker)));
  return Status::OK();
}

Status FilterKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
                          Datum* out) {
  if (!values.is_array() || !filter.is_array()) {
    return Status::Invalid("FilterKernel expects array values and filter");
  }
  auto values_array = values.make_array();
  auto filter_array = checked_pointer_cast<BooleanArray>(filter.make_array());
  std::shared_ptr<Array> out_array;
  RETURN_NOT_OK(this->Filter(ctx, *values_array, *filter_array, OutputSize(*filter_array),
                             &out_array));
  *out = out_array;
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out) {
  Datum out_datum;
  RETURN_NOT_OK(Filter(ctx, Datum(values.data()), Datum(filter.data()), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const Datum& values, const Datum& filter,
              Datum* out) {
  std::unique_ptr<FilterKernel> kernel;
  RETURN_NOT_OK(FilterKernel::Make(values.type(), &kernel));
  return kernel->Call(ctx, values, filter, out);
}

}  // namespace compute
}  // namespace arrow

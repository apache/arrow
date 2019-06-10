// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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

#include <algorithm>
#include <memory>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/filter.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

using internal::checked_cast;

Status Filter(FunctionContext* context, const Array& values, const Array& filter,
              std::shared_ptr<Array>* out) {
  Datum out_datum;
  RETURN_NOT_OK(Filter(context, Datum(values.data()), Datum(filter.data()), &out_datum));
  *out = out_datum.make_array();
  return Status::OK();
}

Status Filter(FunctionContext* context, const Datum& values, const Datum& filter,
              Datum* out) {
  FilterKernel kernel(values.type());
  RETURN_NOT_OK(kernel.Call(context, values, filter, out));
  return Status::OK();
}

struct FilterParameters {
  FunctionContext* context;
  std::shared_ptr<Array> values, filter;
  std::shared_ptr<Array>* out;
};

template <typename Builder, typename Scalar>
static Status UnsafeAppend(Builder* builder, Scalar&& value) {
  builder->UnsafeAppend(std::forward<Scalar>(value));
  return Status::OK();
}

static Status UnsafeAppend(BinaryBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

static Status UnsafeAppend(StringBuilder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

// TODO(bkietz) this can be optimized
static int64_t OutputSize(const BooleanArray& filter) {
  auto offset = filter.offset();
  auto length = filter.length();
  internal::BitmapReader filter_data(filter.data()->buffers[1]->data(), offset, length);
  int64_t size = 0;
  for (auto i = offset; i < offset + length; ++i) {
    if (filter.IsNull(i) || filter_data.IsSet()) {
      ++size;
    }
    filter_data.Next();
  }
  return size;
}

template <bool AllValuesValid, bool WholeFilterValid, typename ValueArray,
          typename OutBuilder>
Status FilterImpl(FunctionContext*, const ValueArray& values, const BooleanArray& filter,
                  OutBuilder* builder) {
  auto offset = filter.offset();
  auto length = filter.length();
  internal::BitmapReader filter_data(filter.data()->buffers[1]->data(), offset, length);
  for (int64_t i = 0; i < filter.length(); filter_data.Next(), ++i) {
    if (!WholeFilterValid && filter.IsNull(i)) {
      builder->UnsafeAppendNull();
      continue;
    }
    if (filter_data.IsNotSet()) {
      continue;
    }
    if (!AllValuesValid && values.IsNull(i)) {
      builder->UnsafeAppendNull();
      continue;
    }
    RETURN_NOT_OK(UnsafeAppend(builder, values.GetView(i)));
  }
  return Status::OK();
}

template <bool AllValuesValid, typename ValueArray, typename FilterArray,
          typename OutBuilder>
Status UnpackFilterNullCount(FunctionContext* context, const ValueArray& values,
                             const FilterArray& filter, OutBuilder* builder) {
  if (filter.null_count() == 0) {
    return FilterImpl<AllValuesValid, true>(context, values, filter, builder);
  }
  return FilterImpl<AllValuesValid, false>(context, values, filter, builder);
}

template <typename ValueArray, typename FilterArray, typename OutBuilder>
Status UnpackValuesNullCount(FunctionContext* context, const ValueArray& values,
                             const FilterArray& filter, OutBuilder* builder) {
  if (values.null_count() == 0) {
    return UnpackFilterNullCount<true>(context, values, filter, builder);
  }
  return UnpackFilterNullCount<false>(context, values, filter, builder);
}

template <typename T>
using ArrayType = typename TypeTraits<T>::ArrayType;

template <typename FilterType>
struct UnpackValues {
  template <typename ValueType>
  Status Visit(const ValueType&) {
    using OutBuilder = typename TypeTraits<ValueType>::BuilderType;
    const auto& filter = checked_cast<const ArrayType<FilterType>&>(*params_.filter);
    const auto& values = checked_cast<const ArrayType<ValueType>&>(*params_.values);
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(params_.context->memory_pool(), values.type(), &builder));
    RETURN_NOT_OK(builder->Reserve(OutputSize(filter)));
    RETURN_NOT_OK(UnpackValuesNullCount(params_.context, values, filter,
                                        checked_cast<OutBuilder*>(builder.get())));
    return builder->Finish(params_.out);
  }

  Status Visit(const NullType& t) {
    const auto& filter = checked_cast<const ArrayType<FilterType>&>(*params_.filter);
    params_.out->reset(new NullArray(OutputSize(filter)));
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) {
    std::shared_ptr<Array> filtered_indices;
    const auto& values = internal::checked_cast<const DictionaryArray&>(*params_.values);
    {
      // To take from a dictionary, apply the current kernel to the dictionary's
      // indices. (Use UnpackValues<FilterType> since FilterType is already unpacked)
      FilterParameters params = params_;
      params.values = values.indices();
      params.out = &filtered_indices;
      UnpackValues<FilterType> unpack = {params};
      RETURN_NOT_OK(VisitTypeInline(*t.index_type(), &unpack));
    }
    // create output dictionary from taken filter
    *params_.out = std::make_shared<DictionaryArray>(values.type(), filtered_indices,
                                                     values.dictionary());
    return Status::OK();
  }

  Status Visit(const ExtensionType& t) {
    // XXX can we just take from its storage?
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const UnionType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const ListType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const FixedSizeListType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  Status Visit(const StructType& t) {
    return Status::NotImplemented("gathering values of type ", t);
  }

  const FilterParameters& params_;
};

struct UnpackFilter {
  Status Visit(const BooleanType&) {
    UnpackValues<BooleanType> unpack = {params_};
    return VisitTypeInline(*params_.values->type(), &unpack);
  }

  Status Visit(const DataType& other) {
    return Status::TypeError("filter type not supported: ", other);
  }

  const FilterParameters& params_;
};

Status FilterKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
                          Datum* out) {
  if (!values.is_array() || !filter.is_array()) {
    return Status::Invalid("FilterKernel expects array values and filter");
  }
  std::shared_ptr<Array> out_array;
  FilterParameters params;
  params.context = ctx;
  params.values = values.make_array();
  params.filter = filter.make_array();
  if (params.values->length() != params.filter->length()) {
    return Status::Invalid("Filter is not the same size as values");
  }
  params.out = &out_array;
  UnpackFilter unpack = {params};
  RETURN_NOT_OK(VisitTypeInline(*filter.type(), &unpack));
  *out = Datum(out_array);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

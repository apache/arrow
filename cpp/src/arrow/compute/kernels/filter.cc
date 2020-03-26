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
#include <vector>

#include "arrow/array/concatenate.h"
#include "arrow/builder.h"
#include "arrow/compute/kernels/take_internal.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

// IndexSequence which yields the indices of positions in a BooleanArray
// which are either null or true
template <FilterOptions::NullSelectionBehavior NullSelectionBehavior>
class FilterIndexSequence {
 public:
  // constexpr so we'll never instantiate bounds checking
  constexpr bool never_out_of_bounds() const { return true; }
  void set_never_out_of_bounds() {}

  constexpr FilterIndexSequence() = default;

  FilterIndexSequence(const BooleanArray& filter, int64_t out_length)
      : filter_(&filter), out_length_(out_length) {}

  std::pair<int64_t, bool> Next() {
    if (NullSelectionBehavior == FilterOptions::DROP) {
      // skip until an index is found at which the filter is true
      while (filter_->IsNull(index_) || !filter_->Value(index_)) {
        ++index_;
      }
      return std::make_pair(index_++, true);
    }

    // skip until an index is found at which the filter is either null or true
    while (filter_->IsValid(index_) && !filter_->Value(index_)) {
      ++index_;
    }
    bool is_valid = filter_->IsValid(index_);
    return std::make_pair(index_++, is_valid);
  }

  int64_t length() const { return out_length_; }

  int64_t null_count() const {
    if (NullSelectionBehavior == FilterOptions::DROP) {
      return 0;
    }
    return filter_->null_count();
  }

 private:
  const BooleanArray* filter_ = nullptr;
  int64_t index_ = 0, out_length_ = -1;
};

static int64_t OutputSize(FilterOptions options, const BooleanArray& filter) {
  // TODO(bkietz) this can be optimized. Use Bitmap::VisitWords
  int64_t size = 0;
  if (options.null_selection_behavior == FilterOptions::EMIT_NULL) {
    for (auto i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i) || filter.Value(i)) {
        ++size;
      }
    }
  } else {
    for (auto i = 0; i < filter.length(); ++i) {
      if (filter.IsValid(i) && filter.Value(i)) {
        ++size;
      }
    }
  }
  return size;
}

static Status CheckFilterType(const std::shared_ptr<DataType>& type) {
  if (type->id() != Type::BOOL) {
    return Status::TypeError("filter array must be of boolean type, got ", *type);
  }
  return Status::OK();
}

static Status CheckFilterValuesLengths(int64_t values, int64_t filter) {
  if (values != filter) {
    return Status::Invalid("filter and value array must have identical lengths");
  }
  return Status::OK();
}

template <typename IndexSequence>
class FilterKernelImpl : public FilterKernel {
 public:
  FilterKernelImpl(std::shared_ptr<DataType> type,
                   std::unique_ptr<Taker<IndexSequence>> taker, FilterOptions options)
      : FilterKernel(std::move(type), options), taker_(std::move(taker)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t out_length, std::shared_ptr<Array>* out) override {
    RETURN_NOT_OK(CheckFilterValuesLengths(values.length(), filter.length()));

    RETURN_NOT_OK(taker_->SetContext(ctx));
    RETURN_NOT_OK(taker_->Take(values, IndexSequence(filter, out_length)));
    return taker_->Finish(out);
  }

  static Status Make(std::shared_ptr<DataType> value_type, FilterOptions options,
                     std::unique_ptr<FilterKernel>* out) {
    std::unique_ptr<Taker<IndexSequence>> taker;
    RETURN_NOT_OK(Taker<IndexSequence>::Make(value_type, &taker));

    out->reset(new FilterKernelImpl(std::move(value_type), std::move(taker), options));
    return Status::OK();
  }

  std::unique_ptr<Taker<IndexSequence>> taker_;
};

Status FilterKernel::Make(std::shared_ptr<DataType> value_type, FilterOptions options,
                          std::unique_ptr<FilterKernel>* out) {
  if (options.null_selection_behavior == FilterOptions::EMIT_NULL) {
    return FilterKernelImpl<FilterIndexSequence<FilterOptions::EMIT_NULL>>::Make(
        std::move(value_type), options, out);
  }
  return FilterKernelImpl<FilterIndexSequence<FilterOptions::DROP>>::Make(
      std::move(value_type), options, out);
}

Status FilterKernel::Call(FunctionContext* ctx, const Datum& values, const Datum& filter,
                          Datum* out) {
  if (!values.is_arraylike() || !filter.is_arraylike()) {
    return Status::Invalid("FilterKernel::Call expects array-like values and filter");
  }

  RETURN_NOT_OK(CheckFilterType(filter.type()));
  RETURN_NOT_OK(CheckFilterValuesLengths(values.length(), filter.length()));

  auto chunks = internal::RechunkArraysConsistently({values.chunks(), filter.chunks()});
  auto value_chunks = std::move(chunks[0]);
  auto filter_chunks = std::move(chunks[1]);

  for (size_t i = 0; i < value_chunks.size(); ++i) {
    auto filter_chunk = checked_pointer_cast<BooleanArray>(filter_chunks[i]);
    RETURN_NOT_OK(this->Filter(ctx, *value_chunks[i], *filter_chunk,
                               OutputSize(options_, *filter_chunk), &value_chunks[i]));
  }

  if (values.is_array() && filter.is_array()) {
    *out = std::move(value_chunks[0]);
  } else {
    // drop empty chunks
    value_chunks.erase(
        std::remove_if(value_chunks.begin(), value_chunks.end(),
                       [](const std::shared_ptr<Array>& a) { return a->length() == 0; }),
        value_chunks.end());

    *out = std::make_shared<ChunkedArray>(std::move(value_chunks), values.type());
  }
  return Status::OK();
}

Status FilterTable(FunctionContext* ctx, const Table& table, const Datum& filter,
                   FilterOptions options, std::shared_ptr<Table>* out) {
  auto new_columns = table.columns();

  for (auto& column : new_columns) {
    Datum out_column;
    RETURN_NOT_OK(Filter(ctx, Datum(column), filter, options, &out_column));
    column = out_column.chunked_array();
  }

  *out = Table::Make(table.schema(), std::move(new_columns));
  return Status::OK();
}

Status FilterRecordBatch(FunctionContext* ctx, const RecordBatch& batch,
                         const Array& filter, FilterOptions options,
                         std::shared_ptr<RecordBatch>* out) {
  RETURN_NOT_OK(CheckFilterType(filter.type()));
  const auto& filter_array = checked_cast<const BooleanArray&>(filter);

  std::vector<std::unique_ptr<FilterKernel>> kernels(batch.num_columns());
  for (int i = 0; i < batch.num_columns(); ++i) {
    RETURN_NOT_OK(
        FilterKernel::Make(batch.schema()->field(i)->type(), options, &kernels[i]));
  }

  std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
  auto out_length = OutputSize(options, filter_array);
  for (int i = 0; i < batch.num_columns(); ++i) {
    RETURN_NOT_OK(
        kernels[i]->Filter(ctx, *batch.column(i), filter_array, out_length, &columns[i]));
  }

  *out = RecordBatch::Make(batch.schema(), out_length, columns);
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const Datum& values, const Datum& filter,
              FilterOptions options, Datum* out) {
  if (values.kind() == Datum::RECORD_BATCH) {
    if (!filter.is_array()) {
      return Status::Invalid("Cannot filter a RecordBatch with a filter of kind ",
                             filter.kind());
    }

    auto values_batch = values.record_batch();
    auto filter_array = filter.make_array();
    std::shared_ptr<RecordBatch> out_batch;
    RETURN_NOT_OK(
        FilterRecordBatch(ctx, *values_batch, *filter_array, options, &out_batch));
    *out = std::move(out_batch);
    return Status::OK();
  }

  if (values.kind() == Datum::TABLE) {
    auto values_table = values.table();

    std::shared_ptr<Table> out_table;
    RETURN_NOT_OK(FilterTable(ctx, *values_table, filter, options, &out_table));
    *out = std::move(out_table);
    return Status::OK();
  }

  std::unique_ptr<FilterKernel> kernel;
  RETURN_NOT_OK(FilterKernel::Make(values.type(), options, &kernel));
  return kernel->Call(ctx, values, filter, out);
}

}  // namespace compute
}  // namespace arrow

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
  int64_t size = 0;
  for (auto i = 0; i < filter.length(); ++i) {
    if (filter.IsNull(i) || filter.Value(i)) {
      ++size;
    }
  }
  return size;
}

static Result<std::shared_ptr<BooleanArray>> GetFilterArray(const Datum& filter) {
  auto filter_type = filter.type();
  if (filter_type->id() != Type::BOOL) {
    return Status::TypeError("filter array must be of boolean type, got ", *filter_type);
  }
  return checked_pointer_cast<BooleanArray>(filter.make_array());
}

class FilterKernelImpl : public FilterKernel {
 public:
  FilterKernelImpl(const std::shared_ptr<DataType>& type,
                   std::unique_ptr<Taker<FilterIndexSequence>> taker)
      : FilterKernel(type), taker_(std::move(taker)) {}

  Status Filter(FunctionContext* ctx, const Array& values, const BooleanArray& filter,
                int64_t out_length, std::shared_ptr<Array>* out) override {
    if (values.length() != filter.length()) {
      return Status::Invalid("filter and value array must have identical lengths");
    }
    RETURN_NOT_OK(taker_->SetContext(ctx));
    RETURN_NOT_OK(taker_->Take(values, FilterIndexSequence(filter, out_length)));
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
    return Status::Invalid("FilterKernel::Call expects array values and filter");
  }
  auto values_array = values.make_array();

  ARROW_ASSIGN_OR_RAISE(auto filter_array, GetFilterArray(filter));
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

Status Filter(FunctionContext* ctx, const RecordBatch& batch, const Array& filter,
              std::shared_ptr<RecordBatch>* out) {
  ARROW_ASSIGN_OR_RAISE(auto filter_array, GetFilterArray(Datum(filter.data())));

  std::vector<std::unique_ptr<FilterKernel>> kernels(batch.num_columns());
  for (int i = 0; i < batch.num_columns(); ++i) {
    RETURN_NOT_OK(FilterKernel::Make(batch.schema()->field(i)->type(), &kernels[i]));
  }

  std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
  auto out_length = OutputSize(*filter_array);
  for (int i = 0; i < batch.num_columns(); ++i) {
    RETURN_NOT_OK(kernels[i]->Filter(ctx, *batch.column(i), *filter_array, out_length,
                                     &columns[i]));
  }

  *out = RecordBatch::Make(batch.schema(), out_length, columns);
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const ChunkedArray& values, const Array& filter,
              std::shared_ptr<ChunkedArray>* out) {
  if (values.length() != filter.length()) {
    return Status::Invalid("filter and value array must have identical lengths");
  }
  auto num_chunks = values.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  std::shared_ptr<Array> current_chunk;
  int64_t offset = 0;
  int64_t len;

  for (int i = 0; i < num_chunks; i++) {
    current_chunk = values.chunk(i);
    len = current_chunk->length();
    RETURN_NOT_OK(
        Filter(ctx, *current_chunk, *filter.Slice(offset, len), &new_chunks[i]));
    offset += len;
  }

  *out = std::make_shared<ChunkedArray>(std::move(new_chunks));
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const ChunkedArray& values,
              const ChunkedArray& filter, std::shared_ptr<ChunkedArray>* out) {
  if (values.length() != filter.length()) {
    return Status::Invalid("filter and value array must have identical lengths");
  }
  auto num_chunks = values.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  std::shared_ptr<Array> current_chunk;
  std::shared_ptr<ChunkedArray> current_chunked_filter;
  std::shared_ptr<Array> current_filter;
  int64_t offset = 0;
  int64_t len;

  for (int i = 0; i < num_chunks; i++) {
    current_chunk = values.chunk(i);
    len = current_chunk->length();
    if (len > 0) {
      current_chunked_filter = filter.Slice(offset, len);
      if (current_chunked_filter->num_chunks() == 1) {
        current_filter = current_chunked_filter->chunk(0);
      } else {
        // Concatenate the chunks of the filter so we have an Array
        RETURN_NOT_OK(Concatenate(current_chunked_filter->chunks(), default_memory_pool(),
                                  &current_filter));
      }
      RETURN_NOT_OK(Filter(ctx, *current_chunk, *current_filter, &new_chunks[i]));
      offset += len;
    } else {
      // Put a zero length array there, which we know our current chunk to be
      new_chunks[i] = current_chunk;
    }
  }

  *out = std::make_shared<ChunkedArray>(std::move(new_chunks));
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const Table& table, const Array& filter,
              std::shared_ptr<Table>* out) {
  auto ncols = table.num_columns();

  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    RETURN_NOT_OK(Filter(ctx, *table.column(j), filter, &columns[j]));
  }
  *out = Table::Make(table.schema(), columns);
  return Status::OK();
}

Status Filter(FunctionContext* ctx, const Table& table, const ChunkedArray& filter,
              std::shared_ptr<Table>* out) {
  auto ncols = table.num_columns();

  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    RETURN_NOT_OK(Filter(ctx, *table.column(j), filter, &columns[j]));
  }
  *out = Table::Make(table.schema(), columns);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow

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

#include "arrow/csv/writer.h"
#include "arrow/array.h"
#include "arrow/compute/cast.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/result_internal.h"
#include "arrow/stl_allocator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

#include "arrow/visitor_inline.h"

namespace arrow {
namespace csv {
// This implementation is intentionally light on configurability to minimize the size of
// the initial PR. Aditional features can be added as there is demand and interest to
// implement them.
//
// The algorithm used here at a high level is to break RecordBatches/Tables into slices
// and convert each slice independently.  A slice is then converted to CSV by first
// scanning each column to determine the size of its contents when rendered as a string in
// CSV. For non-string types this requires casting the value to string (which is cached).
// This data is used to understand the precise length of each row and a single allocation
// for the final CSV data buffer. Once the final size is known each column is then
// iterated over again to place its contents into the CSV data buffer. The rationale for
// choosing this approach is it allows for reuse of the cast functionality in the compute
// module and inline data visiting functionality in the core library. A performance
// comparison has not been done using a naive single-pass approach. This approach might
// still be competitive due to reduction in the number of per row branches necessary with
// a single pass approach. Profiling would likely yield further opportunities for
// optimization with this approach.

namespace {

struct SliceIteratorFunctor {
  Result<std::shared_ptr<RecordBatch>> Next() {
    if (current_offset < batch->num_rows()) {
      std::shared_ptr<RecordBatch> next = batch->Slice(current_offset, slice_size);
      current_offset += slice_size;
      return next;
    }
    return IterationTraits<std::shared_ptr<RecordBatch>>::End();
  }
  const RecordBatch* const batch;
  const int64_t slice_size;
  int64_t current_offset;
};

RecordBatchIterator RecordBatchSliceIterator(const RecordBatch& batch,
                                             int64_t slice_size) {
  SliceIteratorFunctor functor = {&batch, slice_size, /*offset=*/static_cast<int64_t>(0)};
  return RecordBatchIterator(std::move(functor));
}

// Counts the number of characters that need escaping in s.
int64_t CountEscapes(util::string_view s) {
  return static_cast<int64_t>(std::count(s.begin(), s.end(), '"'));
}

// Matching quote pair character length.
constexpr int64_t kQuoteCount = 2;
constexpr int64_t kQuoteDelimiterCount = kQuoteCount + /*end_char*/ 1;

// Interface for generating CSV data per column.
// The intended usage is to iteratively call UpdateRowLengths for a column and
// then PopulateColumns. PopulateColumns must be called in the reverse order of the
// populators (it populates data backwards).
class ColumnPopulator {
 public:
  ColumnPopulator(MemoryPool* pool, char end_char) : end_char_(end_char), pool_(pool) {}

  virtual ~ColumnPopulator() = default;

  // Adds the number of characters each entry in data will add to to elements
  // in row_lengths.
  Status UpdateRowLengths(const Array& data, int32_t* row_lengths) {
    compute::ExecContext ctx(pool_);
    // Populators are intented to be applied to reasonably small data.  In most cases
    // threading overhead would not be justified.
    ctx.set_use_threads(false);
    ASSIGN_OR_RAISE(
        std::shared_ptr<Array> casted,
        compute::Cast(data, /*to_type=*/utf8(), compute::CastOptions(), &ctx));
    casted_array_ = internal::checked_pointer_cast<StringArray>(casted);
    return UpdateRowLengths(row_lengths);
  }

  // Places string data onto each row in output and updates the corresponding row
  // row pointers in preparation for calls to other (preceding) ColumnPopulators.
  // Args:
  //   output: character buffer to write to.
  //   offsets: an array of end of row column within the the output buffer (values are
  //   one past the end of the position to write to).
  virtual void PopulateColumns(char* output, int32_t* offsets) const = 0;

 protected:
  virtual Status UpdateRowLengths(int32_t* row_lengths) = 0;
  std::shared_ptr<StringArray> casted_array_;
  const char end_char_;

 private:
  MemoryPool* const pool_;
};

// Copies the contents of to out properly escaping any necessary characters.
// Returns the position prior to last copied character (out_end is decremented).
char* EscapeReverse(arrow::util::string_view s, char* out_end) {
  for (const char* val = s.data() + s.length() - 1; val >= s.data(); val--, out_end--) {
    if (*val == '"') {
      *out_end = *val;
      out_end--;
    }
    *out_end = *val;
  }
  return out_end;
}

// Populator for non-string types.  This populator relies on compute Cast functionality to
// String if it doesn't exist it will be an error.  it also assumes the resulting string
// from a cast does not require quoting or escaping.
class UnquotedColumnPopulator : public ColumnPopulator {
 public:
  explicit UnquotedColumnPopulator(MemoryPool* memory_pool, char end_char)
      : ColumnPopulator(memory_pool, end_char) {}

  Status UpdateRowLengths(int32_t* row_lengths) override {
    for (int x = 0; x < casted_array_->length(); x++) {
      row_lengths[x] += casted_array_->value_length(x);
    }
    return Status::OK();
  }

  void PopulateColumns(char* output, int32_t* offsets) const override {
    VisitArrayDataInline<StringType>(
        *casted_array_->data(),
        [&](arrow::util::string_view s) {
          int64_t next_column_offset = s.length() + /*end_char*/ 1;
          memcpy((output + *offsets - next_column_offset), s.data(), s.length());
          *(output + *offsets - 1) = end_char_;
          *offsets -= static_cast<int32_t>(next_column_offset);
          offsets++;
        },
        [&]() {
          // Nulls are empty (unquoted) to distinguish with empty string.
          *(output + *offsets - 1) = end_char_;
          *offsets -= 1;
          offsets++;
        });
  }
};

// Strings need special handling to ensure they are escaped properly.
// This class handles escaping assuming that all strings will be quoted
// and that the only character within the string that needs to escaped is
// a quote character (") and escaping is done my adding another quote.
class QuotedColumnPopulator : public ColumnPopulator {
 public:
  QuotedColumnPopulator(MemoryPool* pool, char end_char)
      : ColumnPopulator(pool, end_char) {}

  Status UpdateRowLengths(int32_t* row_lengths) override {
    const StringArray& input = *casted_array_;
    int row_number = 0;
    row_needs_escaping_.resize(casted_array_->length());
    VisitArrayDataInline<StringType>(
        *input.data(),
        [&](arrow::util::string_view s) {
          int64_t escaped_count = CountEscapes(s);
          // TODO: Maybe use 64 bit row lengths or safe cast?
          row_needs_escaping_[row_number] = escaped_count > 0;
          row_lengths[row_number] += static_cast<int32_t>(s.length()) +
                                     static_cast<int32_t>(escaped_count + kQuoteCount);
          row_number++;
        },
        [&]() {
          row_needs_escaping_[row_number] = false;
          row_number++;
        });
    return Status::OK();
  }

  void PopulateColumns(char* output, int32_t* offsets) const override {
    auto needs_escaping = row_needs_escaping_.begin();
    VisitArrayDataInline<StringType>(
        *(casted_array_->data()),
        [&](arrow::util::string_view s) {
          // still needs string content length to be added
          char* row_end = output + *offsets;
          int32_t next_column_offset = 0;
          if (!*needs_escaping) {
            next_column_offset = static_cast<int32_t>(s.length() + kQuoteDelimiterCount);
            memcpy(row_end - next_column_offset + /*quote_offset=*/1, s.data(),
                   s.length());
          } else {
            // Adjust row_end by 3: 1 quote char, 1 end char and 1 to position at the
            // first position to write to.
            next_column_offset =
                static_cast<int32_t>(row_end - EscapeReverse(s, row_end - 3));
          }
          *(row_end - next_column_offset) = '"';
          *(row_end - 2) = '"';
          *(row_end - 1) = end_char_;
          *offsets -= next_column_offset;
          offsets++;
          needs_escaping++;
        },
        [&]() {
          // Nulls are empty (unquoted) to distinguish with empty string.
          *(output + *offsets - 1) = end_char_;
          *offsets -= 1;
          offsets++;
          needs_escaping++;
        });
  }

 private:
  // Older version of GCC don't support custom allocators
  // at some point we should change this to use memory_pool
  // backed allocator.
  std::vector<bool> row_needs_escaping_;
};

struct PopulatorFactory {
  template <typename TypeClass>
  enable_if_t<is_base_binary_type<TypeClass>::value ||
                  std::is_same<FixedSizeBinaryType, TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    populator = new QuotedColumnPopulator(pool, end_char);
    return Status::OK();
  }

  template <typename TypeClass>
  enable_if_dictionary<TypeClass, Status> Visit(const TypeClass& type) {
    return VisitTypeInline(*type.value_type(), this);
  }

  template <typename TypeClass>
  enable_if_t<is_nested_type<TypeClass>::value || is_extension_type<TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    return Status::Invalid("Unsupported Type:", type.ToString());
  }

  template <typename TypeClass>
  enable_if_t<is_primitive_ctype<TypeClass>::value || is_decimal_type<TypeClass>::value ||
                  is_null_type<TypeClass>::value || is_temporal_type<TypeClass>::value,
              Status>
  Visit(const TypeClass& type) {
    populator = new UnquotedColumnPopulator(pool, end_char);
    return Status::OK();
  }

  char end_char;
  MemoryPool* pool;
  ColumnPopulator* populator;
};

Result<std::unique_ptr<ColumnPopulator>> MakePopulator(const Field& field, char end_char,
                                                       MemoryPool* pool) {
  PopulatorFactory factory{end_char, pool, nullptr};
  RETURN_NOT_OK(VisitTypeInline(*field.type(), &factory));
  return std::unique_ptr<ColumnPopulator>(factory.populator);
}

class CSVWriterImpl : public ipc::RecordBatchWriter {
 public:
  static Result<std::shared_ptr<CSVWriterImpl>> Make(
      io::OutputStream* sink, std::shared_ptr<io::OutputStream> owned_sink,
      std::shared_ptr<Schema> schema, const WriteOptions& options) {
    RETURN_NOT_OK(options.Validate());
    std::vector<std::unique_ptr<ColumnPopulator>> populators(schema->num_fields());
    for (int col = 0; col < schema->num_fields(); col++) {
      char end_char = col < schema->num_fields() - 1 ? ',' : '\n';
      ASSIGN_OR_RAISE(populators[col], MakePopulator(*schema->field(col), end_char,
                                                     options.io_context.pool()));
    }
    auto writer = std::make_shared<CSVWriterImpl>(
        sink, std::move(owned_sink), std::move(schema), std::move(populators), options);
    RETURN_NOT_OK(writer->PrepareForContentsWrite());
    if (options.include_header) {
      RETURN_NOT_OK(writer->WriteHeader());
    }
    return writer;
  }

  Status WriteRecordBatch(const RecordBatch& batch) override {
    RecordBatchIterator iterator = RecordBatchSliceIterator(batch, options_.batch_size);
    for (auto maybe_slice : iterator) {
      ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> slice, maybe_slice);
      RETURN_NOT_OK(TranslateMinimalBatch(*slice));
      RETURN_NOT_OK(sink_->Write(data_buffer_));
      stats_.num_record_batches++;
    }
    return Status::OK();
  }

  Status WriteTable(const Table& table, int64_t max_chunksize) override {
    TableBatchReader reader(table);
    reader.set_chunksize(max_chunksize > 0 ? max_chunksize : options_.batch_size);
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader.ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(TranslateMinimalBatch(*batch));
      RETURN_NOT_OK(sink_->Write(data_buffer_));
      RETURN_NOT_OK(reader.ReadNext(&batch));
      stats_.num_record_batches++;
    }

    return Status::OK();
  }

  Status Close() override { return Status::OK(); }

  ipc::WriteStats stats() const override { return stats_; }

  CSVWriterImpl(io::OutputStream* sink, std::shared_ptr<io::OutputStream> owned_sink,
                std::shared_ptr<Schema> schema,
                std::vector<std::unique_ptr<ColumnPopulator>> populators,
                const WriteOptions& options)
      : sink_(sink),
        owned_sink_(std::move(owned_sink)),
        column_populators_(std::move(populators)),
        offsets_(0, 0, ::arrow::stl::allocator<char*>(options.io_context.pool())),
        schema_(std::move(schema)),
        options_(options) {}

 private:
  Status PrepareForContentsWrite() {
    // Only called once, as part of initialization
    if (data_buffer_ == nullptr) {
      ASSIGN_OR_RAISE(data_buffer_,
                      AllocateResizableBuffer(
                          options_.batch_size * schema_->num_fields() * kColumnSizeGuess,
                          options_.io_context.pool()));
    }
    return Status::OK();
  }

  int64_t CalculateHeaderSize() const {
    int64_t header_length = 0;
    for (int col = 0; col < schema_->num_fields(); col++) {
      const std::string& col_name = schema_->field(col)->name();
      header_length += col_name.size();
      header_length += CountEscapes(col_name);
    }
    return header_length + (kQuoteDelimiterCount * schema_->num_fields());
  }

  Status WriteHeader() {
    // Only called once, as part of initialization
    RETURN_NOT_OK(data_buffer_->Resize(CalculateHeaderSize(), /*shrink_to_fit=*/false));
    char* next =
        reinterpret_cast<char*>(data_buffer_->mutable_data() + data_buffer_->size() - 1);
    for (int col = schema_->num_fields() - 1; col >= 0; col--) {
      *next-- = ',';
      *next-- = '"';
      next = EscapeReverse(schema_->field(col)->name(), next);
      *next-- = '"';
    }
    *(data_buffer_->mutable_data() + data_buffer_->size() - 1) = '\n';
    DCHECK_EQ(reinterpret_cast<uint8_t*>(next + 1), data_buffer_->data());
    return sink_->Write(data_buffer_);
  }

  Status TranslateMinimalBatch(const RecordBatch& batch) {
    if (batch.num_rows() == 0) {
      return Status::OK();
    }
    offsets_.resize(batch.num_rows());
    std::fill(offsets_.begin(), offsets_.end(), 0);

    // Calculate relative offsets for each row (excluding delimiters)
    for (int32_t col = 0; col < static_cast<int32_t>(column_populators_.size()); col++) {
      RETURN_NOT_OK(
          column_populators_[col]->UpdateRowLengths(*batch.column(col), offsets_.data()));
    }
    // Calculate cumulalative offsets for each row (including delimiters).
    offsets_[0] += batch.num_columns();
    for (int64_t row = 1; row < batch.num_rows(); row++) {
      offsets_[row] += offsets_[row - 1] + /*delimiter lengths*/ batch.num_columns();
    }
    // Resize the target buffer to required size. We assume batch to batch sizes
    // should be pretty close so don't shrink the buffer to avoid allocation churn.
    RETURN_NOT_OK(data_buffer_->Resize(offsets_.back(), /*shrink_to_fit=*/false));

    // Use the offsets to populate contents.
    for (auto populator = column_populators_.rbegin();
         populator != column_populators_.rend(); populator++) {
      (*populator)
          ->PopulateColumns(reinterpret_cast<char*>(data_buffer_->mutable_data()),
                            offsets_.data());
    }
    DCHECK_EQ(0, offsets_[0]);
    return Status::OK();
  }

  static constexpr int64_t kColumnSizeGuess = 8;
  io::OutputStream* sink_;
  std::shared_ptr<io::OutputStream> owned_sink_;
  std::vector<std::unique_ptr<ColumnPopulator>> column_populators_;
  std::vector<int32_t, arrow::stl::allocator<int32_t>> offsets_;
  std::shared_ptr<ResizableBuffer> data_buffer_;
  const std::shared_ptr<Schema> schema_;
  const WriteOptions options_;
  ipc::WriteStats stats_;
};

}  // namespace

Status WriteCSV(const Table& table, const WriteOptions& options,
                arrow::io::OutputStream* output) {
  ASSIGN_OR_RAISE(auto writer, MakeCSVWriter(output, table.schema(), options));
  RETURN_NOT_OK(writer->WriteTable(table));
  return writer->Close();
}

Status WriteCSV(const RecordBatch& batch, const WriteOptions& options,
                arrow::io::OutputStream* output) {
  ASSIGN_OR_RAISE(auto writer, MakeCSVWriter(output, batch.schema(), options));
  RETURN_NOT_OK(writer->WriteRecordBatch(batch));
  return writer->Close();
}

ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeCSVWriter(
    std::shared_ptr<io::OutputStream> sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options) {
  return CSVWriterImpl::Make(sink.get(), sink, schema, options);
}

ARROW_EXPORT
Result<std::shared_ptr<ipc::RecordBatchWriter>> MakeCSVWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const WriteOptions& options) {
  return CSVWriterImpl::Make(sink, nullptr, schema, options);
}

}  // namespace csv
}  // namespace arrow

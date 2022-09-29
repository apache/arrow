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
#include "arrow/visit_data_inline.h"
#include "arrow/visit_type_inline.h"

#include <memory>

#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
#include <xsimd/xsimd.hpp>
#endif

namespace arrow {

using internal::checked_pointer_cast;

namespace csv {
// This implementation is intentionally light on configurability to minimize the size of
// the initial PR. Additional features can be added as there is demand and interest to
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

// This function is to improve performance. It copies CSV delimiter and eol
// without calling `memcpy`.
// Each CSV field is followed by a delimiter or eol, which is often only one
// or two chars. If copying both the field and delimiter with `memcpy`, CPU
// may suffer from high branch misprediction as we are tripping `memcpy` with
// interleaved (normal/tiny/normal/tiny/...) buffer sizes, which are handled
// separately inside `memcpy`. This function goes fast path if the buffer
// size is one or two chars to leave `memcpy` only for copying CSV fields.
void CopyEndChars(char* dest, const char* src, size_t size) {
  if (size == 1) {
    // for fixed size memcpy, compiler will generate direct load/store opcode
    memcpy(dest, src, 1);
  } else if (size == 2) {
    memcpy(dest, src, 2);
  } else {
    memcpy(dest, src, size);
  }
}

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

// Counts the number of quotes in s.
int64_t CountQuotes(std::string_view s) {
  return static_cast<int64_t>(std::count(s.begin(), s.end(), '"'));
}

// Matching quote pair character length.
constexpr int64_t kQuoteCount = 2;
constexpr int64_t kQuoteDelimiterCount = kQuoteCount + /*end_char*/ 1;

// Interface for generating CSV data per column.
// The intended usage is to iteratively call UpdateRowLengths for a column and
// then PopulateRows.
class ColumnPopulator {
 public:
  ColumnPopulator(MemoryPool* pool, std::string end_chars,
                  std::shared_ptr<Buffer> null_string)
      : end_chars_(std::move(end_chars)),
        null_string_(std::move(null_string)),
        pool_(pool) {}

  virtual ~ColumnPopulator() = default;

  // Adds the number of characters each entry in data will add to to elements
  // in row_lengths.
  Status UpdateRowLengths(const Array& data, int64_t* row_lengths) {
    compute::ExecContext ctx(pool_);
    // Populators are intented to be applied to reasonably small data.  In most cases
    // threading overhead would not be justified.
    ctx.set_use_threads(false);
    ASSIGN_OR_RAISE(
        std::shared_ptr<Array> casted,
        compute::Cast(data, /*to_type=*/utf8(), compute::CastOptions(), &ctx));
    casted_array_ = checked_pointer_cast<StringArray>(casted);
    return UpdateRowLengths(row_lengths);
  }

  // Places string data onto each row in output and updates the corresponding row
  // pointers in preparation for calls to other (next) ColumnPopulators.
  // Implementations may apply certain checks e.g. for illegal values, which in case of
  // failure causes this function to return an error Status.
  // Args:
  //   output: character buffer to write to.
  //   offsets: an array of start of row column within the output buffer.
  virtual Status PopulateRows(char* output, int64_t* offsets) const = 0;

 protected:
  virtual Status UpdateRowLengths(int64_t* row_lengths) = 0;
  std::shared_ptr<StringArray> casted_array_;
  const std::string end_chars_;
  std::shared_ptr<Buffer> null_string_;

 private:
  MemoryPool* const pool_;
};

// Copies the contents of s to out properly escaping any necessary characters.
// Returns the position next to last copied character.
char* Escape(std::string_view s, char* out) {
  for (const char c : s) {
    *out++ = c;
    if (c == '"') {
      *out++ = '"';
    }
  }
  return out;
}

// Populator used for non-string/binary types, or when unquoted strings/binary types are
// desired. It assumes the strings in the casted array do not require quoting or escaping.
// This is enforced by setting reject_values_with_quotes to true, in which case a check
// for quotes is applied and will cause populating the columns to fail. This guarantees
// compliance with RFC4180 section 2.5.
class UnquotedColumnPopulator : public ColumnPopulator {
 public:
  explicit UnquotedColumnPopulator(MemoryPool* memory_pool, std::string end_chars,
                                   char delimiter, std::shared_ptr<Buffer> null_string_,
                                   bool reject_values_with_quotes)
      : ColumnPopulator(memory_pool, std::move(end_chars), std::move(null_string_)),
        delimiter_(delimiter),
        reject_values_with_quotes_(reject_values_with_quotes) {}

  Status UpdateRowLengths(int64_t* row_lengths) override {
    if (reject_values_with_quotes_) {
      // When working on values that, after casting, could produce quotes,
      // we need to return an error in accord with RFC4180.
      RETURN_NOT_OK(CheckStringArrayHasNoStructuralChars(*casted_array_, delimiter_));
    }

    int64_t row_number = 0;
    VisitArraySpanInline<StringType>(
        *casted_array_->data(),
        [&](std::string_view s) {
          row_lengths[row_number] += static_cast<int64_t>(s.length());
          row_number++;
        },
        [&]() {
          row_lengths[row_number] += static_cast<int64_t>(null_string_->size());
          row_number++;
        });
    return Status::OK();
  }

  Status PopulateRows(char* output, int64_t* offsets) const override {
    // Function applied to valid values cast to string.
    auto valid_function = [&](std::string_view s) {
      memcpy(output + *offsets, s.data(), s.length());
      CopyEndChars(output + *offsets + s.length(), end_chars_.c_str(), end_chars_.size());
      *offsets += static_cast<int64_t>(s.length() + end_chars_.size());
      offsets++;
      return Status::OK();
    };

    // Function applied to null values cast to string.
    auto null_function = [&]() {
      // For nulls, the configured null value string is copied into the output.
      memcpy(output + *offsets, null_string_->data(), null_string_->size());
      CopyEndChars(output + *offsets + null_string_->size(), end_chars_.c_str(),
                   end_chars_.size());
      *offsets += static_cast<int64_t>(null_string_->size() + end_chars_.size());
      offsets++;
      return Status::OK();
    };

    return VisitArraySpanInline<StringType>(*casted_array_->data(), valid_function,
                                            null_function);
  }

 private:
  // Returns an error status if string array has any structural characters.
  static Status CheckStringArrayHasNoStructuralChars(const StringArray& array,
                                                     const char delimiter) {
    // scan the underlying string array buffer as a single big string
    const uint8_t* const data = array.raw_data() + array.value_offset(0);
    const int64_t buffer_size = array.total_values_length();
    int64_t offset = 0;
#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_NEON)
    // _mm_cmpistrc gives slightly better performance than the naive approach,
    // probably doesn't deserve the effort
    using simd_batch = xsimd::make_sized_batch_t<uint8_t, 16>;
    while ((offset + 16) <= buffer_size) {
      const auto v = simd_batch::load_unaligned(data + offset);
      if (xsimd::any((v == '\n') | (v == '\r') | (v == '"') | (v == delimiter))) {
        break;
      }
      offset += 16;
    }
#endif
    while (offset < buffer_size) {
      // error happened or remaining bytes to check
      const char c = static_cast<char>(data[offset]);
      if (c == '\n' || c == '\r' || c == '"' || c == delimiter) {
        // extract the offending string from array per offset
        const auto* offsets = array.raw_value_offsets();
        const auto index =
            std::upper_bound(offsets, offsets + array.length(), offset + offsets[0]) -
            offsets;
        DCHECK_GT(index, 0);
        return Status::Invalid(
            "CSV values may not contain structural characters if quoting style is "
            "\"None\". See RFC4180. Invalid value: ",
            array.GetView(index - 1));
      }
      ++offset;
    }
    return Status::OK();
  }

  // Whether to reject values with quotes when populating.
  const char delimiter_;
  const bool reject_values_with_quotes_;
};

// Strings need special handling to ensure they are escaped properly.
// This class handles escaping assuming that all strings will be quoted
// and that the only character within the string that needs to escaped is
// a quote character (") and escaping is done by adding another quote.
class QuotedColumnPopulator : public ColumnPopulator {
 public:
  QuotedColumnPopulator(MemoryPool* pool, std::string end_chars,
                        std::shared_ptr<Buffer> null_string)
      : ColumnPopulator(pool, std::move(end_chars), std::move(null_string)) {}

  Status UpdateRowLengths(int64_t* row_lengths) override {
    const StringArray& input = *casted_array_;

    row_needs_escaping_.resize(casted_array_->length(), false);

    if (NoQuoteInArray(input)) {
      // fast path if no quote
      int row_number = 0;
      VisitArraySpanInline<StringType>(
          *input.data(),
          [&](std::string_view s) {
            row_lengths[row_number] += static_cast<int64_t>(s.length()) + kQuoteCount;
            row_number++;
          },
          [&]() {
            row_lengths[row_number] += static_cast<int64_t>(null_string_->size());
            row_number++;
          });
    } else {
      int row_number = 0;
      VisitArraySpanInline<StringType>(
          *input.data(),
          [&](std::string_view s) {
            // Each quote in the value string needs to be escaped.
            int64_t escaped_count = CountQuotes(s);
            row_needs_escaping_[row_number] = escaped_count > 0;
            row_lengths[row_number] +=
                static_cast<int64_t>(s.length()) + escaped_count + kQuoteCount;
            row_number++;
          },
          [&]() {
            row_lengths[row_number] += static_cast<int64_t>(null_string_->size());
            row_number++;
          });
    }
    return Status::OK();
  }

  Status PopulateRows(char* output, int64_t* offsets) const override {
    auto needs_escaping = row_needs_escaping_.begin();
    VisitArraySpanInline<StringType>(
        *(casted_array_->data()),
        [&](std::string_view s) {
          // still needs string content length to be added
          char* row = output + *offsets;
          *row++ = '"';
          if (!*needs_escaping) {
            memcpy(row, s.data(), s.length());
            row += s.length();
          } else {
            row = Escape(s, row);
          }
          *row++ = '"';
          CopyEndChars(row, end_chars_.data(), end_chars_.length());
          row += end_chars_.length();
          *offsets = static_cast<int64_t>(row - output);
          offsets++;
          needs_escaping++;
        },
        [&]() {
          // For nulls, the configured null value string is copied into the output.
          memcpy(output + *offsets, null_string_->data(), null_string_->size());
          CopyEndChars(output + *offsets + null_string_->size(), end_chars_.c_str(),
                       end_chars_.size());
          *offsets += static_cast<int64_t>(null_string_->size() + end_chars_.size());
          offsets++;
          needs_escaping++;
        });

    return Status::OK();
  }

 private:
  // Returns true if there's no quote in the string array
  static bool NoQuoteInArray(const StringArray& array) {
    const uint8_t* data = array.raw_data() + array.value_offset(0);
    const int64_t buffer_size = array.total_values_length();
    return std::memchr(data, '"', buffer_size) == nullptr;
  }

  // Older version of GCC don't support custom allocators
  // at some point we should change this to use memory_pool
  // backed allocator.
  std::vector<bool> row_needs_escaping_;
};

Result<std::unique_ptr<ColumnPopulator>> MakePopulator(
    const DataType& type, const std::string& end_chars, const char delimiter,
    const std::shared_ptr<Buffer>& null_string, QuotingStyle quoting_style,
    MemoryPool* pool) {
  auto make_populator =
      [&](const auto& type) -> Result<std::unique_ptr<ColumnPopulator>> {
    using Type = std::decay_t<decltype(type)>;

    if constexpr (is_primitive_ctype<Type>::value || is_decimal_type<Type>::value ||
                  is_null_type<Type>::value || is_temporal_type<Type>::value) {
      switch (quoting_style) {
        // These types are assumed not to produce any quotes, so we do not need to
        // check and reject for potential quotes in the casted values in case the
        // QuotingStyle is None.
        case QuotingStyle::None:
          [[fallthrough]];
        case QuotingStyle::Needed:
          return std::make_unique<UnquotedColumnPopulator>(
              pool, end_chars, delimiter, null_string,
              /*reject_values_with_quotes=*/false);
        case QuotingStyle::AllValid:
          return std::make_unique<QuotedColumnPopulator>(pool, end_chars, null_string);
      }
    }

    if constexpr (is_base_binary_type<Type>::value ||
                  std::is_same<Type, FixedSizeBinaryType>::value) {
      // Determine what ColumnPopulator to use based on desired CSV quoting style.
      switch (quoting_style) {
        case QuotingStyle::None:
          // In unquoted output we must reject values with quotes. Since these types
          // can produce quotes in their output rendering, we must check them and
          // reject if quotes appear, hence reject_values_with_quotes is set to true.
          return std::make_unique<UnquotedColumnPopulator>(
              pool, end_chars, delimiter, null_string,
              /*reject_values_with_quotes=*/true);
          // Quoting is needed for strings/binary, or when all valid values need to be
          // quoted.
        case QuotingStyle::Needed:
          [[fallthrough]];
        case QuotingStyle::AllValid:
          return std::make_unique<QuotedColumnPopulator>(pool, end_chars, null_string);
      }
    }

    if constexpr (std::is_same<Type, DictionaryType>::value) {
      return MakePopulator(*type.value_type(), end_chars, delimiter, null_string,
                           quoting_style, pool);
    }

    return Status::Invalid("Unsupported Type:", type.ToString());
  };
  return VisitType(type, make_populator);
}

Result<std::unique_ptr<ColumnPopulator>> MakePopulator(
    const Field& field, const std::string& end_chars, char delimiter,
    const std::shared_ptr<Buffer>& null_string, QuotingStyle quoting_style,
    MemoryPool* pool) {
  return MakePopulator(*field.type(), end_chars, delimiter, null_string, quoting_style,
                       pool);
}

class CSVWriterImpl : public ipc::RecordBatchWriter {
 public:
  static Result<std::shared_ptr<CSVWriterImpl>> Make(
      io::OutputStream* sink, std::shared_ptr<io::OutputStream> owned_sink,
      std::shared_ptr<Schema> schema, const WriteOptions& options) {
    RETURN_NOT_OK(options.Validate());
    // Reject null string values that contain quotes.
    if (CountQuotes(options.null_string) != 0) {
      return Status::Invalid("Null string cannot contain quotes.");
    }

    ASSIGN_OR_RAISE(std::shared_ptr<Buffer> null_string,
                    arrow::AllocateBuffer(options.null_string.length()));
    memcpy(null_string->mutable_data(), options.null_string.data(),
           options.null_string.length());

    std::vector<std::unique_ptr<ColumnPopulator>> populators(schema->num_fields());
    std::string delimiter(1, options.delimiter);
    for (int col = 0; col < schema->num_fields(); col++) {
      const std::string& end_chars =
          col < schema->num_fields() - 1 ? delimiter : options.eol;
      ASSIGN_OR_RAISE(
          populators[col],
          MakePopulator(*schema->field(col), end_chars, options.delimiter, null_string,
                        options.quoting_style, options.io_context.pool()));
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
      header_length += CountQuotes(col_name);
    }
    // header_length + ([quotes + ','] * schema_->num_fields()) + (eol - ',')
    return header_length + (kQuoteDelimiterCount * schema_->num_fields()) +
           (options_.eol.size() - 1);
  }

  Status WriteHeader() {
    // Only called once, as part of initialization
    RETURN_NOT_OK(data_buffer_->Resize(CalculateHeaderSize(), /*shrink_to_fit=*/false));
    char* next = reinterpret_cast<char*>(data_buffer_->mutable_data());
    for (int col = 0; col < schema_->num_fields(); ++col) {
      *next++ = '"';
      next = Escape(schema_->field(col)->name(), next);
      *next++ = '"';
      if (col != schema_->num_fields() - 1) {
        *next++ = options_.delimiter;
      }
    }
    memcpy(next, options_.eol.data(), options_.eol.size());
    next += options_.eol.size();
    DCHECK_EQ(reinterpret_cast<uint8_t*>(next),
              data_buffer_->data() + data_buffer_->size());
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
    // Calculate cumulative offsets for each row (including delimiters).
    // - before conversion: offsets_[i] = length of i-th row
    // - after conversion:  offsets_[i] = offset to the starting of i-th row buffer
    //   - offsets_[0] = 0
    //   - offsets_[i] = offsets_[i-1] + len(i-1-th row) + len(delimiters)
    // Delimiters: ',' * (num_columns - 1) + eol
    const int32_t delimiters_length =
        static_cast<int32_t>(batch.num_columns() - 1 + options_.eol.size());
    int64_t last_row_length = offsets_[0] + delimiters_length;
    offsets_[0] = 0;
    for (size_t row = 1; row < offsets_.size(); ++row) {
      const int64_t this_row_length = offsets_[row] + delimiters_length;
      offsets_[row] = offsets_[row - 1] + last_row_length;
      last_row_length = this_row_length;
    }
    // Resize the target buffer to required size. We assume batch to batch sizes
    // should be pretty close so don't shrink the buffer to avoid allocation churn.
    RETURN_NOT_OK(
        data_buffer_->Resize(offsets_.back() + last_row_length, /*shrink_to_fit=*/false));

    // Use the offsets to populate contents.
    for (auto& populator : column_populators_) {
      RETURN_NOT_OK(populator->PopulateRows(
          reinterpret_cast<char*>(data_buffer_->mutable_data()), offsets_.data()));
    }
    DCHECK_EQ(data_buffer_->size(), offsets_.back());
    return Status::OK();
  }

  static constexpr int64_t kColumnSizeGuess = 8;
  io::OutputStream* sink_;
  std::shared_ptr<io::OutputStream> owned_sink_;
  std::vector<std::unique_ptr<ColumnPopulator>> column_populators_;
  std::vector<int64_t, arrow::stl::allocator<int64_t>> offsets_;
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

Status WriteCSV(const std::shared_ptr<RecordBatchReader>& reader,
                const WriteOptions& options, arrow::io::OutputStream* output) {
  ASSIGN_OR_RAISE(auto writer, MakeCSVWriter(output, reader->schema(), options));
  std::shared_ptr<RecordBatch> batch;
  while (true) {
    ASSIGN_OR_RAISE(batch, reader->Next());
    if (batch == nullptr) break;
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
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

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

#include "arrow/csv/parser.h"

#include <algorithm>
#include <cstdio>
#include <limits>
#include <utility>

#include "arrow/csv/lexing_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/simd.h"

namespace arrow {
namespace csv {

using detail::DataBatch;
using detail::ParsedValueDesc;

namespace {

template <typename... Args>
Status ParseError(Args&&... args) {
  return Status::Invalid("CSV parse error: ", std::forward<Args>(args)...);
}

Status MismatchingColumns(const InvalidRow& row) {
  std::string ellipse;
  auto row_string = row.text;
  if (row_string.length() > 100) {
    row_string = row_string.substr(0, 96);
    ellipse = " ...";
  }
  if (row.number < 0) {
    return ParseError("Expected ", row.expected_columns, " columns, got ",
                      row.actual_columns, ": ", row_string, ellipse);
  }
  return ParseError("Row #", row.number, ": Expected ", row.expected_columns,
                    " columns, got ", row.actual_columns, ": ", row_string, ellipse);
}

inline bool IsControlChar(uint8_t c) { return c < ' '; }

// A helper class allocating the buffer for parsed values and writing into it
// without any further resizes, except at the end.
class PresizedDataWriter {
 public:
  PresizedDataWriter(MemoryPool* pool, uint32_t size)
      : parsed_size_(0), parsed_capacity_(size) {
    parsed_buffer_ = *AllocateResizableBuffer(parsed_capacity_, pool);
    parsed_ = parsed_buffer_->mutable_data();
  }

  void Finish(std::shared_ptr<Buffer>* out_parsed) {
    ARROW_CHECK_OK(parsed_buffer_->Resize(parsed_size_));
    *out_parsed = parsed_buffer_;
  }

  void BeginLine() { saved_parsed_size_ = parsed_size_; }

  void PushFieldChar(char c) {
    DCHECK_LT(parsed_size_, parsed_capacity_);
    parsed_[parsed_size_++] = static_cast<uint8_t>(c);
  }

  template <typename Word>
  void PushFieldWord(Word w) {
    DCHECK_GE(parsed_capacity_ - parsed_size_, static_cast<int64_t>(sizeof(w)));
    memcpy(parsed_ + parsed_size_, &w, sizeof(w));
    parsed_size_ += sizeof(w);
  }

  // Rollback the state that was saved in BeginLine()
  void RollbackLine() { parsed_size_ = saved_parsed_size_; }

  int64_t size() { return parsed_size_; }

 protected:
  std::shared_ptr<ResizableBuffer> parsed_buffer_;
  uint8_t* parsed_;
  int64_t parsed_size_;
  int64_t parsed_capacity_;
  // Checkpointing, for when an incomplete line is encountered at end of block
  int64_t saved_parsed_size_;
};

template <typename Derived>
class ValueDescWriter {
 public:
  Derived* derived() { return static_cast<Derived*>(this); }

  template <typename DataWriter>
  void Start(DataWriter& parsed_writer) {
    derived()->PushValue(
        {static_cast<uint32_t>(parsed_writer.size()) & 0x7fffffffU, false});
  }

  void BeginLine() { saved_values_size_ = values_size_; }

  // Rollback the state that was saved in BeginLine()
  void RollbackLine() { values_size_ = saved_values_size_; }

  void StartField(bool quoted) { quoted_ = quoted; }

  template <typename DataWriter>
  void FinishField(DataWriter* parsed_writer) {
    derived()->PushValue(
        {static_cast<uint32_t>(parsed_writer->size()) & 0x7fffffffU, quoted_});
  }

  Result<std::shared_ptr<Buffer>> Finish() {
    RETURN_NOT_OK(values_buffer_->Resize(values_size_ * sizeof(*values_)));
    return std::move(values_buffer_);
  }

  const Status& status() const { return status_; }

  // Convenience error-checking factory. The arguments are forwarded to the
  // Derived class constructor.
  template <typename... Args>
  static Result<Derived> Make(Args&&... args) {
    auto self = Derived(std::forward<Args>(args)...);
    RETURN_NOT_OK(self.status());
    return self;
  }

 protected:
  ValueDescWriter(MemoryPool* pool, int64_t values_capacity)
      : values_size_(0), values_capacity_(values_capacity), status_(Status::OK()) {
    status_ &= AllocateResizableBuffer(values_capacity_ * sizeof(*values_), pool)
                   .Value(&values_buffer_);
    if (status_.ok()) {
      values_ = reinterpret_cast<ParsedValueDesc*>(values_buffer_->mutable_data());
    }
  }

  std::shared_ptr<ResizableBuffer> values_buffer_;
  ParsedValueDesc* values_;
  int64_t values_size_;
  int64_t values_capacity_;
  bool quoted_;
  // Checkpointing, for when an incomplete line is encountered at end of block
  int64_t saved_values_size_;
  Status status_;
};

// A helper class handling a growable buffer for values offsets.  This class is
// used when the number of columns is not yet known and we therefore cannot
// efficiently presize the target area for a given number of rows.
class ResizableValueDescWriter : public ValueDescWriter<ResizableValueDescWriter> {
 public:
  explicit ResizableValueDescWriter(MemoryPool* pool)
      : ValueDescWriter(pool, /*values_capacity=*/256) {}

  void PushValue(ParsedValueDesc v) {
    if (ARROW_PREDICT_FALSE(values_size_ == values_capacity_)) {
      int64_t new_capacity = values_capacity_ * 2;
      auto resize_status = values_buffer_->Resize(new_capacity * sizeof(*values_));
      if (resize_status.ok()) {
        values_ = reinterpret_cast<ParsedValueDesc*>(values_buffer_->mutable_data());
        values_capacity_ = new_capacity;
      }
      status_ &= std::move(resize_status);
    }
    // The `values_` pointer may have become invalid if the `Resize` call above failed.
    // Note that ResizableValueDescWriter is less performance-critical than
    // PresizedValueDescWriter, as it should only be called on the first line(s)
    // of CSV data.
    if (ARROW_PREDICT_TRUE(status_.ok())) {
      values_[values_size_++] = v;
    }
  }
};

// A helper class allocating the buffer for values offsets and writing into it
// without any further resizes, except at the end.  This class is used once the
// number of columns is known, as it eliminates resizes and generates simpler,
// faster CSV parsing code.
class PresizedValueDescWriter : public ValueDescWriter<PresizedValueDescWriter> {
 public:
  // The number of offsets being written will be `1 + num_rows * num_cols`,
  // however we allow for one extraneous write in case of excessive columns,
  // hence `2 + num_rows * num_cols` (see explanation in PushValue below).
  PresizedValueDescWriter(MemoryPool* pool, int32_t num_rows, int32_t num_cols)
      : ValueDescWriter(pool, /*values_capacity=*/2 + num_rows * num_cols) {}

  void PushValue(ParsedValueDesc v) {
    DCHECK_LT(values_size_, values_capacity_);
    values_[values_size_] = v;
    // We must take care not to write past the buffer's end if the line being
    // parsed has more than `num_cols` columns. The obvious solution of setting
    // an error status hurts too much on benchmarks, which is why we instead
    // cap `values_size_` to stay inside the buffer.
    //
    // Not setting an error immediately is not a problem since the `num_cols`
    // mismatch is detected later in ParseLine.
    //
    // Note that we want `values_size_` to reflect the number of written values
    // in the nominal case, which is why we choose a slightly larger `values_capacity_`.
    values_size_ += (values_size_ != values_capacity_ - 1);
  }
};

}  // namespace

class BlockParserImpl {
 public:
  BlockParserImpl(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                  int64_t first_row, int32_t max_num_rows)
      : pool_(pool),
        options_(std::move(options)),
        first_row_(first_row),
        max_num_rows_(max_num_rows),
        batch_(num_cols) {}

  const DataBatch& parsed_batch() const { return batch_; }

  int64_t first_row_num() const { return first_row_; }

  template <typename ValueDescWriter, typename DataWriter>
  Status HandleInvalidRow(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                          const char* start, const char* data, int32_t num_cols,
                          const char** out_data) {
    // Find the end of the line without newline or carriage return
    auto end = data;
    if (*(end - 1) == '\n') {
      --end;
    }
    if (*(end - 1) == '\r') {
      --end;
    }
    const int32_t batch_row_including_skipped =
        batch_.num_rows_ + batch_.num_skipped_rows();
    InvalidRow row{batch_.num_cols_, num_cols,
                   first_row_ < 0 ? -1 : first_row_ + batch_row_including_skipped,
                   std::string_view(start, end - start)};

    if (options_.invalid_row_handler &&
        options_.invalid_row_handler(row) == InvalidRowResult::Skip) {
      values_writer->RollbackLine();
      parsed_writer->RollbackLine();
      if (!batch_.skipped_rows_.empty()) {
        // Should be increasing (non-strictly)
        DCHECK_GE(batch_.num_rows_, batch_.skipped_rows_.back());
      }
      // Record the logical row number (not including skipped) since that
      // is what we are going to look for later.
      batch_.skipped_rows_.push_back(batch_.num_rows_);
      *out_data = data;
      return Status::OK();
    }

    return MismatchingColumns(row);
  }

  template <typename SpecializedOptions, bool UseBulkFilter, typename ValueDescWriter,
            typename DataWriter, typename BulkFilter>
  Status ParseLine(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                   const char* data, const char* data_end, bool is_final,
                   const char** out_data, const BulkFilter& bulk_filter) {
    int32_t num_cols = 0;
    char c;
    const auto start = data;

    DCHECK_GT(data_end, data);

    auto FinishField = [&]() { values_writer->FinishField(parsed_writer); };

    values_writer->BeginLine();
    parsed_writer->BeginLine();

    // The parsing state machine

    // Special case empty lines: do we start with a newline separator?
    c = *data;
    if (ARROW_PREDICT_FALSE(IsControlChar(c))) {
      if (c == '\r') {
        data++;
        if (data < data_end && *data == '\n') {
          data++;
        }
        goto EmptyLine;
      }
      if (c == '\n') {
        data++;
        goto EmptyLine;
      }
    }

  FieldStart:
    // At the start of a field
    if (*data == options_.delimiter) {
      // Empty cells are very common in some files, shortcut them
      values_writer->StartField(false /* quoted */);
      FinishField();
      ++data;
      ++num_cols;
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        goto AbortLine;
      }
      goto FieldStart;
    }

    // Quoting is only recognized at start of field
    if (SpecializedOptions::quoting &&
        ARROW_PREDICT_FALSE(*data == options_.quote_char)) {
      ++data;
      values_writer->StartField(true /* quoted */);
      goto InQuotedField;
    } else {
      values_writer->StartField(false /* quoted */);
      goto InField;
    }

  InField:
    // Inside a non-quoted part of a field
    if (UseBulkFilter) {
      const char* bulk_end = RunBulkFilter(parsed_writer, data, data_end, bulk_filter);
      if (ARROW_PREDICT_FALSE(bulk_end == nullptr)) {
        if (is_final) {
          data = data_end;
        }
        goto AbortLine;
      }
      data = bulk_end;
    } else {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        goto AbortLine;
      }
    }

    c = *data++;
    if (SpecializedOptions::escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        goto AbortLine;
      }
      c = *data++;
      parsed_writer->PushFieldChar(c);
      goto InField;
    }
    if (ARROW_PREDICT_FALSE(c == options_.delimiter)) {
      goto FieldEnd;
    }
    if (ARROW_PREDICT_FALSE(IsControlChar(c))) {
      if (c == '\r') {
        // In the middle of a newline separator?
        if (ARROW_PREDICT_TRUE(data < data_end) && *data == '\n') {
          data++;
        }
        goto LineEnd;
      }
      if (c == '\n') {
        goto LineEnd;
      }
    }
    parsed_writer->PushFieldChar(c);
    goto InField;

  InQuotedField:
    // Inside a quoted part of a field
    if (UseBulkFilter) {
      const char* bulk_end = RunBulkFilter(parsed_writer, data, data_end, bulk_filter);
      if (ARROW_PREDICT_FALSE(bulk_end == nullptr)) {
        if (is_final) {
          data = data_end;
        }
        goto AbortLine;
      }
      data = bulk_end;
    } else {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        goto AbortLine;
      }
    }
    c = *data++;
    if (SpecializedOptions::escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        goto AbortLine;
      }
      c = *data++;
      parsed_writer->PushFieldChar(c);
      goto InQuotedField;
    }
    if (ARROW_PREDICT_FALSE(c == options_.quote_char)) {
      if (options_.double_quote && ARROW_PREDICT_TRUE(data < data_end) &&
          ARROW_PREDICT_FALSE(*data == options_.quote_char)) {
        // Double-quoting
        ++data;
      } else {
        // End of single-quoting
        goto InField;
      }
    }
    parsed_writer->PushFieldChar(c);
    goto InQuotedField;

  FieldEnd:
    // At the end of a field
    FinishField();
    ++num_cols;
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      goto AbortLine;
    }
    goto FieldStart;

  LineEnd:
    // At the end of line
    FinishField();
    ++num_cols;
    if (ARROW_PREDICT_FALSE(num_cols != batch_.num_cols_)) {
      if (batch_.num_cols_ == -1) {
        batch_.num_cols_ = num_cols;
      } else {
        return HandleInvalidRow(values_writer, parsed_writer, start, data, num_cols,
                                out_data);
      }
    }
    ++batch_.num_rows_;
    *out_data = data;
    return Status::OK();

  AbortLine:
    // Not a full line except perhaps if in final block
    if (is_final) {
      goto LineEnd;
    }
    // Truncated line at end of block, rewind parsed state
    values_writer->RollbackLine();
    parsed_writer->RollbackLine();
    return Status::OK();

  EmptyLine:
    if (!options_.ignore_empty_lines) {
      if (batch_.num_cols_ == -1) {
        // Consider as single value
        batch_.num_cols_ = 1;
      }
      // Record as row of empty (null?) values
      while (num_cols++ < batch_.num_cols_) {
        values_writer->StartField(false /* quoted */);
        FinishField();
      }
      ++batch_.num_rows_;
    }
    *out_data = data;
    return Status::OK();
  }

  template <typename DataWriter, typename SpecializedBulkFilter>
  const char* RunBulkFilter(DataWriter* data_writer, const char* data,
                            const char* data_end,
                            const SpecializedBulkFilter& bulk_filter) {
    while (true) {
      using WordType = typename SpecializedBulkFilter::WordType;

      if (ARROW_PREDICT_FALSE(static_cast<size_t>(data_end - data) < sizeof(WordType))) {
        if (ARROW_PREDICT_FALSE(data == data_end)) {
          return nullptr;
        }
        return data;
      }
      WordType word;
      memcpy(&word, data, sizeof(WordType));
      if (bulk_filter.Matches(word)) {
        return data;
      }
      // No special chars
      data_writer->PushFieldWord(word);
      data += sizeof(WordType);
    }
  }

  template <typename SpecializedOptions, typename ValueDescWriter, typename DataWriter,
            typename BulkFilter>
  Status ParseChunk(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                    const char* data, const char* data_end, bool is_final,
                    int32_t rows_in_chunk, const char** out_data, bool* finished_parsing,
                    const BulkFilter& bulk_filter) {
    const int32_t start_num_rows = batch_.num_rows_;
    const int32_t num_rows_deadline = batch_.num_rows_ + rows_in_chunk;

    if (use_bulk_filter_) {
      while (data < data_end && batch_.num_rows_ < num_rows_deadline) {
        const char* line_end = data;
        RETURN_NOT_OK((ParseLine<SpecializedOptions, true>(values_writer, parsed_writer,
                                                           data, data_end, is_final,
                                                           &line_end, bulk_filter)));
        RETURN_NOT_OK(values_writer->status());
        if (line_end == data) {
          // Cannot parse any further
          *finished_parsing = true;
          break;
        }
        data = line_end;
      }
    } else {
      while (data < data_end && batch_.num_rows_ < num_rows_deadline) {
        const char* line_end = data;
        RETURN_NOT_OK((ParseLine<SpecializedOptions, false>(values_writer, parsed_writer,
                                                            data, data_end, is_final,
                                                            &line_end, bulk_filter)));
        RETURN_NOT_OK(values_writer->status());
        if (line_end == data) {
          // Cannot parse any further
          *finished_parsing = true;
          break;
        }
        data = line_end;
      }
    }

    if (batch_.num_rows_ > start_num_rows && batch_.num_cols_ > 0) {
      // Use bulk filter only if average value length is >= 10 bytes,
      // as the bulk filter has a fixed cost that isn't compensated
      // when values are too short.
      const int64_t bulk_filter_threshold =
          batch_.num_cols_ * (batch_.num_rows_ - start_num_rows) * 10;
      use_bulk_filter_ = (data - *out_data) > bulk_filter_threshold;
    }

    // Append new buffers and update size
    ARROW_ASSIGN_OR_RAISE(auto values_buffer, values_writer->Finish());
    if (values_buffer->size() > 0) {
      values_size_ +=
          static_cast<int32_t>(values_buffer->size() / sizeof(ParsedValueDesc) - 1);
      batch_.values_buffers_.push_back(std::move(values_buffer));
    }
    *out_data = data;
    return Status::OK();
  }

  template <typename SpecializedOptions>
  Status ParseSpecialized(const std::vector<std::string_view>& views, bool is_final,
                          uint32_t* out_size) {
    internal::PreferredBulkFilterType<SpecializedOptions> bulk_filter(options_);

    batch_ = DataBatch{batch_.num_cols_};
    values_size_ = 0;

    size_t total_view_length = 0;
    for (const auto& view : views) {
      total_view_length += view.length();
    }
    if (total_view_length > std::numeric_limits<uint32_t>::max()) {
      return Status::Invalid("CSV block too large");
    }

    PresizedDataWriter parsed_writer(pool_, static_cast<uint32_t>(total_view_length));
    uint32_t total_parsed_length = 0;

    for (const auto& view : views) {
      const char* data = view.data();
      const char* data_end = view.data() + view.length();
      bool finished_parsing = false;

      if (batch_.num_cols_ == -1) {
        // Can't presize values when the number of columns is not known, first parse
        // a single line
        const int32_t rows_in_chunk = 1;
        ARROW_ASSIGN_OR_RAISE(auto values_writer, ResizableValueDescWriter::Make(pool_));
        values_writer.Start(parsed_writer);

        RETURN_NOT_OK(ParseChunk<SpecializedOptions>(
            &values_writer, &parsed_writer, data, data_end, is_final, rows_in_chunk,
            &data, &finished_parsing, bulk_filter));
        if (batch_.num_cols_ == -1) {
          return ParseError("Empty CSV file or block: cannot infer number of columns");
        }
      }

      while (!finished_parsing && data < data_end && batch_.num_rows_ < max_num_rows_) {
        // We know the number of columns, so can presize a values array for
        // a given number of rows
        DCHECK_GE(batch_.num_cols_, 0);

        int32_t rows_in_chunk;
        constexpr int32_t kTargetChunkSize = 32768;  // in number of values
        if (batch_.num_cols_ > 0) {
          rows_in_chunk = std::min(std::max(kTargetChunkSize / batch_.num_cols_, 512),
                                   max_num_rows_ - batch_.num_rows_);
        } else {
          rows_in_chunk = std::min(kTargetChunkSize, max_num_rows_ - batch_.num_rows_);
        }

        ARROW_ASSIGN_OR_RAISE(
            auto values_writer,
            PresizedValueDescWriter::Make(pool_, rows_in_chunk, batch_.num_cols_));
        values_writer.Start(parsed_writer);

        RETURN_NOT_OK(ParseChunk<SpecializedOptions>(
            &values_writer, &parsed_writer, data, data_end, is_final, rows_in_chunk,
            &data, &finished_parsing, bulk_filter));
      }
      DCHECK_GE(data, view.data());
      DCHECK_LE(data, data_end);
      total_parsed_length += static_cast<uint32_t>(data - view.data());

      if (data < data_end) {
        // Stopped early, for some reason
        break;
      }
    }

    parsed_writer.Finish(&batch_.parsed_buffer_);
    batch_.parsed_size_ = static_cast<int32_t>(batch_.parsed_buffer_->size());
    batch_.parsed_ = batch_.parsed_buffer_->data();

    if (batch_.num_cols_ == -1) {
      DCHECK_EQ(batch_.num_rows_, 0);
    }
    DCHECK_EQ(values_size_, batch_.num_rows_ * batch_.num_cols_);
#ifndef NDEBUG
    if (batch_.num_rows_ > 0) {
      // Ending parsed offset should be equal to number of parsed bytes
      DCHECK_GT(batch_.values_buffers_.size(), 0);
      const auto& last_values_buffer = batch_.values_buffers_.back();
      const auto last_values =
          reinterpret_cast<const ParsedValueDesc*>(last_values_buffer->data());
      const auto last_values_size = last_values_buffer->size() / sizeof(ParsedValueDesc);
      const auto check_parsed_size =
          static_cast<int32_t>(last_values[last_values_size - 1].offset);
      DCHECK_EQ(batch_.parsed_size_, check_parsed_size);
    } else {
      DCHECK_EQ(batch_.parsed_size_, 0);
    }
#endif
    *out_size = static_cast<uint32_t>(total_parsed_length);
    return Status::OK();
  }

  Status Parse(const std::vector<std::string_view>& data, bool is_final,
               uint32_t* out_size) {
    if (options_.quoting) {
      if (options_.escaping) {
        return ParseSpecialized<internal::SpecializedOptions<true, true>>(data, is_final,
                                                                          out_size);
      } else {
        return ParseSpecialized<internal::SpecializedOptions<true, false>>(data, is_final,
                                                                           out_size);
      }
    } else {
      if (options_.escaping) {
        return ParseSpecialized<internal::SpecializedOptions<false, true>>(data, is_final,
                                                                           out_size);
      } else {
        return ParseSpecialized<internal::SpecializedOptions<false, false>>(
            data, is_final, out_size);
      }
    }
  }

 protected:
  MemoryPool* pool_;
  const ParseOptions options_;
  const int64_t first_row_;
  // The maximum number of rows to parse from a block
  int32_t max_num_rows_;

  bool use_bulk_filter_ = false;

  // Unparsed data size
  int32_t values_size_;
  // Parsed data batch
  DataBatch batch_;
};

BlockParser::BlockParser(ParseOptions options, int32_t num_cols, int64_t first_row,
                         int32_t max_num_rows)
    : BlockParser(default_memory_pool(), options, num_cols, first_row, max_num_rows) {}

BlockParser::BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                         int64_t first_row, int32_t max_num_rows)
    : impl_(new BlockParserImpl(pool, std::move(options), num_cols, first_row,
                                max_num_rows)) {}

BlockParser::~BlockParser() {}

Status BlockParser::Parse(const std::vector<std::string_view>& data, uint32_t* out_size) {
  return impl_->Parse(data, false /* is_final */, out_size);
}

Status BlockParser::ParseFinal(const std::vector<std::string_view>& data,
                               uint32_t* out_size) {
  return impl_->Parse(data, true /* is_final */, out_size);
}

Status BlockParser::Parse(std::string_view data, uint32_t* out_size) {
  return impl_->Parse({data}, false /* is_final */, out_size);
}

Status BlockParser::ParseFinal(std::string_view data, uint32_t* out_size) {
  return impl_->Parse({data}, true /* is_final */, out_size);
}

const DataBatch& BlockParser::parsed_batch() const { return impl_->parsed_batch(); }

int64_t BlockParser::first_row_num() const { return impl_->first_row_num(); }

int32_t SkipRows(const uint8_t* data, uint32_t size, int32_t num_rows,
                 const uint8_t** out_data) {
  const auto end = data + size;
  int32_t skipped_rows = 0;
  *out_data = data;

  for (; skipped_rows < num_rows; ++skipped_rows) {
    uint8_t c;
    do {
      while (ARROW_PREDICT_FALSE(data < end && !IsControlChar(*data))) {
        ++data;
      }
      if (ARROW_PREDICT_FALSE(data == end)) {
        return skipped_rows;
      }
      c = *data++;
    } while (c != '\r' && c != '\n');
    if (c == '\r' && data < end && *data == '\n') {
      ++data;
    }
    *out_data = data;
  }

  return skipped_rows;
}

}  // namespace csv
}  // namespace arrow

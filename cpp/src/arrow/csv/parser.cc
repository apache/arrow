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
#include <cstdint>
#include <limits>
#include <utility>

#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace csv {

using detail::DataBatch;
using detail::ParsedValueDesc;

namespace {

template <typename... Args>
Status ParseError(Args&&... args) {
  return Status::Invalid("CSV parse error: ", std::forward<Args>(args)...);
}

Status MismatchingColumns(int32_t expected, int32_t actual,
                          const util::string_view& row) {
  if (row.length() > 100) {
    return ParseError("Expected ", expected, " columns, got ", actual, ": ",
                      row.substr(0, 96), " ...");
  }
  return ParseError("Expected ", expected, " columns, got ", actual, ": ", row);
}

inline bool IsControlChar(uint8_t c) { return c < ' '; }

template <bool Quoting, bool Escaping>
class SpecializedOptions {
 public:
  static constexpr bool quoting = Quoting;
  static constexpr bool escaping = Escaping;
};

// A helper class allocating the buffer for parsed values and writing into it
// avoiding any further resizes, except at the end and if full fields are inserted during
// error handling.
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

  // Push the value of a fully complete field. This should only be used to fill in missing
  // values. This method can reallocate the buffer if there isn't enough extra space for
  // the field.
  Status PushField(const std::string& field) {
    const auto field_length = field.length();
    if (field_length == 0) {
      return Status::OK();
    }

    if (field_length > extra_allocated_) {
      // just in case this happens more allocate enough for 10x this amount
      auto to_allocate = static_cast<uint32_t>(
          std::max(field_length * 10, static_cast<std::string::size_type>(128)));
      int64_t new_capacity = parsed_capacity_ + to_allocate;
      RETURN_NOT_OK(parsed_buffer_->Resize(new_capacity));
      parsed_ = parsed_buffer_->mutable_data();
      parsed_capacity_ = new_capacity;
      extra_allocated_ += to_allocate;
    }

    ::memcpy(&parsed_[parsed_size_], field.data(), field_length);
    parsed_size_ += field_length;
    extra_allocated_ -= static_cast<uint32_t>(field_length);
    return Status::OK();
  }

  // Push the value of a fully complete field multiple times. This should only be used to
  // fill in missing values. This method can reallocate the buffer if there isn't enough
  // extra space for the fields. This will only allocate enough space for the fields
  template <typename ValueDescWriter>
  Status PushFields(ValueDescWriter* values_writer, const std::string& field, int count) {
    const auto field_length = field.length();
    const auto fields_length = field_length * static_cast<std::string::size_type>(count);
    if (fields_length > extra_allocated_) {
      // just in case this happens more allocate enough for 10x this amount
      auto to_allocate = static_cast<uint32_t>(
          fields_length - static_cast<std::string::size_type>(extra_allocated_));
      int64_t new_capacity = parsed_capacity_ + to_allocate;
      RETURN_NOT_OK(parsed_buffer_->Resize(new_capacity));
      parsed_ = parsed_buffer_->mutable_data();
      parsed_capacity_ = new_capacity;
      extra_allocated_ += to_allocate;
    }

    for (int i = 0; i < count; ++i) {
      values_writer->StartField(false);
      ::memcpy(&parsed_[parsed_size_], field.data(), field_length);
      parsed_size_ += field_length;
      values_writer->FinishField(this);
    }
    extra_allocated_ -= static_cast<uint32_t>(fields_length);
    return Status::OK();
  }

  // Rollback the state that was saved in BeginLine()
  void RollbackLine() { parsed_size_ = saved_parsed_size_; }

  // Rollback the state to the given offset optionally adding the new space as extra
  Status RollbackTo(int64_t offset, bool add_to_extra = false) {
    if (ARROW_PREDICT_FALSE(offset > parsed_size_)) {
      return Status::Invalid("Offset is beyond size");
    }
    if (ARROW_PREDICT_FALSE(offset < saved_parsed_size_)) {
      return Status::Invalid("Offset is smaller than saved size");
    }

    if (add_to_extra) {
      extra_allocated_ += static_cast<uint32_t>(parsed_size_ - offset);
    }
    parsed_size_ = offset;
    return Status::OK();
  }

  int64_t size() { return parsed_size_; }

 protected:
  std::shared_ptr<ResizableBuffer> parsed_buffer_;
  uint8_t* parsed_;
  int64_t parsed_size_;
  int64_t parsed_capacity_;
  // Checkpointing, for when an incomplete line is encountered at end of block
  int64_t saved_parsed_size_;
  uint32_t extra_allocated_ = 0;
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

  void Finish(std::shared_ptr<Buffer>* out_values) {
    ARROW_CHECK_OK(values_buffer_->Resize(values_size_ * sizeof(*values_)));
    *out_values = values_buffer_;
  }

  // Remove the last count number of fields and return the new end offset of the values
  Result<int64_t> RemoveFields(int count) {
    if (ARROW_PREDICT_FALSE(saved_values_size_ > values_size_ - count)) {
      return Status::Invalid("Not enough fields in row to remove: ", count);
    }

    values_size_ -= count;
    return static_cast<uint64_t>(values_[values_size_ - 1].offset);
  }

 protected:
  ValueDescWriter(MemoryPool* pool, int64_t values_capacity)
      : values_size_(0), values_capacity_(values_capacity) {
    values_buffer_ = *AllocateResizableBuffer(values_capacity_ * sizeof(*values_), pool);
    values_ = reinterpret_cast<ParsedValueDesc*>(values_buffer_->mutable_data());
  }

  std::shared_ptr<ResizableBuffer> values_buffer_;
  ParsedValueDesc* values_;
  int64_t values_size_;
  int64_t values_capacity_;
  bool quoted_;
  // Checkpointing, for when an incomplete line is encountered at end of block
  int64_t saved_values_size_;
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
      values_capacity_ = values_capacity_ * 2;
      ARROW_CHECK_OK(values_buffer_->Resize(values_capacity_ * sizeof(*values_)));
      values_ = reinterpret_cast<ParsedValueDesc*>(values_buffer_->mutable_data());
    }
    values_[values_size_++] = v;
  }
};

// A helper class allocating the buffer for values offsets and writing into it
// without any further resizes, except at the end.  This class is used once the
// number of columns is known, as it eliminates resizes and generates simpler,
// faster CSV parsing code.
class PresizedValueDescWriter : public ValueDescWriter<PresizedValueDescWriter> {
 public:
  PresizedValueDescWriter(MemoryPool* pool, int32_t num_rows, int32_t num_cols)
      : ValueDescWriter(pool, /*values_capacity=*/1 + num_rows * num_cols) {}

  void PushValue(ParsedValueDesc v) {
    DCHECK_LT(values_size_, values_capacity_);
    values_[values_size_++] = v;
  }
};

template <typename ValueDescWriter, typename DataWriter>
class RowModifierImpl : public RowModifier {
 public:
  RowModifierImpl(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                  int32_t expected_num_columns, int32_t num_columns,
                  const util::string_view& line)
      : values_writer_(values_writer),
        parsed_writer_(parsed_writer),
        expected_num_columns_(expected_num_columns),
        num_columns_(num_columns),
        line_(line) {}

  ~RowModifierImpl() override = default;

  void Skip() override {
    values_writer_->RollbackLine();
    parsed_writer_->RollbackLine();
    skipped_ = true;
  }

  Status AddField(const std::string& field) override {
    if (skipped_) {
      return Status::Invalid("Row is already skipped");
    }
    values_writer_->StartField(false);
    ARROW_RETURN_NOT_OK(parsed_writer_->PushField(field));
    values_writer_->FinishField(parsed_writer_);
    ++num_columns_;
    return Status::OK();
  }

  Status AddFields(const std::string& field, int count) override {
    if (ARROW_PREDICT_FALSE(skipped_)) {
      return Status::Invalid("Row is already skipped");
    }
    if (ARROW_PREDICT_FALSE(count <= 0)) {
      return Status::Invalid("Count must be greater than 0: ", count);
    }
    ARROW_RETURN_NOT_OK(parsed_writer_->PushFields(values_writer_, field, count));
    num_columns_ += count;
    return Status::OK();
  }

  Status RemoveFields(int count) override {
    if (ARROW_PREDICT_FALSE(skipped_)) {
      return Status::Invalid("Row is already skipped");
    }
    if (ARROW_PREDICT_FALSE(count <= 0)) {
      return Status::Invalid("Count must be greater than 0: ", count);
    }
    ARROW_ASSIGN_OR_RAISE(auto offset, values_writer_->RemoveFields(count));
    ARROW_RETURN_NOT_OK(parsed_writer_->RollbackTo(offset, true));
    num_columns_ -= count;
    return Status::OK();
  }

  int32_t expected_num_columns() const override { return expected_num_columns_; }

  int32_t num_columns() const override { return num_columns_; }

  const util::string_view& line() const override { return line_; }

  bool skipped() { return skipped_; }

  bool corrected() { return expected_num_columns_ == num_columns_ || skipped_; }

 private:
  ValueDescWriter* values_writer_;
  DataWriter* parsed_writer_;
  const int32_t expected_num_columns_;
  int32_t num_columns_;
  const util::string_view& line_;
  bool skipped_ = false;
};

}  // namespace

class BlockParserImpl {
 public:
  BlockParserImpl(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                  int32_t max_num_rows)
      : pool_(pool), options_(options), max_num_rows_(max_num_rows), batch_(num_cols) {}

  const DataBatch& parsed_batch() const { return batch_; }

  template <typename SpecializedOptions, typename ValueDescWriter, typename DataWriter>
  Status ParseLine(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                   const char* data, const char* data_end, bool is_final,
                   const char** out_data) {
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
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      goto AbortLine;
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
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      goto AbortLine;
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
        // Find the end of the line without newline or carriage return
        auto end = data;
        if (*(end - 1) == '\n') {
          end -= 1;
          if (*(end - 1) == '\r') {
            end -= 1;
          }
        }
        ARROW_ASSIGN_OR_RAISE(auto use_row,
                              HandleBadNumColumns(values_writer, parsed_writer, num_cols,
                                                  util::string_view(start, end - start)));
        if (!use_row) {
          *out_data = data;
          return Status::OK();
        }
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

  template <typename SpecializedOptions, typename ValueDescWriter, typename DataWriter>
  Status ParseChunk(ValueDescWriter* values_writer, DataWriter* parsed_writer,
                    const char* data, const char* data_end, bool is_final,
                    int32_t rows_in_chunk, const char** out_data,
                    bool* finished_parsing) {
    int32_t num_rows_deadline = batch_.num_rows_ + rows_in_chunk;

    while (data < data_end && batch_.num_rows_ < num_rows_deadline) {
      const char* line_end = data;
      RETURN_NOT_OK(ParseLine<SpecializedOptions>(values_writer, parsed_writer, data,
                                                  data_end, is_final, &line_end));
      if (line_end == data) {
        // Cannot parse any further
        *finished_parsing = true;
        break;
      }
      data = line_end;
    }
    // Append new buffers and update size
    std::shared_ptr<Buffer> values_buffer;
    values_writer->Finish(&values_buffer);
    if (values_buffer->size() > 0) {
      values_size_ +=
          static_cast<int32_t>(values_buffer->size() / sizeof(ParsedValueDesc) - 1);
      batch_.values_buffers_.push_back(std::move(values_buffer));
    }
    *out_data = data;
    return Status::OK();
  }

  template <typename SpecializedOptions>
  Status ParseSpecialized(const std::vector<util::string_view>& views, bool is_final,
                          uint32_t* out_size) {
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
        ResizableValueDescWriter values_writer(pool_);
        values_writer.Start(parsed_writer);

        RETURN_NOT_OK(ParseChunk<SpecializedOptions>(&values_writer, &parsed_writer, data,
                                                     data_end, is_final, rows_in_chunk,
                                                     &data, &finished_parsing));
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

        PresizedValueDescWriter values_writer(pool_, rows_in_chunk, batch_.num_cols_);
        values_writer.Start(parsed_writer);

        RETURN_NOT_OK(ParseChunk<SpecializedOptions>(&values_writer, &parsed_writer, data,
                                                     data_end, is_final, rows_in_chunk,
                                                     &data, &finished_parsing));
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

  Status Parse(const std::vector<util::string_view>& data, bool is_final,
               uint32_t* out_size) {
    if (options_.quoting) {
      if (options_.escaping) {
        return ParseSpecialized<SpecializedOptions<true, true>>(data, is_final, out_size);
      } else {
        return ParseSpecialized<SpecializedOptions<true, false>>(data, is_final,
                                                                 out_size);
      }
    } else {
      if (options_.escaping) {
        return ParseSpecialized<SpecializedOptions<false, true>>(data, is_final,
                                                                 out_size);
      } else {
        return ParseSpecialized<SpecializedOptions<false, false>>(data, is_final,
                                                                  out_size);
      }
    }
  }

 protected:
  /// \brief Call the handler for bad number of columns
  ///
  /// If the row is not able to be handled than an error status is returned. If the row is
  /// handled than a result of true indicates if the row should be kept otherwise the
  /// modified row should be skipped
  template <typename ValueDescWriter, typename DataWriter>
  Result<bool> HandleBadNumColumns(ValueDescWriter* values_writer,
                                   DataWriter* parsed_writer, int32_t num_columns,
                                   const util::string_view& line) {
    if (options_.invalid_row_handler) {
      RowModifierImpl<ValueDescWriter, DataWriter> modifier(
          values_writer, parsed_writer, batch_.num_cols_, num_columns, line);

      ARROW_RETURN_NOT_OK(options_.invalid_row_handler.value()(modifier));
      if (modifier.corrected()) {
        return !modifier.skipped();
      }
    }

    return MismatchingColumns(batch_.num_cols_, num_columns, line);
  }

  MemoryPool* pool_;
  const ParseOptions options_;
  // The maximum number of rows to parse from a block
  int32_t max_num_rows_;

  // Unparsed data size
  int32_t values_size_;
  // Parsed data batch
  DataBatch batch_;
};

BlockParser::BlockParser(ParseOptions options, int32_t num_cols, int32_t max_num_rows)
    : BlockParser(default_memory_pool(), options, num_cols, max_num_rows) {}

BlockParser::BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols,
                         int32_t max_num_rows)
    : impl_(new BlockParserImpl(pool, std::move(options), num_cols, max_num_rows)) {}

BlockParser::~BlockParser() {}

Status BlockParser::Parse(const std::vector<util::string_view>& data,
                          uint32_t* out_size) {
  return impl_->Parse(data, false /* is_final */, out_size);
}

Status BlockParser::ParseFinal(const std::vector<util::string_view>& data,
                               uint32_t* out_size) {
  return impl_->Parse(data, true /* is_final */, out_size);
}

Status BlockParser::Parse(util::string_view data, uint32_t* out_size) {
  return impl_->Parse({data}, false /* is_final */, out_size);
}

Status BlockParser::ParseFinal(util::string_view data, uint32_t* out_size) {
  return impl_->Parse({data}, true /* is_final */, out_size);
}

const DataBatch& BlockParser::parsed_batch() const { return impl_->parsed_batch(); }

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

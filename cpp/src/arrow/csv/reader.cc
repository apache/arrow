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

#include "arrow/csv/reader.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/csv/chunker.h"
#include "arrow/csv/column_builder.h"
#include "arrow/csv/column_decoder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/utf8.h"

namespace arrow {

class MemoryPool;

namespace io {

class InputStream;

}  // namespace io

namespace csv {

using internal::GetCpuThreadPool;
using internal::ThreadPool;

struct ConversionSchema {
  struct Column {
    std::string name;
    // Physical column index in CSV file
    int32_t index;
    // If true, make a column of nulls
    bool is_missing;
    // If set, convert the CSV column to this type
    // If unset (and is_missing is false), infer the type from the CSV column
    std::shared_ptr<DataType> type;
  };

  static Column NullColumn(std::string col_name, std::shared_ptr<DataType> type) {
    return Column{std::move(col_name), -1, true, std::move(type)};
  }

  static Column TypedColumn(std::string col_name, int32_t col_index,
                            std::shared_ptr<DataType> type) {
    return Column{std::move(col_name), col_index, false, std::move(type)};
  }

  static Column InferredColumn(std::string col_name, int32_t col_index) {
    return Column{std::move(col_name), col_index, false, nullptr};
  }

  std::vector<Column> columns;
};

// An iterator of Buffers that makes sure there is no straddling CRLF sequence.
class CSVBufferIterator {
 public:
  explicit CSVBufferIterator(Iterator<std::shared_ptr<Buffer>> buffer_iterator)
      : buffer_iterator_(std::move(buffer_iterator)) {}

  static Iterator<std::shared_ptr<Buffer>> Make(
      Iterator<std::shared_ptr<Buffer>> buffer_iterator) {
    CSVBufferIterator it(std::move(buffer_iterator));
    return Iterator<std::shared_ptr<Buffer>>(std::move(it));
  }

  Result<std::shared_ptr<Buffer>> Next() {
    ARROW_ASSIGN_OR_RAISE(auto buf, buffer_iterator_.Next());
    if (buf == nullptr) {
      // EOF
      return nullptr;
    }

    int64_t offset = 0;
    if (first_buffer_) {
      ARROW_ASSIGN_OR_RAISE(auto data, util::SkipUTF8BOM(buf->data(), buf->size()));
      offset += data - buf->data();
      DCHECK_GE(offset, 0);
      first_buffer_ = false;
    }

    if (trailing_cr_ && buf->data()[offset] == '\n') {
      // Skip '\r\n' line separator that started at the end of previous buffer
      ++offset;
    }

    trailing_cr_ = (buf->data()[buf->size() - 1] == '\r');
    buf = SliceBuffer(buf, offset);
    if (buf->size() == 0) {
      // EOF
      return nullptr;
    } else {
      return buf;
    }
  }

 protected:
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;
  bool first_buffer_ = true;
  // Whether there was a trailing CR at the end of last received buffer
  bool trailing_cr_ = false;
};

struct CSVBlock {
  // (partial + completion + buffer) is an entire delimited CSV buffer.
  std::shared_ptr<Buffer> partial;
  std::shared_ptr<Buffer> completion;
  std::shared_ptr<Buffer> buffer;
  int64_t block_index;
  bool is_final;
  std::function<Status(int64_t)> consume_bytes;
};

class BlockReader {
 public:
  BlockReader(std::unique_ptr<Chunker> chunker,
              Iterator<std::shared_ptr<Buffer>> buffer_iterator,
              std::shared_ptr<Buffer> first_buffer)
      : chunker_(std::move(chunker)),
        buffer_iterator_(std::move(buffer_iterator)),
        partial_(std::make_shared<Buffer>("")),
        buffer_(std::move(first_buffer)) {}

 protected:
  std::unique_ptr<Chunker> chunker_;
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;

  std::shared_ptr<Buffer> partial_, buffer_;
  int64_t block_index_ = 0;
  // Whether there was a trailing CR at the end of last received buffer
  bool trailing_cr_ = false;
};

// An object that reads delimited CSV blocks for serial use.
// The number of bytes consumed should be notified after each read,
// using CSVBlock::consume_bytes.
class SerialBlockReader : public BlockReader {
 public:
  using BlockReader::BlockReader;

  Result<arrow::util::optional<CSVBlock>> Next() {
    if (buffer_ == nullptr) {
      // EOF
      return util::optional<CSVBlock>();
    }

    std::shared_ptr<Buffer> next_buffer, completion;
    ARROW_ASSIGN_OR_RAISE(next_buffer, buffer_iterator_.Next());
    bool is_final = (next_buffer == nullptr);

    if (is_final) {
      // End of file reached => compute completion from penultimate block
      RETURN_NOT_OK(chunker_->ProcessFinal(partial_, buffer_, &completion, &buffer_));
    } else {
      // Get completion of partial from previous block.
      RETURN_NOT_OK(
          chunker_->ProcessWithPartial(partial_, buffer_, &completion, &buffer_));
    }
    int64_t bytes_before_buffer = partial_->size() + completion->size();

    auto consume_bytes = [this, bytes_before_buffer,
                          next_buffer](int64_t nbytes) -> Status {
      DCHECK_GE(nbytes, 0);
      auto offset = nbytes - bytes_before_buffer;
      if (offset < 0) {
        // Should not happen
        return Status::Invalid("CSV parser got out of sync with chunker");
      }
      partial_ = SliceBuffer(buffer_, offset);
      buffer_ = next_buffer;
      return Status::OK();
    };

    return CSVBlock{partial_,       completion, buffer_,
                    block_index_++, is_final,   std::move(consume_bytes)};
  }
};

// An object that reads delimited CSV blocks for threaded use.
class ThreadedBlockReader : public BlockReader {
 public:
  using BlockReader::BlockReader;

  Result<arrow::util::optional<CSVBlock>> Next() {
    if (buffer_ == nullptr) {
      // EOF
      return util::optional<CSVBlock>();
    }

    std::shared_ptr<Buffer> next_buffer, whole, completion, next_partial;
    ARROW_ASSIGN_OR_RAISE(next_buffer, buffer_iterator_.Next());
    bool is_final = (next_buffer == nullptr);

    auto current_partial = std::move(partial_);
    auto current_buffer = std::move(buffer_);

    if (is_final) {
      // End of file reached => compute completion from penultimate block
      RETURN_NOT_OK(
          chunker_->ProcessFinal(current_partial, current_buffer, &completion, &whole));
    } else {
      // Get completion of partial from previous block.
      std::shared_ptr<Buffer> starts_with_whole;
      // Get completion of partial from previous block.
      RETURN_NOT_OK(chunker_->ProcessWithPartial(current_partial, current_buffer,
                                                 &completion, &starts_with_whole));

      // Get a complete CSV block inside `partial + block`, and keep
      // the rest for the next iteration.
      RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &next_partial));
    }

    partial_ = std::move(next_partial);
    buffer_ = std::move(next_buffer);

    return CSVBlock{current_partial, completion, whole, block_index_++, is_final, {}};
  }
};

/////////////////////////////////////////////////////////////////////////
// Base class for common functionality

class ReaderMixin {
 public:
  ReaderMixin(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
              const ReadOptions& read_options, const ParseOptions& parse_options,
              const ConvertOptions& convert_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        convert_options_(convert_options),
        input_(std::move(input)) {}

 protected:
  // Read header and column names from buffer, create column builders
  Status ProcessHeader(const std::shared_ptr<Buffer>& buf,
                       std::shared_ptr<Buffer>* rest) {
    const uint8_t* data = buf->data();
    const auto data_end = data + buf->size();
    DCHECK_GT(data_end - data, 0);

    if (read_options_.skip_rows) {
      // Skip initial rows (potentially invalid CSV data)
      auto num_skipped_rows = SkipRows(data, static_cast<uint32_t>(data_end - data),
                                       read_options_.skip_rows, &data);
      if (num_skipped_rows < read_options_.skip_rows) {
        return Status::Invalid(
            "Could not skip initial ", read_options_.skip_rows,
            " rows from CSV file, "
            "either file is too short or header is larger than block size");
      }
    }

    if (read_options_.column_names.empty()) {
      // Parse one row (either to read column names or to know the number of columns)
      BlockParser parser(pool_, parse_options_, num_csv_cols_, 1);
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser.Parse(
          util::string_view(reinterpret_cast<const char*>(data), data_end - data),
          &parsed_size));
      if (parser.num_rows() != 1) {
        return Status::Invalid(
            "Could not read first row from CSV file, either "
            "file is too short or header is larger than block size");
      }
      if (parser.num_cols() == 0) {
        return Status::Invalid("No columns in CSV file");
      }

      if (read_options_.autogenerate_column_names) {
        column_names_ = GenerateColumnNames(parser.num_cols());
      } else {
        // Read column names from header row
        auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
          column_names_.emplace_back(reinterpret_cast<const char*>(data), size);
          return Status::OK();
        };
        RETURN_NOT_OK(parser.VisitLastRow(visit));
        DCHECK_EQ(static_cast<size_t>(parser.num_cols()), column_names_.size());
        // Skip parsed header row
        data += parsed_size;
      }
    } else {
      column_names_ = read_options_.column_names;
    }
    *rest = SliceBuffer(buf, data - buf->data());

    num_csv_cols_ = static_cast<int32_t>(column_names_.size());
    DCHECK_GT(num_csv_cols_, 0);

    return MakeConversionSchema();
  }

  std::vector<std::string> GenerateColumnNames(int32_t num_cols) {
    std::vector<std::string> res;
    res.reserve(num_cols);
    for (int32_t i = 0; i < num_cols; ++i) {
      std::stringstream ss;
      ss << "f" << i;
      res.push_back(ss.str());
    }
    return res;
  }

  // Make conversion schema from options and parsed CSV header
  Status MakeConversionSchema() {
    // Append a column converted from CSV data
    auto append_csv_column = [&](std::string col_name, int32_t col_index) {
      // Does the named column have a fixed type?
      auto it = convert_options_.column_types.find(col_name);
      if (it == convert_options_.column_types.end()) {
        conversion_schema_.columns.push_back(
            ConversionSchema::InferredColumn(std::move(col_name), col_index));
      } else {
        conversion_schema_.columns.push_back(
            ConversionSchema::TypedColumn(std::move(col_name), col_index, it->second));
      }
    };

    // Append a column of nulls
    auto append_null_column = [&](std::string col_name) {
      // If the named column has a fixed type, use it, otherwise use null()
      std::shared_ptr<DataType> type;
      auto it = convert_options_.column_types.find(col_name);
      if (it == convert_options_.column_types.end()) {
        type = null();
      } else {
        type = it->second;
      }
      conversion_schema_.columns.push_back(
          ConversionSchema::NullColumn(std::move(col_name), std::move(type)));
    };

    if (convert_options_.include_columns.empty()) {
      // Include all columns in CSV file order
      for (int32_t col_index = 0; col_index < num_csv_cols_; ++col_index) {
        append_csv_column(column_names_[col_index], col_index);
      }
    } else {
      // Include columns from `include_columns` (in that order)
      // Compute indices of columns in the CSV file
      std::unordered_map<std::string, int32_t> col_indices;
      col_indices.reserve(column_names_.size());
      for (int32_t i = 0; i < static_cast<int32_t>(column_names_.size()); ++i) {
        col_indices.emplace(column_names_[i], i);
      }

      for (const auto& col_name : convert_options_.include_columns) {
        auto it = col_indices.find(col_name);
        if (it != col_indices.end()) {
          append_csv_column(col_name, it->second);
        } else if (convert_options_.include_missing_columns) {
          append_null_column(col_name);
        } else {
          return Status::KeyError("Column '", col_name,
                                  "' in include_columns "
                                  "does not exist in CSV file");
        }
      }
    }
    return Status::OK();
  }

  struct ParseResult {
    std::shared_ptr<BlockParser> parser;
    int64_t parsed_bytes;
  };

  Result<ParseResult> Parse(const std::shared_ptr<Buffer>& partial,
                            const std::shared_ptr<Buffer>& completion,
                            const std::shared_ptr<Buffer>& block, int64_t block_index,
                            bool is_final) {
    static constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    auto parser =
        std::make_shared<BlockParser>(pool_, parse_options_, num_csv_cols_, max_num_rows);

    std::shared_ptr<Buffer> straddling;
    std::vector<util::string_view> views;
    if (partial->size() != 0 || completion->size() != 0) {
      if (partial->size() == 0) {
        straddling = completion;
      } else if (completion->size() == 0) {
        straddling = partial;
      } else {
        ARROW_ASSIGN_OR_RAISE(straddling,
                              ConcatenateBuffers({partial, completion}, pool_));
      }
      views = {util::string_view(*straddling), util::string_view(*block)};
    } else {
      views = {util::string_view(*block)};
    }
    uint32_t parsed_size;
    if (is_final) {
      RETURN_NOT_OK(parser->ParseFinal(views, &parsed_size));
    } else {
      RETURN_NOT_OK(parser->Parse(views, &parsed_size));
    }
    return ParseResult{std::move(parser), static_cast<int64_t>(parsed_size)};
  }

  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ConvertOptions convert_options_;

  // Number of columns in the CSV file
  int32_t num_csv_cols_ = -1;
  // Column names in the CSV file
  std::vector<std::string> column_names_;
  ConversionSchema conversion_schema_;

  std::shared_ptr<io::InputStream> input_;
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;
  std::shared_ptr<internal::TaskGroup> task_group_;
};

/////////////////////////////////////////////////////////////////////////
// Base class for one-shot table readers

class BaseTableReader : public ReaderMixin, public csv::TableReader {
 public:
  using ReaderMixin::ReaderMixin;

  virtual Status Init() = 0;

 protected:
  // Make column builders from conversion schema
  Status MakeColumnBuilders() {
    for (const auto& column : conversion_schema_.columns) {
      std::shared_ptr<ColumnBuilder> builder;
      if (column.is_missing) {
        ARROW_ASSIGN_OR_RAISE(builder,
                              ColumnBuilder::MakeNull(pool_, column.type, task_group_));
      } else if (column.type != nullptr) {
        ARROW_ASSIGN_OR_RAISE(builder,
                              ColumnBuilder::Make(pool_, column.type, column.index,
                                                  convert_options_, task_group_));
      } else {
        ARROW_ASSIGN_OR_RAISE(
            builder,
            ColumnBuilder::Make(pool_, column.index, convert_options_, task_group_));
      }
      column_builders_.push_back(std::move(builder));
    }
    return Status::OK();
  }

  Result<int64_t> ParseAndInsert(const std::shared_ptr<Buffer>& partial,
                                 const std::shared_ptr<Buffer>& completion,
                                 const std::shared_ptr<Buffer>& block,
                                 int64_t block_index, bool is_final) {
    ARROW_ASSIGN_OR_RAISE(auto result,
                          Parse(partial, completion, block, block_index, is_final));
    RETURN_NOT_OK(ProcessData(result.parser, block_index));
    return result.parsed_bytes;
  }

  // Trigger conversion of parsed block data
  Status ProcessData(const std::shared_ptr<BlockParser>& parser, int64_t block_index) {
    for (auto& builder : column_builders_) {
      builder->Insert(block_index, parser);
    }
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> MakeTable() {
    DCHECK_EQ(column_builders_.size(), conversion_schema_.columns.size());

    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;

    for (int32_t i = 0; i < static_cast<int32_t>(column_builders_.size()); ++i) {
      const auto& column = conversion_schema_.columns[i];
      ARROW_ASSIGN_OR_RAISE(auto array, column_builders_[i]->Finish());
      fields.push_back(::arrow::field(column.name, array->type()));
      columns.emplace_back(std::move(array));
    }
    return Table::Make(schema(fields), columns);
  }

  // Column builders for target Table (in ConversionSchema order)
  std::vector<std::shared_ptr<ColumnBuilder>> column_builders_;
};

/////////////////////////////////////////////////////////////////////////
// Base class for streaming readers

class BaseStreamingReader : public ReaderMixin, public csv::StreamingReader {
 public:
  using ReaderMixin::ReaderMixin;

  virtual Status Init() = 0;

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    do {
      RETURN_NOT_OK(ReadNext().Value(batch));
    } while (*batch != nullptr && (*batch)->num_rows() == 0);
    return Status::OK();
  }

 protected:
  virtual Result<std::shared_ptr<RecordBatch>> ReadNext() = 0;

  // Make column decoders from conversion schema
  Status MakeColumnDecoders() {
    for (const auto& column : conversion_schema_.columns) {
      std::shared_ptr<ColumnDecoder> decoder;
      if (column.is_missing) {
        ARROW_ASSIGN_OR_RAISE(decoder,
                              ColumnDecoder::MakeNull(pool_, column.type, task_group_));
      } else if (column.type != nullptr) {
        ARROW_ASSIGN_OR_RAISE(decoder,
                              ColumnDecoder::Make(pool_, column.type, column.index,
                                                  convert_options_, task_group_));
      } else {
        ARROW_ASSIGN_OR_RAISE(
            decoder,
            ColumnDecoder::Make(pool_, column.index, convert_options_, task_group_));
      }
      column_decoders_.push_back(std::move(decoder));
    }
    return Status::OK();
  }

  Result<int64_t> ParseAndInsert(const std::shared_ptr<Buffer>& partial,
                                 const std::shared_ptr<Buffer>& completion,
                                 const std::shared_ptr<Buffer>& block,
                                 int64_t block_index, bool is_final) {
    ARROW_ASSIGN_OR_RAISE(auto result,
                          Parse(partial, completion, block, block_index, is_final));
    RETURN_NOT_OK(ProcessData(result.parser, block_index));
    return result.parsed_bytes;
  }

  // Trigger conversion of parsed block data
  Status ProcessData(const std::shared_ptr<BlockParser>& parser, int64_t block_index) {
    for (auto& decoder : column_decoders_) {
      decoder->Insert(block_index, parser);
    }
    return Status::OK();
  }

  Result<std::shared_ptr<RecordBatch>> DecodeNextBatch() {
    DCHECK(!column_decoders_.empty());
    ArrayVector arrays;
    arrays.reserve(column_decoders_.size());
    Status st;
    for (auto& decoder : column_decoders_) {
      auto maybe_array = decoder->NextChunk();
      if (!maybe_array.ok()) {
        // If there's an error, still fetch results from other decoders to
        // keep them in sync.
        st &= maybe_array.status();
      } else {
        arrays.push_back(*std::move(maybe_array));
      }
    }
    RETURN_NOT_OK(st);
    DCHECK_EQ(arrays.size(), column_decoders_.size());
    const bool is_null = (arrays[0] == nullptr);
#ifndef NDEBUG
    for (const auto& array : arrays) {
      DCHECK_EQ(array == nullptr, is_null);
    }
#endif
    if (is_null) {
      eof_ = true;
      return nullptr;
    }

    if (schema_ == nullptr) {
      FieldVector fields(arrays.size());
      for (size_t i = 0; i < arrays.size(); ++i) {
        fields[i] = field(conversion_schema_.columns[i].name, arrays[i]->type());
      }
      schema_ = arrow::schema(std::move(fields));
    }
    const auto n_rows = arrays[0]->length();
    return RecordBatch::Make(schema_, n_rows, std::move(arrays));
  }

  // Column decoders (in ConversionSchema order)
  std::vector<std::shared_ptr<ColumnDecoder>> column_decoders_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<RecordBatch> pending_batch_;
  bool eof_ = false;
};

/////////////////////////////////////////////////////////////////////////
// Serial StreamingReader implementation

class SerialStreamingReader : public BaseStreamingReader {
 public:
  using BaseStreamingReader::BaseStreamingReader;

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    // Since we're converting serially, no need to readahead more than one block
    int32_t block_queue_size = 1;
    ARROW_ASSIGN_OR_RAISE(auto rh_it,
                          MakeReadaheadIterator(std::move(istream_it), block_queue_size));
    buffer_iterator_ = CSVBufferIterator::Make(std::move(rh_it));
    task_group_ = internal::TaskGroup::MakeSerial();

    // Read schema from first batch
    ARROW_ASSIGN_OR_RAISE(pending_batch_, ReadNext());
    DCHECK_NE(schema_, nullptr);
    return Status::OK();
  }

 protected:
  Result<std::shared_ptr<RecordBatch>> ReadNext() override {
    if (eof_) {
      return nullptr;
    }
    if (block_reader_ == nullptr) {
      Status st = SetupReader();
      if (!st.ok()) {
        // Can't setup reader => bail out
        eof_ = true;
        return st;
      }
    }
    auto batch = std::move(pending_batch_);
    if (batch != nullptr) {
      return batch;
    }

    if (!source_eof_) {
      ARROW_ASSIGN_OR_RAISE(auto maybe_block, block_reader_->Next());
      if (maybe_block.has_value()) {
        last_block_index_ = maybe_block->block_index;
        auto maybe_parsed = ParseAndInsert(maybe_block->partial, maybe_block->completion,
                                           maybe_block->buffer, maybe_block->block_index,
                                           maybe_block->is_final);
        if (!maybe_parsed.ok()) {
          // Parse error => bail out
          eof_ = true;
          return maybe_parsed.status();
        }
        RETURN_NOT_OK(maybe_block->consume_bytes(*maybe_parsed));
      } else {
        source_eof_ = true;
        for (auto& decoder : column_decoders_) {
          decoder->SetEOF(last_block_index_ + 1);
        }
      }
    }

    auto maybe_batch = DecodeNextBatch();
    if (schema_ == nullptr && maybe_batch.ok()) {
      schema_ = (*maybe_batch)->schema();
    }
    return maybe_batch;
  };

  Status SetupReader() {
    ARROW_ASSIGN_OR_RAISE(auto first_buffer, buffer_iterator_.Next());
    if (first_buffer == nullptr) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(first_buffer, &first_buffer));
    RETURN_NOT_OK(MakeColumnDecoders());

    block_reader_ = std::make_shared<SerialBlockReader>(MakeChunker(parse_options_),
                                                        std::move(buffer_iterator_),
                                                        std::move(first_buffer));
    return Status::OK();
  }

  bool source_eof_ = false;
  int64_t last_block_index_ = 0;
  std::shared_ptr<SerialBlockReader> block_reader_;
};

/////////////////////////////////////////////////////////////////////////
// Serial TableReader implementation

class SerialTableReader : public BaseTableReader {
 public:
  using BaseTableReader::BaseTableReader;

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    // Since we're converting serially, no need to readahead more than one block
    int32_t block_queue_size = 1;
    ARROW_ASSIGN_OR_RAISE(auto rh_it,
                          MakeReadaheadIterator(std::move(istream_it), block_queue_size));
    buffer_iterator_ = CSVBufferIterator::Make(std::move(rh_it));
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> Read() override {
    task_group_ = internal::TaskGroup::MakeSerial();

    // First block
    ARROW_ASSIGN_OR_RAISE(auto first_buffer, buffer_iterator_.Next());
    if (first_buffer == nullptr) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(first_buffer, &first_buffer));
    RETURN_NOT_OK(MakeColumnBuilders());

    SerialBlockReader block_reader(MakeChunker(parse_options_),
                                   std::move(buffer_iterator_), std::move(first_buffer));

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto maybe_block, block_reader.Next());
      if (!maybe_block.has_value()) {
        // EOF
        break;
      }
      ARROW_ASSIGN_OR_RAISE(int64_t parsed_bytes,
                            ParseAndInsert(maybe_block->partial, maybe_block->completion,
                                           maybe_block->buffer, maybe_block->block_index,
                                           maybe_block->is_final));
      RETURN_NOT_OK(maybe_block->consume_bytes(parsed_bytes));
    }
    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());
    return MakeTable();
  }
};

/////////////////////////////////////////////////////////////////////////
// Parallel TableReader implementation

class ThreadedTableReader : public BaseTableReader {
 public:
  using BaseTableReader::BaseTableReader;

  ThreadedTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                      const ReadOptions& read_options, const ParseOptions& parse_options,
                      const ConvertOptions& convert_options, ThreadPool* thread_pool)
      : BaseTableReader(pool, input, read_options, parse_options, convert_options),
        thread_pool_(thread_pool) {}

  ~ThreadedTableReader() override {
    if (task_group_) {
      // In case of error, make sure all pending tasks are finished before
      // we start destroying BaseTableReader members
      ARROW_UNUSED(task_group_->Finish());
    }
  }

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    int32_t block_queue_size = thread_pool_->GetCapacity();
    ARROW_ASSIGN_OR_RAISE(auto rh_it,
                          MakeReadaheadIterator(std::move(istream_it), block_queue_size));
    buffer_iterator_ = CSVBufferIterator::Make(std::move(rh_it));
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> Read() override {
    task_group_ = internal::TaskGroup::MakeThreaded(thread_pool_);

    // First block
    ARROW_ASSIGN_OR_RAISE(auto first_buffer, buffer_iterator_.Next());
    if (first_buffer == nullptr) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(first_buffer, &first_buffer));
    RETURN_NOT_OK(MakeColumnBuilders());

    ThreadedBlockReader block_reader(MakeChunker(parse_options_),
                                     std::move(buffer_iterator_),
                                     std::move(first_buffer));

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto maybe_block, block_reader.Next());
      if (!maybe_block.has_value()) {
        // EOF
        break;
      }
      DCHECK(!maybe_block->consume_bytes);

      // Launch parse task
      task_group_->Append([this, maybe_block] {
        return ParseAndInsert(maybe_block->partial, maybe_block->completion,
                              maybe_block->buffer, maybe_block->block_index,
                              maybe_block->is_final)
            .status();
      });
    }

    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());
    return MakeTable();
  }

 protected:
  ThreadPool* thread_pool_;
};

/////////////////////////////////////////////////////////////////////////
// Factory functions

Result<std::shared_ptr<TableReader>> TableReader::Make(
    MemoryPool* pool, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  std::shared_ptr<BaseTableReader> reader;
  if (read_options.use_threads) {
    reader = std::make_shared<ThreadedTableReader>(
        pool, input, read_options, parse_options, convert_options, GetCpuThreadPool());
  } else {
    reader = std::make_shared<SerialTableReader>(pool, input, read_options, parse_options,
                                                 convert_options);
  }
  RETURN_NOT_OK(reader->Init());
  return reader;
}

Result<std::shared_ptr<StreamingReader>> StreamingReader::Make(
    MemoryPool* pool, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  std::shared_ptr<BaseStreamingReader> reader;
  reader = std::make_shared<SerialStreamingReader>(pool, input, read_options,
                                                   parse_options, convert_options);
  RETURN_NOT_OK(reader->Init());
  return reader;
}

}  // namespace csv
}  // namespace arrow

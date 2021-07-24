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
#include "arrow/type_fwd.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/utf8.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace csv {

using internal::Executor;

namespace {

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
  static Iterator<std::shared_ptr<Buffer>> Make(
      Iterator<std::shared_ptr<Buffer>> buffer_iterator) {
    Transformer<std::shared_ptr<Buffer>, std::shared_ptr<Buffer>> fn =
        CSVBufferIterator();
    return MakeTransformedIterator(std::move(buffer_iterator), fn);
  }

  static AsyncGenerator<std::shared_ptr<Buffer>> MakeAsync(
      AsyncGenerator<std::shared_ptr<Buffer>> buffer_iterator) {
    Transformer<std::shared_ptr<Buffer>, std::shared_ptr<Buffer>> fn =
        CSVBufferIterator();
    return MakeTransformedGenerator(std::move(buffer_iterator), fn);
  }

  Result<TransformFlow<std::shared_ptr<Buffer>>> operator()(std::shared_ptr<Buffer> buf) {
    if (buf == nullptr) {
      // EOF
      return TransformFinish();
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
      return TransformFinish();
    } else {
      return TransformYield(buf);
    }
  }

 protected:
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
  int64_t bytes_skipped;
  std::function<Status(int64_t)> consume_bytes;
};

}  // namespace
}  // namespace csv

template <>
struct IterationTraits<csv::CSVBlock> {
  static csv::CSVBlock End() { return csv::CSVBlock{{}, {}, {}, -1, true, 0, {}}; }
  static bool IsEnd(const csv::CSVBlock& val) { return val.block_index < 0; }
};

namespace csv {
namespace {

// This is a callable that can be used to transform an iterator.  The source iterator
// will contain buffers of data and the output iterator will contain delimited CSV
// blocks.  util::optional is used so that there is an end token (required by the
// iterator APIs (e.g. Visit)) even though an empty optional is never used in this code.
class BlockReader {
 public:
  BlockReader(std::unique_ptr<Chunker> chunker, std::shared_ptr<Buffer> first_buffer,
              int64_t skip_rows)
      : chunker_(std::move(chunker)),
        partial_(std::make_shared<Buffer>("")),
        buffer_(std::move(first_buffer)),
        skip_rows_(skip_rows) {}

 protected:
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<Buffer> partial_, buffer_;
  int64_t skip_rows_;
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

  static Iterator<CSVBlock> MakeIterator(
      Iterator<std::shared_ptr<Buffer>> buffer_iterator, std::unique_ptr<Chunker> chunker,
      std::shared_ptr<Buffer> first_buffer, int64_t skip_rows) {
    auto block_reader =
        std::make_shared<SerialBlockReader>(std::move(chunker), first_buffer, skip_rows);
    // Wrap shared pointer in callable
    Transformer<std::shared_ptr<Buffer>, CSVBlock> block_reader_fn =
        [block_reader](std::shared_ptr<Buffer> buf) {
          return (*block_reader)(std::move(buf));
        };
    return MakeTransformedIterator(std::move(buffer_iterator), block_reader_fn);
  }

  static AsyncGenerator<CSVBlock> MakeAsyncIterator(
      AsyncGenerator<std::shared_ptr<Buffer>> buffer_generator,
      std::unique_ptr<Chunker> chunker, std::shared_ptr<Buffer> first_buffer,
      int64_t skip_rows) {
    auto block_reader =
        std::make_shared<SerialBlockReader>(std::move(chunker), first_buffer, skip_rows);
    // Wrap shared pointer in callable
    Transformer<std::shared_ptr<Buffer>, CSVBlock> block_reader_fn =
        [block_reader](std::shared_ptr<Buffer> next) {
          return (*block_reader)(std::move(next));
        };
    return MakeTransformedGenerator(std::move(buffer_generator), block_reader_fn);
  }

  Result<TransformFlow<CSVBlock>> operator()(std::shared_ptr<Buffer> next_buffer) {
    if (buffer_ == nullptr) {
      return TransformFinish();
    }

    bool is_final = (next_buffer == nullptr);
    int64_t bytes_skipped = 0;

    if (skip_rows_) {
      bytes_skipped += partial_->size();
      auto orig_size = buffer_->size();
      RETURN_NOT_OK(
          chunker_->ProcessSkip(partial_, buffer_, is_final, &skip_rows_, &buffer_));
      bytes_skipped += orig_size - buffer_->size();
      auto empty = std::make_shared<Buffer>(nullptr, 0);
      if (skip_rows_) {
        // Still have rows beyond this buffer to skip return empty block
        partial_ = std::move(buffer_);
        buffer_ = next_buffer;
        return TransformYield<CSVBlock>(CSVBlock{empty, empty, empty, block_index_++,
                                                 is_final, bytes_skipped,
                                                 [](int64_t) { return Status::OK(); }});
      }
      partial_ = std::move(empty);
    }

    std::shared_ptr<Buffer> completion;

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

    return TransformYield<CSVBlock>(CSVBlock{partial_, completion, buffer_,
                                             block_index_++, is_final, bytes_skipped,
                                             std::move(consume_bytes)});
  }
};

// An object that reads delimited CSV blocks for threaded use.
class ThreadedBlockReader : public BlockReader {
 public:
  using BlockReader::BlockReader;

  static AsyncGenerator<CSVBlock> MakeAsyncIterator(
      AsyncGenerator<std::shared_ptr<Buffer>> buffer_generator,
      std::unique_ptr<Chunker> chunker, std::shared_ptr<Buffer> first_buffer,
      int64_t skip_rows) {
    auto block_reader = std::make_shared<ThreadedBlockReader>(std::move(chunker),
                                                              first_buffer, skip_rows);
    // Wrap shared pointer in callable
    Transformer<std::shared_ptr<Buffer>, CSVBlock> block_reader_fn =
        [block_reader](std::shared_ptr<Buffer> next) { return (*block_reader)(next); };
    return MakeTransformedGenerator(std::move(buffer_generator), block_reader_fn);
  }

  Result<TransformFlow<CSVBlock>> operator()(std::shared_ptr<Buffer> next_buffer) {
    if (buffer_ == nullptr) {
      // EOF
      return TransformFinish();
    }

    bool is_final = (next_buffer == nullptr);

    auto current_partial = std::move(partial_);
    auto current_buffer = std::move(buffer_);
    int64_t bytes_skipped = 0;

    if (skip_rows_) {
      auto orig_size = current_buffer->size();
      bytes_skipped = current_partial->size();
      RETURN_NOT_OK(chunker_->ProcessSkip(current_partial, current_buffer, is_final,
                                          &skip_rows_, &current_buffer));
      bytes_skipped += orig_size - current_buffer->size();
      current_partial = std::make_shared<Buffer>(nullptr, 0);
      if (skip_rows_) {
        partial_ = std::move(current_buffer);
        buffer_ = std::move(next_buffer);
        return TransformYield<CSVBlock>(CSVBlock{current_partial,
                                                 current_partial,
                                                 current_partial,
                                                 block_index_++,
                                                 is_final,
                                                 bytes_skipped,
                                                 {}});
      }
    }

    std::shared_ptr<Buffer> whole, completion, next_partial;

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

    return TransformYield<CSVBlock>(CSVBlock{
        current_partial, completion, whole, block_index_++, is_final, bytes_skipped, {}});
  }
};

struct ParsedBlock {
  std::shared_ptr<BlockParser> parser;
  int64_t block_index;
  int64_t bytes_parsed_or_skipped;
};

struct DecodedBlock {
  std::shared_ptr<RecordBatch> record_batch;
  // Represents the number of input bytes represented by this batch
  // This will include bytes skipped when skipping rows after the header
  int64_t bytes_processed;
};

}  // namespace

}  // namespace csv

template <>
struct IterationTraits<csv::ParsedBlock> {
  static csv::ParsedBlock End() { return csv::ParsedBlock{nullptr, -1, -1}; }
  static bool IsEnd(const csv::ParsedBlock& val) { return val.block_index < 0; }
};

template <>
struct IterationTraits<csv::DecodedBlock> {
  static csv::DecodedBlock End() { return csv::DecodedBlock{nullptr, -1}; }
  static bool IsEnd(const csv::DecodedBlock& val) { return val.bytes_processed < 0; }
};

namespace csv {
namespace {

// A function object that takes in a buffer of CSV data and returns a parsed batch of CSV
// data (CSVBlock -> ParsedBlock) for use with MakeMappedGenerator.
// The parsed batch contains a list of offsets for each of the columns so that columns
// can be individually scanned
//
// This operator is not re-entrant
class BlockParsingOperator {
 public:
  BlockParsingOperator(io::IOContext io_context, ParseOptions parse_options,
                       int num_csv_cols, int64_t first_row)
      : io_context_(io_context),
        parse_options_(parse_options),
        num_csv_cols_(num_csv_cols),
        count_rows_(first_row >= 0),
        num_rows_seen_(first_row) {}

  Result<ParsedBlock> operator()(const CSVBlock& block) {
    constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    auto parser = std::make_shared<BlockParser>(
        io_context_.pool(), parse_options_, num_csv_cols_, num_rows_seen_, max_num_rows);

    std::shared_ptr<Buffer> straddling;
    std::vector<util::string_view> views;
    if (block.partial->size() != 0 || block.completion->size() != 0) {
      if (block.partial->size() == 0) {
        straddling = block.completion;
      } else if (block.completion->size() == 0) {
        straddling = block.partial;
      } else {
        ARROW_ASSIGN_OR_RAISE(
            straddling,
            ConcatenateBuffers({block.partial, block.completion}, io_context_.pool()));
      }
      views = {util::string_view(*straddling), util::string_view(*block.buffer)};
    } else {
      views = {util::string_view(*block.buffer)};
    }
    uint32_t parsed_size;
    if (block.is_final) {
      RETURN_NOT_OK(parser->ParseFinal(views, &parsed_size));
    } else {
      RETURN_NOT_OK(parser->Parse(views, &parsed_size));
    }
    if (count_rows_) {
      num_rows_seen_ += parser->num_rows();
    }
    RETURN_NOT_OK(block.consume_bytes(parsed_size));
    return ParsedBlock{std::move(parser), block.block_index,
                       static_cast<int64_t>(parsed_size) + block.bytes_skipped};
  }

 private:
  io::IOContext io_context_;
  ParseOptions parse_options_;
  int num_csv_cols_;
  bool count_rows_;
  int64_t num_rows_seen_;
};

// A function object that takes in parsed batch of CSV data and decodes it to an arrow
// record batch (ParsedBlock -> DecodedBlock) for use with MakeMappedGenerator.
class BlockDecodingOperator {
 public:
  Future<DecodedBlock> operator()(const ParsedBlock& block) {
    DCHECK(!state_->column_decoders.empty());
    std::vector<Future<std::shared_ptr<Array>>> decoded_array_futs;
    for (auto& decoder : state_->column_decoders) {
      decoded_array_futs.push_back(decoder->Decode(block.parser));
    }
    auto bytes_parsed_or_skipped = block.bytes_parsed_or_skipped;
    auto decoded_arrays_fut = All(std::move(decoded_array_futs));
    auto state = state_;
    return decoded_arrays_fut.Then(
        [state, bytes_parsed_or_skipped](
            const std::vector<Result<std::shared_ptr<Array>>>& maybe_decoded_arrays)
            -> Result<DecodedBlock> {
          ARROW_ASSIGN_OR_RAISE(auto decoded_arrays,
                                internal::UnwrapOrRaise(maybe_decoded_arrays));

          ARROW_ASSIGN_OR_RAISE(auto batch,
                                state->DecodedArraysToBatch(std::move(decoded_arrays)));
          return DecodedBlock{std::move(batch), bytes_parsed_or_skipped};
        });
  }

  static Result<BlockDecodingOperator> Make(io::IOContext io_context,
                                            ConvertOptions convert_options,
                                            ConversionSchema conversion_schema) {
    BlockDecodingOperator op(std::move(io_context), std::move(convert_options),
                             std::move(conversion_schema));
    RETURN_NOT_OK(op.state_->MakeColumnDecoders(io_context));
    return op;
  }

 private:
  BlockDecodingOperator(io::IOContext io_context, ConvertOptions convert_options,
                        ConversionSchema conversion_schema)
      : state_(std::make_shared<State>(std::move(io_context), std::move(convert_options),
                                       std::move(conversion_schema))) {}

  struct State {
    State(io::IOContext io_context, ConvertOptions convert_options,
          ConversionSchema conversion_schema)
        : convert_options(std::move(convert_options)),
          conversion_schema(std::move(conversion_schema)) {}

    Result<std::shared_ptr<RecordBatch>> DecodedArraysToBatch(
        std::vector<std::shared_ptr<Array>> arrays) {
      if (schema == nullptr) {
        FieldVector fields(arrays.size());
        for (size_t i = 0; i < arrays.size(); ++i) {
          fields[i] = field(conversion_schema.columns[i].name, arrays[i]->type());
        }
        schema = arrow::schema(std::move(fields));
      }
      const auto n_rows = arrays[0]->length();
      return RecordBatch::Make(schema, n_rows, std::move(arrays));
    }

    // Make column decoders from conversion schema
    Status MakeColumnDecoders(io::IOContext io_context) {
      for (const auto& column : conversion_schema.columns) {
        std::shared_ptr<ColumnDecoder> decoder;
        if (column.is_missing) {
          ARROW_ASSIGN_OR_RAISE(decoder,
                                ColumnDecoder::MakeNull(io_context.pool(), column.type));
        } else if (column.type != nullptr) {
          ARROW_ASSIGN_OR_RAISE(
              decoder, ColumnDecoder::Make(io_context.pool(), column.type, column.index,
                                           convert_options));
        } else {
          ARROW_ASSIGN_OR_RAISE(
              decoder,
              ColumnDecoder::Make(io_context.pool(), column.index, convert_options));
        }
        column_decoders.push_back(std::move(decoder));
      }
      return Status::OK();
    }

    ConvertOptions convert_options;
    ConversionSchema conversion_schema;
    std::vector<std::shared_ptr<ColumnDecoder>> column_decoders;
    std::shared_ptr<Schema> schema;
  };

  std::shared_ptr<State> state_;
};

/////////////////////////////////////////////////////////////////////////
// Base class for common functionality

class ReaderMixin {
 public:
  ReaderMixin(io::IOContext io_context, std::shared_ptr<io::InputStream> input,
              const ReadOptions& read_options, const ParseOptions& parse_options,
              const ConvertOptions& convert_options, bool count_rows)
      : io_context_(std::move(io_context)),
        read_options_(read_options),
        parse_options_(parse_options),
        convert_options_(convert_options),
        count_rows_(count_rows),
        num_rows_seen_(count_rows_ ? 1 : -1),
        input_(std::move(input)) {}

 protected:
  // Read header and column names from buffer, create column builders
  // Returns the # of bytes consumed
  Result<int64_t> ProcessHeader(const std::shared_ptr<Buffer>& buf,
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
      if (count_rows_) {
        num_rows_seen_ += num_skipped_rows;
      }
    }

    if (read_options_.column_names.empty()) {
      // Parse one row (either to read column names or to know the number of columns)
      BlockParser parser(io_context_.pool(), parse_options_, num_csv_cols_,
                         num_rows_seen_, 1);
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
        if (count_rows_) {
          ++num_rows_seen_;
        }
      }
    } else {
      column_names_ = read_options_.column_names;
    }

    if (count_rows_) {
      // increase rows seen to skip past rows which will be skipped
      num_rows_seen_ += read_options_.skip_rows_after_names;
    }

    auto bytes_consumed = data - buf->data();
    *rest = SliceBuffer(buf, bytes_consumed);

    num_csv_cols_ = static_cast<int32_t>(column_names_.size());
    DCHECK_GT(num_csv_cols_, 0);

    RETURN_NOT_OK(MakeConversionSchema());
    return bytes_consumed;
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
    auto parser = std::make_shared<BlockParser>(
        io_context_.pool(), parse_options_, num_csv_cols_, num_rows_seen_, max_num_rows);

    std::shared_ptr<Buffer> straddling;
    std::vector<util::string_view> views;
    if (partial->size() != 0 || completion->size() != 0) {
      if (partial->size() == 0) {
        straddling = completion;
      } else if (completion->size() == 0) {
        straddling = partial;
      } else {
        ARROW_ASSIGN_OR_RAISE(
            straddling, ConcatenateBuffers({partial, completion}, io_context_.pool()));
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
    if (count_rows_) {
      num_rows_seen_ += parser->num_rows();
    }
    return ParseResult{std::move(parser), static_cast<int64_t>(parsed_size)};
  }

  io::IOContext io_context_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ConvertOptions convert_options_;

  // Number of columns in the CSV file
  int32_t num_csv_cols_ = -1;
  // Whether num_rows_seen_ tracks the number of rows seen in the CSV being parsed
  bool count_rows_;
  // Number of rows seen in the csv. Not used if count_rows is false
  int64_t num_rows_seen_;
  // Column names in the CSV file
  std::vector<std::string> column_names_;
  ConversionSchema conversion_schema_;

  std::shared_ptr<io::InputStream> input_;
  std::shared_ptr<internal::TaskGroup> task_group_;
};

/////////////////////////////////////////////////////////////////////////
// Base class for one-shot table readers

class BaseTableReader : public ReaderMixin, public csv::TableReader {
 public:
  using ReaderMixin::ReaderMixin;

  virtual Status Init() = 0;

  Future<std::shared_ptr<Table>> ReadAsync() override {
    return Future<std::shared_ptr<Table>>::MakeFinished(Read());
  }

 protected:
  // Make column builders from conversion schema
  Status MakeColumnBuilders() {
    for (const auto& column : conversion_schema_.columns) {
      std::shared_ptr<ColumnBuilder> builder;
      if (column.is_missing) {
        ARROW_ASSIGN_OR_RAISE(builder, ColumnBuilder::MakeNull(io_context_.pool(),
                                                               column.type, task_group_));
      } else if (column.type != nullptr) {
        ARROW_ASSIGN_OR_RAISE(
            builder, ColumnBuilder::Make(io_context_.pool(), column.type, column.index,
                                         convert_options_, task_group_));
      } else {
        ARROW_ASSIGN_OR_RAISE(builder,
                              ColumnBuilder::Make(io_context_.pool(), column.index,
                                                  convert_options_, task_group_));
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
    return Table::Make(schema(std::move(fields)), std::move(columns));
  }

  // Column builders for target Table (in ConversionSchema order)
  std::vector<std::shared_ptr<ColumnBuilder>> column_builders_;
};

/////////////////////////////////////////////////////////////////////////
// Base class for streaming readers

class StreamingReaderImpl : public ReaderMixin,
                            public csv::StreamingReader,
                            public std::enable_shared_from_this<StreamingReaderImpl> {
 public:
  StreamingReaderImpl(io::IOContext io_context, std::shared_ptr<io::InputStream> input,
                      const ReadOptions& read_options, const ParseOptions& parse_options,
                      const ConvertOptions& convert_options, bool count_rows)
      : ReaderMixin(io_context, std::move(input), read_options, parse_options,
                    convert_options, count_rows),
        bytes_decoded_(std::make_shared<std::atomic<int64_t>>(0)) {}

  Future<> Init(Executor* cpu_executor) {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    // TODO Consider exposing readahead as a read option (ARROW-12090)
    ARROW_ASSIGN_OR_RAISE(auto bg_it, MakeBackgroundGenerator(std::move(istream_it),
                                                              io_context_.executor()));

    auto transferred_it = MakeTransferredGenerator(bg_it, cpu_executor);

    auto buffer_generator = CSVBufferIterator::MakeAsync(std::move(transferred_it));

    int max_readahead = cpu_executor->GetCapacity();
    auto self = shared_from_this();

    return buffer_generator().Then([self, buffer_generator, max_readahead](
                                       const std::shared_ptr<Buffer>& first_buffer) {
      return self->InitAfterFirstBuffer(first_buffer, buffer_generator, max_readahead);
    });
  }

  std::shared_ptr<Schema> schema() const override { return schema_; }

  int64_t bytes_read() const override { return bytes_decoded_->load(); }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    auto next_fut = ReadNextAsync();
    auto next_result = next_fut.result();
    return std::move(next_result).Value(batch);
  }

  Future<std::shared_ptr<RecordBatch>> ReadNextAsync() override {
    return record_batch_gen_();
  }

 protected:
  Future<> InitAfterFirstBuffer(const std::shared_ptr<Buffer>& first_buffer,
                                AsyncGenerator<std::shared_ptr<Buffer>> buffer_generator,
                                int max_readahead) {
    if (first_buffer == nullptr) {
      return Status::Invalid("Empty CSV file");
    }

    std::shared_ptr<Buffer> after_header;
    ARROW_ASSIGN_OR_RAISE(auto header_bytes_consumed,
                          ProcessHeader(first_buffer, &after_header));
    bytes_decoded_->fetch_add(header_bytes_consumed);

    auto parser_op =
        BlockParsingOperator(io_context_, parse_options_, num_csv_cols_, num_rows_seen_);
    ARROW_ASSIGN_OR_RAISE(
        auto decoder_op,
        BlockDecodingOperator::Make(io_context_, convert_options_, conversion_schema_));

    auto block_gen = SerialBlockReader::MakeAsyncIterator(
        std::move(buffer_generator), MakeChunker(parse_options_), std::move(after_header),
        read_options_.skip_rows_after_names);
    auto parsed_block_gen =
        MakeMappedGenerator(std::move(block_gen), std::move(parser_op));
    auto rb_gen = MakeMappedGenerator(std::move(parsed_block_gen), std::move(decoder_op));

    auto self = shared_from_this();
    return rb_gen().Then([self, rb_gen, max_readahead](const DecodedBlock& first_block) {
      return self->InitAfterFirstBatch(first_block, std::move(rb_gen), max_readahead);
    });
  }

  Status InitAfterFirstBatch(const DecodedBlock& first_block,
                             AsyncGenerator<DecodedBlock> batch_gen, int max_readahead) {
    schema_ = first_block.record_batch->schema();

    AsyncGenerator<DecodedBlock> readahead_gen;
    if (read_options_.use_threads) {
      readahead_gen = MakeReadaheadGenerator(std::move(batch_gen), max_readahead);
    } else {
      readahead_gen = std::move(batch_gen);
    }

    AsyncGenerator<DecodedBlock> restarted_gen;
    // Streaming reader should not emit empty record batches
    if (first_block.record_batch->num_rows() > 0) {
      restarted_gen = MakeGeneratorStartsWith({first_block}, std::move(readahead_gen));
    } else {
      restarted_gen = std::move(readahead_gen);
    }

    auto bytes_decoded = bytes_decoded_;
    auto unwrap_and_record_bytes =
        [bytes_decoded](
            const DecodedBlock& block) -> Result<std::shared_ptr<RecordBatch>> {
      bytes_decoded->fetch_add(block.bytes_processed);
      return block.record_batch;
    };

    auto unwrapped =
        MakeMappedGenerator(std::move(restarted_gen), std::move(unwrap_and_record_bytes));

    record_batch_gen_ = MakeCancellable(std::move(unwrapped), io_context_.stop_token());
    return Status::OK();
  }

  std::shared_ptr<Schema> schema_;
  AsyncGenerator<std::shared_ptr<RecordBatch>> record_batch_gen_;
  // bytes which have been decoded and asked for by the caller
  std::shared_ptr<std::atomic<int64_t>> bytes_decoded_;
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
    task_group_ = internal::TaskGroup::MakeSerial(io_context_.stop_token());

    // First block
    ARROW_ASSIGN_OR_RAISE(auto first_buffer, buffer_iterator_.Next());
    if (first_buffer == nullptr) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(first_buffer, &first_buffer));
    RETURN_NOT_OK(MakeColumnBuilders());

    auto block_iterator = SerialBlockReader::MakeIterator(
        std::move(buffer_iterator_), MakeChunker(parse_options_), std::move(first_buffer),
        read_options_.skip_rows_after_names);
    while (true) {
      RETURN_NOT_OK(io_context_.stop_token().Poll());

      ARROW_ASSIGN_OR_RAISE(auto maybe_block, block_iterator.Next());
      if (IsIterationEnd(maybe_block)) {
        // EOF
        break;
      }
      ARROW_ASSIGN_OR_RAISE(
          int64_t parsed_bytes,
          ParseAndInsert(maybe_block.partial, maybe_block.completion, maybe_block.buffer,
                         maybe_block.block_index, maybe_block.is_final));
      RETURN_NOT_OK(maybe_block.consume_bytes(parsed_bytes));
    }
    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());
    return MakeTable();
  }

 protected:
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;
};

class AsyncThreadedTableReader
    : public BaseTableReader,
      public std::enable_shared_from_this<AsyncThreadedTableReader> {
 public:
  using BaseTableReader::BaseTableReader;

  AsyncThreadedTableReader(io::IOContext io_context,
                           std::shared_ptr<io::InputStream> input,
                           const ReadOptions& read_options,
                           const ParseOptions& parse_options,
                           const ConvertOptions& convert_options, Executor* cpu_executor)
      // Count rows is currently not supported during parallel read
      : BaseTableReader(std::move(io_context), input, read_options, parse_options,
                        convert_options, /*count_rows=*/false),
        cpu_executor_(cpu_executor) {}

  ~AsyncThreadedTableReader() override {
    if (task_group_) {
      // In case of error, make sure all pending tasks are finished before
      // we start destroying BaseTableReader members
      ARROW_UNUSED(task_group_->Finish());
    }
  }

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    int max_readahead = cpu_executor_->GetCapacity();
    int readahead_restart = std::max(1, max_readahead / 2);

    ARROW_ASSIGN_OR_RAISE(
        auto bg_it, MakeBackgroundGenerator(std::move(istream_it), io_context_.executor(),
                                            max_readahead, readahead_restart));

    auto transferred_it = MakeTransferredGenerator(bg_it, cpu_executor_);
    buffer_generator_ = CSVBufferIterator::MakeAsync(std::move(transferred_it));
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> Read() override { return ReadAsync().result(); }

  Future<std::shared_ptr<Table>> ReadAsync() override {
    task_group_ =
        internal::TaskGroup::MakeThreaded(cpu_executor_, io_context_.stop_token());

    auto self = shared_from_this();
    return ProcessFirstBuffer().Then([self](const std::shared_ptr<Buffer>& first_buffer) {
      auto block_generator = ThreadedBlockReader::MakeAsyncIterator(
          self->buffer_generator_, MakeChunker(self->parse_options_),
          std::move(first_buffer), self->read_options_.skip_rows_after_names);

      std::function<Status(CSVBlock)> block_visitor =
          [self](CSVBlock maybe_block) -> Status {
        // The logic in VisitAsyncGenerator ensures that we will never be
        // passed an empty block (visit does not call with the end token) so
        // we can be assured maybe_block has a value.
        DCHECK_GE(maybe_block.block_index, 0);
        DCHECK(!maybe_block.consume_bytes);

        // Launch parse task
        self->task_group_->Append([self, maybe_block] {
          return self
              ->ParseAndInsert(maybe_block.partial, maybe_block.completion,
                               maybe_block.buffer, maybe_block.block_index,
                               maybe_block.is_final)
              .status();
        });
        return Status::OK();
      };

      return VisitAsyncGenerator(std::move(block_generator), block_visitor)
          .Then([self]() -> Future<> {
            // By this point we've added all top level tasks so it is safe to call
            // FinishAsync
            return self->task_group_->FinishAsync();
          })
          .Then([self]() -> Result<std::shared_ptr<Table>> {
            // Finish conversion, create schema and table
            return self->MakeTable();
          });
    });
  }

 protected:
  Future<std::shared_ptr<Buffer>> ProcessFirstBuffer() {
    // First block
    auto first_buffer_future = buffer_generator_();
    return first_buffer_future.Then([this](const std::shared_ptr<Buffer>& first_buffer)
                                        -> Result<std::shared_ptr<Buffer>> {
      if (first_buffer == nullptr) {
        return Status::Invalid("Empty CSV file");
      }
      std::shared_ptr<Buffer> first_buffer_processed;
      RETURN_NOT_OK(ProcessHeader(first_buffer, &first_buffer_processed));
      RETURN_NOT_OK(MakeColumnBuilders());
      return first_buffer_processed;
    });
  }

  Executor* cpu_executor_;
  AsyncGenerator<std::shared_ptr<Buffer>> buffer_generator_;
};

Result<std::shared_ptr<TableReader>> MakeTableReader(
    MemoryPool* pool, io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  RETURN_NOT_OK(parse_options.Validate());
  RETURN_NOT_OK(read_options.Validate());
  RETURN_NOT_OK(convert_options.Validate());
  std::shared_ptr<BaseTableReader> reader;
  if (read_options.use_threads) {
    auto cpu_executor = internal::GetCpuThreadPool();
    reader = std::make_shared<AsyncThreadedTableReader>(
        io_context, input, read_options, parse_options, convert_options, cpu_executor);
  } else {
    reader = std::make_shared<SerialTableReader>(io_context, input, read_options,
                                                 parse_options, convert_options,
                                                 /*count_rows=*/true);
  }
  RETURN_NOT_OK(reader->Init());
  return reader;
}

Future<std::shared_ptr<StreamingReader>> MakeStreamingReader(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    internal::Executor* cpu_executor, const ReadOptions& read_options,
    const ParseOptions& parse_options, const ConvertOptions& convert_options) {
  RETURN_NOT_OK(parse_options.Validate());
  RETURN_NOT_OK(read_options.Validate());
  RETURN_NOT_OK(convert_options.Validate());
  std::shared_ptr<StreamingReaderImpl> reader;
  reader = std::make_shared<StreamingReaderImpl>(
      io_context, input, read_options, parse_options, convert_options,
      /*count_rows=*/!read_options.use_threads || cpu_executor->GetCapacity() == 1);
  return reader->Init(cpu_executor).Then([reader] {
    return std::dynamic_pointer_cast<StreamingReader>(reader);
  });
}

/////////////////////////////////////////////////////////////////////////
// Row count implementation

class CSVRowCounter : public ReaderMixin,
                      public std::enable_shared_from_this<CSVRowCounter> {
 public:
  CSVRowCounter(io::IOContext io_context, Executor* cpu_executor,
                std::shared_ptr<io::InputStream> input, const ReadOptions& read_options,
                const ParseOptions& parse_options)
      : ReaderMixin(io_context, std::move(input), read_options, parse_options,
                    ConvertOptions::Defaults(), /*count_rows=*/true),
        cpu_executor_(cpu_executor),
        row_count_(0) {}

  Future<int64_t> Count() {
    auto self = shared_from_this();
    return Init(self).Then([self]() { return self->DoCount(self); });
  }

 private:
  Future<> Init(const std::shared_ptr<CSVRowCounter>& self) {
    ARROW_ASSIGN_OR_RAISE(auto istream_it,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));
    // TODO Consider exposing readahead as a read option (ARROW-12090)
    ARROW_ASSIGN_OR_RAISE(auto bg_it, MakeBackgroundGenerator(std::move(istream_it),
                                                              io_context_.executor()));
    auto transferred_it = MakeTransferredGenerator(bg_it, cpu_executor_);
    auto buffer_generator = CSVBufferIterator::MakeAsync(std::move(transferred_it));

    return buffer_generator().Then(
        [self, buffer_generator](std::shared_ptr<Buffer> first_buffer) {
          if (!first_buffer) {
            return Status::Invalid("Empty CSV file");
          }
          RETURN_NOT_OK(self->ProcessHeader(first_buffer, &first_buffer));
          self->block_generator_ = SerialBlockReader::MakeAsyncIterator(
              buffer_generator, MakeChunker(self->parse_options_),
              std::move(first_buffer), 0);
          return Status::OK();
        });
  }

  Future<int64_t> DoCount(const std::shared_ptr<CSVRowCounter>& self) {
    // count_cb must return a value instead of Status/Future<> to work with
    // MakeMappedGenerator, and it must use a type with a valid end value to work with
    // IterationEnd.
    std::function<Result<util::optional<int64_t>>(const CSVBlock&)> count_cb =
        [self](const CSVBlock& maybe_block) -> Result<util::optional<int64_t>> {
      ARROW_ASSIGN_OR_RAISE(
          auto parser,
          self->Parse(maybe_block.partial, maybe_block.completion, maybe_block.buffer,
                      maybe_block.block_index, maybe_block.is_final));
      RETURN_NOT_OK(maybe_block.consume_bytes(parser.parsed_bytes));
      self->row_count_ += parser.parser->num_rows();
      return parser.parser->num_rows();
    };
    auto count_gen = MakeMappedGenerator(block_generator_, std::move(count_cb));
    return DiscardAllFromAsyncGenerator(count_gen).Then(
        [self]() { return self->row_count_; });
  }

  Executor* cpu_executor_;
  AsyncGenerator<CSVBlock> block_generator_;
  int64_t row_count_;
};

}  // namespace

/////////////////////////////////////////////////////////////////////////
// Factory functions

Result<std::shared_ptr<TableReader>> TableReader::Make(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  return MakeTableReader(io_context.pool(), io_context, std::move(input), read_options,
                         parse_options, convert_options);
}

Result<std::shared_ptr<TableReader>> TableReader::Make(
    MemoryPool* pool, io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  return MakeTableReader(pool, io_context, std::move(input), read_options, parse_options,
                         convert_options);
}

Result<std::shared_ptr<StreamingReader>> StreamingReader::Make(
    MemoryPool* pool, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  auto io_context = io::IOContext(pool);
  auto cpu_executor = internal::GetCpuThreadPool();
  auto reader_fut = MakeStreamingReader(io_context, std::move(input), cpu_executor,
                                        read_options, parse_options, convert_options);
  auto reader_result = reader_fut.result();
  ARROW_ASSIGN_OR_RAISE(auto reader, reader_result);
  return reader;
}

Result<std::shared_ptr<StreamingReader>> StreamingReader::Make(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options,
    const ConvertOptions& convert_options) {
  auto cpu_executor = internal::GetCpuThreadPool();
  auto reader_fut = MakeStreamingReader(io_context, std::move(input), cpu_executor,
                                        read_options, parse_options, convert_options);
  auto reader_result = reader_fut.result();
  ARROW_ASSIGN_OR_RAISE(auto reader, reader_result);
  return reader;
}

Future<std::shared_ptr<StreamingReader>> StreamingReader::MakeAsync(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    internal::Executor* cpu_executor, const ReadOptions& read_options,
    const ParseOptions& parse_options, const ConvertOptions& convert_options) {
  return MakeStreamingReader(io_context, std::move(input), cpu_executor, read_options,
                             parse_options, convert_options);
}

Future<int64_t> CountRowsAsync(io::IOContext io_context,
                               std::shared_ptr<io::InputStream> input,
                               internal::Executor* cpu_executor,
                               const ReadOptions& read_options,
                               const ParseOptions& parse_options) {
  RETURN_NOT_OK(parse_options.Validate());
  RETURN_NOT_OK(read_options.Validate());
  auto counter = std::make_shared<CSVRowCounter>(
      io_context, cpu_executor, std::move(input), read_options, parse_options);
  return counter->Count();
}

}  // namespace csv

}  // namespace arrow

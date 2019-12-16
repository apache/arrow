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
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/csv/chunker.h"
#include "arrow/csv/column_builder.h"
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

/////////////////////////////////////////////////////////////////////////
// Base class for common functionality

class BaseTableReader : public csv::TableReader {
 public:
  BaseTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                  const ReadOptions& read_options, const ParseOptions& parse_options,
                  const ConvertOptions& convert_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        convert_options_(convert_options),
        input_(std::move(input)) {}

  virtual Status Init() = 0;

 protected:
  Status ReadNextBlock(bool first_block, std::shared_ptr<Buffer>* out) {
    ARROW_ASSIGN_OR_RAISE(auto buf, block_iterator_.Next());
    if (buf == nullptr) {
      // EOF
      out->reset();
      return Status::OK();
    }

    int64_t offset = 0;
    if (first_block) {
      ARROW_ASSIGN_OR_RAISE(auto data, util::SkipUTF8BOM(buf->data(), buf->size()));
      offset += data - buf->data();
      DCHECK_GE(offset, 0);
    }

    if (trailing_cr_ && buf->data()[offset] == '\n') {
      // Skip '\r\n' line separator that started at the end of previous block
      ++offset;
    }

    trailing_cr_ = (buf->data()[buf->size() - 1] == '\r');
    buf = SliceBuffer(buf, offset);
    if (buf->size() == 0) {
      // EOF
      out->reset();
    } else {
      *out = std::move(buf);
    }

    return Status::OK();
  }

  Status ReadNextBlock(std::shared_ptr<Buffer>* out) { return ReadNextBlock(false, out); }

  Status ReadFirstBlock(std::shared_ptr<Buffer>* out) { return ReadNextBlock(true, out); }

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

    if (convert_options_.include_columns.empty()) {
      return MakeColumnBuilders();
    } else {
      return MakeColumnBuilders(convert_options_.include_columns);
    }
  }

  // Make column builders, assuming inclusion of all columns in CSV file order
  Status MakeColumnBuilders() {
    for (int32_t col_index = 0; col_index < num_csv_cols_; ++col_index) {
      const auto& col_name = column_names_[col_index];

      ARROW_ASSIGN_OR_RAISE(auto builder, MakeCSVColumnBuilder(col_name, col_index));
      column_builders_.push_back(builder);
      builder_names_.push_back(col_name);
    }
    return Status::OK();
  }

  // Make column builders, assuming inclusion of columns in `include_columns` order
  Status MakeColumnBuilders(const std::vector<std::string>& include_columns) {
    // Compute indices of columns in the CSV file
    std::unordered_map<std::string, int32_t> col_indices;
    col_indices.reserve(column_names_.size());
    for (int32_t i = 0; i < static_cast<int32_t>(column_names_.size()); ++i) {
      col_indices.emplace(column_names_[i], i);
    }

    // For each column name in include_columns, build the corresponding ColumnBuilder
    for (const auto& col_name : include_columns) {
      std::shared_ptr<ColumnBuilder> builder;
      auto it = col_indices.find(col_name);
      if (it != col_indices.end()) {
        auto col_index = it->second;
        ARROW_ASSIGN_OR_RAISE(builder, MakeCSVColumnBuilder(col_name, col_index));
      } else {
        // Column not in the CSV file
        if (convert_options_.include_missing_columns) {
          ARROW_ASSIGN_OR_RAISE(builder, MakeNullColumnBuilder(col_name));
        } else {
          return Status::KeyError("Column '", col_name,
                                  "' in include_columns "
                                  "does not exist in CSV file");
        }
      }
      column_builders_.push_back(builder);
      builder_names_.push_back(col_name);
    }
    return Status::OK();
  }

  // Make a column builder for the given CSV column name and index
  Result<std::shared_ptr<ColumnBuilder>> MakeCSVColumnBuilder(const std::string& col_name,
                                                              int32_t col_index) {
    // Does the named column have a fixed type?
    auto it = convert_options_.column_types.find(col_name);
    if (it == convert_options_.column_types.end()) {
      return ColumnBuilder::Make(pool_, col_index, convert_options_, task_group_);
    } else {
      return ColumnBuilder::Make(pool_, it->second, col_index, convert_options_,
                                 task_group_);
    }
  }

  // Make a column builder for a column of nulls
  Result<std::shared_ptr<ColumnBuilder>> MakeNullColumnBuilder(
      const std::string& col_name) {
    std::shared_ptr<DataType> type;
    // If the named column have a fixed type, use it, otherwise use null()
    auto it = convert_options_.column_types.find(col_name);
    if (it != convert_options_.column_types.end()) {
      type = it->second;
    } else {
      type = null();
    }
    return ColumnBuilder::MakeNull(pool_, type, task_group_);
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

  Status ParseAndInsert(const std::shared_ptr<Buffer>& partial,
                        const std::shared_ptr<Buffer>& completion,
                        const std::shared_ptr<Buffer>& block, int64_t block_index,
                        bool is_final, uint32_t* out_parsed_size = nullptr) {
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
        RETURN_NOT_OK(ConcatenateBuffers({partial, completion}, pool_, &straddling));
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
    if (out_parsed_size) {
      *out_parsed_size = parsed_size;
    }
    return ProcessData(parser, block_index);
  }

  // Trigger conversion of parsed block data
  Status ProcessData(const std::shared_ptr<BlockParser>& parser, int64_t block_index) {
    for (auto& builder : column_builders_) {
      builder->Insert(block_index, parser);
    }
    return Status::OK();
  }

  Result<std::shared_ptr<Table>> MakeTable() {
    DCHECK_EQ(column_builders_.size(), builder_names_.size());

    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<ChunkedArray>> columns;

    for (int32_t i = 0; i < static_cast<int32_t>(builder_names_.size()); ++i) {
      ARROW_ASSIGN_OR_RAISE(auto array, column_builders_[i]->Finish());
      fields.push_back(::arrow::field(builder_names_[i], array->type()));
      columns.emplace_back(std::move(array));
    }
    return Table::Make(schema(fields), columns);
  }

  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ConvertOptions convert_options_;

  // Number of columns in the CSV file
  int32_t num_csv_cols_ = -1;
  // Column names in the CSV file
  std::vector<std::string> column_names_;
  // Column builders for target Table (not necessarily in CSV file order)
  std::vector<std::shared_ptr<ColumnBuilder>> column_builders_;
  // Names of columns, in same order as column_builders_
  std::vector<std::string> builder_names_;

  std::shared_ptr<io::InputStream> input_;
  Iterator<std::shared_ptr<Buffer>> block_iterator_;
  std::shared_ptr<internal::TaskGroup> task_group_;

  // Whether there was a trailing CR at the end of last parsed line
  bool trailing_cr_ = false;
};

/////////////////////////////////////////////////////////////////////////
// Serial TableReader implementation

class SerialTableReader : public BaseTableReader {
 public:
  using BaseTableReader::BaseTableReader;

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(block_iterator_,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    // Since we're converting serially, no need to readahead more than one block
    int32_t block_queue_size = 1;
    return MakeReadaheadIterator(std::move(block_iterator_), block_queue_size)
        .Value(&block_iterator_);
  }

  Result<std::shared_ptr<Table>> Read() override {
    task_group_ = internal::TaskGroup::MakeSerial();

    // First block
    std::shared_ptr<Buffer> block;
    RETURN_NOT_OK(ReadFirstBlock(&block));
    if (!block) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(block, &block));

    auto chunker = MakeChunker(parse_options_);
    auto empty = std::make_shared<Buffer>("");
    auto partial = empty;
    int64_t block_index = 0;

    while (block) {
      std::shared_ptr<Buffer> next_block, completion;

      ARROW_ASSIGN_OR_RAISE(next_block, block_iterator_.Next());
      bool is_final = (next_block == nullptr);

      if (is_final) {
        // End of file reached => compute completion from penultimate block
        RETURN_NOT_OK(chunker->ProcessFinal(partial, block, &completion, &block));
      } else {
        // Get completion of partial from previous block.
        RETURN_NOT_OK(chunker->ProcessWithPartial(partial, block, &completion, &block));
      }

      uint32_t parsed_size;
      RETURN_NOT_OK(ParseAndInsert(partial, completion, block, block_index, is_final,
                                   &parsed_size));
      ++block_index;

      auto offset =
          static_cast<int64_t>(parsed_size) - partial->size() - completion->size();
      DCHECK_GE(offset, 0);  // Ensured by chunker
      partial = SliceBuffer(block, offset);
      block = next_block;
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
    ARROW_ASSIGN_OR_RAISE(block_iterator_,
                          io::MakeInputStreamIterator(input_, read_options_.block_size));

    int32_t block_queue_size = thread_pool_->GetCapacity();
    return MakeReadaheadIterator(std::move(block_iterator_), block_queue_size)
        .Value(&block_iterator_);
  }

  Result<std::shared_ptr<Table>> Read() override {
    task_group_ = internal::TaskGroup::MakeThreaded(thread_pool_);

    // Read first block and process header serially
    std::shared_ptr<Buffer> block;
    RETURN_NOT_OK(ReadFirstBlock(&block));
    if (!block) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader(block, &block));

    auto chunker = MakeChunker(parse_options_);
    auto empty = std::make_shared<Buffer>("");
    auto partial = empty;
    int64_t block_index = 0;

    while (block) {
      std::shared_ptr<Buffer> next_block, whole, completion, next_partial;

      ARROW_ASSIGN_OR_RAISE(next_block, block_iterator_.Next());
      bool is_final = (next_block == nullptr);

      if (is_final) {
        // End of file reached => compute completion from penultimate block
        RETURN_NOT_OK(chunker->ProcessFinal(partial, block, &completion, &whole));
      } else {
        std::shared_ptr<Buffer> starts_with_whole;
        // Get completion of partial from previous block.
        RETURN_NOT_OK(
            chunker->ProcessWithPartial(partial, block, &completion, &starts_with_whole));

        // Get a complete CSV block inside `partial + block`, and keep
        // the rest for the next iteration.
        RETURN_NOT_OK(chunker->Process(starts_with_whole, &whole, &next_partial));
      }

      // Launch parse task
      task_group_->Append([this, partial, completion, whole, block_index, is_final] {
        return ParseAndInsert(partial, completion, whole, block_index, is_final);
      });
      block_index++;

      partial = next_partial;
      block = next_block;
    }

    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());
    return MakeTable();
  }

 protected:
  ThreadPool* thread_pool_;
};

/////////////////////////////////////////////////////////////////////////
// TableReader factory function

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

/////////////////////////////////////////////////////////////////////////
// Deprecated API(s)

Status TableReader::Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                         const ReadOptions& read_options,
                         const ParseOptions& parse_options,
                         const ConvertOptions& convert_options,
                         std::shared_ptr<TableReader>* out) {
  return Make(pool, std::move(input), read_options, parse_options, convert_options)
      .Value(out);
}

Status TableReader::Read(std::shared_ptr<Table>* out) { return Read().Value(out); }

}  // namespace csv
}  // namespace arrow

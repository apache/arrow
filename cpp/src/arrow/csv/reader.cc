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
#include "arrow/csv/column-builder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/io/readahead.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/task-group.h"
#include "arrow/util/thread-pool.h"

namespace arrow {

class MemoryPool;

namespace io {

class InputStream;

}  // namespace io

namespace csv {

using internal::GetCpuThreadPool;
using internal::ThreadPool;
using io::internal::ReadaheadBuffer;
using io::internal::ReadaheadSpooler;

static constexpr int64_t kDefaultLeftPadding = 2048;  // 2 kB
static constexpr int64_t kDefaultRightPadding = 16;

/////////////////////////////////////////////////////////////////////////
// Base class for common functionality

class BaseTableReader : public csv::TableReader {
 public:
  BaseTableReader(MemoryPool* pool, const ReadOptions& read_options,
                  const ParseOptions& parse_options,
                  const ConvertOptions& convert_options)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        convert_options_(convert_options) {}

 protected:
  // Read a next data block, stitch it to trailing data
  Status ReadNextBlock() {
    bool trailing_data = cur_size_ > 0;
    ReadaheadBuffer rh;

    if (trailing_data) {
      if (readahead_->GetLeftPadding() < cur_size_) {
        // Growth heuristic to try and ensure sufficient left padding
        // in subsequent reads
        readahead_->SetLeftPadding(cur_size_ * 3 / 2);
      }
    }

    RETURN_NOT_OK(readahead_->Read(&rh));
    if (!rh.buffer) {
      // EOF, let caller finish with existing data
      eof_ = true;
      return Status::OK();
    }

    std::shared_ptr<Buffer> new_block = rh.buffer;
    uint8_t* new_data = rh.buffer->mutable_data() + rh.left_padding;
    int64_t new_size = rh.buffer->size() - rh.left_padding - rh.right_padding;
    DCHECK_GT(new_size, 0);  // ensured by ReadaheadSpooler

    if (trailing_cr_ && new_data[0] == '\n') {
      // Skip '\r\n' line separator that started at the end of previous block
      ++new_data;
      --new_size;
    }
    trailing_cr_ = (new_data[new_size - 1] == '\r');

    if (trailing_data) {
      // Try to copy trailing data at the beginning of new block
      if (cur_size_ <= rh.left_padding) {
        // Can left-extend new block inside padding area
        new_data -= cur_size_;
        new_size += cur_size_;
        std::memcpy(new_data, cur_data_, cur_size_);
      } else {
        // Need to allocate bigger block and concatenate trailing + present data
        RETURN_NOT_OK(
            AllocateBuffer(pool_, cur_size_ + new_size + rh.right_padding, &new_block));
        std::memcpy(new_block->mutable_data(), cur_data_, cur_size_);
        std::memcpy(new_block->mutable_data() + cur_size_, new_data, new_size);
        std::memset(new_block->mutable_data() + cur_size_ + new_size, 0,
                    rh.right_padding);
        new_data = new_block->mutable_data();
        new_size = cur_size_ + new_size;
      }
    }
    cur_block_ = new_block;
    cur_data_ = new_data;
    cur_size_ = new_size;
    return Status::OK();
  }

  // Read header and column names from current block, create column builders
  Status ProcessHeader() {
    DCHECK_GT(cur_size_, 0);
    if (parse_options_.header_rows == 0) {
      // TODO allow passing names and/or generate column numbers?
      return Status::Invalid("header_rows == 0 needs explicit column names");
    }

    BlockParser parser(pool_, parse_options_, num_cols_, parse_options_.header_rows);

    uint32_t parsed_size = 0;
    RETURN_NOT_OK(parser.Parse(reinterpret_cast<const char*>(cur_data_),
                               static_cast<uint32_t>(cur_size_), &parsed_size));
    if (parser.num_rows() != parse_options_.header_rows) {
      return Status::Invalid(
          "Could not read header rows from CSV file, either "
          "file is too short or header is larger than block size");
    }
    if (parser.num_cols() == 0) {
      return Status::Invalid("No columns in CSV file");
    }
    num_cols_ = parser.num_cols();
    DCHECK_GT(num_cols_, 0);

    for (int32_t col_index = 0; col_index < num_cols_; ++col_index) {
      auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
        DCHECK_EQ(column_names_.size(), static_cast<uint32_t>(col_index));
        column_names_.emplace_back(reinterpret_cast<const char*>(data), size);
        return Status::OK();
      };
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
      std::shared_ptr<ColumnBuilder> builder;
      // Does the named column have a fixed type?
      auto it = convert_options_.column_types.find(column_names_[col_index]);
      if (it == convert_options_.column_types.end()) {
        RETURN_NOT_OK(
            ColumnBuilder::Make(col_index, convert_options_, task_group_, &builder));
      } else {
        RETURN_NOT_OK(ColumnBuilder::Make(it->second, col_index, convert_options_,
                                          task_group_, &builder));
      }
      column_builders_.push_back(builder);
    }

    // Skip parsed header rows
    cur_data_ += parsed_size;
    cur_size_ -= parsed_size;
    return Status::OK();
  }

  // Trigger conversion of parsed block data
  Status ProcessData(const std::shared_ptr<BlockParser>& parser, int64_t block_index) {
    for (auto& builder : column_builders_) {
      builder->Insert(block_index, parser);
    }
    return Status::OK();
  }

  Status MakeTable(std::shared_ptr<Table>* out) {
    DCHECK_GT(num_cols_, 0);
    DCHECK_EQ(column_names_.size(), static_cast<uint32_t>(num_cols_));
    DCHECK_EQ(column_builders_.size(), static_cast<uint32_t>(num_cols_));

    std::vector<std::shared_ptr<Field>> fields;
    std::vector<std::shared_ptr<Column>> columns;

    for (int32_t i = 0; i < num_cols_; ++i) {
      std::shared_ptr<ChunkedArray> array;
      RETURN_NOT_OK(column_builders_[i]->Finish(&array));
      columns.push_back(std::make_shared<Column>(column_names_[i], array));
      fields.push_back(columns.back()->field());
    }
    *out = Table::Make(schema(fields), columns);
    return Status::OK();
  }

  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  ConvertOptions convert_options_;

  int32_t num_cols_ = -1;
  std::shared_ptr<ReadaheadSpooler> readahead_;
  // Column names
  std::vector<std::string> column_names_;
  std::shared_ptr<internal::TaskGroup> task_group_;
  std::vector<std::shared_ptr<ColumnBuilder>> column_builders_;

  // Current block and data pointer
  std::shared_ptr<Buffer> cur_block_;
  const uint8_t* cur_data_ = nullptr;
  int64_t cur_size_ = 0;
  // Index of current block inside data stream
  int64_t cur_block_index_ = 0;
  // Whether there was a trailing CR at the end of last parsed line
  bool trailing_cr_ = false;
  // Whether we reached input stream EOF.  There may still be data left to
  // process in current block.
  bool eof_ = false;
};

/////////////////////////////////////////////////////////////////////////
// Serial TableReader implementation

class SerialTableReader : public BaseTableReader {
 public:
  SerialTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                    const ReadOptions& read_options, const ParseOptions& parse_options,
                    const ConvertOptions& convert_options)
      : BaseTableReader(pool, read_options, parse_options, convert_options) {
    // Since we're converting serially, no need to readahead more than one block
    int32_t block_queue_size = 1;
    readahead_ = std::make_shared<ReadaheadSpooler>(
        pool_, input, read_options_.block_size, block_queue_size, kDefaultLeftPadding,
        kDefaultRightPadding);
  }

  Status Read(std::shared_ptr<Table>* out) {
    task_group_ = internal::TaskGroup::MakeSerial();

    // First block
    RETURN_NOT_OK(ReadNextBlock());
    if (eof_) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader());

    static constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    auto parser =
        std::make_shared<BlockParser>(pool_, parse_options_, num_cols_, max_num_rows);
    while (!eof_) {
      // Consume current block
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->Parse(reinterpret_cast<const char*>(cur_data_),
                                  static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        // Got some data
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
        cur_data_ += parsed_size;
        cur_size_ -= parsed_size;
        if (!task_group_->ok()) {
          // Conversion error => early exit
          break;
        }
      } else {
        // Need to fetch more data to get at least one row
        RETURN_NOT_OK(ReadNextBlock());
      }
    }
    if (eof_ && cur_size_ > 0) {
      // Parse remaining data
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->ParseFinal(reinterpret_cast<const char*>(cur_data_),
                                       static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
      }
    }

    // Finish conversion, create schema and table
    RETURN_NOT_OK(task_group_->Finish());
    return MakeTable(out);
  }
};

/////////////////////////////////////////////////////////////////////////
// Parallel TableReader implementation

class ThreadedTableReader : public BaseTableReader {
 public:
  ThreadedTableReader(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                      ThreadPool* thread_pool, const ReadOptions& read_options,
                      const ParseOptions& parse_options,
                      const ConvertOptions& convert_options)
      : BaseTableReader(pool, read_options, parse_options, convert_options),
        thread_pool_(thread_pool) {
    // Readahead one block per worker thread
    int32_t block_queue_size = thread_pool->GetCapacity();
    readahead_ = std::make_shared<ReadaheadSpooler>(
        pool_, input, read_options_.block_size, block_queue_size, kDefaultLeftPadding,
        kDefaultRightPadding);
  }

  ~ThreadedTableReader() {
    if (task_group_) {
      // In case of error, make sure all pending tasks are finished before
      // we start destroying BaseTableReader members
      ARROW_UNUSED(task_group_->Finish());
    }
  }

  Status Read(std::shared_ptr<Table>* out) {
    task_group_ = internal::TaskGroup::MakeThreaded(thread_pool_);
    static constexpr int32_t max_num_rows = std::numeric_limits<int32_t>::max();
    Chunker chunker(parse_options_);

    // Get first block and process header serially
    RETURN_NOT_OK(ReadNextBlock());
    if (eof_) {
      return Status::Invalid("Empty CSV file");
    }
    RETURN_NOT_OK(ProcessHeader());

    while (!eof_ && task_group_->ok()) {
      // Consume current chunk
      uint32_t chunk_size = 0;
      RETURN_NOT_OK(chunker.Process(reinterpret_cast<const char*>(cur_data_),
                                    static_cast<uint32_t>(cur_size_), &chunk_size));
      if (chunk_size > 0) {
        // Got a chunk of rows
        const uint8_t* chunk_data = cur_data_;
        std::shared_ptr<Buffer> chunk_buffer = cur_block_;
        int64_t chunk_index = cur_block_index_;

        // "mutable" allows to modify captured by-copy chunk_buffer
        task_group_->Append([=]() mutable -> Status {
          auto parser = std::make_shared<BlockParser>(pool_, parse_options_, num_cols_,
                                                      max_num_rows);
          uint32_t parsed_size = 0;
          RETURN_NOT_OK(parser->Parse(reinterpret_cast<const char*>(chunk_data),
                                      chunk_size, &parsed_size));
          if (parsed_size != chunk_size) {
            DCHECK_EQ(parsed_size, chunk_size);
            std::stringstream ss;
            ss << "Chunker and parser disagree on block size: " << chunk_size << " vs "
               << parsed_size;
            return Status::Invalid(ss.str());
          }
          RETURN_NOT_OK(ProcessData(parser, chunk_index));
          // Keep chunk buffer alive within closure and release it at the end
          chunk_buffer.reset();
          return Status::OK();
        });
        cur_data_ += chunk_size;
        cur_size_ -= chunk_size;
        cur_block_index_++;
      } else {
        // Need to fetch more data to get at least one row
        RETURN_NOT_OK(ReadNextBlock());
      }
    }

    // Finish all pending parallel tasks
    RETURN_NOT_OK(task_group_->Finish());

    if (eof_ && cur_size_ > 0) {
      // Parse remaining data (serial)
      task_group_ = internal::TaskGroup::MakeSerial();
      for (auto& builder : column_builders_) {
        builder->SetTaskGroup(task_group_);
      }
      auto parser =
          std::make_shared<BlockParser>(pool_, parse_options_, num_cols_, max_num_rows);
      uint32_t parsed_size = 0;
      RETURN_NOT_OK(parser->ParseFinal(reinterpret_cast<const char*>(cur_data_),
                                       static_cast<uint32_t>(cur_size_), &parsed_size));
      if (parser->num_rows() > 0) {
        RETURN_NOT_OK(ProcessData(parser, cur_block_index_++));
      }
      RETURN_NOT_OK(task_group_->Finish());
    }

    // Create schema and table
    return MakeTable(out);
  }

 protected:
  ThreadPool* thread_pool_;
};

/////////////////////////////////////////////////////////////////////////
// TableReader factory function

Status TableReader::Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                         const ReadOptions& read_options,
                         const ParseOptions& parse_options,
                         const ConvertOptions& convert_options,
                         std::shared_ptr<TableReader>* out) {
  std::shared_ptr<TableReader> result;
  if (read_options.use_threads) {
    result = std::make_shared<ThreadedTableReader>(
        pool, input, GetCpuThreadPool(), read_options, parse_options, convert_options);
    *out = result;
    return Status::OK();
  } else {
    result = std::make_shared<SerialTableReader>(pool, input, read_options, parse_options,
                                                 convert_options);
    *out = result;
    return Status::OK();
  }
}

}  // namespace csv
}  // namespace arrow

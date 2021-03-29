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

#include "arrow/json/reader.h"

#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/json/chunked_builder.h"
#include "arrow/json/chunker.h"
#include "arrow/json/converter.h"
#include "arrow/json/parser.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using util::string_view;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;
using internal::ThreadPool;

namespace json {

class TableReaderImpl : public TableReader,
                        public std::enable_shared_from_this<TableReaderImpl> {
 public:
  TableReaderImpl(MemoryPool* pool, const ReadOptions& read_options,
                  const ParseOptions& parse_options,
                  std::shared_ptr<TaskGroup> task_group)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        chunker_(MakeChunker(parse_options_)),
        task_group_(std::move(task_group)) {}

  Status Init(std::shared_ptr<io::InputStream> input) {
    ARROW_ASSIGN_OR_RAISE(auto it,
                          io::MakeInputStreamIterator(input, read_options_.block_size));
    return MakeReadaheadIterator(std::move(it), task_group_->parallelism())
        .Value(&block_iterator_);
  }

  Result<std::shared_ptr<Table>> Read() override {
    RETURN_NOT_OK(MakeBuilder());

    ARROW_ASSIGN_OR_RAISE(auto block, block_iterator_.Next());
    if (block == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    auto self = shared_from_this();
    auto empty = std::make_shared<Buffer>("");

    int64_t block_index = 0;
    std::shared_ptr<Buffer> partial = empty;

    while (block != nullptr) {
      std::shared_ptr<Buffer> next_block, whole, completion, next_partial;

      ARROW_ASSIGN_OR_RAISE(next_block, block_iterator_.Next());

      if (next_block == nullptr) {
        // End of file reached => compute completion from penultimate block
        RETURN_NOT_OK(chunker_->ProcessFinal(partial, block, &completion, &whole));
      } else {
        std::shared_ptr<Buffer> starts_with_whole;
        // Get completion of partial from previous block.
        RETURN_NOT_OK(chunker_->ProcessWithPartial(partial, block, &completion,
                                                   &starts_with_whole));

        // Get all whole objects entirely inside the current buffer
        RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &next_partial));
      }

      // Launch parse task
      task_group_->Append([self, partial, completion, whole, block_index] {
        return self->ParseAndInsert(partial, completion, whole, block_index);
      });
      block_index++;

      partial = next_partial;
      block = next_block;
    }

    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(builder_->Finish(&array));
    return Table::FromChunkedStructArray(array);
  }

 private:
  Status MakeBuilder() {
    auto type = parse_options_.explicit_schema
                    ? struct_(parse_options_.explicit_schema->fields())
                    : struct_({});

    auto promotion_graph =
        parse_options_.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
            ? GetPromotionGraph()
            : nullptr;

    return MakeChunkedArrayBuilder(task_group_, pool_, promotion_graph, type, &builder_);
  }

  Status ParseAndInsert(const std::shared_ptr<Buffer>& partial,
                        const std::shared_ptr<Buffer>& completion,
                        const std::shared_ptr<Buffer>& whole, int64_t block_index) {
    std::unique_ptr<BlockParser> parser;
    RETURN_NOT_OK(BlockParser::Make(pool_, parse_options_, &parser));
    RETURN_NOT_OK(parser->ReserveScalarStorage(partial->size() + completion->size() +
                                               whole->size()));

    if (partial->size() != 0 || completion->size() != 0) {
      std::shared_ptr<Buffer> straddling;
      if (partial->size() == 0) {
        straddling = completion;
      } else if (completion->size() == 0) {
        straddling = partial;
      } else {
        ARROW_ASSIGN_OR_RAISE(straddling,
                              ConcatenateBuffers({partial, completion}, pool_));
      }
      RETURN_NOT_OK(parser->Parse(straddling));
    }

    if (whole->size() != 0) {
      RETURN_NOT_OK(parser->Parse(whole));
    }

    std::shared_ptr<Array> parsed;
    RETURN_NOT_OK(parser->Finish(&parsed));
    builder_->Insert(block_index, field("", parsed->type()), parsed);
    return Status::OK();
  }

  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<TaskGroup> task_group_;
  Iterator<std::shared_ptr<Buffer>> block_iterator_;
  std::shared_ptr<ChunkedArrayBuilder> builder_;
};

Status TableReader::Read(std::shared_ptr<Table>* out) { return Read().Value(out); }

Result<std::shared_ptr<TableReader>> TableReader::Make(
    MemoryPool* pool, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options) {
  std::shared_ptr<TableReaderImpl> ptr;
  if (read_options.use_threads) {
    ptr = std::make_shared<TableReaderImpl>(pool, read_options, parse_options,
                                            TaskGroup::MakeThreaded(GetCpuThreadPool()));
  } else {
    ptr = std::make_shared<TableReaderImpl>(pool, read_options, parse_options,
                                            TaskGroup::MakeSerial());
  }
  RETURN_NOT_OK(ptr->Init(input));
  return ptr;
}

Status TableReader::Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                         const ReadOptions& read_options,
                         const ParseOptions& parse_options,
                         std::shared_ptr<TableReader>* out) {
  return TableReader::Make(pool, input, read_options, parse_options).Value(out);
}

Result<std::shared_ptr<RecordBatch>> ParseOne(ParseOptions options,
                                              std::shared_ptr<Buffer> json) {
  std::unique_ptr<BlockParser> parser;
  RETURN_NOT_OK(BlockParser::Make(options, &parser));
  RETURN_NOT_OK(parser->Parse(json));
  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser->Finish(&parsed));

  auto type =
      options.explicit_schema ? struct_(options.explicit_schema->fields()) : struct_({});
  auto promotion_graph =
      options.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
          ? GetPromotionGraph()
          : nullptr;
  std::shared_ptr<ChunkedArrayBuilder> builder;
  RETURN_NOT_OK(MakeChunkedArrayBuilder(TaskGroup::MakeSerial(), default_memory_pool(),
                                        promotion_graph, type, &builder));

  builder->Insert(0, field("", type), parsed);
  std::shared_ptr<ChunkedArray> converted_chunked;
  RETURN_NOT_OK(builder->Finish(&converted_chunked));
  const auto& converted = checked_cast<const StructArray&>(*converted_chunked->chunk(0));

  std::vector<std::shared_ptr<Array>> columns(converted.num_fields());
  for (int i = 0; i < converted.num_fields(); ++i) {
    columns[i] = converted.field(i);
  }
  return RecordBatch::Make(schema(converted.type()->fields()), converted.length(),
                           std::move(columns));
}

}  // namespace json
}  // namespace arrow

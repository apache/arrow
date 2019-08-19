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
#include "arrow/io/readahead.h"
#include "arrow/json/chunked_builder.h"
#include "arrow/json/chunker.h"
#include "arrow/json/converter.h"
#include "arrow/json/parser.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using util::string_view;

using internal::GetCpuThreadPool;
using internal::TaskGroup;
using internal::ThreadPool;

using io::internal::ReadaheadBuffer;
using io::internal::ReadaheadSpooler;

namespace json {

class TableReaderImpl : public TableReader {
 public:
  TableReaderImpl(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                  const ReadOptions& read_options, const ParseOptions& parse_options,
                  std::shared_ptr<TaskGroup> task_group)
      : pool_(pool),
        read_options_(read_options),
        parse_options_(parse_options),
        chunker_(Chunker::Make(parse_options_)),
        task_group_(std::move(task_group)),
        readahead_(pool_, std::move(input), read_options_.block_size,
                   task_group_->parallelism()) {}

  Status Read(std::shared_ptr<Table>* out) override {
    RETURN_NOT_OK(MakeBuilder());

    ReadaheadBuffer rh;
    RETURN_NOT_OK(readahead_.Read(&rh));
    if (rh.buffer == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    auto empty = std::make_shared<Buffer>("");

    int64_t block_index = 0;
    for (std::shared_ptr<Buffer> partial = empty, completion = empty,
                                 starts_with_whole = rh.buffer;
         rh.buffer; ++block_index) {
      // get completion of partial from previous block
      RETURN_NOT_OK(chunker_->ProcessWithPartial(partial, rh.buffer, &completion,
                                                 &starts_with_whole));

      // get all whole objects entirely inside the current buffer
      std::shared_ptr<Buffer> whole, next_partial;
      RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &next_partial));

      // launch parse task
      task_group_->Append([this, partial, completion, whole, block_index] {
        return ParseAndInsert(partial, completion, whole, block_index);
      });

      RETURN_NOT_OK(readahead_.Read(&rh));
      if (rh.buffer == nullptr) {
        DCHECK_EQ(string_view(*next_partial).find_first_not_of(" \t\n\r"),
                  string_view::npos);
      }
      partial = next_partial;
    }

    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(builder_->Finish(&array));
    return Table::FromChunkedStructArray(array, out);
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

    if (completion->size() != 0) {
      std::shared_ptr<Buffer> straddling;
      RETURN_NOT_OK(ConcatenateBuffers({partial, completion}, pool_, &straddling));
      RETURN_NOT_OK(parser->Parse(straddling));
    }

    RETURN_NOT_OK(parser->Parse(whole));

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
  ReadaheadSpooler readahead_;
  std::unique_ptr<ChunkedArrayBuilder> builder_;
};

Status TableReader::Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                         const ReadOptions& read_options,
                         const ParseOptions& parse_options,
                         std::shared_ptr<TableReader>* out) {
  if (read_options.use_threads) {
    *out = std::make_shared<TableReaderImpl>(pool, input, read_options, parse_options,
                                             TaskGroup::MakeThreaded(GetCpuThreadPool()));
  } else {
    *out = std::make_shared<TableReaderImpl>(pool, input, read_options, parse_options,
                                             TaskGroup::MakeSerial());
  }
  return Status::OK();
}

Status ParseOne(ParseOptions options, std::shared_ptr<Buffer> json,
                std::shared_ptr<RecordBatch>* out) {
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
  std::unique_ptr<ChunkedArrayBuilder> builder;
  RETURN_NOT_OK(MakeChunkedArrayBuilder(internal::TaskGroup::MakeSerial(),
                                        default_memory_pool(), promotion_graph, type,
                                        &builder));

  builder->Insert(0, field("", type), parsed);
  std::shared_ptr<ChunkedArray> converted_chunked;
  RETURN_NOT_OK(builder->Finish(&converted_chunked));
  auto converted = static_cast<const StructArray*>(converted_chunked->chunk(0).get());

  std::vector<std::shared_ptr<Array>> columns(converted->num_fields());
  for (int i = 0; i < converted->num_fields(); ++i) {
    columns[i] = converted->field(i);
  }
  *out = RecordBatch::Make(schema(converted->type()->children()), converted->length(),
                           std::move(columns));
  return Status::OK();
}

}  // namespace json
}  // namespace arrow

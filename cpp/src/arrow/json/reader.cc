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

#include <string_view>
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
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {

using std::string_view;

using internal::checked_cast;
using internal::Executor;
using internal::GetCpuThreadPool;
using internal::TaskGroup;
using internal::ThreadPool;

namespace json {
namespace {

struct JSONBlock {
  std::shared_ptr<Buffer> partial;
  std::shared_ptr<Buffer> completion;
  std::shared_ptr<Buffer> whole;
  int64_t index = -1;
};

struct DecodedBlock {
  std::shared_ptr<RecordBatch> record_batch;
  int64_t num_bytes = 0;
  int64_t index = -1;
};

Result<std::shared_ptr<RecordBatch>> ToRecordBatch(const StructArray& converted) {
  std::vector<std::shared_ptr<Array>> columns;
  columns.reserve(converted.num_fields());
  for (const auto& f : converted.fields()) columns.push_back(f);
  return RecordBatch::Make(schema(converted.type()->fields()), converted.length(),
                           std::move(columns));
}

auto ToRecordBatch(const Array& converted) {
  return ToRecordBatch(checked_cast<const StructArray&>(converted));
}

Result<std::shared_ptr<Array>> ParseBlock(const JSONBlock& block,
                                          const ParseOptions& parse_options,
                                          MemoryPool* pool, int64_t* out_size = nullptr) {
  std::unique_ptr<BlockParser> parser;
  RETURN_NOT_OK(BlockParser::Make(pool, parse_options, &parser));

  int64_t size = block.partial->size() + block.completion->size() + block.whole->size();
  RETURN_NOT_OK(parser->ReserveScalarStorage(size));

  if (block.partial->size() || block.completion->size()) {
    std::shared_ptr<Buffer> straddling;
    if (!block.completion->size()) {
      straddling = block.partial;
    } else if (!block.partial->size()) {
      straddling = block.completion;
    } else {
      ARROW_ASSIGN_OR_RAISE(straddling,
                            ConcatenateBuffers({block.partial, block.completion}, pool));
    }
    RETURN_NOT_OK(parser->Parse(straddling));
  }
  if (block.whole->size()) {
    RETURN_NOT_OK(parser->Parse(block.whole));
  }

  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser->Finish(&parsed));

  if (out_size) *out_size = size;

  return parsed;
}

// Utility for incrementally generating chunked JSON blocks from source buffers
//
// Note: Retains state from prior calls
class BlockReader {
 public:
  BlockReader(std::unique_ptr<Chunker> chunker, std::shared_ptr<Buffer> first_buffer)
      : chunker_(std::move(chunker)),
        partial_(std::make_shared<Buffer>("")),
        buffer_(std::move(first_buffer)) {}

  // Compose an iterator from a source iterator
  template <typename... Args>
  static Iterator<JSONBlock> MakeIterator(Iterator<std::shared_ptr<Buffer>> buf_it,
                                          Args&&... args) {
    auto reader = std::make_shared<BlockReader>(std::forward<Args>(args)...);
    Transformer<std::shared_ptr<Buffer>, JSONBlock> transformer =
        [reader](std::shared_ptr<Buffer> next_buffer) {
          return (*reader)(std::move(next_buffer));
        };
    return MakeTransformedIterator(std::move(buf_it), transformer);
  }

  // Compose a callable generator from a source generator
  template <typename... Args>
  static AsyncGenerator<JSONBlock> MakeGenerator(
      AsyncGenerator<std::shared_ptr<Buffer>> buf_gen, Args&&... args) {
    auto reader = std::make_shared<BlockReader>(std::forward<Args>(args)...);
    Transformer<std::shared_ptr<Buffer>, JSONBlock> transformer =
        [reader](std::shared_ptr<Buffer> next_buffer) {
          return (*reader)(std::move(next_buffer));
        };
    return MakeTransformedGenerator(std::move(buf_gen), transformer);
  }

  Result<TransformFlow<JSONBlock>> operator()(std::shared_ptr<Buffer> next_buffer) {
    if (!buffer_) return TransformFinish();

    std::shared_ptr<Buffer> whole, completion, next_partial;
    if (!next_buffer) {
      // End of file reached => compute completion from penultimate block
      RETURN_NOT_OK(chunker_->ProcessFinal(partial_, buffer_, &completion, &whole));
    } else {
      std::shared_ptr<Buffer> starts_with_whole;
      // Get completion of partial from previous block.
      RETURN_NOT_OK(chunker_->ProcessWithPartial(partial_, buffer_, &completion,
                                                 &starts_with_whole));
      // Get all whole objects entirely inside the current buffer
      RETURN_NOT_OK(chunker_->Process(starts_with_whole, &whole, &next_partial));
    }

    buffer_ = std::move(next_buffer);
    return TransformYield(JSONBlock{std::exchange(partial_, next_partial),
                                    std::move(completion), std::move(whole), index_++});
  }

 private:
  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<Buffer> partial_;
  std::shared_ptr<Buffer> buffer_;
  int64_t index_ = 0;
};

}  // namespace
}  // namespace json

template <>
struct IterationTraits<json::JSONBlock> {
  static json::JSONBlock End() { return json::JSONBlock{}; }
  static bool IsEnd(const json::JSONBlock& val) { return val.index < 0; }
};

template <>
struct IterationTraits<json::DecodedBlock> {
  static json::DecodedBlock End() { return json::DecodedBlock{}; }
  static bool IsEnd(const json::DecodedBlock& val) { return val.index < 0; }
};

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
        task_group_(std::move(task_group)) {}

  Status Init(std::shared_ptr<io::InputStream> input) {
    ARROW_ASSIGN_OR_RAISE(auto it,
                          io::MakeInputStreamIterator(input, read_options_.block_size));
    return MakeReadaheadIterator(std::move(it), task_group_->parallelism())
        .Value(&buffer_iterator_);
  }

  Result<std::shared_ptr<Table>> Read() override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, buffer_iterator_.Next());
    if (buffer == nullptr) {
      return Status::Invalid("Empty JSON file");
    }

    RETURN_NOT_OK(MakeBuilder());

    auto block_it = BlockReader::MakeIterator(
        std::move(buffer_iterator_), MakeChunker(parse_options_), std::move(buffer));
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto block, block_it.Next());
      if (IsIterationEnd(block)) break;
      task_group_->Append(
          [self = shared_from_this(), block] { return self->ParseAndInsert(block); });
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

  Status ParseAndInsert(const JSONBlock& block) {
    ARROW_ASSIGN_OR_RAISE(auto parsed, ParseBlock(block, parse_options_, pool_));
    builder_->Insert(block.index, field("", parsed->type()), parsed);
    return Status::OK();
  }

  MemoryPool* pool_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  std::shared_ptr<TaskGroup> task_group_;
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;
  std::shared_ptr<ChunkedArrayBuilder> builder_;
};

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

  return ToRecordBatch(*converted_chunked->chunk(0));
}

namespace {

// Callable object for decoding a pre-chunked JSON block into a RecordBatch
class BlockDecoder {
 public:
  BlockDecoder(MemoryPool* pool, const ParseOptions& parse_options)
      : pool_(pool),
        parse_options_(parse_options),
        conversion_type_(parse_options_.explicit_schema
                             ? struct_(parse_options_.explicit_schema->fields())
                             : struct_({})),
        promotion_graph_(parse_options_.unexpected_field_behavior ==
                                 UnexpectedFieldBehavior::InferType
                             ? GetPromotionGraph()
                             : nullptr) {}

  Result<DecodedBlock> operator()(const JSONBlock& block) const {
    int64_t num_bytes;
    ARROW_ASSIGN_OR_RAISE(auto unconverted,
                          ParseBlock(block, parse_options_, pool_, &num_bytes));

    std::shared_ptr<ChunkedArrayBuilder> builder;
    RETURN_NOT_OK(MakeChunkedArrayBuilder(TaskGroup::MakeSerial(), pool_,
                                          promotion_graph_, conversion_type_, &builder));
    builder->Insert(0, field("", unconverted->type()), unconverted);

    std::shared_ptr<ChunkedArray> chunked;
    RETURN_NOT_OK(builder->Finish(&chunked));
    ARROW_ASSIGN_OR_RAISE(auto batch, ToRecordBatch(*chunked->chunk(0)));

    return DecodedBlock{std::move(batch), num_bytes, block.index};
  }

 private:
  MemoryPool* pool_;
  ParseOptions parse_options_;
  std::shared_ptr<DataType> conversion_type_;
  const PromotionGraph* promotion_graph_;
};

}  // namespace

class StreamingReaderImpl : public StreamingReader,
                            public std::enable_shared_from_this<StreamingReaderImpl> {
 public:
  StreamingReaderImpl(io::IOContext io_context, Executor* executor,
                      const ReadOptions& read_options, const ParseOptions& parse_options)
      : io_context_(std::move(io_context)),
        executor_(executor),
        read_options_(read_options),
        parse_options_(parse_options),
        bytes_processed_(std::make_shared<std::atomic<int64_t>>(0)) {}

  Future<> Init(std::shared_ptr<io::InputStream> input) {
    ARROW_ASSIGN_OR_RAISE(auto it,
                          io::MakeInputStreamIterator(input, read_options_.block_size));
    ARROW_ASSIGN_OR_RAISE(auto bg_it,
                          MakeBackgroundGenerator(std::move(it), io_context_.executor()));
    auto buf_gen = MakeTransferredGenerator(bg_it, executor_);
    // We pre-fetch the first buffer during instantiation to resolve the schema and ensure
    // the stream isn't empty
    return buf_gen().Then(
        [self = shared_from_this(), buf_gen](const std::shared_ptr<Buffer>& buffer) {
          return self->InitFromFirstBuffer(buffer, buf_gen);
        });
  }

  std::shared_ptr<Schema> schema() const override {
    return parse_options_.explicit_schema;
  }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    auto future = ReadNextAsync();
    auto result = future.result();
    return std::move(result).Value(out);
  }

  Future<std::shared_ptr<RecordBatch>> ReadNextAsync() override {
    return record_batch_gen_();
  }

  int64_t bytes_read() const override { return bytes_processed_->load(); }

 private:
  Future<> InitFromFirstBuffer(const std::shared_ptr<Buffer>& buffer,
                               AsyncGenerator<std::shared_ptr<Buffer>> buf_gen) {
    if (!buffer) return Status::Invalid("Empty JSON stream");

    // Generator for pre-chunked JSON data
    auto block_gen = BlockReader::MakeGenerator(std::move(buf_gen),
                                                MakeChunker(parse_options_), buffer);
    // Decoder for the first block using the initial parse options
    auto decoder = BlockDecoder(io_context_.pool(), parse_options_);

    return block_gen().Then(
        [self = shared_from_this(), block_gen, decoder](JSONBlock block) -> Future<> {
          // Skip any initial empty record batches so we can try to get a useful schema
          int64_t skipped_bytes = 0;
          ARROW_ASSIGN_OR_RAISE(auto decoded, decoder(block));
          while (!IsIterationEnd(decoded) && !decoded.record_batch->num_rows()) {
            skipped_bytes = decoded.num_bytes;
            auto fut = block_gen();
            ARROW_ASSIGN_OR_RAISE(block, fut.result());
            if (IsIterationEnd(block)) {
              decoded = IterationEnd<DecodedBlock>();
            } else {
              ARROW_ASSIGN_OR_RAISE(decoded, decoder(block));
              decoded.num_bytes += skipped_bytes;
            }
          }
          return self->InitFromFirstDecoded(decoded, block_gen);
        });
  }

  Future<> InitFromFirstDecoded(const DecodedBlock& decoded,
                                AsyncGenerator<JSONBlock> block_gen) {
    // End of stream and no non-empty batches were yielded, so just return empty ones
    if (IsIterationEnd(decoded)) {
      record_batch_gen_ = MakeEmptyGenerator<std::shared_ptr<RecordBatch>>();
      parse_options_.explicit_schema = nullptr;
      return Status::OK();
    }

    // Use the schema from the first batch as the basis for all future reads. If type
    // inference wasn't requested then this should be the same as the provided
    // explicit_schema. Otherwise, ignore unexpected fields for future batches to ensure
    // their schemas are consistent
    parse_options_.explicit_schema = decoded.record_batch->schema();
    if (parse_options_.unexpected_field_behavior == UnexpectedFieldBehavior::InferType) {
      parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
    }

    // The final decoder, which uses the resolved parse options for type deduction
    auto decoded_gen = MakeMappedGenerator(
        std::move(block_gen), BlockDecoder(io_context_.pool(), parse_options_));
    if (read_options_.use_threads) {
      decoded_gen =
          MakeReadaheadGenerator(std::move(decoded_gen), executor_->GetCapacity());
    }
    // Return the batch we just read on first invocation
    decoded_gen = MakeGeneratorStartsWith({decoded}, std::move(decoded_gen));

    // Compose the final generator
    record_batch_gen_ = MakeMappedGenerator(
        std::move(decoded_gen),
        [bytes_processed = bytes_processed_](const DecodedBlock& decoded) {
          bytes_processed->fetch_add(decoded.num_bytes);
          return decoded.record_batch;
        });
    record_batch_gen_ =
        MakeCancellable(std::move(record_batch_gen_), io_context_.stop_token());

    return Status::OK();
  }

  io::IOContext io_context_;
  Executor* executor_;
  ReadOptions read_options_;
  ParseOptions parse_options_;
  AsyncGenerator<std::shared_ptr<RecordBatch>> record_batch_gen_;
  std::shared_ptr<std::atomic<int64_t>> bytes_processed_;
};

Future<std::shared_ptr<StreamingReader>> StreamingReader::MakeAsync(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input, Executor* executor,
    const ReadOptions& read_options, const ParseOptions& parse_options) {
  auto reader = std::make_shared<StreamingReaderImpl>(io_context, executor, read_options,
                                                      parse_options);
  return reader->Init(input).Then(
      [reader] { return std::static_pointer_cast<StreamingReader>(reader); });
}

Result<std::shared_ptr<StreamingReader>> StreamingReader::Make(
    io::IOContext io_context, std::shared_ptr<io::InputStream> input,
    const ReadOptions& read_options, const ParseOptions& parse_options) {
  auto future = StreamingReader::MakeAsync(io_context, input, GetCpuThreadPool(),
                                           read_options, parse_options);
  return future.result();
}

}  // namespace json
}  // namespace arrow

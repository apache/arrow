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

struct ChunkedBlock {
  std::shared_ptr<Buffer> partial;
  std::shared_ptr<Buffer> completion;
  std::shared_ptr<Buffer> whole;
  int64_t index = -1;
};

struct DecodedBlock {
  std::shared_ptr<RecordBatch> record_batch;
  int64_t num_bytes = 0;
};

}  // namespace
}  // namespace json

template <>
struct IterationTraits<json::ChunkedBlock> {
  static json::ChunkedBlock End() { return json::ChunkedBlock{}; }
  static bool IsEnd(const json::ChunkedBlock& val) { return val.index < 0; }
};

template <>
struct IterationTraits<json::DecodedBlock> {
  static json::DecodedBlock End() { return json::DecodedBlock{}; }
  static bool IsEnd(const json::DecodedBlock& val) { return !val.record_batch; }
};

namespace json {
namespace {

// Holds related parameters for parsing and type conversion
class DecodeContext {
 public:
  explicit DecodeContext(MemoryPool* pool)
      : DecodeContext(ParseOptions::Defaults(), pool) {}
  explicit DecodeContext(ParseOptions options = ParseOptions::Defaults(),
                         MemoryPool* pool = default_memory_pool())
      : pool_(pool) {
    SetParseOptions(std::move(options));
  }

  void SetParseOptions(ParseOptions options) {
    parse_options_ = std::move(options);
    if (parse_options_.explicit_schema) {
      conversion_type_ = struct_(parse_options_.explicit_schema->fields());
    } else {
      parse_options_.unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
      conversion_type_ = struct_({});
    }
    promotion_graph_ =
        parse_options_.unexpected_field_behavior == UnexpectedFieldBehavior::InferType
            ? GetPromotionGraph()
            : nullptr;
  }

  void SetSchema(std::shared_ptr<Schema> explicit_schema,
                 UnexpectedFieldBehavior unexpected_field_behavior) {
    parse_options_.explicit_schema = std::move(explicit_schema);
    parse_options_.unexpected_field_behavior = unexpected_field_behavior;
    SetParseOptions(std::move(parse_options_));
  }
  void SetSchema(std::shared_ptr<Schema> explicit_schema) {
    SetSchema(std::move(explicit_schema), parse_options_.unexpected_field_behavior);
  }
  // Set the schema but ensure unexpected fields won't be accepted
  void SetStrictSchema(std::shared_ptr<Schema> explicit_schema) {
    auto unexpected_field_behavior = parse_options_.unexpected_field_behavior;
    if (unexpected_field_behavior == UnexpectedFieldBehavior::InferType) {
      unexpected_field_behavior = UnexpectedFieldBehavior::Error;
    }
    SetSchema(std::move(explicit_schema), unexpected_field_behavior);
  }

  [[nodiscard]] MemoryPool* pool() const { return pool_; }
  [[nodiscard]] const ParseOptions& parse_options() const { return parse_options_; }
  [[nodiscard]] const PromotionGraph* promotion_graph() const { return promotion_graph_; }
  [[nodiscard]] const std::shared_ptr<DataType>& conversion_type() const {
    return conversion_type_;
  }

 private:
  ParseOptions parse_options_;
  std::shared_ptr<DataType> conversion_type_;
  const PromotionGraph* promotion_graph_;
  MemoryPool* pool_;
};

Result<std::shared_ptr<Array>> ParseBlock(const ChunkedBlock& block,
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

class ChunkingTransformer {
 public:
  explicit ChunkingTransformer(std::unique_ptr<Chunker> chunker)
      : chunker_(std::move(chunker)) {}

  template <typename... Args>
  static Transformer<std::shared_ptr<Buffer>, ChunkedBlock> Make(Args&&... args) {
    return [self = std::make_shared<ChunkingTransformer>(std::forward<Args>(args)...)](
               std::shared_ptr<Buffer> buffer) { return (*self)(std::move(buffer)); };
  }

 private:
  Result<TransformFlow<ChunkedBlock>> operator()(std::shared_ptr<Buffer> next_buffer) {
    if (!buffer_) {
      if (ARROW_PREDICT_TRUE(!next_buffer)) {
        DCHECK_EQ(partial_, nullptr) << "Logic error: non-null partial with null buffer";
        return TransformFinish();
      }
      partial_ = std::make_shared<Buffer>("");
      buffer_ = std::move(next_buffer);
      return TransformSkip();
    }
    DCHECK_NE(partial_, nullptr);

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
    return TransformYield(ChunkedBlock{std::exchange(partial_, next_partial),
                                       std::move(completion), std::move(whole),
                                       index_++});
  }

  std::unique_ptr<Chunker> chunker_;
  std::shared_ptr<Buffer> partial_;
  std::shared_ptr<Buffer> buffer_;
  int64_t index_ = 0;
};

template <typename... Args>
Iterator<ChunkedBlock> MakeChunkingIterator(Iterator<std::shared_ptr<Buffer>> source,
                                            Args&&... args) {
  return MakeTransformedIterator(std::move(source),
                                 ChunkingTransformer::Make(std::forward<Args>(args)...));
}

// NOTE: Not reentrant. Incoming buffers are processed sequentially and the transformer's
// internal state gets updated on each call.
template <typename... Args>
AsyncGenerator<ChunkedBlock> MakeChunkingGenerator(
    AsyncGenerator<std::shared_ptr<Buffer>> source, Args&&... args) {
  return MakeTransformedGenerator(std::move(source),
                                  ChunkingTransformer::Make(std::forward<Args>(args)...));
}

class TableReaderImpl : public TableReader,
                        public std::enable_shared_from_this<TableReaderImpl> {
 public:
  TableReaderImpl(MemoryPool* pool, const ReadOptions& read_options,
                  const ParseOptions& parse_options,
                  std::shared_ptr<TaskGroup> task_group)
      : decode_context_(parse_options, pool),
        read_options_(read_options),
        task_group_(std::move(task_group)) {}

  Status Init(std::shared_ptr<io::InputStream> input) {
    ARROW_ASSIGN_OR_RAISE(auto it,
                          io::MakeInputStreamIterator(input, read_options_.block_size));
    return MakeReadaheadIterator(std::move(it), task_group_->parallelism())
        .Value(&buffer_iterator_);
  }

  Result<std::shared_ptr<Table>> Read() override {
    auto block_it = MakeChunkingIterator(std::move(buffer_iterator_),
                                         MakeChunker(decode_context_.parse_options()));

    bool did_read = false;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto block, block_it.Next());
      if (IsIterationEnd(block)) break;
      if (!did_read) {
        did_read = true;
        RETURN_NOT_OK(MakeBuilder());
      }
      task_group_->Append(
          [self = shared_from_this(), block] { return self->ParseAndInsert(block); });
    }
    if (!did_read) {
      return Status::Invalid("Empty JSON file");
    }

    std::shared_ptr<ChunkedArray> array;
    RETURN_NOT_OK(builder_->Finish(&array));
    return Table::FromChunkedStructArray(array);
  }

 private:
  Status MakeBuilder() {
    return MakeChunkedArrayBuilder(task_group_, decode_context_.pool(),
                                   decode_context_.promotion_graph(),
                                   decode_context_.conversion_type(), &builder_);
  }

  Status ParseAndInsert(const ChunkedBlock& block) {
    ARROW_ASSIGN_OR_RAISE(auto parsed, ParseBlock(block, decode_context_.parse_options(),
                                                  decode_context_.pool()));
    builder_->Insert(block.index, field("", parsed->type()), parsed);
    return Status::OK();
  }

  DecodeContext decode_context_;
  ReadOptions read_options_;
  std::shared_ptr<TaskGroup> task_group_;
  Iterator<std::shared_ptr<Buffer>> buffer_iterator_;
  std::shared_ptr<ChunkedArrayBuilder> builder_;
};

// Callable object for parsing/converting individual JSON blocks. The class itself can be
// called concurrently but reads from the `DecodeContext` aren't synchronized
class DecodingOperator {
 public:
  explicit DecodingOperator(std::shared_ptr<const DecodeContext> context)
      : context_(std::move(context)) {}

  Result<DecodedBlock> operator()(const ChunkedBlock& block) const {
    int64_t num_bytes;
    ARROW_ASSIGN_OR_RAISE(auto unconverted, ParseBlock(block, context_->parse_options(),
                                                       context_->pool(), &num_bytes));

    std::shared_ptr<ChunkedArrayBuilder> builder;
    RETURN_NOT_OK(MakeChunkedArrayBuilder(TaskGroup::MakeSerial(), context_->pool(),
                                          context_->promotion_graph(),
                                          context_->conversion_type(), &builder));
    builder->Insert(0, field("", unconverted->type()), unconverted);

    std::shared_ptr<ChunkedArray> chunked;
    RETURN_NOT_OK(builder->Finish(&chunked));
    ARROW_ASSIGN_OR_RAISE(
        auto batch, RecordBatch::FromStructArray(chunked->chunk(0), context_->pool()));

    return DecodedBlock{std::move(batch), num_bytes};
  }

 private:
  std::shared_ptr<const DecodeContext> context_;
};

// Reads from a source iterator, completes the subsequent decode task on the calling
// thread. This is only really used for compatibility with the async pipeline when CPU
// threading is disabled
AsyncGenerator<DecodedBlock> MakeDecodingGenerator(
    Iterator<ChunkedBlock> source,
    std::function<Result<DecodedBlock>(const ChunkedBlock&)> decoder) {
  struct State {
    Iterator<ChunkedBlock> source;
    std::function<Result<DecodedBlock>(const ChunkedBlock&)> decoder;
  } state{std::move(source), std::move(decoder)};
  return [state = std::make_shared<State>(std::move(state))] {
    auto maybe_block = state->source.Next();
    if (!maybe_block.ok()) {
      return Future<DecodedBlock>::MakeFinished(maybe_block.status());
    }
    const auto& block = maybe_block.ValueUnsafe();
    if (IsIterationEnd(block)) {
      return ToFuture(IterationEnd<DecodedBlock>());
    }
    return ToFuture(state->decoder(block));
  };
}

class StreamingReaderImpl : public StreamingReader {
 public:
  StreamingReaderImpl(DecodedBlock first_block, AsyncGenerator<DecodedBlock> source,
                      const std::shared_ptr<DecodeContext>& context, int max_readahead)
      : first_block_(std::move(first_block)),
        schema_(first_block_->record_batch->schema()),
        bytes_processed_(std::make_shared<std::atomic<int64_t>>(0)) {
    // Set the final schema for future invocations of the source generator
    context->SetStrictSchema(schema_);
    if (max_readahead > 0) {
      source = MakeReadaheadGenerator(std::move(source), max_readahead);
    }
    generator_ = MakeMappedGenerator(
        std::move(source), [counter = bytes_processed_](const DecodedBlock& out) {
          counter->fetch_add(out.num_bytes);
          return out.record_batch;
        });
  }

  static Future<std::shared_ptr<StreamingReaderImpl>> MakeAsync(
      std::shared_ptr<DecodeContext> context, std::shared_ptr<io::InputStream> stream,
      io::IOContext io_context, Executor* cpu_executor, const ReadOptions& read_options) {
    ARROW_ASSIGN_OR_RAISE(
        auto buffer_it,
        io::MakeInputStreamIterator(std::move(stream), read_options.block_size));
    ARROW_ASSIGN_OR_RAISE(
        auto buffer_gen,
        MakeBackgroundGenerator(std::move(buffer_it), io_context.executor()));

    AsyncGenerator<DecodedBlock> decoding_gen;
    int max_readahead = 0;
    if (read_options.use_threads) {
      // Prepare a source generator capable of async-reentrancy and parallel execution
      if (!cpu_executor) {
        cpu_executor = GetCpuThreadPool();
      }
      max_readahead = cpu_executor->GetCapacity();

      // Since the chunking/decoding steps are heavy we want to schedule them as a
      // separate task so as to maximize task distribution across CPU cores
      //
      // TODO: Add an `always_transfer` parameter to `MakeTransferredGenerator`?
      buffer_gen = [source = std::move(buffer_gen), cpu_executor] {
        return cpu_executor->TransferAlways(source());
      };
      auto chunking_gen = MakeChunkingGenerator(std::move(buffer_gen),
                                                MakeChunker(context->parse_options()));

      // At this stage, we want to allow the decoding tasks for each chunked block to run
      // in parallel on the CPU executor. However:
      //  - Chunking is inherently serial and not thread-safe
      //  - The chunking generator is not async-reentrant, won't play well with readahead
      //
      // Fortunately, `MappingGenerator` queues pending jobs and keeps only one future
      // from its source active at a time - which takes care of those concerns. In
      // addition, it will start the next job within the continuation of the previous one,
      // but before invoking its map function (in our case, `DecodingOperator`). This
      // allows for decoding tasks to gradually saturate multiple CPU cores over multiple
      // iterations. At a high level, this is how the full pipeline would operate in cases
      // where decoding tasks are disproportionately expensive:
      //
      // --------------------------------------------------------------------------
      // Reading: IoThread(?) --> Chunking: CpuThread(0) ... Decoding: CpuThread(0)
      // --------------------------------------------------------------------------
      //                                                     Decoding: CpuThread(0)
      // Reading: IoThread(?) --> Chunking: CpuThread(1) ... Decoding: CpuThread(1)
      // --------------------------------------------------------------------------
      //                                                     Decoding: CpuThread(0)
      //                                                     Decoding: CpuThread(1)
      // Reading: IoThread(?) --> Chunking: CpuThread(2) ... Decoding: CpuThread(2)
      // --------------------------------------------------------------------------
      //
      // Remember that we should already be on the CPU executor following chunking, so the
      // decoding task simply continues to use that thread rather than spawning a new one.
      decoding_gen =
          MakeMappedGenerator(std::move(chunking_gen), DecodingOperator(context));
    } else {
      buffer_gen = MakeTransferredGenerator(std::move(buffer_gen), io_context.executor());
      // We convert the background generator back to an iterator so its work can remain on
      // the IO pool while we process its buffers on the calling thread
      auto chunking_it =
          MakeChunkingIterator(MakeGeneratorIterator(std::move(buffer_gen)),
                               MakeChunker(context->parse_options()));
      decoding_gen =
          MakeDecodingGenerator(std::move(chunking_it), DecodingOperator(context));
    }

    return FirstBlock(decoding_gen)
        .Then([source = std::move(decoding_gen), context = std::move(context),
               max_readahead](const DecodedBlock& block) {
          return std::make_shared<StreamingReaderImpl>(block, std::move(source), context,
                                                       max_readahead);
        });
  }

  [[nodiscard]] std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* out) override {
    auto result = ReadNextAsync().result();
    return std::move(result).Value(out);
  }

  Future<std::shared_ptr<RecordBatch>> ReadNextAsync() override {
    // On the first call, return the batch we used for initialization
    if (ARROW_PREDICT_FALSE(first_block_)) {
      bytes_processed_->fetch_add(first_block_->num_bytes);
      auto batch = std::exchange(first_block_, std::nullopt)->record_batch;
      return ToFuture(std::move(batch));
    }
    return generator_();
  }

  [[nodiscard]] int64_t bytes_processed() const override {
    return bytes_processed_->load();
  }

 private:
  static Future<DecodedBlock> FirstBlock(AsyncGenerator<DecodedBlock> gen) {
    // Read from the stream until we get a non-empty record batch that we can use to
    // declare the schema. Along the way, accumulate the bytes read so they can be
    // recorded on the first `ReadNextAsync`
    auto loop_body =
        [gen = std::move(gen),
         out = std::make_shared<DecodedBlock>()]() -> Future<ControlFlow<DecodedBlock>> {
      return gen().Then(
          [out](const DecodedBlock& block) -> Result<ControlFlow<DecodedBlock>> {
            if (IsIterationEnd(block)) {
              return Status::Invalid("Empty JSON stream");
            }
            out->num_bytes += block.num_bytes;
            if (block.record_batch->num_rows() == 0) {
              return Continue();
            }
            out->record_batch = block.record_batch;
            return Break(*out);
          });
    };
    return Loop(std::move(loop_body));
  }

  std::optional<DecodedBlock> first_block_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<std::atomic<int64_t>> bytes_processed_;
  AsyncGenerator<std::shared_ptr<RecordBatch>> generator_;
};

}  // namespace

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

Future<std::shared_ptr<StreamingReader>> StreamingReader::MakeAsync(
    std::shared_ptr<io::InputStream> stream, const ReadOptions& read_options,
    const ParseOptions& parse_options, const io::IOContext& io_context,
    Executor* cpu_executor) {
  auto future = StreamingReaderImpl::MakeAsync(
      std::make_shared<DecodeContext>(parse_options, io_context.pool()),
      std::move(stream), io_context, cpu_executor, read_options);
  return future.Then([](const std::shared_ptr<StreamingReaderImpl>& reader) {
    return std::static_pointer_cast<StreamingReader>(reader);
  });
}

Result<std::shared_ptr<StreamingReader>> StreamingReader::Make(
    std::shared_ptr<io::InputStream> stream, const ReadOptions& read_options,
    const ParseOptions& parse_options, const io::IOContext& io_context,
    Executor* cpu_executor) {
  auto future =
      MakeAsync(std::move(stream), read_options, parse_options, io_context, cpu_executor);
  return future.result();
}

Result<std::shared_ptr<RecordBatch>> ParseOne(ParseOptions options,
                                              std::shared_ptr<Buffer> json) {
  DecodeContext context(std::move(options));

  std::unique_ptr<BlockParser> parser;
  RETURN_NOT_OK(BlockParser::Make(context.parse_options(), &parser));
  RETURN_NOT_OK(parser->Parse(json));
  std::shared_ptr<Array> parsed;
  RETURN_NOT_OK(parser->Finish(&parsed));

  std::shared_ptr<ChunkedArrayBuilder> builder;
  RETURN_NOT_OK(MakeChunkedArrayBuilder(TaskGroup::MakeSerial(), context.pool(),
                                        context.promotion_graph(),
                                        context.conversion_type(), &builder));

  builder->Insert(0, field("", context.conversion_type()), parsed);
  std::shared_ptr<ChunkedArray> converted_chunked;
  RETURN_NOT_OK(builder->Finish(&converted_chunked));

  return RecordBatch::FromStructArray(converted_chunked->chunk(0), context.pool());
}

}  // namespace json
}  // namespace arrow

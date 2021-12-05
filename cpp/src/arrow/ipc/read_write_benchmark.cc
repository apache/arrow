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

#include "benchmark/benchmark.h"

#include <cstdint>
#include <sstream>
#include <string>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"

namespace arrow {

std::shared_ptr<RecordBatch> MakeRecordBatch(int64_t total_size, int64_t num_fields) {
  int64_t length = total_size / num_fields / sizeof(int64_t);
  random::RandomArrayGenerator rand(0x4f32a908);
  auto type = arrow::int64();

  ArrayVector arrays;
  std::vector<std::shared_ptr<Field>> fields;
  for (int64_t i = 0; i < num_fields; ++i) {
    std::stringstream ss;
    ss << "f" << i;
    fields.push_back(field(ss.str(), type));
    arrays.push_back(rand.Int64(length, 0, 100, 0.1));
  }

  auto schema = std::make_shared<Schema>(fields);
  return RecordBatch::Make(schema, length, arrays);
}

static void WriteRecordBatch(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;
  auto options = ipc::IpcWriteOptions::Defaults();

  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);
  auto record_batch = MakeRecordBatch(kTotalSize, state.range(0));

  while (state.KeepRunning()) {
    io::BufferOutputStream stream(buffer);
    int32_t metadata_length;
    int64_t body_length;
    ABORT_NOT_OK(ipc::WriteRecordBatch(*record_batch, 0, &stream, &metadata_length,
                                       &body_length, options));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

static void ReadRecordBatch(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;
  auto options = ipc::IpcWriteOptions::Defaults();

  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);
  auto record_batch = MakeRecordBatch(kTotalSize, state.range(0));

  io::BufferOutputStream stream(buffer);

  int32_t metadata_length;
  int64_t body_length;
  ABORT_NOT_OK(ipc::WriteRecordBatch(*record_batch, 0, &stream, &metadata_length,
                                     &body_length, options));

  ipc::DictionaryMemo empty_memo;
  while (state.KeepRunning()) {
    io::BufferReader reader(buffer);
    ABORT_NOT_OK(ipc::ReadRecordBatch(record_batch->schema(), &empty_memo,
                                      ipc::IpcReadOptions::Defaults(), &reader));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

static void ReadStream(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;
  auto options = ipc::IpcWriteOptions::Defaults();

  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);
  {
    // Make Arrow IPC stream
    auto record_batch = MakeRecordBatch(kTotalSize, state.range(0));

    io::BufferOutputStream stream(buffer);

    auto writer_result = ipc::MakeStreamWriter(&stream, record_batch->schema(), options);
    ABORT_NOT_OK(writer_result);
    auto writer = *writer_result;
    ABORT_NOT_OK(writer->WriteRecordBatch(*record_batch));
    ABORT_NOT_OK(writer->Close());
    ABORT_NOT_OK(stream.Close());
  }

  ipc::DictionaryMemo empty_memo;
  while (state.KeepRunning()) {
    io::BufferReader input(buffer);
    auto reader_result =
        ipc::RecordBatchStreamReader::Open(&input, ipc::IpcReadOptions::Defaults());
    ABORT_NOT_OK(reader_result);
    auto reader = *reader_result;
    while (true) {
      std::shared_ptr<RecordBatch> batch;
      ABORT_NOT_OK(reader->ReadNext(&batch));
      if (batch.get() == nullptr) {
        break;
      }
    }
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

static void DecodeStream(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;
  auto options = ipc::IpcWriteOptions::Defaults();

  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);
  auto record_batch = MakeRecordBatch(kTotalSize, state.range(0));

  io::BufferOutputStream stream(buffer);

  auto writer_result = ipc::MakeStreamWriter(&stream, record_batch->schema(), options);
  ABORT_NOT_OK(writer_result);
  auto writer = *writer_result;
  ABORT_NOT_OK(writer->WriteRecordBatch(*record_batch));
  ABORT_NOT_OK(writer->Close());

  ipc::DictionaryMemo empty_memo;
  while (state.KeepRunning()) {
    class NullListener : public ipc::Listener {
      Status OnRecordBatchDecoded(std::shared_ptr<RecordBatch> batch) override {
        return Status::OK();
      }
    } listener;
    ipc::StreamDecoder decoder(std::shared_ptr<NullListener>(&listener, [](void*) {}),
                               ipc::IpcReadOptions::Defaults());
    ABORT_NOT_OK(decoder.Consume(buffer));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

#ifdef ARROW_WITH_ZSTD
#define GENERATE_COMPRESSED_DATA_IN_MEMORY()                                      \
  constexpr int64_t kBatchSize = 1 << 20; /* 1 MB */                              \
  constexpr int64_t kBatches = 16;                                                \
  auto options = ipc::IpcWriteOptions::Defaults();                                \
  ASSIGN_OR_ABORT(options.codec,                                                  \
                  arrow::util::Codec::Create(arrow::Compression::type::ZSTD));    \
  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);       \
  {                                                                               \
    auto record_batch = MakeRecordBatch(kBatchSize, state.range(0));              \
    io::BufferOutputStream stream(buffer);                                        \
    auto writer = *ipc::MakeFileWriter(&stream, record_batch->schema(), options); \
    for (int i = 0; i < kBatches; i++) {                                          \
      ABORT_NOT_OK(writer->WriteRecordBatch(*record_batch));                      \
    }                                                                             \
    ABORT_NOT_OK(writer->Close());                                                \
    ABORT_NOT_OK(stream.Close());                                                 \
  }
#endif

#define GENERATE_DATA_IN_MEMORY()                                                 \
  constexpr int64_t kBatchSize = 1 << 20; /* 1 MB */                              \
  constexpr int64_t kBatches = 1;                                                 \
  auto options = ipc::IpcWriteOptions::Defaults();                                \
  std::shared_ptr<ResizableBuffer> buffer = *AllocateResizableBuffer(1024);       \
  {                                                                               \
    auto record_batch = MakeRecordBatch(kBatchSize, state.range(0));              \
    io::BufferOutputStream stream(buffer);                                        \
    auto writer = *ipc::MakeFileWriter(&stream, record_batch->schema(), options); \
    ABORT_NOT_OK(writer->WriteRecordBatch(*record_batch));                        \
    ABORT_NOT_OK(writer->Close());                                                \
    ABORT_NOT_OK(stream.Close());                                                 \
  }

#define GENERATE_DATA_TEMP_FILE()                                                 \
  constexpr int64_t kBatchSize = 1 << 20; /* 1 MB */                              \
  constexpr int64_t kBatches = 16;                                                \
  auto options = ipc::IpcWriteOptions::Defaults();                                \
  ASSIGN_OR_ABORT(auto sink, io::FileOutputStream::Open("/tmp/benchmark.arrow")); \
  {                                                                               \
    auto record_batch = MakeRecordBatch(kBatchSize, state.range(0));              \
    auto writer = *ipc::MakeFileWriter(sink, record_batch->schema(), options);    \
    ABORT_NOT_OK(writer->WriteRecordBatch(*record_batch));                        \
    ABORT_NOT_OK(writer->Close());                                                \
    ABORT_NOT_OK(sink->Close());                                                  \
  }

#define READ_DATA_IN_MEMORY() auto input = std::make_shared<io::BufferReader>(buffer);
#define READ_DATA_TEMP_FILE() \
  ASSIGN_OR_ABORT(auto input, io::ReadableFile::Open("/tmp/benchmark.arrow"));
#define READ_DATA_MMAP_FILE()                                                    \
  ASSIGN_OR_ABORT(auto input, io::MemoryMappedFile::Open("/tmp/benchmark.arrow", \
                                                         io::FileMode::type::READ));

#define READ_SYNC(NAME, GENERATE, READ)                                                 \
  static void NAME(benchmark::State& state) {                                           \
    GENERATE();                                                                         \
    for (auto _ : state) {                                                              \
      READ();                                                                           \
      auto reader = *ipc::RecordBatchFileReader::Open(input.get(),                      \
                                                      ipc::IpcReadOptions::Defaults()); \
      const int num_batches = reader->num_record_batches();                             \
      for (int i = 0; i < num_batches; ++i) {                                           \
        auto batch = *reader->ReadRecordBatch(i);                                       \
      }                                                                                 \
    }                                                                                   \
    state.SetBytesProcessed(int64_t(state.iterations()) * kBatchSize * kBatches);       \
  }                                                                                     \
  BENCHMARK(NAME)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();

#define READ_ASYNC(NAME, GENERATE, READ)                                                \
  static void NAME##Async(benchmark::State& state) {                                    \
    GENERATE();                                                                         \
    for (auto _ : state) {                                                              \
      READ();                                                                           \
      auto reader = *ipc::RecordBatchFileReader::Open(input.get(),                      \
                                                      ipc::IpcReadOptions::Defaults()); \
      ASSIGN_OR_ABORT(auto generator, reader->GetRecordBatchGenerator());               \
      const int num_batches = reader->num_record_batches();                             \
      for (int i = 0; i < num_batches; ++i) {                                           \
        auto batch = *generator().result();                                             \
      }                                                                                 \
    }                                                                                   \
    state.SetBytesProcessed(int64_t(state.iterations()) * kBatchSize * kBatches);       \
  }                                                                                     \
  BENCHMARK(NAME##Async)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();

#define READ_BENCHMARK(NAME, GENERATE, READ) \
  READ_SYNC(NAME, GENERATE, READ);           \
  READ_ASYNC(NAME, GENERATE, READ);

READ_BENCHMARK(ReadFile, GENERATE_DATA_IN_MEMORY, READ_DATA_IN_MEMORY);
READ_BENCHMARK(ReadTempFile, GENERATE_DATA_TEMP_FILE, READ_DATA_TEMP_FILE);
READ_BENCHMARK(ReadMmapFile, GENERATE_DATA_TEMP_FILE, READ_DATA_MMAP_FILE);
#ifdef ARROW_WITH_ZSTD
READ_BENCHMARK(ReadCompressedFile, GENERATE_COMPRESSED_DATA_IN_MEMORY,
               READ_DATA_IN_MEMORY);
#endif

BENCHMARK(WriteRecordBatch)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();
BENCHMARK(ReadRecordBatch)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();
BENCHMARK(ReadStream)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();
BENCHMARK(DecodeStream)->RangeMultiplier(4)->Range(1, 1 << 13)->UseRealTime();

}  // namespace arrow

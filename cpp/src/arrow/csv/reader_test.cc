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

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/array_dict.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

// Allows the streaming reader to be used in tests that expect a table reader
class StreamingReaderAsTableReader : public TableReader {
 public:
  explicit StreamingReaderAsTableReader(std::shared_ptr<StreamingReader> reader)
      : reader_(std::move(reader)) {}
  virtual ~StreamingReaderAsTableReader() = default;
  virtual Result<std::shared_ptr<Table>> Read() {
    ARROW_ASSIGN_OR_RAISE(auto table, reader_->ToTable());
    return table;
  }
  virtual Future<std::shared_ptr<Table>> ReadAsync() {
    auto reader = reader_;
    AsyncGenerator<std::shared_ptr<RecordBatch>> gen = [reader] {
      return reader->ReadNextAsync();
    };
    return CollectAsyncGenerator(std::move(gen))
        .Then([](const RecordBatchVector& batches) {
          return Table::FromRecordBatches(batches);
        });
  }

 private:
  std::shared_ptr<StreamingReader> reader_;
};

using TableReaderFactory = std::function<Result<std::shared_ptr<TableReader>>(
    std::shared_ptr<io::InputStream>, ParseOptions, std::optional<int32_t> block_size)>;
using StreamingReaderFactory = std::function<Result<std::shared_ptr<StreamingReader>>(
    std::shared_ptr<io::InputStream>)>;

void TestEmptyTable(TableReaderFactory reader_factory) {
  auto empty_buffer = std::make_shared<Buffer>("");
  auto empty_input = std::make_shared<io::BufferReader>(empty_buffer);
  auto maybe_reader =
      reader_factory(empty_input, ParseOptions::Defaults(), /*block_size=*/{});
  // Streaming reader fails on open, table readers fail on first read
  if (maybe_reader.ok()) {
    ASSERT_FINISHES_AND_RAISES(Invalid, (*maybe_reader)->ReadAsync());
  } else {
    ASSERT_TRUE(maybe_reader.status().IsInvalid());
  }
}

void TestHeaderOnly(TableReaderFactory reader_factory) {
  auto header_only_buffer = std::make_shared<Buffer>("a,b,c\n");
  auto input = std::make_shared<io::BufferReader>(header_only_buffer);
  ASSERT_OK_AND_ASSIGN(
      auto reader, reader_factory(input, ParseOptions::Defaults(), /*block_size=*/{}));
  ASSERT_FINISHES_OK_AND_ASSIGN(auto table, reader->ReadAsync());
  ASSERT_EQ(table->schema()->num_fields(), 3);
  ASSERT_EQ(table->num_rows(), 0);
}

void TestHeaderOnlyStreaming(StreamingReaderFactory reader_factory) {
  auto header_only_buffer = std::make_shared<Buffer>("a,b,c\n");
  auto input = std::make_shared<io::BufferReader>(header_only_buffer);
  ASSERT_OK_AND_ASSIGN(auto reader, reader_factory(input));
  std::shared_ptr<RecordBatch> next_batch;
  ASSERT_OK(reader->ReadNext(&next_batch));
  ASSERT_EQ(next_batch, nullptr);
}

void TestStraddling(TableReaderFactory reader_factory) {
  // The largest line is 25 bytes, it is surrounded by many valid lines
  constexpr std::string_view kData =
      "a,b\n1,2\n3,4\n5,6\n7,8\n111111111111,22222222222\n1,2\n3,4\n5,6\n7,8\n";
  constexpr int kHeaderLineLen = 4;
  constexpr int kLargestLineLen = 25;

  auto data = std::make_shared<Buffer>(kData);
  auto input = std::make_shared<io::BufferReader>(data);
  // Sanity check that a large enough block size reads the data successfully
  ASSERT_OK_AND_ASSIGN(
      auto reader, reader_factory(input, ParseOptions::Defaults(), /*block_size=*/{}));
  ASSERT_FINISHES_OK(reader->ReadAsync());

  for (int block_size = kHeaderLineLen; block_size < kLargestLineLen / 2; ++block_size) {
    input = std::make_shared<io::BufferReader>(data);
    ASSERT_OK_AND_ASSIGN(auto reader,
                         reader_factory(input, ParseOptions::Defaults(), block_size));
    ASSERT_FINISHES_AND_RAISES(Invalid, reader->ReadAsync());
    reader.reset();
  }
  // This could deadlock in the TableReader destructor if it is not able to recover
  // gracefully after an error (GH-48741). Alternatively, the destructor could
  // deadlock at process shutdown when the thread pool is reaped.
  internal::GetCpuThreadPool()->WaitForIdle();
}

void TestQuotedCRLFStraddlingBlockBoundary(TableReaderFactory reader_factory) {
  // GH-51368: a CRLF inside a quoted field must survive intact when the CR is
  // the last byte of a read block and the LF is the first byte of the next
  // block.  Previously the LF was silently dropped.
  constexpr int32_t kBlockSize = 1024;
  const std::string prefix = "id,s,p\n0,\"";
  const std::string embedded = "A\r\nB";
  const std::string suffix = "\",x\n1,plain,x\n2,plain,x\n";
  // Place 'A' at byte kBlockSize - 2 and '\r' at the last byte of the block
  const int32_t pad = kBlockSize - static_cast<int32_t>(prefix.size()) - 2;
  const std::string first_value = std::string(static_cast<size_t>(pad), 'z') + embedded;
  const std::string data =
      prefix + std::string(static_cast<size_t>(pad), 'z') + embedded + suffix;

  auto parse_options = ParseOptions::Defaults();
  parse_options.newlines_in_values = true;

  auto input = std::make_shared<io::BufferReader>(std::make_shared<Buffer>(data));
  ASSERT_OK_AND_ASSIGN(auto reader, reader_factory(input, parse_options, kBlockSize));
  ASSERT_FINISHES_OK_AND_ASSIGN(auto table, reader->ReadAsync());

  ASSERT_EQ(table->num_columns(), 3);
  ASSERT_EQ(table->num_rows(), 3);
  auto column = table->GetColumnByName("s");
  ASSERT_NE(column, nullptr);
  std::vector<std::string> values;
  for (int i = 0; i < column->num_chunks(); ++i) {
    auto arr = std::static_pointer_cast<StringArray>(column->chunk(i));
    for (int64_t j = 0; j < arr->length(); ++j) {
      values.push_back(arr->GetString(j));
    }
  }
  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(values[0], first_value);
  EXPECT_EQ(values[1], "plain");
  EXPECT_EQ(values[2], "plain");
}

void TestHeaderRowEndingAtBlockBoundary(TableReaderFactory reader_factory) {
  // The header row's line terminator is the last byte of the first read
  // block.  ProcessHeader parses the first buffer on its own, so the buffer
  // handed to it must still contain the end of the header line; withholding
  // a trailing '\r' from the first buffer (as done for later buffers to fix
  // GH-51368) makes the header unreadable here.
  for (const std::string& data :
       {std::string("a,b\r1,2\r"), std::string("a,b\r\n1,2\r\n")}) {
    // block_size=4 splits the input right after the header terminator;
    // block_size=64 makes it a single block (also exercising the case of a
    // lone first buffer ending with '\r').
    for (const int32_t block_size : {4, 64}) {
      auto input = std::make_shared<io::BufferReader>(std::make_shared<Buffer>(data));
      ASSERT_OK_AND_ASSIGN(auto reader,
                           reader_factory(input, ParseOptions::Defaults(), block_size));
      ASSERT_FINISHES_OK_AND_ASSIGN(auto table, reader->ReadAsync());

      ASSERT_EQ(table->num_columns(), 2);
      ASSERT_EQ(table->num_rows(), 1);
      auto col_a = table->GetColumnByName("a");
      auto col_b = table->GetColumnByName("b");
      ASSERT_NE(col_a, nullptr);
      ASSERT_NE(col_b, nullptr);
      std::vector<int64_t> values_a, values_b;
      for (int i = 0; i < col_a->num_chunks(); ++i) {
        auto arr = std::static_pointer_cast<Int64Array>(col_a->chunk(i));
        for (int64_t j = 0; j < arr->length(); ++j) {
          values_a.push_back(arr->Value(j));
        }
      }
      for (int i = 0; i < col_b->num_chunks(); ++i) {
        auto arr = std::static_pointer_cast<Int64Array>(col_b->chunk(i));
        for (int64_t j = 0; j < arr->length(); ++j) {
          values_b.push_back(arr->Value(j));
        }
      }
      ASSERT_EQ(values_a.size(), 1);
      ASSERT_EQ(values_b.size(), 1);
      EXPECT_EQ(values_a[0], 1);
      EXPECT_EQ(values_b[0], 2);
    }
  }
}

// An InputStream that delivers its data in prescribed chunk sizes, to
// exercise short reads: a stream is allowed to return fewer bytes than
// requested, so block boundaries as seen by the reader do not necessarily
// fall on multiples of the configured block size.
class ChunkedInputStream : public io::InputStream {
 public:
  ChunkedInputStream(std::shared_ptr<Buffer> data, std::vector<int64_t> chunks)
      : data_(std::move(data)), chunks_(std::move(chunks)) {}

  Status Close() override {
    closed_ = true;
    return Status::OK();
  }
  bool closed() const override { return closed_; }
  Result<int64_t> Tell() const override { return pos_; }
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    int64_t to_read = NextChunk(nbytes);
    if (to_read > 0) {
      memcpy(out, data_->data() + pos_, static_cast<size_t>(to_read));
      pos_ += to_read;
    }
    return to_read;
  }
  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    int64_t start = pos_;
    int64_t to_read = NextChunk(nbytes);
    pos_ += to_read;
    return SliceBuffer(data_, start, to_read);
  }

 private:
  int64_t NextChunk(int64_t nbytes) {
    int64_t chunk = chunks_[std::min(chunk_idx_, chunks_.size() - 1)];
    int64_t to_read = std::min({nbytes, chunk, data_->size() - pos_});
    if (to_read == chunk) ++chunk_idx_;
    return to_read;
  }
  std::shared_ptr<Buffer> data_;
  std::vector<int64_t> chunks_;
  size_t chunk_idx_ = 0;
  int64_t pos_ = 0;
  bool closed_ = false;
};

// Check a parsed two-column table ("a": int64, "b": string) against the
// expected values.  Returns "" on success, a description of the mismatch
// otherwise (so that loop-driven tests can report every failing case).
std::string CheckTableContents(const std::shared_ptr<Table>& table,
                               const std::vector<int64_t>& expected_a,
                               const std::vector<std::string>& expected_b) {
  if (table->num_columns() != 2) {
    return "expected 2 columns, got " + std::to_string(table->num_columns());
  }
  if (table->num_rows() != static_cast<int64_t>(expected_a.size())) {
    return "expected " + std::to_string(expected_a.size()) + " rows, got " +
           std::to_string(table->num_rows());
  }
  std::vector<int64_t> values_a;
  for (int i = 0; i < table->column(0)->num_chunks(); ++i) {
    auto arr = std::dynamic_pointer_cast<Int64Array>(table->column(0)->chunk(i));
    if (arr == nullptr) {
      return "column 'a' is not int64";
    }
    for (int64_t j = 0; j < arr->length(); ++j) {
      values_a.push_back(arr->Value(j));
    }
  }
  std::vector<std::string> values_b;
  for (int i = 0; i < table->column(1)->num_chunks(); ++i) {
    auto arr = std::dynamic_pointer_cast<StringArray>(table->column(1)->chunk(i));
    if (arr == nullptr) {
      return "column 'b' is not a string column";
    }
    for (int64_t j = 0; j < arr->length(); ++j) {
      values_b.push_back(arr->GetString(j));
    }
  }
  if (values_a != expected_a) {
    return "column 'a' values differ";
  }
  if (values_b != expected_b) {
    return "column 'b' values differ";
  }
  return "";
}

// Read a whole table from `input` and check its contents, with the same
// return convention as CheckTableContents.
std::string ReadAndCheckTable(TableReaderFactory reader_factory,
                              std::shared_ptr<io::InputStream> input,
                              const ParseOptions& parse_options, int32_t block_size,
                              const std::vector<int64_t>& expected_a,
                              const std::vector<std::string>& expected_b) {
  auto maybe_reader = reader_factory(input, parse_options, block_size);
  if (!maybe_reader.ok()) {
    return "could not create reader: " + maybe_reader.status().message();
  }
  auto maybe_table = (*maybe_reader)->Read();
  if (!maybe_table.ok()) {
    return "read failed: " + maybe_table.status().message();
  }
  return CheckTableContents(*maybe_table, expected_a, expected_b);
}

void TestMixedLineEndingsStraddling(TableReaderFactory reader_factory) {
  // LF-terminated rows followed by a final CRLF-terminated row, read with
  // block_size=8.  The last row ("333,z\r\n") is 7 bytes: it must parse
  // even when short reads split the stream so that the '\r' arrives in a
  // different read than the data it terminates (e.g. chunk sizes 8, 8, 2, 1
  // deliver "...333," / "z\r" / "\n").  Splitting the '\r' from the "z"
  // used to make the chunker reject the file with "straddling object
  // straddles two block boundaries", even though the row is smaller than
  // the block size.
  const std::string data = "a,b\n1,x\n2,y\n333,z\r\n";
  const std::vector<int64_t> expected_a = {1, 2, 333};
  const std::vector<std::string> expected_b = {"x", "y", "z"};

  // Aligned reads of exactly block_size bytes
  {
    auto input = std::make_shared<io::BufferReader>(std::make_shared<Buffer>(data));
    ASSERT_EQ("", ReadAndCheckTable(reader_factory, input, ParseOptions::Defaults(),
                                    /*block_size=*/8, expected_a, expected_b));
  }
  // Short reads in various chunk patterns (each read still returns at most
  // block_size bytes).  The first read of each pattern covers the header
  // line: the first block handed to the reader must contain its end (the
  // header parser reads the first block on its own).
  for (const auto& chunks :
       {std::vector<int64_t>{8, 8, 2, 1}, std::vector<int64_t>{16, 2, 1},
        std::vector<int64_t>{4, 4, 8, 2, 1}, std::vector<int64_t>{5, 7}}) {
    auto input =
        std::make_shared<ChunkedInputStream>(std::make_shared<Buffer>(data), chunks);
    ASSERT_EQ("", ReadAndCheckTable(reader_factory, input, ParseOptions::Defaults(),
                                    /*block_size=*/8, expected_a, expected_b))
        << "with chunk pattern starting with " << chunks[0];
  }
}

void TestSmallInputsAcrossBlockSizes(TableReaderFactory reader_factory) {
  // Systematically read small inputs across line-ending styles, presence
  // or absence of a final line ending, small block sizes and field styles,
  // checking the exact parsed values each time.  Block boundaries must not
  // change the parse result (GH-51368 and follow-ups): in particular, a
  // block must never lose the line terminator of the data it carries.
  //
  // The block sizes are chosen per field style so that every row still
  // completes within the block that follows the one it starts in: the
  // chunker legitimately rejects rows straddling more than that ("straddling
  // object straddles two block boundaries", see TestStraddling), on the
  // unmodified reader too.
  const std::vector<std::string> ending_names = {"LF", "CR", "CRLF"};
  const std::vector<std::string> line_endings = {"\n", "\r", "\r\n"};
  struct FieldStyle {
    std::string name;
    std::vector<std::string> rows;
    std::vector<int64_t> expected_a;
    std::vector<std::string> expected_b;
    bool newlines_in_values;
    std::vector<int32_t> block_sizes;
  };
  const std::vector<FieldStyle> styles = {
      {"plain",
       {"1,x", "2,y", "33,z"},
       {1, 2, 33},
       {"x", "y", "z"},
       /*newlines_in_values=*/false,
       {4, 5, 6, 7, 8, 9, 11, 16}},
      {"quoted",
       {"1,\"p\nq\"", "2,\"r\rs\"", "3,\"t\r\nu\""},
       {1, 2, 3},
       {"p\nq", "r\rs", "t\r\nu"},
       /*newlines_in_values=*/true,
       {8, 9, 11, 16}},
  };

  std::vector<std::string> failures;
  auto check = [&](const FieldStyle& style, size_t ending_index, bool final_ending,
                   const std::string& data, int32_t block_size) {
    auto input = std::make_shared<io::BufferReader>(std::make_shared<Buffer>(data));
    auto parse_options = ParseOptions::Defaults();
    parse_options.newlines_in_values = style.newlines_in_values;
    auto problem = ReadAndCheckTable(reader_factory, input, parse_options, block_size,
                                     style.expected_a, style.expected_b);
    if (!problem.empty()) {
      failures.push_back("style=" + style.name +
                         " endings=" + ending_names[ending_index] +
                         " final_ending=" + (final_ending ? "yes" : "no") +
                         " block_size=" + std::to_string(block_size) + ": " + problem);
    }
  };

  for (const auto& style : styles) {
    for (size_t e = 0; e < line_endings.size(); ++e) {
      for (const bool final_ending : {true, false}) {
        std::string data = "a,b";
        for (const auto& row : style.rows) {
          data += line_endings[e] + row;
        }
        if (final_ending) {
          data += line_endings[e];
        }
        for (const int32_t block_size : style.block_sizes) {
          check(style, e, final_ending, data, block_size);
        }
      }
    }
  }

  // Quoted fields with embedded CRLF at block_size=7: a block can end
  // right after an embedded '\r', whose '\n' must not be dropped
  // (GH-51368; the unmodified reader silently corrupts the values here).
  {
    const auto& style = styles[1];
    const size_t e = 2;  // CRLF
    for (const bool final_ending : {true, false}) {
      std::string data = "a,b";
      for (const auto& row : style.rows) {
        data += line_endings[e] + row;
      }
      if (final_ending) {
        data += line_endings[e];
      }
      check(style, e, final_ending, data, /*block_size=*/7);
    }
  }

  EXPECT_TRUE(failures.empty()) << [&] {
    std::string out;
    for (const auto& failure : failures) {
      out += "\n  " + failure;
    }
    return out;
  }();
}

void StressTableReader(TableReaderFactory reader_factory) {
#ifdef ARROW_VALGRIND
  const int NTASKS = 10;
  const int NROWS = 100;
#else
  const int NTASKS = 100;
  const int NROWS = 1000;
#endif
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS));

  std::vector<Future<std::shared_ptr<Table>>> task_futures(NTASKS);
  for (int i = 0; i < NTASKS; i++) {
    auto input = std::make_shared<io::BufferReader>(table_buffer);
    ASSERT_OK_AND_ASSIGN(
        auto reader, reader_factory(input, ParseOptions::Defaults(), /*block_size=*/{}));
    task_futures[i] = reader->ReadAsync();
  }
  auto combined_future = All(task_futures);
  combined_future.Wait();

  ASSERT_OK_AND_ASSIGN(std::vector<Result<std::shared_ptr<Table>>> results,
                       combined_future.result());
  for (auto&& result : results) {
    ASSERT_OK_AND_ASSIGN(auto table, result);
    ASSERT_EQ(NROWS, table->num_rows());
  }
}

void StressInvalidTableReader(TableReaderFactory reader_factory) {
#ifdef ARROW_VALGRIND
  const int NTASKS = 10;
  const int NROWS = 100;
#else
  const int NTASKS = 100;
  const int NROWS = 1000;
#endif
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS, [=](size_t row_num) {
                         return row_num != NROWS / 2;
                       }));

  std::vector<Future<std::shared_ptr<Table>>> task_futures(NTASKS);
  for (int i = 0; i < NTASKS; i++) {
    auto input = std::make_shared<io::BufferReader>(table_buffer);
    ASSERT_OK_AND_ASSIGN(
        auto reader, reader_factory(input, ParseOptions::Defaults(), /*block_size=*/{}));
    task_futures[i] = reader->ReadAsync();
  }
  auto combined_future = All(task_futures);
  combined_future.Wait();

  ASSERT_OK_AND_ASSIGN(std::vector<Result<std::shared_ptr<Table>>> results,
                       combined_future.result());
  for (auto&& result : results) {
    ASSERT_RAISES(Invalid, result);
  }
}

void TestNestedParallelism(std::shared_ptr<internal::ThreadPool> thread_pool,
                           TableReaderFactory reader_factory) {
  const int NROWS = 1000;
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS));
  auto input = std::make_shared<io::BufferReader>(table_buffer);
  ASSERT_OK_AND_ASSIGN(
      auto reader, reader_factory(input, ParseOptions::Defaults(), /*block_size=*/{}));

  Future<std::shared_ptr<Table>> table_future;

  auto read_task = [&reader, &table_future]() mutable {
    table_future = reader->ReadAsync();
    return Status::OK();
  };
  ASSERT_OK_AND_ASSIGN(auto future, thread_pool->Submit(read_task));

  ASSERT_FINISHES_OK(future);
  ASSERT_FINISHES_OK_AND_ASSIGN(auto table, table_future);
  ASSERT_EQ(table->num_rows(), NROWS);
}

void TestInvalidRowsSkipped(TableReaderFactory reader_factory, bool async) {
  const int NROWS = 1000;
  const int INVALID_EVERY = 20;
  const int NINVALID = 50;

  auto opts = ParseOptions::Defaults();
  std::atomic<int> num_invalid_rows(0);
  opts.invalid_row_handler = [&](const InvalidRow& row) {
    auto cur_invalid_rows = ++num_invalid_rows;
    if (async) {
      // Row numbers are not counted in batches during async processing
      EXPECT_EQ(-1, row.number);
    } else {
      // actual data starts at row #2 in the CSV "file"
      EXPECT_EQ(cur_invalid_rows * INVALID_EVERY + 1, row.number);
    }
    return InvalidRowResult::Skip;
  };

  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS, [=](size_t row_num) {
                         // row_num is 0-based
                         return (row_num + 1) % static_cast<size_t>(INVALID_EVERY) != 0;
                       }));
  auto input = std::make_shared<io::BufferReader>(table_buffer);
  ASSERT_OK_AND_ASSIGN(auto reader,
                       reader_factory(input, std::move(opts), /*block_size=*/{}));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());
  ASSERT_EQ(NROWS - NINVALID, table->num_rows());
  ASSERT_EQ(NINVALID, num_invalid_rows);
}

TableReaderFactory MakeSerialFactory() {
  return [](std::shared_ptr<io::InputStream> input_stream, ParseOptions parse_options,
            std::optional<int32_t> block_size) {
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = block_size.value_or(1 << 10);
    read_options.use_threads = false;
    return TableReader::Make(io::default_io_context(), input_stream, read_options,
                             std::move(parse_options), ConvertOptions::Defaults());
  };
}

TEST(SerialReaderTests, Empty) { TestEmptyTable(MakeSerialFactory()); }
TEST(SerialReaderTests, HeaderOnly) { TestHeaderOnly(MakeSerialFactory()); }
TEST(SerialReaderTests, Straddling) { TestStraddling(MakeSerialFactory()); }
TEST(SerialReaderTests, QuotedCRLFStraddlingBlockBoundary) {
  TestQuotedCRLFStraddlingBlockBoundary(MakeSerialFactory());
}
TEST(SerialReaderTests, HeaderRowEndingAtBlockBoundary) {
  TestHeaderRowEndingAtBlockBoundary(MakeSerialFactory());
}
TEST(SerialReaderTests, MixedLineEndingsStraddling) {
  TestMixedLineEndingsStraddling(MakeSerialFactory());
}
TEST(SerialReaderTests, SmallInputsAcrossBlockSizes) {
  TestSmallInputsAcrossBlockSizes(MakeSerialFactory());
}
TEST(SerialReaderTests, Stress) { StressTableReader(MakeSerialFactory()); }
TEST(SerialReaderTests, StressInvalid) { StressInvalidTableReader(MakeSerialFactory()); }
TEST(SerialReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  TestNestedParallelism(thread_pool, MakeSerialFactory());
}
TEST(SerialReaderTests, InvalidRowsSkipped) {
  TestInvalidRowsSkipped(MakeSerialFactory(), /*async=*/false);
}

Result<TableReaderFactory> MakeAsyncFactory(
    std::shared_ptr<internal::ThreadPool> thread_pool = nullptr) {
  if (!thread_pool) {
    ARROW_ASSIGN_OR_RAISE(thread_pool, internal::ThreadPool::Make(1));
  }
  return [thread_pool](
             std::shared_ptr<io::InputStream> input_stream, ParseOptions parse_options,
             std::optional<int32_t> block_size) -> Result<std::shared_ptr<TableReader>> {
    ReadOptions read_options = ReadOptions::Defaults();
    read_options.use_threads = true;
    read_options.block_size = block_size.value_or(1 << 10);
    auto table_reader =
        TableReader::Make(io::IOContext(thread_pool.get()), input_stream, read_options,
                          std::move(parse_options), ConvertOptions::Defaults());
    return table_reader;
  };
}

TEST(AsyncReaderTests, Empty) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestEmptyTable(table_factory);
}
TEST(AsyncReaderTests, HeaderOnly) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestHeaderOnly(table_factory);
}
TEST(AsyncReaderTests, QuotedCRLFStraddlingBlockBoundary) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestQuotedCRLFStraddlingBlockBoundary(table_factory);
}
TEST(AsyncReaderTests, HeaderRowEndingAtBlockBoundary) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestHeaderRowEndingAtBlockBoundary(table_factory);
}
TEST(AsyncReaderTests, MixedLineEndingsStraddling) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestMixedLineEndingsStraddling(table_factory);
}
TEST(AsyncReaderTests, SmallInputsAcrossBlockSizes) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestSmallInputsAcrossBlockSizes(table_factory);
}
TEST(AsyncReaderTests, Straddling) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestStraddling(table_factory);
}
TEST(AsyncReaderTests, Stress) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  StressTableReader(table_factory);
}
TEST(AsyncReaderTests, StressInvalid) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  StressInvalidTableReader(table_factory);
}
TEST(AsyncReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory(thread_pool));
  TestNestedParallelism(thread_pool, table_factory);
}
TEST(AsyncReaderTests, InvalidRowsSkipped) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeAsyncFactory());
  TestInvalidRowsSkipped(table_factory, /*async=*/true);
}

TableReaderFactory MakeStreamingFactory(bool use_threads = true) {
  return [use_threads](
             std::shared_ptr<io::InputStream> input_stream, ParseOptions parse_options,
             std::optional<int32_t> block_size) -> Result<std::shared_ptr<TableReader>> {
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = block_size.value_or(1 << 10);
    read_options.use_threads = use_threads;
    ARROW_ASSIGN_OR_RAISE(
        auto streaming_reader,
        StreamingReader::Make(io::default_io_context(), input_stream, read_options,
                              std::move(parse_options), ConvertOptions::Defaults()));
    return std::make_shared<StreamingReaderAsTableReader>(std::move(streaming_reader));
  };
}

Result<StreamingReaderFactory> MakeStreamingReaderFactory() {
  return [](std::shared_ptr<io::InputStream> input_stream)
             -> Result<std::shared_ptr<StreamingReader>> {
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 1 << 10;
    read_options.use_threads = true;
    return StreamingReader::Make(io::default_io_context(), input_stream, read_options,
                                 ParseOptions::Defaults(), ConvertOptions::Defaults());
  };
}

TEST(StreamingReaderTests, Empty) { TestEmptyTable(MakeStreamingFactory()); }
TEST(StreamingReaderTests, HeaderOnly) {
  ASSERT_OK_AND_ASSIGN(auto table_factory, MakeStreamingReaderFactory());
  TestHeaderOnlyStreaming(table_factory);
}
TEST(StreamingReaderTests, Straddling) { TestStraddling(MakeStreamingFactory()); }
TEST(StreamingReaderTests, QuotedCRLFStraddlingBlockBoundary) {
  TestQuotedCRLFStraddlingBlockBoundary(MakeStreamingFactory());
}
TEST(StreamingReaderTests, HeaderRowEndingAtBlockBoundary) {
  TestHeaderRowEndingAtBlockBoundary(MakeStreamingFactory());
}
TEST(StreamingReaderTests, MixedLineEndingsStraddling) {
  TestMixedLineEndingsStraddling(MakeStreamingFactory());
}
TEST(StreamingReaderTests, SmallInputsAcrossBlockSizes) {
  TestSmallInputsAcrossBlockSizes(MakeStreamingFactory());
}
TEST(StreamingReaderTests, Stress) { StressTableReader(MakeStreamingFactory()); }
TEST(StreamingReaderTests, StressInvalid) {
  StressInvalidTableReader(MakeStreamingFactory());
}
TEST(StreamingReaderTests, NestedParallelism) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  TestNestedParallelism(thread_pool, MakeStreamingFactory());
}
TEST(StreamingReaderTests, InvalidRowsSkipped) {
  TestInvalidRowsSkipped(MakeStreamingFactory(/*use_threads=*/false), /*async=*/false);
}
TEST(StreamingReaderTests, InvalidRowsSkippedAsync) {
  if (internal::GetCpuThreadPool()->GetCapacity() < 2) {
    GTEST_SKIP() << "Test requires at least 2 threads";
  }
  TestInvalidRowsSkipped(MakeStreamingFactory(), /*async=*/true);
}

TEST(StreamingReaderTests, BytesRead) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  auto table_buffer =
      std::make_shared<Buffer>("a,b,c\n123,456,789\n101,112,131\n415,161,718\n");

  // Basic read without any skips and small block size
  {
    auto input = std::make_shared<io::BufferReader>(table_buffer);

    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 20;
    read_options.use_threads = false;
    ASSERT_OK_AND_ASSIGN(
        auto streaming_reader,
        StreamingReader::Make(io::default_io_context(), input, read_options,
                              ParseOptions::Defaults(), ConvertOptions::Defaults()));
    std::shared_ptr<RecordBatch> batch;
    int64_t bytes = 6;  // Size of header (counted during StreamingReader::Make)
    do {
      ASSERT_EQ(bytes, streaming_reader->bytes_read());
      ASSERT_OK(streaming_reader->ReadNext(&batch));
      bytes += 12;  // Add size of each row
    } while (bytes <= 42);
    ASSERT_EQ(42, streaming_reader->bytes_read());
    ASSERT_EQ(batch.get(), nullptr);
  }

  // Interaction of skip_rows and bytes_read()
  {
    auto input = std::make_shared<io::BufferReader>(table_buffer);

    auto read_options = ReadOptions::Defaults();
    read_options.skip_rows = 1;
    read_options.block_size = 32;
    ASSERT_OK_AND_ASSIGN(
        auto streaming_reader,
        StreamingReader::Make(io::default_io_context(), input, read_options,
                              ParseOptions::Defaults(), ConvertOptions::Defaults()));
    std::shared_ptr<RecordBatch> batch;
    // The header (6 bytes) and first skipped row (12 bytes) are counted during
    // StreamingReader::Make
    ASSERT_EQ(18, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_NE(batch.get(), nullptr);
    ASSERT_EQ(30, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_NE(batch.get(), nullptr);
    ASSERT_EQ(42, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_EQ(batch.get(), nullptr);
  }

  // Interaction of skip_rows_after_names and bytes_read()
  {
    auto input = std::make_shared<io::BufferReader>(table_buffer);

    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 32;
    read_options.skip_rows_after_names = 1;

    ASSERT_OK_AND_ASSIGN(
        auto streaming_reader,
        StreamingReader::Make(io::default_io_context(), input, read_options,
                              ParseOptions::Defaults(), ConvertOptions::Defaults()));
    std::shared_ptr<RecordBatch> batch;

    // The header is read as part of StreamingReader::Make
    ASSERT_EQ(6, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_NE(batch.get(), nullptr);
    // Next the skipped batch (12 bytes) and 1 row (12 bytes)
    ASSERT_EQ(30, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_NE(batch.get(), nullptr);
    ASSERT_EQ(42, streaming_reader->bytes_read());
    ASSERT_OK(streaming_reader->ReadNext(&batch));
    ASSERT_EQ(batch.get(), nullptr);
  }
}

TEST(StreamingReaderTests, SkipMultipleEmptyBlocksAtStart) {
  ASSERT_OK_AND_ASSIGN(auto thread_pool, internal::ThreadPool::Make(1));
  auto table_buffer = std::make_shared<Buffer>(
      "aaa,bbb,ccc\n123,456,789\n101,112,131\n415,161,718\n192,021,222\n324,252,627\n"
      "282,930,313\n233,343,536\n");

  auto input = std::make_shared<io::BufferReader>(table_buffer);

  auto read_options = ReadOptions::Defaults();
  read_options.block_size = 34;
  read_options.skip_rows_after_names = 6;

  ASSERT_OK_AND_ASSIGN(
      auto streaming_reader,
      StreamingReader::Make(io::default_io_context(), input, read_options,
                            ParseOptions::Defaults(), ConvertOptions::Defaults()));
  std::shared_ptr<RecordBatch> batch;
  ASSERT_EQ(12, streaming_reader->bytes_read());

  // The first batch should have the one and only row in it
  ASSERT_OK(streaming_reader->ReadNext(&batch));
  ASSERT_NE(nullptr, batch.get());
  ASSERT_EQ(1, batch->num_rows());
  ASSERT_EQ(96, streaming_reader->bytes_read());

  auto expected_schema =
      schema({field("aaa", int64()), field("bbb", int64()), field("ccc", int64())});
  AssertSchemaEqual(expected_schema, streaming_reader->schema());
  auto expected_batch = RecordBatchFromJSON(expected_schema, "[[233,343,536]]");
  ASSERT_TRUE(expected_batch->Equals(*batch));

  ASSERT_OK(streaming_reader->ReadNext(&batch));
  ASSERT_EQ(nullptr, batch.get());
}

TEST(CountRowsAsync, Basics) {
  constexpr int NROWS = 4096;
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(NROWS));
  {
    auto reader = std::make_shared<io::BufferReader>(table_buffer);
    auto read_options = ReadOptions::Defaults();
    auto parse_options = ParseOptions::Defaults();
    ASSERT_FINISHES_OK_AND_EQ(
        NROWS, CountRowsAsync(io::default_io_context(), reader,
                              internal::GetCpuThreadPool(), read_options, parse_options));
  }
  {
    auto reader = std::make_shared<io::BufferReader>(table_buffer);
    auto read_options = ReadOptions::Defaults();
    read_options.skip_rows = 20;
    auto parse_options = ParseOptions::Defaults();
    ASSERT_FINISHES_OK_AND_EQ(NROWS - 20, CountRowsAsync(io::default_io_context(), reader,
                                                         internal::GetCpuThreadPool(),
                                                         read_options, parse_options));
  }
  {
    auto reader = std::make_shared<io::BufferReader>(table_buffer);
    auto read_options = ReadOptions::Defaults();
    read_options.autogenerate_column_names = true;
    auto parse_options = ParseOptions::Defaults();
    ASSERT_FINISHES_OK_AND_EQ(NROWS + 1, CountRowsAsync(io::default_io_context(), reader,
                                                        internal::GetCpuThreadPool(),
                                                        read_options, parse_options));
  }
  {
    auto reader = std::make_shared<io::BufferReader>(table_buffer);
    auto read_options = ReadOptions::Defaults();
    read_options.block_size = 1024;
    auto parse_options = ParseOptions::Defaults();
    ASSERT_FINISHES_OK_AND_EQ(
        NROWS, CountRowsAsync(io::default_io_context(), reader,
                              internal::GetCpuThreadPool(), read_options, parse_options));
  }
}

TEST(CountRowsAsync, Errors) {
  ASSERT_OK_AND_ASSIGN(auto table_buffer, MakeSampleCsvBuffer(4096, [](size_t row_num) {
                         return row_num != 2048;
                       }));
  auto reader = std::make_shared<io::BufferReader>(table_buffer);
  auto read_options = ReadOptions::Defaults();
  auto parse_options = ParseOptions::Defaults();
  ASSERT_FINISHES_AND_RAISES(
      Invalid, CountRowsAsync(io::default_io_context(), reader,
                              internal::GetCpuThreadPool(), read_options, parse_options));
}

TEST(ReaderTests, DefaultColumnTypePartialDefault) {
  auto table_buffer = std::make_shared<Buffer>(
      "id,name,value,date\n"
      "0000101,apple,0003.1400,2024-01-15\n"
      "00102,banana,001.6180,2024-02-20\n"
      "0003,cherry,02.71800,2024-03-25\n");

  auto input = std::make_shared<io::BufferReader>(table_buffer);
  auto read_options = ReadOptions::Defaults();
  auto parse_options = ParseOptions::Defaults();
  auto convert_options = ConvertOptions::Defaults();
  convert_options.column_types["id"] = int64();
  convert_options.default_column_type = utf8();

  ASSERT_OK_AND_ASSIGN(auto reader,
                       TableReader::Make(io::default_io_context(), input, read_options,
                                         parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());

  auto expected_schema = schema({field("id", int64()), field("name", utf8()),
                                 field("value", utf8()), field("date", utf8())});
  AssertSchemaEqual(expected_schema, table->schema());

  auto expected_table = TableFromJSON(
      expected_schema,
      {R"([{"id":101, "name":"apple",  "value":"0003.1400", "date":"2024-01-15"},
            {"id":102, "name":"banana", "value":"001.6180", "date":"2024-02-20"},
            {"id":3,   "name":"cherry", "value":"02.71800", "date":"2024-03-25"}])"});
  ASSERT_TRUE(table->Equals(*expected_table));
}

TEST(ReaderTests, DefaultColumnTypeForcesTypedColumns) {
  auto table_buffer = std::make_shared<Buffer>(
      "id,amount,code\n"
      "0000404,000045.6700,001\n"
      "0000505,000000.10,010\n");

  auto input = std::make_shared<io::BufferReader>(table_buffer);
  auto read_options = ReadOptions::Defaults();
  auto parse_options = ParseOptions::Defaults();
  auto convert_options = ConvertOptions::Defaults();
  convert_options.default_column_type = utf8();

  ASSERT_OK_AND_ASSIGN(auto reader,
                       TableReader::Make(io::default_io_context(), input, read_options,
                                         parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());

  auto expected_schema =
      schema({field("id", utf8()), field("amount", utf8()), field("code", utf8())});
  AssertSchemaEqual(expected_schema, table->schema());

  auto expected_table = TableFromJSON(
      expected_schema, {R"([{"id":"0000404", "amount":"000045.6700", "code":"001"},
            {"id":"0000505", "amount":"000000.10", "code":"010"}])"});
  ASSERT_TRUE(table->Equals(*expected_table));
}

TEST(ReaderTests, DefaultColumnTypeAllStringsNoHeader) {
  // Input without header; autogenerate column names and default all to strings
  auto table_buffer = std::make_shared<Buffer>("AB|000388907|000045.6700\n");

  auto input = std::make_shared<io::BufferReader>(table_buffer);
  auto read_options = ReadOptions::Defaults();
  read_options.autogenerate_column_names = true;  // treat first row as data
  auto parse_options = ParseOptions::Defaults();
  parse_options.delimiter = '|';
  auto convert_options = ConvertOptions::Defaults();
  convert_options.default_column_type = utf8();

  ASSERT_OK_AND_ASSIGN(auto reader,
                       TableReader::Make(io::default_io_context(), input, read_options,
                                         parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());

  auto expected_schema =
      schema({field("f0", utf8()), field("f1", utf8()), field("f2", utf8())});
  AssertSchemaEqual(expected_schema, table->schema());

  auto expected_table = TableFromJSON(expected_schema, {R"([{
        "f0":"AB",
        "f1":"000388907",
        "f2":"000045.6700"
      }])"});
  ASSERT_TRUE(table->Equals(*expected_table));
}

TEST(ReaderTests, ShortRows) {
  auto input =
      std::make_shared<io::BufferReader>(std::make_shared<Buffer>("a,b,c\n1,2\n3,,"));
  auto read_options = ReadOptions::Defaults();
  read_options.block_size = 8;
  auto parse_options = ParseOptions::Defaults();
  parse_options.pad_short_rows = true;
  auto convert_options = ConvertOptions::Defaults();
  convert_options.default_column_type = utf8();
  convert_options.null_values.clear();
  convert_options.strings_can_be_null = false;

  ASSERT_OK_AND_ASSIGN(auto reader,
                       TableReader::Make(io::default_io_context(), input, read_options,
                                         parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());

  auto expected_schema =
      schema({field("a", utf8()), field("b", utf8()), field("c", utf8())});
  auto expected_table = TableFromJSON(expected_schema, {R"([{"a":"1", "b":"2", "c":null},
            {"a":"3", "b":"",  "c":""}])"});
  ASSERT_TRUE(table->Equals(*expected_table));
}

TEST(ReaderTests, IgnoreExtraColumns) {
  auto input =
      std::make_shared<io::BufferReader>(std::make_shared<Buffer>("a,b\n1,2,3\n4,5\n"));
  auto parse_options = ParseOptions::Defaults();
  parse_options.ignore_extra_columns = true;
  auto convert_options = ConvertOptions::Defaults();
  convert_options.default_column_type = int64();

  ASSERT_OK_AND_ASSIGN(auto reader, TableReader::Make(io::default_io_context(), input,
                                                      ReadOptions::Defaults(),
                                                      parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());

  auto expected_schema = schema({field("a", int64()), field("b", int64())});
  auto expected_table = TableFromJSON(expected_schema, {R"([
    {"a":1, "b":2},
    {"a":4, "b":5}
  ])"});
  ASSERT_TRUE(table->Equals(*expected_table));
}

TEST(ReaderTests, ShortRowsTypedConverters) {
  auto input = std::make_shared<io::BufferReader>(std::make_shared<Buffer>("1,10\n2\n"));
  auto read_options = ReadOptions::Defaults();
  read_options.autogenerate_column_names = true;
  auto parse_options = ParseOptions::Defaults();
  parse_options.pad_short_rows = true;
  auto convert_options = ConvertOptions::Defaults();
  convert_options.column_types["f0"] = int64();
  convert_options.column_types["f1"] = dictionary(int32(), utf8());
  ASSERT_OK_AND_ASSIGN(auto reader,
                       TableReader::Make(io::default_io_context(), input, read_options,
                                         parse_options, convert_options));
  ASSERT_OK_AND_ASSIGN(auto table, reader->Read());
  ASSERT_TRUE(table->column(0)->chunk(0)->Equals(*ArrayFromJSON(int64(), "[1, 2]")));
  const auto& dict_array =
      internal::checked_cast<const DictionaryArray&>(*table->column(1)->chunk(0));
  ASSERT_TRUE(dict_array.indices()->Equals(*ArrayFromJSON(int32(), "[0, null]")));
  ASSERT_TRUE(dict_array.dictionary()->Equals(*ArrayFromJSON(utf8(), "[\"10\"]")));
}

}  // namespace csv
}  // namespace arrow

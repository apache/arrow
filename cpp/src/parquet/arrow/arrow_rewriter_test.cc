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

#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/file_rewriter.h"
#ifdef _MSC_VER
#  pragma warning(push)
// Disable forcing value to bool warnings
#  pragma warning(disable : 4800)
#endif

#include <memory>

#include "gtest/gtest.h"

#include "parquet/arrow/test_util.h"
#include "parquet/platform.h"
#include "parquet/properties.h"

using arrow::Table;
using arrow::io::BufferReader;

namespace parquet::arrow {

TEST(ParquetRewriterTest, ConcatRoundTrip) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});

  std::shared_ptr<Buffer> buffer_up;
  std::shared_ptr<Buffer> buffer_down;

  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"]])"}), buffer_up);
  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(schema, {R"([[3, "c"]])"}), buffer_down);

  auto sink = CreateOutputStream();
  auto rewriter =
      ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer_up),
                                  std::make_shared<BufferReader>(buffer_down)}},
                                sink, {{NULLPTR, NULLPTR}}, NULLPTR, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      FileReader::Make(::arrow::default_memory_pool(), std::move(file_reader), &reader));

  std::shared_ptr<Table> table;
  ASSERT_OK(reader->ReadTable(&table));
  ASSERT_OK(table->ValidateFull());

  auto expected_table =
      ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, DISABLED_ExtendRoundTrip) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto left_schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});
  auto right_schema = ::arrow::schema({::arrow::field("c", ::arrow::int64())});

  std::shared_ptr<Buffer> buffer_left;
  std::shared_ptr<Buffer> buffer_right;

  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(left_schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"}),
            buffer_left);
  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(right_schema, {R"([[10], [20], [30]])"}),
            buffer_right);

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open(
      {{std::make_shared<BufferReader>(buffer_left)},
       {std::make_shared<BufferReader>(buffer_right)}},
      sink, {{NULLPTR}, {NULLPTR}}, NULLPTR, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      FileReader::Make(::arrow::default_memory_pool(), std::move(file_reader), &reader));

  std::shared_ptr<Table> table;
  ASSERT_OK(reader->ReadTable(&table));
  ASSERT_OK(table->ValidateFull());

  auto expected_schema = ::arrow::schema({::arrow::field("a", ::arrow::int32()),
                                          ::arrow::field("b", ::arrow::utf8()),
                                          ::arrow::field("c", ::arrow::int64())});
  auto expected_table = ::arrow::TableFromJSON(
      expected_schema, {R"([[1, "a", 10], [2, "b", 20], [3, "c", 30]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, DISABLED_SimpleRoundTrip) {
  auto rewriter_properties = RewriterProperties::Builder()
                                 .writer_properties(WriterProperties::Builder()
                                                        .enable_write_page_index()
                                                        ->max_row_group_length(1)
                                                        ->build())
                                 ->build();

  auto left_schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});
  auto right_schema = ::arrow::schema({::arrow::field("c", ::arrow::int64())});

  std::shared_ptr<Buffer> buffer_left_up;
  std::shared_ptr<Buffer> buffer_left_down;
  std::shared_ptr<Buffer> buffer_right_up;
  std::shared_ptr<Buffer> buffer_right_down;

  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(left_schema, {R"([[1, "a"], [2, "b"]])"}),
            buffer_left_up);
  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(left_schema, {R"([[3, "c"]])"}), buffer_left_down);
  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(right_schema, {R"([[10]])"}), buffer_right_up);
  WriteFile(rewriter_properties->writer_properties(),
            ::arrow::TableFromJSON(right_schema, {R"([[20], [30]])"}), buffer_right_down);

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open(
      {{std::make_shared<BufferReader>(buffer_left_up),
        std::make_shared<BufferReader>(buffer_left_down)},
       {std::make_shared<BufferReader>(buffer_right_up),
        std::make_shared<BufferReader>(buffer_right_down)}},
      sink, {{NULLPTR, NULLPTR}, {NULLPTR, NULLPTR}}, NULLPTR, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      FileReader::Make(::arrow::default_memory_pool(), std::move(file_reader), &reader));

  std::shared_ptr<Table> table;
  ASSERT_OK(reader->ReadTable(&table));
  ASSERT_OK(table->ValidateFull());

  auto expected_schema = ::arrow::schema({::arrow::field("a", ::arrow::int32()),
                                          ::arrow::field("b", ::arrow::utf8()),
                                          ::arrow::field("c", ::arrow::int64())});
  auto expected_table = ::arrow::TableFromJSON(
      expected_schema, {R"([[1, "a", 10], [2, "b", 20], [3, "c", 30]])"});
  AssertTablesEqual(*expected_table, *table);
}

}  // namespace parquet::arrow

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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "arrow/io/memory.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/config.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/test_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_rewriter.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/test_util.h"

using arrow::Table;
using arrow::io::BufferReader;

namespace parquet::arrow {

TEST(ParquetRewriterTest, SimpleRoundTrip) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().disable_write_page_index()->build())
          ->build();

  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});

  ASSERT_OK_AND_ASSIGN(auto buffer, WriteFile(rewriter_properties->writer_properties(),
                                              ::arrow::TableFromJSON(
                                                  schema, {R"([[1, "a"], [2, "b"]])"})));

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer)}},
                                            sink, {{NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));

  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_table = ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, ConcatRoundTrip) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});

  ASSERT_OK_AND_ASSIGN(
      auto buffer_up,
      WriteFile(rewriter_properties->writer_properties(),
                ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"]])"})));
  ASSERT_OK_AND_ASSIGN(auto buffer_down,
                       WriteFile(rewriter_properties->writer_properties(),
                                 ::arrow::TableFromJSON(schema, {R"([[3, "c"]])"})));

  auto sink = CreateOutputStream();
  auto rewriter =
      ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer_up),
                                  std::make_shared<BufferReader>(buffer_down)}},
                                sink, {{NULLPTR, NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));

  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_table =
      ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, JoinRoundTrip) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto left_schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});
  auto right_schema = ::arrow::schema({::arrow::field("c", ::arrow::int64())});

  ASSERT_OK_AND_ASSIGN(
      auto buffer_left,
      WriteFile(
          rewriter_properties->writer_properties(),
          ::arrow::TableFromJSON(left_schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"})));
  ASSERT_OK_AND_ASSIGN(
      auto buffer_right,
      WriteFile(rewriter_properties->writer_properties(),
                ::arrow::TableFromJSON(right_schema, {R"([[10], [20], [30]])"})));

  auto sink = CreateOutputStream();
  auto rewriter =
      ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer_left)},
                                 {std::make_shared<BufferReader>(buffer_right)}},
                                sink, {{NULLPTR}, {NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));

  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_schema = ::arrow::schema({::arrow::field("a", ::arrow::int32()),
                                          ::arrow::field("b", ::arrow::utf8()),
                                          ::arrow::field("c", ::arrow::int64())});
  auto expected_table = ::arrow::TableFromJSON(
      expected_schema, {R"([[1, "a", 10], [2, "b", 20], [3, "c", 30]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, ConcatJoinRoundTrip) {
  auto rewriter_properties = RewriterProperties::Builder()
                                 .writer_properties(WriterProperties::Builder()
                                                        .enable_write_page_index()
                                                        ->max_row_group_length(2)
                                                        ->build())
                                 ->build();

  auto left_schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});
  auto right_schema = ::arrow::schema({::arrow::field("c", ::arrow::int64())});

  ASSERT_OK_AND_ASSIGN(
      auto buffer_left_up,
      WriteFile(rewriter_properties->writer_properties(),
                ::arrow::TableFromJSON(left_schema, {R"([[1, "a"], [2, "b"]])"})));
  ASSERT_OK_AND_ASSIGN(auto buffer_left_down,
                       WriteFile(rewriter_properties->writer_properties(),
                                 ::arrow::TableFromJSON(left_schema, {R"([[3, "c"]])"})));
  ASSERT_OK_AND_ASSIGN(
      auto buffer_right,
      WriteFile(rewriter_properties->writer_properties(),
                ::arrow::TableFromJSON(right_schema, {R"([[10], [20], [30]])"})));

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open(
      {{std::make_shared<BufferReader>(buffer_left_up),
        std::make_shared<BufferReader>(buffer_left_down)},
       {std::make_shared<BufferReader>(buffer_right)}},
      sink, {{NULLPTR, NULLPTR}, {NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));
  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));

  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_schema = ::arrow::schema({::arrow::field("a", ::arrow::int32()),
                                          ::arrow::field("b", ::arrow::utf8()),
                                          ::arrow::field("c", ::arrow::int64())});
  auto expected_table = ::arrow::TableFromJSON(
      expected_schema, {R"([[1, "a", 10], [2, "b", 20], [3, "c", 30]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, JoinRowCountsMismatch) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto left_schema = ::arrow::schema({::arrow::field("a", ::arrow::int32())});
  auto right_schema = ::arrow::schema({::arrow::field("b", ::arrow::int32())});

  ASSERT_OK_AND_ASSIGN(auto buffer_left,
                       WriteFile(rewriter_properties->writer_properties(),
                                 ::arrow::TableFromJSON(left_schema, {R"([[1], [2]])"})));
  ASSERT_OK_AND_ASSIGN(
      auto buffer_right,
      WriteFile(rewriter_properties->writer_properties(),
                ::arrow::TableFromJSON(right_schema, {R"([[3], [4], [5]])"})));

  auto sink = CreateOutputStream();

  EXPECT_THROW_THAT(
      [&]() {
        ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer_left)},
                                   {std::make_shared<BufferReader>(buffer_right)}},
                                  sink, {{NULLPTR}, {NULLPTR}}, rewriter_properties);
      },
      ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr("The number of rows in each block must match")));
}

TEST(ParquetRewriterTest, InvalidInputDimensions) {
  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(
              WriterProperties::Builder().enable_write_page_index()->build())
          ->build();

  auto schema = ::arrow::schema({::arrow::field("a", ::arrow::int32())});

  ASSERT_OK_AND_ASSIGN(auto buffer,
                       WriteFile(rewriter_properties->writer_properties(),
                                 ::arrow::TableFromJSON(schema, {R"([[1]])"})));

  auto sink = CreateOutputStream();

  EXPECT_THROW_THAT(
      [&]() {
        ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer)}}, sink, {},
                                  rewriter_properties);
      },
      ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr(
              "The number of sources and sources_metadata must be the same")));

  EXPECT_THROW_THAT(
      [&]() {
        ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer)}}, sink, {{}},
                                  rewriter_properties);
      },
      ParquetException,
      ::testing::Property(
          &ParquetException::what,
          ::testing::HasSubstr(
              "The number of sources and sources_metadata must be the same")));
}

TEST(ParquetRewriterTest, AddCompression) {
#ifndef ARROW_WITH_ZLIB
  GTEST_SKIP() << "Test requires Gzip compression";
#endif

  auto original_writer_props = default_writer_properties();

  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});

  ASSERT_OK_AND_ASSIGN(
      auto buffer,
      WriteFile(original_writer_props,
                ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"})));

  {
    auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer));
    auto metadata = file_reader->metadata();
    ASSERT_EQ(1, metadata->num_row_groups());
    for (int i = 0; i < metadata->num_columns(); ++i) {
      ASSERT_EQ(Compression::UNCOMPRESSED,
                metadata->RowGroup(0)->ColumnChunk(i)->compression());
    }
  }

  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(WriterProperties::Builder(*original_writer_props)
                                 .compression(Compression::GZIP)
                                 ->build())
          ->build();

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer)}},
                                            sink, {{NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));

  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  for (int i = 0; i < metadata->num_columns(); ++i) {
    ASSERT_EQ(Compression::GZIP, metadata->RowGroup(0)->ColumnChunk(i)->compression());
  }

  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));
  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_table =
      ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"});
  AssertTablesEqual(*expected_table, *table);
}

TEST(ParquetRewriterTest, ChangeCompression) {
#if !defined(ARROW_WITH_ZLIB) || !defined(ARROW_WITH_SNAPPY)
  GTEST_SKIP() << "Test requires Gzip and Snappy compression";
#endif

  auto original_writer_props =
      WriterProperties::Builder().compression(Compression::SNAPPY)->build();

  auto schema = ::arrow::schema(
      {::arrow::field("a", ::arrow::int32()), ::arrow::field("b", ::arrow::utf8())});

  ASSERT_OK_AND_ASSIGN(
      auto buffer,
      WriteFile(original_writer_props,
                ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"})));

  {
    auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(buffer));
    auto metadata = file_reader->metadata();
    ASSERT_EQ(1, metadata->num_row_groups());
    for (int i = 0; i < metadata->num_columns(); ++i) {
      ASSERT_EQ(Compression::SNAPPY,
                metadata->RowGroup(0)->ColumnChunk(i)->compression());
    }
  }

  auto rewriter_properties =
      RewriterProperties::Builder()
          .writer_properties(WriterProperties::Builder(*original_writer_props)
                                 .compression(Compression::GZIP)
                                 ->build())
          ->build();

  auto sink = CreateOutputStream();
  auto rewriter = ParquetFileRewriter::Open({{std::make_shared<BufferReader>(buffer)}},
                                            sink, {{NULLPTR}}, rewriter_properties);
  rewriter->Rewrite();
  rewriter->Close();

  ASSERT_OK_AND_ASSIGN(auto out_buffer, sink->Finish());
  auto file_reader = ParquetFileReader::Open(std::make_shared<BufferReader>(out_buffer));

  auto metadata = file_reader->metadata();
  ASSERT_EQ(1, metadata->num_row_groups());
  for (int i = 0; i < metadata->num_columns(); ++i) {
    ASSERT_EQ(Compression::GZIP, metadata->RowGroup(0)->ColumnChunk(i)->compression());
  }

  ASSERT_OK_AND_ASSIGN(auto reader, FileReader::Make(::arrow::default_memory_pool(),
                                                     std::move(file_reader)));
  ASSERT_OK_AND_ASSIGN(auto table, reader->ReadTable());
  ASSERT_OK(table->ValidateFull());

  auto expected_table =
      ::arrow::TableFromJSON(schema, {R"([[1, "a"], [2, "b"], [3, "c"]])"});
  AssertTablesEqual(*expected_table, *table);
}

}  // namespace parquet::arrow

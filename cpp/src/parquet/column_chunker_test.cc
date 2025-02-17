// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/schema.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"
#include "parquet/page_index.h"
#include "parquet/test_util.h"

namespace parquet {

using ::arrow::Array;
using ::arrow::ChunkedArray;
using ::arrow::ConcatenateTables;
using ::arrow::default_memory_pool;
using ::arrow::Field;
using ::arrow::Result;
using ::arrow::Table;
using ::arrow::io::BufferReader;
using ::arrow::random::GenerateArray;
using ::arrow::random::GenerateBatch;
using ::parquet::arrow::FileReader;
using ::parquet::arrow::FileReaderBuilder;
using ::parquet::arrow::MakeSimpleTable;
using ::parquet::arrow::NonNullArray;
using ::parquet::arrow::WriteTable;

using ::testing::Bool;
using ::testing::Combine;
using ::testing::Values;

std::shared_ptr<Table> GenerateTable(const std::vector<std::shared_ptr<Field>>& fields,
                                     int64_t size, int32_t seed = 42) {
  auto batch = GenerateBatch(fields, size, seed);
  return Table::FromRecordBatches({batch}).ValueOrDie();
}

std::shared_ptr<Table> ConcatAndCombine(
    const std::vector<std::shared_ptr<Table>>& parts) {
  auto table = ConcatenateTables(parts).ValueOrDie();
  return table->CombineChunks().ValueOrDie();
}

Result<std::shared_ptr<Buffer>> WriteTableToBuffer(const std::shared_ptr<Table>& table,
                                                   uint64_t min_chunk_size,
                                                   uint64_t max_chunk_size,
                                                   int64_t row_group_size = 1024 * 1024) {
  auto sink = CreateOutputStream();

  auto write_props = WriterProperties::Builder()
                         .disable_dictionary()
                         ->enable_cdc()
                         ->cdc_size_range(min_chunk_size, max_chunk_size)
                         ->build();
  auto arrow_props = default_arrow_writer_properties();
  RETURN_NOT_OK(WriteTable(*table, default_memory_pool(), sink, row_group_size,
                           write_props, arrow_props));
  return sink->Finish();
}

Result<std::shared_ptr<Table>> ReadTableFromBuffer(const std::shared_ptr<Buffer>& data) {
  std::shared_ptr<Table> result;
  FileReaderBuilder builder;
  std::unique_ptr<FileReader> reader;
  RETURN_NOT_OK(builder.Open(std::make_shared<BufferReader>(data)));
  RETURN_NOT_OK(builder.memory_pool(::arrow::default_memory_pool())
                    ->properties(default_arrow_reader_properties())
                    ->Build(&reader));
  RETURN_NOT_OK(reader->ReadTable(&result));
  return result;
}

std::vector<uint64_t> GetColumnPageLengths(const std::shared_ptr<Buffer>& data,
                                           int column_index = 0) {
  std::vector<uint64_t> page_lengths;

  auto buffer_reader = std::make_shared<BufferReader>(data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));

  auto metadata = parquet_reader->metadata();
  for (int rg = 0; rg < metadata->num_row_groups(); rg++) {
    auto page_reader = parquet_reader->RowGroup(rg)->GetColumnPageReader(column_index);
    while (auto page = page_reader->NextPage()) {
      if (page->type() == PageType::DATA_PAGE || page->type() == PageType::DATA_PAGE_V2) {
        auto data_page = static_cast<DataPage*>(page.get());
        page_lengths.push_back(data_page->num_values());
      }
    }
  }

  return page_lengths;
}

Result<std::vector<uint64_t>> WriteAndGetPageLengths(const std::shared_ptr<Table>& table,
                                                     uint64_t min_chunk_size,
                                                     uint64_t max_chunk_size,
                                                     int column_index = 0) {
  ARROW_ASSIGN_OR_RAISE(auto buffer,
                        WriteTableToBuffer(table, min_chunk_size, max_chunk_size));
  ARROW_ASSIGN_OR_RAISE(auto readback, ReadTableFromBuffer(buffer));

  RETURN_NOT_OK(readback->ValidateFull());
  ARROW_RETURN_IF(!readback->Equals(*table),
                  Status::Invalid("Readback table not equal to original"));
  return GetColumnPageLengths(buffer, column_index);
}

void AssertAllBetween(const std::vector<uint64_t>& values, uint64_t min, uint64_t max) {
  // expect the last chunk since it is not guaranteed to be within the range
  for (size_t i = 0; i < values.size() - 1; i++) {
    ASSERT_GE(values[i], min);
    ASSERT_LE(values[i], max);
  }
  ASSERT_LE(values.back(), max);
}

void AssertUpdateCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified) {
  ASSERT_EQ(original.size(), modified.size());
  for (size_t i = 0; i < original.size(); i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
}

void AssertDeleteCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified,
                      uint8_t n_modifications = 1) {
  ASSERT_EQ(original.size(), modified.size());
  size_t smaller_count = 0;
  for (size_t i = 0; i < original.size(); i++) {
    if (modified[i] < original[i]) {
      smaller_count++;
      ASSERT_LT(modified[i], original[i]);
    } else {
      ASSERT_EQ(modified[i], original[i]);
    }
  }
  ASSERT_EQ(smaller_count, n_modifications);
}

void AssertInsertCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified,
                      uint8_t n_modifications = 1) {
  ASSERT_EQ(original.size(), modified.size());
  size_t larger_count = 0;
  for (size_t i = 0; i < original.size(); i++) {
    if (modified[i] > original[i]) {
      larger_count++;
      ASSERT_GT(modified[i], original[i]);
    } else {
      ASSERT_EQ(modified[i], original[i]);
    }
  }
  ASSERT_EQ(larger_count, n_modifications);
}

void AssertAppendCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified) {
  ASSERT_GE(modified.size(), original.size());
  for (size_t i = 0; i < original.size() - 1; i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
  ASSERT_GT(modified[original.size() - 1], original.back());
}

uint64_t ElementCount(uint64_t size, int32_t byte_width, bool nullable) {
  if (nullable) {
    byte_width += 2;
  }
  return size / byte_width;
}

constexpr uint64_t kMinChunkSize = 128 * 1024;
constexpr uint64_t kMaxChunkSize = 256 * 1024;

// TODO:
// - test nullable types
// - test nested types
// - test dictionary encoding
// - test multiple row groups

class TestColumnChunker : public ::testing::TestWithParam<
                              std::tuple<std::shared_ptr<::arrow::DataType>, bool>> {};

TEST_P(TestColumnChunker, DeleteOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part3});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertDeleteCase(base_lengths, modified_lengths, 1);
}

TEST_P(TestColumnChunker, DeleteTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 32, /*seed=*/2);
  auto part5 = GenerateTable({field}, 128 * 1024);

  auto base = ConcatAndCombine({part1, part2, part3, part4, part5});
  auto modified = ConcatAndCombine({part1, part3, part5});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertDeleteCase(base_lengths, modified_lengths, 2);
}

TEST_P(TestColumnChunker, UpdateOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 32, /*seed=*/2);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part4, part3});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertUpdateCase(base_lengths, modified_lengths);
}

TEST_P(TestColumnChunker, UpdateTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 32, /*seed=*/2);
  auto part5 = GenerateTable({field}, 128 * 1024);
  auto part6 = GenerateTable({field}, 32, /*seed=*/3);
  auto part7 = GenerateTable({field}, 32, /*seed=*/4);

  auto base = ConcatAndCombine({part1, part2, part3, part4, part5});
  auto modified = ConcatAndCombine({part1, part6, part3, part7, part5});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertUpdateCase(base_lengths, modified_lengths);
}

TEST_P(TestColumnChunker, InsertOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 64);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part2, part4, part3});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertInsertCase(base_lengths, modified_lengths, 1);
}

TEST_P(TestColumnChunker, InsertTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 32, /*seed=*/2);
  auto part5 = GenerateTable({field}, 128 * 1024);
  auto part6 = GenerateTable({field}, 64);
  auto part7 = GenerateTable({field}, 64);

  auto base = ConcatAndCombine({part1, part2, part3, part4, part5});
  auto modified = ConcatAndCombine({part1, part2, part6, part3, part4, part7, part5});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertInsertCase(base_lengths, modified_lengths, 2);
}

TEST_P(TestColumnChunker, Append) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 32 * 1024);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part2, part3, part4});

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize));

  AssertAllBetween(base_lengths, min_length, max_length);
  AssertAllBetween(modified_lengths, min_length, max_length);
  AssertAppendCase(base_lengths, modified_lengths);
}

INSTANTIATE_TEST_SUITE_P(
    TypeRoundtrip, TestColumnChunker,
    Combine(Values(::arrow::uint8(), ::arrow::uint16(), ::arrow::uint32(),
                   ::arrow::uint64(), ::arrow::int8(), ::arrow::int16(), ::arrow::int32(),
                   ::arrow::int64(), ::arrow::float16(), ::arrow::float32(),
                   ::arrow::float64()),
            Bool()));

}  // namespace parquet

// - check that the state is maintained across rowgroups, so the edits should be
// consistent
// - check that the edits are consistent between writes
// - some smoke testing like approach would be nice to test several arrow types

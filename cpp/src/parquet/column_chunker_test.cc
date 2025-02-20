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

std::vector<uint64_t> WriteAndGetPageLengths(const std::shared_ptr<Table>& table,
                                             uint64_t min_chunk_size,
                                             uint64_t max_chunk_size,
                                             int column_index = 0) {
  auto buffer = WriteTableToBuffer(table, min_chunk_size, max_chunk_size).ValueOrDie();
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
                      const std::vector<uint64_t>& modified) {
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
  ASSERT_EQ(smaller_count, 1);
}

void AssertInsertCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified) {
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
  ASSERT_EQ(larger_count, 1);
}

void AssertAppendCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified) {
  ASSERT_GE(modified.size(), original.size());
  for (size_t i = 0; i < original.size() - 1; i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
  ASSERT_GT(modified[original.size()], original[original.size()]);
}

// TODO:
// - test multiple edits
// - test nullable types
// - test nested types
// - test dictionary encoding
// - test multiple row groups

class TestColumnChunker : public ::testing::Test {};

TEST_F(TestColumnChunker, BasicOperation) {
  auto dtype = ::arrow::uint64();
  auto field = ::arrow::field("f0", dtype, false);

  auto part1 = GenerateTable({field}, 128 * 1024);
  auto part2 = GenerateTable({field}, 32, /*seed=*/1);
  auto part3 = GenerateTable({field}, 128 * 1024);
  auto part4 = GenerateTable({field}, 64);
  auto part5 = GenerateTable({field}, 32 * 1024);
  auto part6 = GenerateTable({field}, 32, /*seed=*/2);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto updated = ConcatAndCombine({part1, part6, part3});
  auto deleted = ConcatAndCombine({part1, part3});
  auto inserted = ConcatAndCombine({part1, part2, part4, part3});
  auto appended = ConcatAndCombine({part1, part2, part3, part5});

  auto min_size = 128 * 1024;
  auto max_size = 256 * 1024;

  auto base_lengths = WriteAndGetPageLengths(base, min_size, max_size);
  auto updated_lengths = WriteAndGetPageLengths(updated, min_size, max_size);
  auto deleted_lengths = WriteAndGetPageLengths(deleted, min_size, max_size);
  auto inserted_lengths = WriteAndGetPageLengths(inserted, min_size, max_size);
  auto appended_lengths = WriteAndGetPageLengths(appended, min_size, max_size);

  AssertAllBetween(base_lengths, min_size / 8, max_size / 8);
  AssertAllBetween(updated_lengths, min_size / 8, max_size / 8);
  AssertAllBetween(deleted_lengths, min_size / 8, max_size / 8);
  AssertAllBetween(inserted_lengths, min_size / 8, max_size / 8);
  AssertAllBetween(appended_lengths, min_size / 8, max_size / 8);

  AssertUpdateCase(base_lengths, updated_lengths);
  AssertDeleteCase(base_lengths, deleted_lengths);
  AssertInsertCase(base_lengths, inserted_lengths);
  AssertAppendCase(base_lengths, appended_lengths);
}

}  // namespace parquet

// - check that the state is maintained across rowgroups, so the edits should be
// consistent
// - check that the edits are consistent between writes
// - some smoke testing like approach would be nice to test several arrow types

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

#include <gtest/gtest.h>
#include <algorithm>
#include <utility>
#include <vector>

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
                                                   bool enable_dictionary = false,

                                                   int64_t row_group_size = 1024 * 1024) {
  auto sink = CreateOutputStream();

  auto builder = WriterProperties::Builder();
  // enable content defined chunking
  builder.enable_cdc()->cdc_size_range(min_chunk_size, max_chunk_size);
  if (enable_dictionary) {
    builder.enable_dictionary();
  } else {
    builder.disable_dictionary();
  }
  auto write_props = builder.build();
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

                                                     bool enable_dictionary = false,
                                                     int column_index = 0) {
  ARROW_ASSIGN_OR_RAISE(
      auto buffer,
      WriteTableToBuffer(table, min_chunk_size, max_chunk_size, enable_dictionary));
  ARROW_ASSIGN_OR_RAISE(auto readback, ReadTableFromBuffer(buffer));

  RETURN_NOT_OK(readback->ValidateFull());
  ARROW_RETURN_IF(!readback->Equals(*table),
                  Status::Invalid("Readback table not equal to original"));
  return GetColumnPageLengths(buffer, column_index);
}

void AssertAllBetween(const std::vector<uint64_t>& values, uint64_t min, uint64_t max,
                      bool expect_dictionary_fallback = false) {
  // expect the last chunk since it is not guaranteed to be within the range
  if (expect_dictionary_fallback) {
    // if dictionary encoding is enabled, the writer can fallback to plain
    // encoding splitting within a content defined chunk, so we can't
    // guarantee that all chunks are within the range in this case, but we
    // know that there can be at most 2 pages smaller than the min_chunk_size
    size_t smaller_count = 0;
    for (size_t i = 0; i < values.size() - 1; i++) {
      if (values[i] < min) {
        smaller_count++;
      } else {
        ASSERT_LE(values[i], max);
      }
    }
    ASSERT_LE(smaller_count, 2);
  } else {
    for (size_t i = 0; i < values.size() - 1; i++) {
      ASSERT_GE(values[i], min);
      ASSERT_LE(values[i], max);
    }
  }
  ASSERT_LE(values.back(), max);
}

std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> FindDifferences(
    const std::vector<uint64_t>& first, const std::vector<uint64_t>& second) {
  auto n = first.size(), m = second.size();

  // Build DP table for LCS.
  std::vector<std::vector<size_t>> dp(n + 1, std::vector<size_t>(m + 1, 0));
  for (size_t i = 0; i < n; ++i) {
    for (size_t j = 0; j < m; ++j) {
      dp[i + 1][j + 1] =
          (first[i] == second[j]) ? dp[i][j] + 1 : std::max(dp[i + 1][j], dp[i][j + 1]);
    }
  }

  // Backtrack to recover LCS indices.
  std::vector<std::pair<size_t, size_t>> common;
  for (auto i = n, j = m; i > 0 && j > 0;) {
    if (first[i - 1] == second[j - 1]) {
      common.emplace_back(i - 1, j - 1);
      --i, --j;
    } else if (dp[i - 1][j] >= dp[i][j - 1]) {
      --i;
    } else {
      --j;
    }
  }
  std::reverse(common.begin(), common.end());

  // Extract differences using the common indices as anchors.
  std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> result;
  size_t last_i = 0, last_j = 0;
  for (auto [ci, cj] : common) {
    std::vector<uint64_t> diff1(first.begin() + last_i, first.begin() + ci);
    std::vector<uint64_t> diff2(second.begin() + last_j, second.begin() + cj);
    if (!diff1.empty() || !diff2.empty()) {
      result.emplace_back(std::move(diff1), std::move(diff2));
    }
    last_i = ci + 1;
    last_j = cj + 1;
  }
  // Add any remaining elements after the last common index.
  std::vector<uint64_t> diff1(first.begin() + last_i, first.end());
  std::vector<uint64_t> diff2(second.begin() + last_j, second.end());
  if (!diff1.empty() || !diff2.empty()) {
    result.emplace_back(std::move(diff1), std::move(diff2));
  }

  return result;
}

TEST(TestFindDifferences, Basic) {
  std::vector<uint64_t> first = {1, 2, 3, 4, 5};
  std::vector<uint64_t> second = {1, 7, 8, 4, 5};

  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, std::vector<uint64_t>({2, 3}));
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({7, 8}));
}

TEST(TestFindDifferences, MultipleDifferences) {
  std::vector<uint64_t> first = {1, 2, 3, 4, 5, 6, 7};
  std::vector<uint64_t> second = {1, 8, 9, 4, 10, 6, 11};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 3);

  ASSERT_EQ(diffs[0].first, std::vector<uint64_t>({2, 3}));
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({8, 9}));

  ASSERT_EQ(diffs[1].first, std::vector<uint64_t>({5}));
  ASSERT_EQ(diffs[1].second, std::vector<uint64_t>({10}));

  ASSERT_EQ(diffs[2].first, std::vector<uint64_t>({7}));
  ASSERT_EQ(diffs[2].second, std::vector<uint64_t>({11}));
}

TEST(TestFindDifferences, DifferentLengths) {
  std::vector<uint64_t> first = {1, 2, 3};
  std::vector<uint64_t> second = {1, 2, 3, 4, 5};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_TRUE(diffs[0].first.empty());
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({4, 5}));
}

TEST(TestFindDifferences, EmptyArrays) {
  std::vector<uint64_t> first = {};
  std::vector<uint64_t> second = {};
  auto diffs = FindDifferences(first, second);
  ASSERT_TRUE(diffs.empty());
}

TEST(TestFindDifferences, LongSequenceWithSingleDifference) {
  std::vector<uint64_t> first = {
      1994, 2193, 2700, 1913, 2052,
  };
  std::vector<uint64_t> second = {2048, 43, 2080, 2700, 1913, 2052};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, std::vector<uint64_t>({1994, 2193}));
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({2048, 43, 2080}));

  // Verify that elements after the difference are identical
  for (size_t i = 3; i < second.size(); i++) {
    ASSERT_EQ(first[i - 1], second[i]);
  }
}

TEST(TestFindDifferences, LongSequenceWithMiddleChanges) {
  std::vector<uint64_t> first = {2169, 1976, 2180, 2147, 1934, 1772,
                                 1914, 2075, 2154, 1940, 1934, 1970};
  std::vector<uint64_t> second = {2169, 1976, 2180, 2147, 2265, 1804,
                                  1717, 1925, 2122, 1940, 1934, 1970};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, std::vector<uint64_t>({1934, 1772, 1914, 2075, 2154}));
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({2265, 1804, 1717, 1925, 2122}));

  // Verify elements before and after the difference are identical
  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(first[i], second[i]);
  }
  for (size_t i = 9; i < first.size(); i++) {
    ASSERT_EQ(first[i], second[i]);
  }
}

void AssertUpdateCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_LE(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    uint64_t left_sum = 0, right_sum = 0;
    for (const auto& val : diff.first) left_sum += val;
    for (const auto& val : diff.second) right_sum += val;
    ASSERT_EQ(left_sum, right_sum);
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }

  if (diffs.size() == 0) {
    // no differences found, the arrays are equal
    ASSERT_TRUE(original == modified);
  }
}

void AssertDeleteCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications,
                      uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    uint64_t left_sum = 0, right_sum = 0;
    for (const auto& val : diff.first) left_sum += val;
    for (const auto& val : diff.second) right_sum += val;
    ASSERT_EQ(left_sum, right_sum + edit_length);
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }
}

void AssertInsertCase(const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications,
                      uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    uint64_t left_sum = 0, right_sum = 0;
    for (const auto& val : diff.first) left_sum += val;
    for (const auto& val : diff.second) right_sum += val;
    ASSERT_EQ(left_sum + edit_length, right_sum);
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }
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
    // in case of nullable types the def_levels are also fed through the chunker
    // to identify changes in the null bitmap, this will increase the byte width
    // and decrease the number of elements per chunk
    byte_width += 2;
  }
  return size / byte_width;
}

constexpr uint64_t kMinChunkSize = 32 * 1024;
constexpr uint64_t kMaxChunkSize = 128 * 1024;
constexpr uint64_t kPartLength = 128 * 1024;
constexpr uint64_t kEditLength = 32;

// TODO:
// - test nested types
// - test multiple row groups

class TestContentDefinedChunker
    : public ::testing::TestWithParam<
          std::tuple<std::shared_ptr<::arrow::DataType>, bool, bool>> {};

TEST_P(TestContentDefinedChunker, DeleteOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part3});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);

  AssertDeleteCase(base_lengths, modified_lengths, 1, kEditLength);
}

TEST_P(TestContentDefinedChunker, DeleteTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);
  auto part4 = GenerateTable({field}, kEditLength, /*seed=*/4);
  auto part5 = GenerateTable({field}, kPartLength, /*seed=*/5);

  auto base = ConcatAndCombine({part1, part2, part3, part4, part5});
  auto modified = ConcatAndCombine({part1, part3, part5});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertDeleteCase(base_lengths, modified_lengths, 2, kEditLength);
}

TEST_P(TestContentDefinedChunker, UpdateOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);
  auto part4 = GenerateTable({field}, kEditLength, /*seed=*/4);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part4, part3});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertUpdateCase(base_lengths, modified_lengths, 1);
}

TEST_P(TestContentDefinedChunker, UpdateTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);
  auto part4 = GenerateTable({field}, kEditLength, /*seed=*/4);
  auto part5 = GenerateTable({field}, kPartLength, /*seed=*/5);
  auto part6 = GenerateTable({field}, kEditLength, /*seed=*/6);
  auto part7 = GenerateTable({field}, kEditLength, /*seed=*/7);

  auto base = ConcatAndCombine({part1, part2, part3, part4, part5});
  auto modified = ConcatAndCombine({part1, part6, part3, part7, part5});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertUpdateCase(base_lengths, modified_lengths, 2);
}

TEST_P(TestContentDefinedChunker, InsertOnce) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);

  auto base = ConcatAndCombine({part1, part3});
  auto modified = ConcatAndCombine({part1, part2, part3});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertInsertCase(base_lengths, modified_lengths, 1, kEditLength);
}

TEST_P(TestContentDefinedChunker, InsertTwice) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());
  enable_dictionary = false;

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);
  auto part4 = GenerateTable({field}, kEditLength, /*seed=*/4);
  auto part5 = GenerateTable({field}, kPartLength, /*seed=*/5);

  auto base = ConcatAndCombine({part1, part3, part5});
  auto modified = ConcatAndCombine({part1, part2, part3, part4, part5});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertInsertCase(base_lengths, modified_lengths, 2, kEditLength);
}

TEST_P(TestContentDefinedChunker, Append) {
  auto dtype = std::get<0>(GetParam());
  auto nullable = std::get<1>(GetParam());
  auto enable_dictionary = std::get<2>(GetParam());

  auto field = ::arrow::field("f0", dtype, nullable);

  auto part1 = GenerateTable({field}, kPartLength, /*seed=*/1);
  auto part2 = GenerateTable({field}, kEditLength, /*seed=*/2);
  auto part3 = GenerateTable({field}, kPartLength, /*seed=*/3);
  auto part4 = GenerateTable({field}, kEditLength, /*seed=*/4);

  auto base = ConcatAndCombine({part1, part2, part3});
  auto modified = ConcatAndCombine({part1, part2, part3, part4});
  ASSERT_FALSE(base->Equals(*modified));

  auto min_length = ElementCount(kMinChunkSize, dtype->byte_width(), nullable);
  auto max_length = ElementCount(kMaxChunkSize, dtype->byte_width(), nullable);

  ASSERT_OK_AND_ASSIGN(auto base_lengths,
                       WriteAndGetPageLengths(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
  ASSERT_OK_AND_ASSIGN(auto modified_lengths,
                       WriteAndGetPageLengths(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

  AssertAllBetween(base_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAllBetween(modified_lengths, min_length, max_length,
                   /*expect_dictionary_fallback=*/enable_dictionary);
  AssertAppendCase(base_lengths, modified_lengths);
}

INSTANTIATE_TEST_SUITE_P(
    FixedSizedTypes, TestContentDefinedChunker,
    Combine(Values(::arrow::uint8(), ::arrow::uint16(), ::arrow::uint32(),
                   ::arrow::uint64(), ::arrow::int8(), ::arrow::int16(), ::arrow::int32(),
                   ::arrow::int64(), ::arrow::float16(), ::arrow::float32(),
                   ::arrow::float64()),
            Bool(), Bool()));

}  // namespace parquet

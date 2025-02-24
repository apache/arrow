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
#include "arrow/util/logging.h"
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
  // Concatenate and combine chunks so the table doesn't carry information about
  // the modification points
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
  builder.enable_cdc()
      ->cdc_size_range(min_chunk_size, max_chunk_size)
      ->cdc_norm_factor(0);
  if (enable_dictionary) {
    builder.enable_dictionary();
  } else {
    builder.disable_dictionary();
  }
  auto write_props = builder.build();
  auto arrow_props = ArrowWriterProperties::Builder().store_schema()->build();
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

struct PageSizes {
  std::vector<uint64_t> lengths;
  std::vector<uint64_t> sizes;
};

PageSizes GetColumnPageSizes(const std::shared_ptr<Buffer>& data, int column_index = 0) {
  // Read the parquet data out of the buffer and get the sizes and lengths of the
  // data pages in given column. We assert on the sizes and lengths of the pages
  // to ensure that the chunking is done correctly.
  PageSizes result;

  auto buffer_reader = std::make_shared<BufferReader>(data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));

  auto metadata = parquet_reader->metadata();
  for (int rg = 0; rg < metadata->num_row_groups(); rg++) {
    auto page_reader = parquet_reader->RowGroup(rg)->GetColumnPageReader(column_index);
    while (auto page = page_reader->NextPage()) {
      if (page->type() == PageType::DATA_PAGE || page->type() == PageType::DATA_PAGE_V2) {
        auto data_page = static_cast<DataPage*>(page.get());
        result.sizes.push_back(data_page->size());
        result.lengths.push_back(data_page->num_values());
      }
    }
  }

  return result;
}

Result<PageSizes> WriteAndGetPageSizes(const std::shared_ptr<Table>& table,
                                       uint64_t min_chunk_size, uint64_t max_chunk_size,
                                       bool enable_dictionary = false,
                                       int column_index = 0) {
  // Write the table to a buffer and read it back to get the page sizes
  ARROW_ASSIGN_OR_RAISE(
      auto buffer,
      WriteTableToBuffer(table, min_chunk_size, max_chunk_size, enable_dictionary));
  ARROW_ASSIGN_OR_RAISE(auto readback, ReadTableFromBuffer(buffer));

  RETURN_NOT_OK(readback->ValidateFull());
  if (readback->schema()->Equals(*table->schema())) {
    ARROW_RETURN_IF(!readback->Equals(*table),
                    Status::Invalid("Readback table not equal to original"));
  }
  return GetColumnPageSizes(buffer, column_index);
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
  // Compute LCS table.
  size_t n = first.size(), m = second.size();
  std::vector<std::vector<size_t>> dp(n + 1, std::vector<size_t>(m + 1, 0));
  for (size_t i = 0; i < n; i++) {
    for (size_t j = 0; j < m; j++) {
      if (first[i] == second[j]) {
        dp[i + 1][j + 1] = dp[i][j] + 1;
      } else {
        dp[i + 1][j + 1] = std::max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
  }

  // Backtrack to get common indices.
  std::vector<std::pair<size_t, size_t>> common;
  for (size_t i = n, j = m; i > 0 && j > 0;) {
    if (first[i - 1] == second[j - 1]) {
      common.emplace_back(i - 1, j - 1);
      i--, j--;
    } else if (dp[i - 1][j] >= dp[i][j - 1]) {
      i--;
    } else {
      j--;
    }
  }
  std::reverse(common.begin(), common.end());

  // Build raw differences.
  std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> result;
  size_t last_i = 0, last_j = 0;
  for (auto& c : common) {
    auto ci = c.first;
    auto cj = c.second;
    if (ci > last_i || cj > last_j) {
      result.push_back({{first.begin() + last_i, first.begin() + ci},
                        {second.begin() + last_j, second.begin() + cj}});
    }
    last_i = ci + 1;
    last_j = cj + 1;
  }
  if (last_i < n || last_j < m) {
    result.push_back(
        {{first.begin() + last_i, first.end()}, {second.begin() + last_j, second.end()}});
  }

  // Merge adjacent diffs if one side is empty in the first diff and the other side
  // is empty in the next diff, to avoid splitting single changes into two parts.
  std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> merged;
  for (auto& diff : result) {
    if (!merged.empty()) {
      auto& prev = merged.back();
      bool can_merge_a = prev.first.empty() && !prev.second.empty() &&
                         !diff.first.empty() && diff.second.empty();
      bool can_merge_b = prev.second.empty() && !prev.first.empty() &&
                         !diff.second.empty() && diff.first.empty();
      if (can_merge_a) {
        // Combine into one change
        prev.first = std::move(diff.first);
        continue;
      } else if (can_merge_b) {
        prev.second = std::move(diff.second);
        continue;
      }
    }
    merged.push_back(std::move(diff));
  }

  return merged;
}

void PrintDifferences(
    const std::vector<uint64_t>& original, const std::vector<uint64_t>& modified,
    std::vector<std::pair<std::vector<uint64_t>, std::vector<uint64_t>>>& diffs) {
  std::cout << "Original: ";
  for (const auto& val : original) {
    std::cout << val << " ";
  }
  std::cout << std::endl;

  std::cout << "Modified: ";
  for (const auto& val : modified) {
    std::cout << val << " ";
  }
  std::cout << std::endl;

  for (const auto& diff : diffs) {
    std::cout << "First: ";
    for (const auto& val : diff.first) {
      std::cout << val << " ";
    }
    std::cout << std::endl;

    std::cout << "Second: ";
    for (const auto& val : diff.second) {
      std::cout << val << " ";
    }
    std::cout << std::endl;
  }
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

TEST(TestFindDifferences, AdditionalCase) {
  std::vector<uint64_t> original = {445, 312, 393, 401, 410, 138, 558, 457};
  std::vector<uint64_t> modified = {445, 312, 393, 393, 410, 138, 558, 457};

  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), 1);

  ASSERT_EQ(diffs[0].first, std::vector<uint64_t>({401}));
  ASSERT_EQ(diffs[0].second, std::vector<uint64_t>({393}));

  // Verify elements before and after the difference are identical
  for (size_t i = 0; i < 3; i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
  for (size_t i = 4; i < original.size(); i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
}

void AssertUpdateCase(const std::shared_ptr<::arrow::DataType>& dtype,
                      const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_LE(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    if (!::arrow::is_list_like(dtype->id())) {
      uint64_t left_sum = 0, right_sum = 0;
      for (const auto& val : diff.first) left_sum += val;
      for (const auto& val : diff.second) right_sum += val;
      ASSERT_EQ(left_sum, right_sum);
    }
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }

  if (diffs.size() == 0) {
    // no differences found, the arrays are equal
    ASSERT_TRUE(original == modified);
  }
}

void AssertDeleteCase(const std::shared_ptr<::arrow::DataType>& dtype,
                      const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications,
                      uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    if (!::arrow::is_list_like(dtype->id())) {
      uint64_t left_sum = 0, right_sum = 0;
      for (const auto& val : diff.first) left_sum += val;
      for (const auto& val : diff.second) right_sum += val;
      ASSERT_EQ(left_sum, right_sum + edit_length);
    }
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }
}

void AssertInsertCase(const std::shared_ptr<::arrow::DataType>& dtype,
                      const std::vector<uint64_t>& original,
                      const std::vector<uint64_t>& modified, uint8_t n_modifications,
                      uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), n_modifications);

  for (const auto& diff : diffs) {
    if (!::arrow::is_list_like(dtype->id())) {
      uint64_t left_sum = 0, right_sum = 0;
      for (const auto& val : diff.first) left_sum += val;
      for (const auto& val : diff.second) right_sum += val;
      ASSERT_EQ(left_sum + edit_length, right_sum);
    }
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

void AssertChunkSizes(const std::shared_ptr<::arrow::DataType>& dtype,
                      PageSizes base_result, PageSizes modified_result, bool nullable,
                      bool enable_dictionary, uint64_t min_chunk_size,
                      uint64_t max_chunk_size) {
  if (::arrow::is_fixed_width(dtype->id())) {
    auto min_length = ElementCount(min_chunk_size, dtype->byte_width(), nullable);
    auto max_length = ElementCount(max_chunk_size, dtype->byte_width(), nullable);
    AssertAllBetween(base_result.lengths, min_length, max_length,
                     /*expect_dictionary_fallback=*/enable_dictionary);
    AssertAllBetween(modified_result.lengths, min_length, max_length,
                     /*expect_dictionary_fallback=*/enable_dictionary);
  } else if (::arrow::is_base_binary_like(dtype->id()) && !nullable &&
             !enable_dictionary) {
    AssertAllBetween(base_result.sizes, min_chunk_size, max_chunk_size);
    AssertAllBetween(modified_result.sizes, min_chunk_size, max_chunk_size);
  }
}

constexpr uint64_t kMinChunkSize = 8 * 1024;
constexpr uint64_t kMaxChunkSize = 64 * 1024;
constexpr uint64_t kPartSize = 64 * 1024;
constexpr uint64_t kEditSize = 256;

class TestColumnCDC : public ::testing::TestWithParam<
                          std::tuple<std::shared_ptr<::arrow::DataType>, bool, size_t>> {
 protected:
  // Column random table parts for testing
  std::shared_ptr<Field> field_;
  std::shared_ptr<Table> part1_, part2_, part3_, part4_, part5_, part6_, part7_;

  void SetUp() override {
    auto [dtype, nullable, byte_per_record] = GetParam();
    auto field_ = ::arrow::field("f0", dtype, nullable);

    auto part_length = kPartSize / byte_per_record;
    auto edit_length = kEditSize / byte_per_record;
    // Generate random table parts, these are later concatenated to simulate
    // different scenarios like insert, update, delete, and append.
    part1_ = GenerateTable({field_}, part_length, /*seed=*/1);
    part2_ = GenerateTable({field_}, edit_length, /*seed=*/2);
    part3_ = GenerateTable({field_}, part_length, /*seed=*/3);
    part4_ = GenerateTable({field_}, edit_length, /*seed=*/4);
    part5_ = GenerateTable({field_}, part_length, /*seed=*/5);
    part6_ = GenerateTable({field_}, edit_length, /*seed=*/6);
    part7_ = GenerateTable({field_}, edit_length, /*seed=*/7);
  }
};

TEST_P(TestColumnCDC, DeleteOnce) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part2_, part3_});
  auto modified = ConcatAndCombine({part1_, part3_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);

    AssertDeleteCase(dtype, base_result.lengths, modified_result.lengths, 1,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, DeleteTwice) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part2_, part3_, part4_, part5_});
  auto modified = ConcatAndCombine({part1_, part3_, part5_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertDeleteCase(dtype, base_result.lengths, modified_result.lengths, 2,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, UpdateOnce) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part2_, part3_});
  auto modified = ConcatAndCombine({part1_, part4_, part3_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertUpdateCase(dtype, base_result.lengths, modified_result.lengths, 1);
  }
}

TEST_P(TestColumnCDC, UpdateTwice) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part2_, part3_, part4_, part5_});
  auto modified = ConcatAndCombine({part1_, part6_, part3_, part7_, part5_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertUpdateCase(dtype, base_result.lengths, modified_result.lengths, 2);
  }
}

TEST_P(TestColumnCDC, InsertOnce) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part3_});
  auto modified = ConcatAndCombine({part1_, part2_, part3_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertInsertCase(dtype, base_result.lengths, modified_result.lengths, 1,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, InsertTwice) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part3_, part5_});
  auto modified = ConcatAndCombine({part1_, part2_, part3_, part4_, part5_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertInsertCase(dtype, base_result.lengths, modified_result.lengths, 2,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, Append) {
  auto [dtype, nullable, _] = GetParam();

  auto base = ConcatAndCombine({part1_, part2_, part3_});
  auto modified = ConcatAndCombine({part1_, part2_, part3_, part4_});
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_result,
                         WriteAndGetPageSizes(base, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));
    ASSERT_OK_AND_ASSIGN(auto modified_result,
                         WriteAndGetPageSizes(modified, kMinChunkSize, kMaxChunkSize,
                                              /*enable_dictionary=*/enable_dictionary));

    AssertChunkSizes(dtype, base_result, modified_result, nullable, enable_dictionary,
                     kMinChunkSize, kMaxChunkSize);
    AssertAppendCase(base_result.lengths, modified_result.lengths);
  }
}

// TODO(kszucs): add extension type and dictionary type
INSTANTIATE_TEST_SUITE_P(
    FixedSizedTypes, TestColumnCDC,
    testing::Values(
        // Numeric
        std::make_tuple(::arrow::uint8(), false, 1),
        std::make_tuple(::arrow::uint16(), true, 2),
        std::make_tuple(::arrow::uint32(), false, 4),
        std::make_tuple(::arrow::uint64(), true, 8),
        std::make_tuple(::arrow::int8(), false, 1),
        std::make_tuple(::arrow::int16(), false, 2),
        std::make_tuple(::arrow::int32(), false, 4),
        std::make_tuple(::arrow::int64(), true, 8),
        std::make_tuple(::arrow::float16(), false, 2),
        std::make_tuple(::arrow::float32(), false, 4),
        std::make_tuple(::arrow::float64(), true, 8),
        std::make_tuple(::arrow::decimal128(18, 6), false, 16),
        std::make_tuple(::arrow::decimal256(40, 6), false, 32),
        // Binary-like
        std::make_tuple(::arrow::binary(), true, 16),

        std::make_tuple(::arrow::large_binary(), false, 16),
        std::make_tuple(::arrow::fixed_size_binary(16), true, 16),
        std::make_tuple(::arrow::utf8(), false, 16),
        std::make_tuple(::arrow::utf8(), true, 16),
        std::make_tuple(::arrow::large_utf8(), false, 16),
        // Temporal
        std::make_tuple(::arrow::date32(), false, 4),
        std::make_tuple(::arrow::date64(), false, 8),
        std::make_tuple(::arrow::time32(::arrow::TimeUnit::SECOND), true, 4),
        std::make_tuple(::arrow::time64(::arrow::TimeUnit::NANO), false, 8),
        std::make_tuple(::arrow::timestamp(::arrow::TimeUnit::NANO), true, 8),
        std::make_tuple(::arrow::duration(::arrow::TimeUnit::NANO), false, 8),
        // Nested types
        std::make_tuple(::arrow::list(::arrow::int32()), false, 64),
        std::make_tuple(::arrow::list(::arrow::int32()), true, 64),
        std::make_tuple(::arrow::list(::arrow::utf8()), true, 64),
        std::make_tuple(::arrow::large_list(::arrow::int32()), true, 64),
        std::make_tuple(::arrow::struct_({::arrow::field("f0", ::arrow::int32())}), false,
                        8),
        std::make_tuple(::arrow::struct_({::arrow::field("f0", ::arrow::float64())}),
                        true, 10)));

}  // namespace parquet

// TODO:
// - test multiple row groups
// - test empty

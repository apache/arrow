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
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/util/float16.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"

namespace parquet {

using ::arrow::Array;
using ::arrow::ChunkedArray;
using ::arrow::ConcatenateTables;
using ::arrow::DataType;
using ::arrow::default_memory_pool;
using ::arrow::Field;
using ::arrow::Result;
using ::arrow::Table;
using ::arrow::io::BufferReader;
using ::parquet::arrow::FileReader;
using ::parquet::arrow::FileReaderBuilder;
using ::parquet::arrow::MakeSimpleTable;
using ::parquet::arrow::NonNullArray;
using ::parquet::arrow::WriteTable;

using ::testing::Bool;
using ::testing::Combine;
using ::testing::Values;

// generate determinisic and platform-independent data
inline uint64_t hash(uint64_t seed, uint64_t index) {
  uint64_t h = (index + seed) * 0xc4ceb9fe1a85ec53ull;
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdull;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ull;
  h ^= h >> 33;
  return h;
}

template <typename BuilderType, typename ValueFunc>
Result<std::shared_ptr<Array>> GenerateArray(const std::shared_ptr<DataType>& type,
                                             bool nullable, int64_t length, uint64_t seed,
                                             ValueFunc value_func) {
  BuilderType builder(type, default_memory_pool());

  if (nullable) {
    for (int64_t i = 0; i < length; ++i) {
      uint64_t val = hash(seed, i);
      if (val % 10 == 0) {
        RETURN_NOT_OK(builder.AppendNull());
      } else {
        RETURN_NOT_OK(builder.Append(value_func(val)));
      }
    }
  } else {
    for (int64_t i = 0; i < length; ++i) {
      uint64_t val = hash(seed, i);
      RETURN_NOT_OK(builder.Append(value_func(val)));
    }
  }

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(builder.Finish(&array));
  RETURN_NOT_OK(array->ValidateFull());
  return array;
}

#define GENERATE_CASE(TYPE_ID, BUILDER_TYPE, VALUE_EXPR)                          \
  case ::arrow::Type::TYPE_ID: {                                                  \
    auto value_func = [](uint64_t val) { return VALUE_EXPR; };                    \
    return GenerateArray<BUILDER_TYPE>(type, nullable, length, seed, value_func); \
  }

Result<std::shared_ptr<Array>> GenerateArray(const std::shared_ptr<Field>& field,
                                             int64_t length, int64_t seed) {
  const std::shared_ptr<DataType>& type = field->type();
  bool nullable = field->nullable();

  switch (type->id()) {
    GENERATE_CASE(BOOL, ::arrow::BooleanBuilder, (val % 2 == 0))

    // Numeric types.
    GENERATE_CASE(INT8, ::arrow::Int8Builder, static_cast<int8_t>(val))
    GENERATE_CASE(INT16, ::arrow::Int16Builder, static_cast<int16_t>(val))
    GENERATE_CASE(INT32, ::arrow::Int32Builder, static_cast<int32_t>(val))
    GENERATE_CASE(INT64, ::arrow::Int64Builder, static_cast<int64_t>(val))
    GENERATE_CASE(UINT8, ::arrow::UInt8Builder, static_cast<uint8_t>(val))
    GENERATE_CASE(UINT16, ::arrow::UInt16Builder, static_cast<uint16_t>(val))
    GENERATE_CASE(UINT32, ::arrow::UInt32Builder, static_cast<uint32_t>(val))
    GENERATE_CASE(UINT64, ::arrow::UInt64Builder, static_cast<uint64_t>(val))
    GENERATE_CASE(HALF_FLOAT, ::arrow::HalfFloatBuilder,
                  static_cast<uint16_t>(val % 1000))
    GENERATE_CASE(FLOAT, ::arrow::FloatBuilder, static_cast<float>(val % 1000) / 1000.0f)
    GENERATE_CASE(DOUBLE, ::arrow::DoubleBuilder,
                  static_cast<double>(val % 100000) / 1000.0)
    case ::arrow::Type::DECIMAL128: {
      const auto& decimal_type = static_cast<const ::arrow::Decimal128Type&>(*type);
      // Limit the value to fit within the specified precision
      int32_t max_exponent = decimal_type.precision() - decimal_type.scale();
      int64_t max_value = static_cast<int64_t>(std::pow(10, max_exponent) - 1);
      auto value_func = [&](uint64_t val) {
        return ::arrow::Decimal128(val % max_value);
      };
      return GenerateArray<::arrow::Decimal128Builder>(type, nullable, length, seed,
                                                       value_func);
    }
    case ::arrow::Type::DECIMAL256: {
      const auto& decimal_type = static_cast<const ::arrow::Decimal256Type&>(*type);
      // Limit the value to fit within the specified precision, capped at 9 to avoid
      // int64_t overflow
      int32_t max_exponent = std::min(9, decimal_type.precision() - decimal_type.scale());
      int64_t max_value = static_cast<int64_t>(std::pow(10, max_exponent) - 1);
      auto value_func = [&](uint64_t val) {
        return ::arrow::Decimal256(val % max_value);
      };
      return GenerateArray<::arrow::Decimal256Builder>(type, nullable, length, seed,
                                                       value_func);
    }

      // Temporal types
      GENERATE_CASE(DATE32, ::arrow::Date32Builder, static_cast<int32_t>(val))
      GENERATE_CASE(TIME32, ::arrow::Time32Builder,
                    std::abs(static_cast<int32_t>(val) % 86400000))
      GENERATE_CASE(TIME64, ::arrow::Time64Builder,
                    std::abs(static_cast<int64_t>(val) % 86400000000))
      GENERATE_CASE(TIMESTAMP, ::arrow::TimestampBuilder, static_cast<int64_t>(val))
      GENERATE_CASE(DURATION, ::arrow::DurationBuilder, static_cast<int64_t>(val))

      // Binary and string types.
      GENERATE_CASE(STRING, ::arrow::StringBuilder,
                    std::string("str_") + std::to_string(val))
      GENERATE_CASE(LARGE_STRING, ::arrow::LargeStringBuilder,
                    std::string("str_") + std::to_string(val))
      GENERATE_CASE(BINARY, ::arrow::BinaryBuilder,
                    std::string("bin_") + std::to_string(val))
    case ::arrow::Type::FIXED_SIZE_BINARY: {
      auto size = static_cast<::arrow::FixedSizeBinaryType*>(type.get())->byte_width();
      auto value_func = [size](uint64_t val) {
        return std::string("bin_") + std::to_string(val).substr(0, size - 4);
      };
      return GenerateArray<::arrow::FixedSizeBinaryBuilder>(type, nullable, length, seed,
                                                            value_func);
    }

    case ::arrow::Type::STRUCT: {
      auto struct_type = static_cast<::arrow::StructType*>(type.get());
      std::vector<std::shared_ptr<Array>> child_arrays;
      for (auto i = 0; i < struct_type->num_fields(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto child_array,
                              GenerateArray(struct_type->field(i), length,
                                            seed + static_cast<uint64_t>(i + 300)));
        child_arrays.push_back(child_array);
      }
      auto struct_array =
          std::make_shared<::arrow::StructArray>(type, length, child_arrays);
      return struct_array;
    }

    case ::arrow::Type::LIST: {
      auto list_type = static_cast<::arrow::ListType*>(type.get());
      auto value_field = ::arrow::field("item", list_type->value_type());
      ARROW_ASSIGN_OR_RAISE(auto values_array, GenerateArray(value_field, length, seed));
      auto offset_builder = ::arrow::Int32Builder();
      auto bitmap_builder = ::arrow::TypedBufferBuilder<bool>();

      int32_t num_nulls = 0;
      int32_t num_elements = 0;
      uint8_t element_size = 0;
      int32_t current_offset = 0;
      RETURN_NOT_OK(offset_builder.Append(current_offset));
      while (current_offset < length) {
        num_elements++;
        auto is_valid = !(nullable && (num_elements % 10 == 0));
        if (is_valid) {
          RETURN_NOT_OK(bitmap_builder.Append(true));
          current_offset += element_size;
          if (current_offset > length) {
            RETURN_NOT_OK(offset_builder.Append(static_cast<int32_t>(length)));
            break;
          } else {
            RETURN_NOT_OK(offset_builder.Append(current_offset));
          }
        } else {
          RETURN_NOT_OK(offset_builder.Append(static_cast<int32_t>(current_offset)));
          RETURN_NOT_OK(bitmap_builder.Append(false));
          num_nulls++;
        }

        if (element_size > 4) {
          element_size = 0;
        } else {
          element_size++;
        }
      }

      std::shared_ptr<Array> offsets_array;
      RETURN_NOT_OK(offset_builder.Finish(&offsets_array));
      std::shared_ptr<Buffer> bitmap_buffer;
      RETURN_NOT_OK(bitmap_builder.Finish(&bitmap_buffer));
      ARROW_ASSIGN_OR_RAISE(
          auto list_array, ::arrow::ListArray::FromArrays(
                               type, *offsets_array, *values_array, default_memory_pool(),
                               bitmap_buffer, num_nulls));
      RETURN_NOT_OK(list_array->ValidateFull());
      return list_array;
    }

    default:
      return ::arrow::Status::NotImplemented("Unsupported data type " + type->ToString());
  }
}

Result<std::shared_ptr<Table>> GenerateTable(
    const std::shared_ptr<::arrow::Schema>& schema, int64_t size, int64_t seed = 0) {
  std::vector<std::shared_ptr<Array>> arrays;
  for (const auto& field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto array, GenerateArray(field, size, seed));
    arrays.push_back(array);
  }
  return Table::Make(schema, arrays, size);
}

Result<std::shared_ptr<Table>> ConcatAndCombine(
    const std::vector<std::shared_ptr<Table>>& parts) {
  // Concatenate and combine chunks so the table doesn't carry information about
  // the modification points
  ARROW_ASSIGN_OR_RAISE(auto table, ConcatenateTables(parts));
  return table->CombineChunks();
}

Result<std::shared_ptr<Buffer>> WriteTableToBuffer(const std::shared_ptr<Table>& table,
                                                   int64_t min_chunk_size,
                                                   int64_t max_chunk_size,
                                                   bool enable_dictionary = false,
                                                   int64_t row_group_size = 1024 * 1024) {
  auto sink = CreateOutputStream();

  auto builder = WriterProperties::Builder();
  builder.enable_content_defined_chunking()->content_defined_chunking_options(
      min_chunk_size, max_chunk_size, /*norm_factor=*/0);
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

// Type to represent a list of chunks where each element is the size of the chunk.
using ChunkList = std::vector<int64_t>;

// Type to represent the sizes and lengths of the data pages in a column.
struct PageInfo {
  ChunkList lengths;
  ChunkList sizes;
};

PageInfo GetColumnPageInfo(const std::shared_ptr<Buffer>& data, int column_index = 0) {
  // Read the parquet data out of the buffer and get the sizes and lengths of the
  // data pages in given column. We assert on the sizes and lengths of the pages
  // to ensure that the chunking is done correctly.
  PageInfo result;

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

Result<PageInfo> WriteAndGetPageInfo(const std::shared_ptr<Table>& table,
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
  return GetColumnPageInfo(buffer, column_index);
}

// A git-hunk like side-by-side data structure to represent the differences between two
// vectors of uint64_t values.
using ChunkDiff = std::pair<ChunkList, ChunkList>;

/**
 * Finds the differences between two sequences of chunk lengths or sizes.
 * Uses a longest common subsequence algorithm to identify matching elements
 * and extract the differences between the sequences.
 *
 * @param first The first sequence of chunk values
 * @param second The second sequence of chunk values
 * @return A vector of differences, where each difference is a pair of
 *         subsequences (one from each input) that differ
 */
std::vector<ChunkDiff> FindDifferences(const ChunkList& first, const ChunkList& second) {
  // Compute the longest common subsequence using dynamic programming
  size_t n = first.size(), m = second.size();
  std::vector<std::vector<size_t>> dp(n + 1, std::vector<size_t>(m + 1, 0));

  // Fill the dynamic programming table
  for (size_t i = 0; i < n; i++) {
    for (size_t j = 0; j < m; j++) {
      if (first[i] == second[j]) {
        // If current elements match, extend the LCS
        dp[i + 1][j + 1] = dp[i][j] + 1;
      } else {
        // If current elements don't match, take the best option
        dp[i + 1][j + 1] = std::max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
  }

  // Backtrack through the dynamic programming table to reconstruct the common
  // parts and their positions in the original sequences
  std::vector<std::pair<size_t, size_t>> common;
  for (size_t i = n, j = m; i > 0 && j > 0;) {
    if (first[i - 1] == second[j - 1]) {
      // Found a common element, add to common list
      common.emplace_back(i - 1, j - 1);
      i--, j--;
    } else if (dp[i - 1][j] >= dp[i][j - 1]) {
      // Move in the direction of the larger LCS value
      i--;
    } else {
      j--;
    }
  }
  // Reverse to get indices in ascending order
  std::reverse(common.begin(), common.end());

  // Build the differences by finding sequences between common elements
  std::vector<ChunkDiff> result;
  size_t last_i = 0, last_j = 0;
  for (auto& c : common) {
    auto ci = c.first;
    auto cj = c.second;
    // If there's a gap between the last common element and this one,
    // record the difference
    if (ci > last_i || cj > last_j) {
      result.push_back({{first.begin() + last_i, first.begin() + ci},
                        {second.begin() + last_j, second.begin() + cj}});
    }
    // Move past this common element
    last_i = ci + 1;
    last_j = cj + 1;
  }

  // Handle any remaining elements after the last common element
  if (last_i < n || last_j < m) {
    result.push_back(
        {{first.begin() + last_i, first.end()}, {second.begin() + last_j, second.end()}});
  }

  // Post-process: merge adjacent diffs to avoid splitting single changes into multiple
  // parts
  std::vector<ChunkDiff> merged;
  for (auto& diff : result) {
    if (!merged.empty()) {
      auto& prev = merged.back();
      // Check if we can merge with the previous diff
      bool can_merge_a = prev.first.empty() && !prev.second.empty() &&
                         !diff.first.empty() && diff.second.empty();
      bool can_merge_b = prev.second.empty() && !prev.first.empty() &&
                         !diff.second.empty() && diff.first.empty();

      if (can_merge_a) {
        // Combine into one diff: keep prev's second, use diff's first
        prev.first = std::move(diff.first);
        continue;
      } else if (can_merge_b) {
        // Combine into one diff: keep prev's first, use diff's second
        prev.second = std::move(diff.second);
        continue;
      }
    }
    // If we can't merge, add this diff to the result
    merged.push_back(std::move(diff));
  }
  return merged;
}

void PrintDifferences(const ChunkList& original, const ChunkList& modified,
                      std::vector<ChunkDiff>& diffs) {
  // Utility function to print the original and modified sequences, and the diffs
  // between them. Used in case of failing assertions to display the differences.
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
  ChunkList first = {1, 2, 3, 4, 5};
  ChunkList second = {1, 7, 8, 4, 5};

  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, ChunkList({2, 3}));
  ASSERT_EQ(diffs[0].second, ChunkList({7, 8}));
}

TEST(TestFindDifferences, MultipleDifferences) {
  ChunkList first = {1, 2, 3, 4, 5, 6, 7};
  ChunkList second = {1, 8, 9, 4, 10, 6, 11};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 3);

  ASSERT_EQ(diffs[0].first, ChunkList({2, 3}));
  ASSERT_EQ(diffs[0].second, ChunkList({8, 9}));

  ASSERT_EQ(diffs[1].first, ChunkList({5}));
  ASSERT_EQ(diffs[1].second, ChunkList({10}));

  ASSERT_EQ(diffs[2].first, ChunkList({7}));
  ASSERT_EQ(diffs[2].second, ChunkList({11}));
}

TEST(TestFindDifferences, DifferentLengths) {
  ChunkList first = {1, 2, 3};
  ChunkList second = {1, 2, 3, 4, 5};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_TRUE(diffs[0].first.empty());
  ASSERT_EQ(diffs[0].second, ChunkList({4, 5}));
}

TEST(TestFindDifferences, EmptyArrays) {
  ChunkList first = {};
  ChunkList second = {};
  auto diffs = FindDifferences(first, second);
  ASSERT_TRUE(diffs.empty());
}

TEST(TestFindDifferences, LongSequenceWithSingleDifference) {
  ChunkList first = {
      1994, 2193, 2700, 1913, 2052,
  };
  ChunkList second = {2048, 43, 2080, 2700, 1913, 2052};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, ChunkList({1994, 2193}));
  ASSERT_EQ(diffs[0].second, ChunkList({2048, 43, 2080}));

  // Verify that elements after the difference are identical
  for (size_t i = 3; i < second.size(); i++) {
    ASSERT_EQ(first[i - 1], second[i]);
  }
}

TEST(TestFindDifferences, LongSequenceWithMiddleChanges) {
  ChunkList first = {2169, 1976, 2180, 2147, 1934, 1772,
                     1914, 2075, 2154, 1940, 1934, 1970};
  ChunkList second = {2169, 1976, 2180, 2147, 2265, 1804,
                      1717, 1925, 2122, 1940, 1934, 1970};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 1);
  ASSERT_EQ(diffs[0].first, ChunkList({1934, 1772, 1914, 2075, 2154}));
  ASSERT_EQ(diffs[0].second, ChunkList({2265, 1804, 1717, 1925, 2122}));

  // Verify elements before and after the difference are identical
  for (size_t i = 0; i < 4; i++) {
    ASSERT_EQ(first[i], second[i]);
  }
  for (size_t i = 9; i < first.size(); i++) {
    ASSERT_EQ(first[i], second[i]);
  }
}

TEST(TestFindDifferences, AdditionalCase) {
  ChunkList original = {445, 312, 393, 401, 410, 138, 558, 457};
  ChunkList modified = {445, 312, 393, 393, 410, 138, 558, 457};

  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), 1);

  ASSERT_EQ(diffs[0].first, ChunkList({401}));
  ASSERT_EQ(diffs[0].second, ChunkList({393}));

  // Verify elements before and after the difference are identical
  for (size_t i = 0; i < 3; i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
  for (size_t i = 4; i < original.size(); i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
}

void AssertUpdateCase(const std::shared_ptr<::arrow::DataType>& dtype,
                      const ChunkList& original, const ChunkList& modified,
                      uint8_t n_modifications) {
  auto diffs = FindDifferences(original, modified);
  if (diffs.size() > n_modifications) {
    PrintDifferences(original, modified, diffs);
  }
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
                      const ChunkList& original, const ChunkList& modified,
                      uint8_t n_modifications, uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  if (diffs.size() != n_modifications) {
    PrintDifferences(original, modified, diffs);
  }
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
                      const ChunkList& original, const ChunkList& modified,
                      uint8_t n_modifications, uint64_t edit_length) {
  auto diffs = FindDifferences(original, modified);
  if (diffs.size() != n_modifications) {
    PrintDifferences(original, modified, diffs);
  }
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

void AssertAppendCase(const ChunkList& original, const ChunkList& modified) {
  ASSERT_GE(modified.size(), original.size());
  for (size_t i = 0; i < original.size() - 1; i++) {
    ASSERT_EQ(original[i], modified[i]);
  }
  ASSERT_GT(modified[original.size() - 1], original.back());
}

uint64_t ElementCount(int64_t size, int32_t byte_width, bool nullable) {
  if (nullable) {
    // in case of nullable types the def_levels are also fed through the chunker
    // to identify changes in the null bitmap, this will increase the byte width
    // and decrease the number of elements per chunk
    byte_width += 2;
  }
  return size / byte_width;
}

void AssertAllBetween(const ChunkList& chunks, int64_t min, int64_t max,
                      bool expect_dictionary_fallback = false) {
  // expect the last chunk since it is not guaranteed to be within the range
  if (expect_dictionary_fallback) {
    // if dictionary encoding is enabled, the writer can fallback to plain
    // encoding splitting within a content defined chunk, so we can't
    // guarantee that all chunks are within the range in this case, but we
    // know that there can be at most 2 pages smaller than the min_chunk_size
    size_t smaller_count = 0;
    for (size_t i = 0; i < chunks.size() - 1; i++) {
      if (chunks[i] < min) {
        smaller_count++;
      } else {
        ASSERT_LE(chunks[i], max);
      }
    }
    ASSERT_LE(smaller_count, 2);
  } else {
    for (size_t i = 0; i < chunks.size() - 1; i++) {
      ASSERT_GE(chunks[i], min);
      ASSERT_LE(chunks[i], max);
    }
  }
  ASSERT_LE(chunks.back(), max);
}

void AssertChunkSizes(const std::shared_ptr<::arrow::DataType>& dtype,
                      PageInfo base_result, PageInfo modified_result, bool nullable,
                      bool enable_dictionary, int64_t min_chunk_size,
                      int64_t max_chunk_size) {
  if (::arrow::is_fixed_width(dtype->id()) && !nullable) {
    // for nullable types we cannot calculate the exact number of elements because
    // not all elements are fed through the chunker (null elements are skipped)
    auto byte_width = (dtype->id() == ::arrow::Type::BOOL) ? 1 : dtype->byte_width();
    auto min_length = ElementCount(min_chunk_size, byte_width, nullable);
    auto max_length = ElementCount(max_chunk_size, byte_width, nullable);
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

constexpr int64_t kMinChunkSize = 8 * 1024;
constexpr int64_t kMaxChunkSize = 32 * 1024;
constexpr int64_t kPartSize = 128 * 1024;
constexpr int64_t kEditSize = 128;

struct CaseConfig {
  // Arrow data type to generate the testing data for
  std::shared_ptr<::arrow::DataType> dtype;
  // Whether the data type is nullable
  bool is_nullable;
  // Approximate number of bytes per record to calculate the number of elements to
  // generate
  size_t bytes_per_record;
};

// Define PrintTo for MyStruct
void PrintTo(const CaseConfig& param, std::ostream* os) {
  *os << "{ " << param.dtype->ToString();
  if (param.is_nullable) {
    *os << " nullable";
  }
  *os << " }";
}

class TestColumnCDC : public ::testing::TestWithParam<CaseConfig> {
 protected:
  // Column random table parts for testing
  std::shared_ptr<Field> field_;
  std::shared_ptr<Table> part1_, part2_, part3_, part4_, part5_, part6_, part7_;

  void SetUp() override {
    const auto& param = GetParam();
    auto field_ = ::arrow::field("f0", param.dtype, param.is_nullable);
    auto schema = ::arrow::schema({field_});

    auto part_length = kPartSize / param.bytes_per_record;
    auto edit_length = kEditSize / param.bytes_per_record;
    ASSERT_OK_AND_ASSIGN(part1_, GenerateTable(schema, part_length, 0));
    ASSERT_OK_AND_ASSIGN(part2_, GenerateTable(schema, edit_length, 1));
    ASSERT_OK_AND_ASSIGN(part3_, GenerateTable(schema, part_length, part_length));
    ASSERT_OK_AND_ASSIGN(part4_, GenerateTable(schema, edit_length, 2));
    ASSERT_OK_AND_ASSIGN(part5_, GenerateTable(schema, part_length, 2 * part_length));
    ASSERT_OK_AND_ASSIGN(part6_, GenerateTable(schema, edit_length, 3));
    ASSERT_OK_AND_ASSIGN(part7_, GenerateTable(schema, edit_length, 4));
  }
};

TEST_P(TestColumnCDC, DeleteOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);

    AssertDeleteCase(param.dtype, base_result.lengths, modified_result.lengths, 1,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, DeleteTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertDeleteCase(param.dtype, base_result.lengths, modified_result.lengths, 2,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, UpdateOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part4_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertUpdateCase(param.dtype, base_result.lengths, modified_result.lengths, 1);
  }
}

TEST_P(TestColumnCDC, UpdateTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, part6_, part3_, part7_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertUpdateCase(param.dtype, base_result.lengths, modified_result.lengths, 2);
  }
}

TEST_P(TestColumnCDC, InsertOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertInsertCase(param.dtype, base_result.lengths, modified_result.lengths, 1,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, InsertTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertInsertCase(param.dtype, base_result.lengths, modified_result.lengths, 2,
                     part2_->num_rows());
  }
}

TEST_P(TestColumnCDC, Append) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_, part4_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_result,
        WriteAndGetPageInfo(base, kMinChunkSize, kMaxChunkSize, enable_dictionary));
    ASSERT_OK_AND_ASSIGN(
        auto modified_result,
        WriteAndGetPageInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary));

    AssertChunkSizes(param.dtype, base_result, modified_result, param.is_nullable,
                     enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertAppendCase(base_result.lengths, modified_result.lengths);
  }
}

TEST_P(TestColumnCDC, EmptyTable) {
  const auto& param = GetParam();

  auto schema = ::arrow::schema({::arrow::field("f0", param.dtype, param.is_nullable)});
  ASSERT_OK_AND_ASSIGN(auto empty_table, GenerateTable(schema, 0, 0));
  ASSERT_EQ(empty_table->num_rows(), 0);

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         WriteAndGetPageInfo(empty_table, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary));

    // An empty table should result in no data pages
    ASSERT_TRUE(result.lengths.empty());
    ASSERT_TRUE(result.sizes.empty());
  }
}

INSTANTIATE_TEST_SUITE_P(
    FixedSizedTypes, TestColumnCDC,
    testing::Values(
        // Boolean
        CaseConfig{::arrow::boolean(), false, 1},
        // Numeric
        CaseConfig{::arrow::uint8(), false, 1}, CaseConfig{::arrow::uint16(), false, 2},
        CaseConfig{::arrow::uint32(), false, 4}, CaseConfig{::arrow::uint64(), true, 8},
        CaseConfig{::arrow::int8(), false, 1}, CaseConfig{::arrow::int16(), false, 2},
        CaseConfig{::arrow::int32(), false, 4}, CaseConfig{::arrow::int64(), true, 8},
        CaseConfig{::arrow::float16(), false, 2},
        CaseConfig{::arrow::float32(), false, 4}, CaseConfig{::arrow::float64(), true, 8},
        CaseConfig{::arrow::decimal128(18, 6), false, 16},
        CaseConfig{::arrow::decimal256(40, 6), false, 32},
        // Binary-like
        CaseConfig{::arrow::utf8(), false, 16}, CaseConfig{::arrow::binary(), true, 16},
        CaseConfig{::arrow::fixed_size_binary(16), true, 16},
        // Temporal
        CaseConfig{::arrow::date32(), false, 4},
        CaseConfig{::arrow::time32(::arrow::TimeUnit::MILLI), true, 4},
        CaseConfig{::arrow::time64(::arrow::TimeUnit::NANO), false, 8},
        CaseConfig{::arrow::timestamp(::arrow::TimeUnit::NANO), true, 8},
        CaseConfig{::arrow::duration(::arrow::TimeUnit::NANO), false, 8},
        // Nested types
        CaseConfig{::arrow::list(::arrow::int32()), false, 16},
        CaseConfig{::arrow::list(::arrow::int32()), true, 18},
        CaseConfig{::arrow::list(::arrow::utf8()), true, 18},
        CaseConfig{::arrow::struct_({::arrow::field("f0", ::arrow::int32())}), false, 8},
        CaseConfig{::arrow::struct_({::arrow::field("f0", ::arrow::float64())}), true,
                   10}));

TEST(TestColumnCDC, WriteSingleColumnParquetFile) {
  // Define the schema with a single column "number"
  auto schema = std::dynamic_pointer_cast<schema::GroupNode>(schema::GroupNode::Make(
      "root", Repetition::REQUIRED,
      {schema::PrimitiveNode::Make("number", Repetition::REQUIRED, Type::INT32)}));

  auto sink = CreateOutputStream();
  auto builder = WriterProperties::Builder();
  auto props = builder.enable_content_defined_chunking()->build();

  auto writer = ParquetFileWriter::Open(sink, schema, props);
  auto row_group_writer = writer->AppendRowGroup();

  // Create a column writer for the "number" column
  auto column_writer = row_group_writer->NextColumn();
  auto& int_column_writer = dynamic_cast<Int32Writer&>(*column_writer);

  std::vector<int32_t> numbers = {1, 2, 3, 4, 5};
  std::vector<uint8_t> valid_bits = {1, 0, 1, 0, 1};
  EXPECT_THROW(
      int_column_writer.WriteBatch(numbers.size(), nullptr, nullptr, numbers.data()),
      ParquetException);
  EXPECT_THROW(int_column_writer.WriteBatchSpaced(numbers.size(), nullptr, nullptr,
                                                  valid_bits.data(), 0, numbers.data()),
               ParquetException);
}

}  // namespace parquet

// TODO:
// - test multiple row groups
// - place information about the used CDC parameters to the metadata
// - test the effect of the normalization factor
// - do more validation on min/max chunk size

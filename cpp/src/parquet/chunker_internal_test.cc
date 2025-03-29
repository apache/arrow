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
#include "arrow/testing/extension_type.h"
#include "arrow/testing/generator.h"
#include "arrow/type_fwd.h"
#include "arrow/util/float16.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/reader_internal.h"
#include "parquet/arrow/test_util.h"
#include "parquet/arrow/writer.h"
#include "parquet/chunker_internal.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"

namespace parquet::internal {

using ::arrow::Array;
using ::arrow::ChunkedArray;
using ::arrow::ConcatenateTables;
using ::arrow::DataType;
using ::arrow::default_memory_pool;
using ::arrow::Field;
using ::arrow::Result;
using ::arrow::Schema;
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
      GENERATE_CASE(LARGE_BINARY, ::arrow::LargeBinaryBuilder,
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

    case ::arrow::Type::EXTENSION: {
      auto extension_type = dynamic_cast<::arrow::ExtensionType*>(type.get());
      auto storage_type = extension_type->storage_type();
      auto storage_field = ::arrow::field("storage", storage_type, true);
      ARROW_ASSIGN_OR_RAISE(auto storage_array,
                            GenerateArray(storage_field, length, seed));
      return ::arrow::ExtensionType::WrapArray(type, storage_array);
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

Result<std::shared_ptr<Buffer>> WriteTableToBuffer(
    const std::shared_ptr<Table>& table, int64_t min_chunk_size, int64_t max_chunk_size,
    bool enable_dictionary = false, int64_t row_group_size = 1024 * 1024,
    ParquetDataPageVersion data_page_version = ParquetDataPageVersion::V1) {
  auto sink = CreateOutputStream();

  auto builder = WriterProperties::Builder();
  builder.enable_content_defined_chunking()->content_defined_chunking_options(
      min_chunk_size, max_chunk_size, /*norm_factor=*/0);
  builder.data_page_version(data_page_version);
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

struct RowGroupInfo {
  ChunkList page_lengths;
  ChunkList page_sizes;
  bool has_dictionary_page = false;
};

using ParquetInfo = std::vector<RowGroupInfo>;

ParquetInfo GetColumnParquetInfo(const std::shared_ptr<Buffer>& data,
                                 int column_index = 0) {
  // Read the parquet data out of the buffer and get the sizes and lengths of the
  // data pages in given column. We assert on the sizes and lengths of the pages
  // to ensure that the chunking is done correctly.
  ParquetInfo result;

  auto buffer_reader = std::make_shared<BufferReader>(data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));

  auto metadata = parquet_reader->metadata();
  for (int rg = 0; rg < metadata->num_row_groups(); rg++) {
    auto page_reader = parquet_reader->RowGroup(rg)->GetColumnPageReader(column_index);
    RowGroupInfo rg_info;
    while (auto page = page_reader->NextPage()) {
      if (page->type() == PageType::DATA_PAGE || page->type() == PageType::DATA_PAGE_V2) {
        auto data_page = static_cast<DataPage*>(page.get());
        rg_info.page_sizes.push_back(data_page->size());
        rg_info.page_lengths.push_back(data_page->num_values());
      } else if (page->type() == PageType::DICTIONARY_PAGE) {
        rg_info.has_dictionary_page = true;
      }
    }
    result.push_back(rg_info);
  }

  return result;
}

Result<ParquetInfo> WriteAndGetParquetInfo(
    const std::shared_ptr<Table>& table, uint64_t min_chunk_size, uint64_t max_chunk_size,
    bool enable_dictionary = false,
    ParquetDataPageVersion data_page_version = ParquetDataPageVersion::V1,
    int64_t row_group_size = 1024 * 1024,

    int column_index = 0) {
  // Write the table to a buffer and read it back to get the page sizes
  ARROW_ASSIGN_OR_RAISE(
      auto buffer,
      WriteTableToBuffer(table, min_chunk_size, max_chunk_size, enable_dictionary,
                         row_group_size, data_page_version));
  ARROW_ASSIGN_OR_RAISE(auto readback, ReadTableFromBuffer(buffer));

  RETURN_NOT_OK(readback->ValidateFull());
  if (readback->schema()->Equals(*table->schema())) {
    ARROW_RETURN_IF(!readback->Equals(*table),
                    Status::Invalid("Readback table not equal to original"));
  }
  return GetColumnParquetInfo(buffer, column_index);
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

void AssertPageLengthDifferences(const RowGroupInfo& original,
                                 const RowGroupInfo& modified,
                                 int8_t exact_number_of_equal_diffs,
                                 int8_t exact_number_of_larger_diffs,
                                 int8_t exact_number_of_smaller_diffs,
                                 int64_t edit_length = 0) {
  // Asserts that the differences between the original and modified page lengths
  // are as expected. A longest common subsequence diff is calculated on the original
  // and modified sequences of page lengths. The exact_number_of_equal_diffs,
  // exact_number_of_larger_diffs, and exact_number_of_smaller_diffs parameters specify
  // the expected number of differences with equal, larger, and smaller sums of the page
  // lengths respectively. The edit_length parameter is used to verify that the page
  // lenght differences are exactly equal to the edit_length.
  auto diffs = FindDifferences(original.page_lengths, modified.page_lengths);
  size_t expected_number_of_diffs = exact_number_of_equal_diffs +
                                    exact_number_of_larger_diffs +
                                    exact_number_of_smaller_diffs;
  if (diffs.size() != expected_number_of_diffs) {
    PrintDifferences(original.page_lengths, modified.page_lengths, diffs);
  }
  if (diffs.size() == 0) {
    // no differences found, the arrays are equal
    ASSERT_TRUE(original.page_lengths == modified.page_lengths);
  }
  ASSERT_EQ(diffs.size(), expected_number_of_diffs);

  uint8_t equal_diffs = 0;
  int8_t larger_diffs = 0;
  int8_t smaller_diffs = 0;
  for (const auto& diff : diffs) {
    uint64_t original_sum = 0, modified_sum = 0;
    for (const auto& val : diff.first) original_sum += val;
    for (const auto& val : diff.second) modified_sum += val;

    if (original_sum == modified_sum) {
      equal_diffs++;
    } else if (original_sum < modified_sum) {
      larger_diffs++;
      ASSERT_EQ(original_sum + edit_length, modified_sum);
    } else if (original_sum > modified_sum) {
      smaller_diffs++;
      ASSERT_EQ(original_sum, modified_sum + edit_length);
    }
    ASSERT_LE(diff.first.size(), 2);
    ASSERT_LE(diff.second.size(), 2);
  }

  ASSERT_EQ(equal_diffs, exact_number_of_equal_diffs);
  ASSERT_EQ(larger_diffs, exact_number_of_larger_diffs);
  ASSERT_EQ(smaller_diffs, exact_number_of_smaller_diffs);
}

void AssertPageLengthDifferences(const RowGroupInfo& original,
                                 const RowGroupInfo& modified,
                                 uint8_t max_number_of_equal_diffs) {
  // A less restrictive version of the above assertion function mainly used to
  // assert the update case.
  auto diffs = FindDifferences(original.page_lengths, modified.page_lengths);
  if (diffs.size() > max_number_of_equal_diffs) {
    PrintDifferences(original.page_lengths, modified.page_lengths, diffs);
  }
  ASSERT_LE(diffs.size(), max_number_of_equal_diffs);

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
    ASSERT_TRUE(original.page_lengths == modified.page_lengths);
  }
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

void AssertAllBetween(const ChunkList& chunks, int64_t min, int64_t max) {
  // except the last chunk since it is not guaranteed to be within the range
  for (size_t i = 0; i < chunks.size() - 1; i++) {
    ASSERT_GE(chunks[i], min);
    ASSERT_LE(chunks[i], max);
  }
  ASSERT_LE(chunks.back(), max);
}

void AssertChunkSizes(const std::shared_ptr<::arrow::DataType>& dtype,
                      const RowGroupInfo& base_info, const RowGroupInfo& modified_info,
                      bool nullable, bool enable_dictionary, int64_t min_chunk_size,
                      int64_t max_chunk_size) {
  if (dtype->id() != ::arrow::Type::BOOL) {
    ASSERT_EQ(base_info.has_dictionary_page, enable_dictionary);
    ASSERT_EQ(modified_info.has_dictionary_page, enable_dictionary);
  }
  if (::arrow::is_fixed_width(dtype->id())) {
    // for nullable types we cannot calculate the exact number of elements because
    // not all elements are fed through the chunker (null elements are skipped)
    auto byte_width = (dtype->id() == ::arrow::Type::BOOL) ? 1 : dtype->byte_width();
    auto min_length = ElementCount(min_chunk_size, byte_width, nullable);
    auto max_length = ElementCount(max_chunk_size, byte_width, nullable);
    AssertAllBetween(base_info.page_lengths, min_length, max_length);
    AssertAllBetween(modified_info.page_lengths, min_length, max_length);
  } else if (::arrow::is_base_binary_like(dtype->id()) && !enable_dictionary) {
    AssertAllBetween(base_info.page_sizes, min_chunk_size, max_chunk_size);
    AssertAllBetween(modified_info.page_sizes, min_chunk_size, max_chunk_size);
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
  // Data page version to use
  ParquetDataPageVersion data_page_version = ParquetDataPageVersion::V1;
};

// Define PrintTo for MyStruct
void PrintTo(const CaseConfig& param, std::ostream* os) {
  *os << "{ " << param.dtype->ToString();
  if (param.is_nullable) {
    *os << " nullable";
  }
  *os << " }";
}

class TestCDC : public ::testing::Test {
 public:
  uint64_t GetMask(const ContentDefinedChunker& cdc) const { return cdc.GetMask(); }
};

TEST_F(TestCDC, RollingHashMaskCalculation) {
  auto le = LevelInfo();
  auto min_size = 256 * 1024;
  auto max_size = 1024 * 1024;

  auto cdc0 = ContentDefinedChunker(le, min_size, max_size, 0);
  ASSERT_EQ(GetMask(cdc0), 0xFFFE000000000000);

  auto cdc1 = ContentDefinedChunker(le, min_size, max_size, 1);
  ASSERT_EQ(GetMask(cdc1), 0xFFFC000000000000);

  auto cdc2 = ContentDefinedChunker(le, min_size, max_size, 2);
  ASSERT_EQ(GetMask(cdc2), 0xFFF8000000000000);

  auto cdc3 = ContentDefinedChunker(le, min_size, max_size, 3);
  ASSERT_EQ(GetMask(cdc3), 0xFFF0000000000000);

  auto cdc4 = ContentDefinedChunker(le, min_size, max_size, -1);
  ASSERT_EQ(GetMask(cdc4), 0xFFFF000000000000);

  // this is the smallest possible mask always matching, by using 8 hashtables
  // we are going to have a match every 8 bytes; this is an unrealistic case
  // but checking for the correctness of the mask calculation
  auto cdc5 = ContentDefinedChunker(le, 0, 16, 0);
  ASSERT_EQ(GetMask(cdc5), 0x0000000000000000);

  auto cdc6 = ContentDefinedChunker(le, 0, 32, 1);
  ASSERT_EQ(GetMask(cdc6), 0x0000000000000000);

  auto cdc7 = ContentDefinedChunker(le, 0, 16, -1);
  ASSERT_EQ(GetMask(cdc7), 0x8000000000000000);

  // another unrealistic case, checking for the validation
  auto cdc8 = ContentDefinedChunker(le, 128, 384, -60);
  ASSERT_EQ(GetMask(cdc8), 0xFFFFFFFFFFFFFFFF);
}

TEST_F(TestCDC, WriteSingleColumnParquetFile) {
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

TEST_F(TestCDC, LastChunkDoesntTriggerAddDataPage) {
  // Define the schema with a single column "number"
  auto schema = std::dynamic_pointer_cast<schema::GroupNode>(schema::GroupNode::Make(
      "root", Repetition::REQUIRED,
      {schema::PrimitiveNode::Make("number", Repetition::REQUIRED, Type::INT32)}));

  auto sink = CreateOutputStream();
  auto builder = WriterProperties::Builder();
  auto props = builder.enable_content_defined_chunking()
                   ->content_defined_chunking_options(kMinChunkSize, kMaxChunkSize, 0)
                   ->disable_dictionary()
                   ->build();

  auto writer = ParquetFileWriter::Open(sink, schema, props);
  auto row_group_writer = writer->AppendRowGroup();

  // Create a column writer for the "number" column
  auto column_writer = row_group_writer->NextColumn();
  auto& int_column_writer = dynamic_cast<Int32Writer&>(*column_writer);

  ASSERT_OK_AND_ASSIGN(auto array, ::arrow::gen::Step()->Generate(8000));
  auto arrow_props = default_arrow_writer_properties();
  auto arrow_ctx = ArrowWriteContext(default_memory_pool(), arrow_props.get());

  // Calling WriteArrow twice, we expect that the first call doesn't add a new data page
  // at the end allowing subsequent calls to append to the same page
  ASSERT_OK(int_column_writer.WriteArrow(nullptr, nullptr, array->length(), *array,
                                         &arrow_ctx, false));
  ASSERT_OK(int_column_writer.WriteArrow(nullptr, nullptr, array->length(), *array,
                                         &arrow_ctx, false));

  int_column_writer.Close();
  writer->Close();
  ASSERT_OK_AND_ASSIGN(auto buffer, sink->Finish());

  auto info = GetColumnParquetInfo(buffer);
  ASSERT_EQ(info.size(), 1);

  // AssertAllBetween allow the last chunk size to be smaller than the min_chunk_size
  AssertAllBetween(info[0].page_sizes, kMinChunkSize, kMaxChunkSize);
  AssertAllBetween(info[0].page_lengths, 3000, 5000);
}

TEST_F(TestCDC, ChunkSizeParameterValidation) {
  // Test that constructor validates min/max chunk size parameters
  auto li = LevelInfo();

  ASSERT_NO_THROW(ContentDefinedChunker(li, 256 * 1024, 1024 * 1024));

  // with norm_factor=0 the difference between min and max chunk size must be
  // at least 16
  ASSERT_THROW(ContentDefinedChunker(li, 0, -1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1024, 512), ParquetException);

  ASSERT_THROW(ContentDefinedChunker(li, -1, 0), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 0, 0), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 16));
  ASSERT_THROW(ContentDefinedChunker(li, -16, -16), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 16, 0), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 32, 32), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 32, 48));
  ASSERT_NO_THROW(ContentDefinedChunker(li, 1024 * 1024, 2 * 1024 * 1024));
  ASSERT_NO_THROW(
      ContentDefinedChunker(li, 1024 * 1024 * 1024L, 2LL * 1024 * 1024 * 1024L));

  // with norm_factor=1 the difference between min and max chunk size must be
  // at least 64
  ASSERT_THROW(ContentDefinedChunker(li, 1, -1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, -1, 1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1, 1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1, 32, 1), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 1, 33, 1));

  // with norm_factor=2 the difference between min and max chunk size must be
  // at least 128
  ASSERT_THROW(ContentDefinedChunker(li, 0, 63, 2), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 64, 2));

  // with norm_factor=-1 the difference between min and max chunk size must be
  // at least 8
  ASSERT_THROW(ContentDefinedChunker(li, 0, 7, -1), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 8, -1));
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 16, -1));

  // test the norm_factor extremes
  ASSERT_THROW(ContentDefinedChunker(li, 0, 0, -68), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 1, -67));
  ASSERT_THROW(ContentDefinedChunker(li, 0, std::numeric_limits<int64_t>::max(), 59),
               ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, std::numeric_limits<int64_t>::max(), 58));
}

class TestCDCSingleRowGroup : public ::testing::TestWithParam<CaseConfig> {
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

TEST_P(TestCDCSingleRowGroup, DeleteOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);

    auto edit_length = part2_->num_rows();
    if (::arrow::is_list_like(param.dtype->id())) {
      edit_length += 1;
    }
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/0,
                                /*exact_number_of_smaller_diffs=*/1, edit_length);
  }
}

TEST_P(TestCDCSingleRowGroup, DeleteTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);

    auto edit_length = part2_->num_rows();
    if (::arrow::is_list_like(param.dtype->id())) {
      edit_length += 1;
    }
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/0,
                                /*exact_number_of_smaller_diffs=*/2, edit_length);
  }
}

TEST_P(TestCDCSingleRowGroup, UpdateOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part4_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));
    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*max_number_of_equal_diffs=*/1);
  }
}

TEST_P(TestCDCSingleRowGroup, UpdateTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, part6_, part3_, part7_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));
    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*max_number_of_equal_diffs=*/2);
  }
}

TEST_P(TestCDCSingleRowGroup, InsertOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));
    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);

    auto edit_length = part2_->num_rows();
    if (::arrow::is_list_like(param.dtype->id())) {
      edit_length += 1;
    }
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/0, edit_length);
  }
}

TEST_P(TestCDCSingleRowGroup, InsertTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));
    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);

    auto edit_length = part2_->num_rows();
    if (::arrow::is_list_like(param.dtype->id())) {
      edit_length += 1;
    }
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/2,
                                /*exact_number_of_smaller_diffs=*/0, edit_length);
  }
}

TEST_P(TestCDCSingleRowGroup, Append) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_, part4_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(auto base_info, WriteAndGetParquetInfo(
                                             base, kMinChunkSize, kMaxChunkSize,
                                             enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_info,
        WriteAndGetParquetInfo(modified, kMinChunkSize, kMaxChunkSize, enable_dictionary,
                               param.data_page_version));
    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);
    AssertChunkSizes(param.dtype, base_info.front(), modified_info.front(),
                     param.is_nullable, enable_dictionary, kMinChunkSize, kMaxChunkSize);

    auto original_page_lengths = base_info.front().page_lengths;
    auto modified_page_lengths = modified_info.front().page_lengths;
    ASSERT_GE(original_page_lengths.size(), modified_page_lengths.size());
    for (size_t i = 0; i < original_page_lengths.size() - 1; i++) {
      ASSERT_EQ(original_page_lengths[i], modified_page_lengths[i]);
    }
    ASSERT_GT(modified_page_lengths.back(), original_page_lengths.back());
  }
}

TEST_P(TestCDCSingleRowGroup, EmptyTable) {
  const auto& param = GetParam();

  auto schema = ::arrow::schema({::arrow::field("f0", param.dtype, param.is_nullable)});
  ASSERT_OK_AND_ASSIGN(auto empty_table, GenerateTable(schema, 0, 0));
  ASSERT_EQ(empty_table->num_rows(), 0);

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto result, WriteAndGetParquetInfo(empty_table, kMinChunkSize, kMaxChunkSize,
                                            enable_dictionary, param.data_page_version));

    // An empty table should result in no data pages
    ASSERT_EQ(result.size(), 1);
    ASSERT_TRUE(result.front().page_lengths.empty());
    ASSERT_TRUE(result.front().page_sizes.empty());
  }
}

TEST_P(TestCDCSingleRowGroup, ArrayOffsets) {
  const auto& param = GetParam();
  ASSERT_OK_AND_ASSIGN(auto table, ConcatAndCombine({part1_, part2_, part3_}));

  for (auto offset : {0, 512, 1024}) {
    auto sliced_table = table->Slice(offset);

    // assert that the first column has a non-zero offset
    auto column = sliced_table->column(0);
    auto first_chunk = column->chunk(0);
    ASSERT_EQ(first_chunk->offset(), offset);

    // write out the sliced table, read it back and compare
    ASSERT_OK(WriteAndGetParquetInfo(sliced_table, kMinChunkSize, kMaxChunkSize, true,
                                     param.data_page_version));
  }
}

INSTANTIATE_TEST_SUITE_P(
    FixedSizedTypes, TestCDCSingleRowGroup,
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
                   10},
        CaseConfig{
            ::arrow::list(::arrow::struct_({::arrow::field("f0", ::arrow::int32())})),
            false, 16},
        // Extension type
        CaseConfig{::arrow::uuid(), true, 16},
        // Use ParquetDataPageVersion::V2
        CaseConfig{::arrow::large_binary(), false, 16, ParquetDataPageVersion::V2},
        CaseConfig{::arrow::list(::arrow::utf8()), true, 18,
                   ParquetDataPageVersion::V2}));

class TestCDCMultipleRowGroups : public ::testing::Test {
 protected:
  // Column random table parts for testing
  std::shared_ptr<DataType> dtype_;
  std::shared_ptr<Table> part1_, part2_, part3_;
  std::shared_ptr<Table> edit1_, edit2_, edit3_;

  void SetUp() override {
    auto constexpr kPartLength = 256 * 1024;
    auto constexpr kEditLength = 128;

    dtype_ = ::arrow::int32();
    auto field = ::arrow::field("f0", dtype_, true);
    auto schema = ::arrow::schema({field});

    ASSERT_OK_AND_ASSIGN(part1_, GenerateTable(schema, kPartLength, 0));
    ASSERT_OK_AND_ASSIGN(part2_, GenerateTable(schema, kPartLength, 2));
    ASSERT_OK_AND_ASSIGN(part3_, GenerateTable(schema, kPartLength, 4));

    ASSERT_OK_AND_ASSIGN(edit1_, GenerateTable(schema, kEditLength, 1));
    ASSERT_OK_AND_ASSIGN(edit2_, GenerateTable(schema, kEditLength, 3));
    ASSERT_OK_AND_ASSIGN(edit3_, GenerateTable(schema, kEditLength, 5));
  }
};

TEST_F(TestCDCMultipleRowGroups, InsertOnce) {
  auto constexpr kRowGroupLength = 128 * 1024;
  auto constexpr kEnableDictionary = false;
  auto constexpr kMinChunkSize = 0 * 1024;
  auto constexpr kMaxChunkSize = 128 * 1024;

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, edit1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto inserted,
                       ConcatAndCombine({part1_, edit1_, edit2_, part2_, part3_}));
  ASSERT_FALSE(base->Equals(*inserted));
  ASSERT_EQ(inserted->num_rows(), base->num_rows() + edit2_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_info,
      WriteAndGetParquetInfo(base, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));
  ASSERT_OK_AND_ASSIGN(
      auto inserted_info,
      WriteAndGetParquetInfo(inserted, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));

  ASSERT_EQ(base_info.size(), 7);
  ASSERT_EQ(inserted_info.size(), 7);

  ASSERT_EQ(base_info.at(0).page_lengths, inserted_info.at(0).page_lengths);
  ASSERT_EQ(base_info.at(1).page_lengths, inserted_info.at(1).page_lengths);
  for (size_t i = 2; i < inserted_info.size() - 1; i++) {
    AssertPageLengthDifferences(base_info.at(i), inserted_info.at(i),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/1, edit2_->num_rows());
  }
  AssertPageLengthDifferences(base_info.back(), inserted_info.back(),
                              /*exact_number_of_equal_diffs=*/0,
                              /*exact_number_of_larger_diffs=*/1,
                              /*exact_number_of_smaller_diffs=*/0, edit2_->num_rows());
}

TEST_F(TestCDCMultipleRowGroups, DeleteOnce) {
  auto constexpr kRowGroupLength = 128 * 1024;
  auto constexpr kEnableDictionary = false;
  auto constexpr kMinChunkSize = 0 * 1024;
  auto constexpr kMaxChunkSize = 128 * 1024;

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_OK_AND_ASSIGN(auto deleted, ConcatAndCombine({part1_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*deleted));
  ASSERT_EQ(deleted->num_rows(), base->num_rows() - edit1_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_info,
      WriteAndGetParquetInfo(base, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));
  ASSERT_OK_AND_ASSIGN(
      auto deleted_info,
      WriteAndGetParquetInfo(deleted, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));

  ASSERT_EQ(base_info.size(), 7);
  ASSERT_EQ(deleted_info.size(), 7);

  ASSERT_EQ(base_info.at(0).page_lengths, deleted_info.at(0).page_lengths);
  ASSERT_EQ(base_info.at(1).page_lengths, deleted_info.at(1).page_lengths);
  for (size_t i = 2; i < deleted_info.size() - 1; i++) {
    AssertPageLengthDifferences(base_info.at(i), deleted_info.at(i),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/1, edit1_->num_rows());
  }
  AssertPageLengthDifferences(base_info.back(), deleted_info.back(),
                              /*exact_number_of_equal_diffs=*/0,
                              /*exact_number_of_larger_diffs=*/0,
                              /*exact_number_of_smaller_diffs=*/1, edit1_->num_rows());
}

TEST_F(TestCDCMultipleRowGroups, UpdateOnce) {
  auto constexpr kRowGroupLength = 128 * 1024;
  auto constexpr kEnableDictionary = false;
  auto constexpr kMinChunkSize = 0 * 1024;
  auto constexpr kMaxChunkSize = 128 * 1024;

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_OK_AND_ASSIGN(auto updated,
                       ConcatAndCombine({part1_, edit3_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*updated));

  ASSERT_OK_AND_ASSIGN(
      auto base_info,
      WriteAndGetParquetInfo(base, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));
  ASSERT_OK_AND_ASSIGN(
      auto updated_info,
      WriteAndGetParquetInfo(updated, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));

  ASSERT_EQ(base_info.size(), 7);
  ASSERT_EQ(updated_info.size(), 7);

  ASSERT_EQ(base_info.at(0).page_lengths, updated_info.at(0).page_lengths);
  ASSERT_EQ(base_info.at(1).page_lengths, updated_info.at(1).page_lengths);
  AssertPageLengthDifferences(base_info.at(2), updated_info.at(2),
                              /*max_number_of_equal_diffs=*/1);
  for (size_t i = 2; i < updated_info.size(); i++) {
    ASSERT_EQ(base_info.at(i).page_lengths, updated_info.at(i).page_lengths);
  }
}

TEST_F(TestCDCMultipleRowGroups, Append) {
  auto constexpr kRowGroupLength = 128 * 1024;
  auto constexpr kEnableDictionary = false;
  auto constexpr kMinChunkSize = 0 * 1024;
  auto constexpr kMaxChunkSize = 128 * 1024;

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, edit1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto appended,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*appended));
  ASSERT_EQ(appended->num_rows(), base->num_rows() + edit2_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_info,
      WriteAndGetParquetInfo(base, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));
  ASSERT_OK_AND_ASSIGN(
      auto appended_info,
      WriteAndGetParquetInfo(appended, kMinChunkSize, kMaxChunkSize, kEnableDictionary,
                             ParquetDataPageVersion::V1, kRowGroupLength));

  ASSERT_EQ(base_info.size(), 7);
  ASSERT_EQ(appended_info.size(), 7);

  for (size_t i = 0; i < appended_info.size() - 1; i++) {
    ASSERT_EQ(base_info.at(i).page_lengths, appended_info.at(i).page_lengths);
  }
  // only the last row group should have more or equal number of pages
  auto original_page_lengths = base_info.back().page_lengths;
  auto appended_page_lengths = appended_info.back().page_lengths;
  ASSERT_GE(original_page_lengths.size(), appended_page_lengths.size());
  for (size_t i = 0; i < original_page_lengths.size() - 1; i++) {
    ASSERT_EQ(original_page_lengths[i], appended_page_lengths[i]);
  }
  ASSERT_GT(appended_page_lengths.back(), original_page_lengths.back());
}

}  // namespace parquet::internal

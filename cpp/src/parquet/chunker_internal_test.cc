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

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/extension/json.h"
#include "arrow/table.h"
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
using ::arrow::internal::checked_cast;
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
      const auto& decimal_type = checked_cast<const ::arrow::Decimal128Type&>(*type);
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
      const auto& decimal_type = checked_cast<const ::arrow::Decimal256Type&>(*type);
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
      auto size =
          checked_cast<const ::arrow::FixedSizeBinaryType*>(type.get())->byte_width();
      auto value_func = [size](uint64_t val) {
        return std::string("bin_") + std::to_string(val).substr(0, size - 4);
      };
      return GenerateArray<::arrow::FixedSizeBinaryBuilder>(type, nullable, length, seed,
                                                            value_func);
    }

    case ::arrow::Type::STRUCT: {
      auto struct_type = checked_cast<const ::arrow::StructType*>(type.get());
      std::vector<std::shared_ptr<Array>> child_arrays;
      for (auto i = 0; i < struct_type->num_fields(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto child_array, GenerateArray(struct_type->field(i),
                                                              length, seed + i * 10));
        child_arrays.push_back(child_array);
      }
      auto struct_array =
          std::make_shared<::arrow::StructArray>(type, length, child_arrays);
      return struct_array;
    }

    case ::arrow::Type::LIST: {
      // Repeat the same pattern in the list array:
      // null, empty list, list of 1 element, list of 3 elements
      if (length % 4 != 0) {
        return Status::Invalid(
            "Length must be divisible by 4 when generating list arrays, but got: ",
            length);
      }
      auto values_array_length = length * 4;
      auto list_type = checked_cast<const ::arrow::ListType*>(type.get());
      auto value_field = ::arrow::field("item", list_type->value_type());
      ARROW_ASSIGN_OR_RAISE(auto values_array,
                            GenerateArray(value_field, values_array_length, seed));
      auto offset_builder = ::arrow::Int32Builder();
      auto bitmap_builder = ::arrow::TypedBufferBuilder<bool>();

      RETURN_NOT_OK(offset_builder.Reserve(length + 1));
      RETURN_NOT_OK(bitmap_builder.Reserve(length));

      int32_t num_nulls = 0;
      RETURN_NOT_OK(offset_builder.Append(0));
      for (auto offset = 0; offset < length; offset += 4) {
        if (nullable) {
          // add a null
          RETURN_NOT_OK(bitmap_builder.Append(false));
          RETURN_NOT_OK(offset_builder.Append(offset));
          num_nulls += 1;
        } else {
          // add an empty list
          RETURN_NOT_OK(bitmap_builder.Append(true));
          RETURN_NOT_OK(offset_builder.Append(offset));
        }
        // add an empty list
        RETURN_NOT_OK(bitmap_builder.Append(true));
        RETURN_NOT_OK(offset_builder.Append(offset));
        // add a list of 1 element
        RETURN_NOT_OK(bitmap_builder.Append(true));
        RETURN_NOT_OK(offset_builder.Append(offset + 1));
        // add a list of 3 elements
        RETURN_NOT_OK(bitmap_builder.Append(true));
        RETURN_NOT_OK(offset_builder.Append(offset + 4));
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
      auto extension_type = checked_cast<const ::arrow::ExtensionType*>(type.get());
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

TEST(TestGenerateArray, Integer) {
  auto field = ::arrow::field("a", ::arrow::int32());
  ASSERT_OK_AND_ASSIGN(auto array, GenerateArray(field, /*length=*/10, /*seed=*/0));
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(array->length(), 10);
  ASSERT_TRUE(array->type()->Equals(::arrow::int32()));
  ASSERT_EQ(array->null_count(), 1);
}

TEST(TestGenerateArray, ListOfInteger) {
  auto field = ::arrow::field("a", ::arrow::list(::arrow::int32()));
  auto length = 12;
  ASSERT_OK_AND_ASSIGN(auto array, GenerateArray(field, length, /*seed=*/0));
  ASSERT_OK(array->ValidateFull());
  ASSERT_EQ(array->length(), length);

  for (size_t i = 0; i < 12; i += 4) {
    // Assert the first element is null
    ASSERT_TRUE(array->IsNull(i));

    // Assert the second element is an empty list
    ASSERT_TRUE(array->IsValid(i + 1));
    auto list_array = std::static_pointer_cast<::arrow::ListArray>(array);
    ASSERT_EQ(list_array->value_length(i + 1), 0);

    // Assert the third element has length 1
    ASSERT_TRUE(array->IsValid(i + 2));
    ASSERT_EQ(list_array->value_length(i + 2), 1);

    // Assert the fourth element has length 3
    ASSERT_TRUE(array->IsValid(i + 3));
    ASSERT_EQ(list_array->value_length(i + 3), 3);
  }

  ASSERT_NOT_OK(GenerateArray(field, 3, /*seed=*/0));
  ASSERT_OK(GenerateArray(field, 8, /*seed=*/0));
}

Result<std::shared_ptr<Table>> GenerateTable(
    const std::shared_ptr<::arrow::Schema>& schema, int64_t size, int64_t seed = 0) {
  std::vector<std::shared_ptr<Array>> arrays;
  for (const auto& field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto array, GenerateArray(field, size, ++seed));
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

Result<std::shared_ptr<Table>> ReadTableFromBuffer(const std::shared_ptr<Buffer>& data) {
  std::shared_ptr<Table> result;
  FileReaderBuilder builder;
  std::unique_ptr<FileReader> reader;
  auto props = default_arrow_reader_properties();
  props.set_arrow_extensions_enabled(true);

  RETURN_NOT_OK(builder.Open(std::make_shared<BufferReader>(data)));
  RETURN_NOT_OK(builder.memory_pool(::arrow::default_memory_pool())
                    ->properties(props)
                    ->Build(&reader));
  RETURN_NOT_OK(reader->ReadTable(&result));
  return result;
}

Result<std::shared_ptr<Buffer>> WriteTableToBuffer(
    const std::shared_ptr<Table>& table, int64_t min_chunk_size, int64_t max_chunk_size,
    int64_t row_group_length = 1024 * 1024, bool enable_dictionary = false,
    ParquetDataPageVersion data_page_version = ParquetDataPageVersion::V1) {
  auto sink = CreateOutputStream();

  auto builder = WriterProperties::Builder();
  builder.enable_content_defined_chunking()->content_defined_chunking_options(
      {min_chunk_size, max_chunk_size, /*norm_level=*/0});
  builder.data_page_version(data_page_version);
  if (enable_dictionary) {
    builder.enable_dictionary();
  } else {
    builder.disable_dictionary();
  }
  auto write_props = builder.build();
  auto arrow_props = ArrowWriterProperties::Builder().store_schema()->build();
  RETURN_NOT_OK(WriteTable(*table, default_memory_pool(), sink, row_group_length,
                           write_props, arrow_props));
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());

  // validate that the data correctly roundtrips
  ARROW_ASSIGN_OR_RAISE(auto readback, ReadTableFromBuffer(buffer));
  RETURN_NOT_OK(readback->ValidateFull());
  ARROW_RETURN_IF(!readback->Equals(*table),
                  Status::Invalid("Readback table not equal to original"));

  return buffer;
}

// Type to represent a list of chunks where each element is the size of the chunk.
using ChunkList = std::vector<int64_t>;

// Type to represent the sizes and lengths of the data pages in a column.

struct ColumnInfo {
  ChunkList page_lengths;
  ChunkList page_sizes;
  bool has_dictionary_page = false;
};

using ParquetInfo = std::vector<ColumnInfo>;

ParquetInfo GetColumnParquetInfo(const std::shared_ptr<Buffer>& data, int column_index) {
  // Read the parquet data out of the buffer and get the sizes and lengths of the
  // data pages in given column. We assert on the sizes and lengths of the pages
  // to ensure that the chunking is done correctly.
  ParquetInfo result;

  auto buffer_reader = std::make_shared<BufferReader>(data);
  auto parquet_reader = ParquetFileReader::Open(std::move(buffer_reader));

  auto metadata = parquet_reader->metadata();
  for (int rg = 0; rg < metadata->num_row_groups(); rg++) {
    auto page_reader = parquet_reader->RowGroup(rg)->GetColumnPageReader(column_index);
    ColumnInfo column_info;
    while (auto page = page_reader->NextPage()) {
      if (page->type() == PageType::DATA_PAGE || page->type() == PageType::DATA_PAGE_V2) {
        auto data_page = static_cast<DataPage*>(page.get());
        column_info.page_sizes.push_back(data_page->uncompressed_size());
        column_info.page_lengths.push_back(data_page->num_values());
      } else if (page->type() == PageType::DICTIONARY_PAGE) {
        column_info.has_dictionary_page = true;
      }
    }
    result.push_back(column_info);
  }

  return result;
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
      bool can_merge_a = prev.first.empty() && diff.second.empty();
      bool can_merge_b = prev.second.empty() && diff.first.empty();
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
                      const std::vector<ChunkDiff>& diffs) {
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

TEST(TestFindDifferences, ChangesAtBothEnds) {
  ChunkList first = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  ChunkList second = {0, 0, 2, 3, 4, 5, 7, 7, 8};
  auto diffs = FindDifferences(first, second);

  ASSERT_EQ(diffs.size(), 3);
  ASSERT_EQ(diffs[0].first, ChunkList({1}));
  ASSERT_EQ(diffs[0].second, ChunkList({0, 0}));

  ASSERT_EQ(diffs[1].first, ChunkList({6}));
  ASSERT_EQ(diffs[1].second, ChunkList({7}));

  ASSERT_EQ(diffs[2].first, ChunkList({9}));
  ASSERT_EQ(diffs[2].second, ChunkList({}));
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
}

TEST(TestFindDifferences, AdditionalCase) {
  ChunkList original = {445, 312, 393, 401, 410, 138, 558, 457};
  ChunkList modified = {445, 312, 393, 393, 410, 138, 558, 457};

  auto diffs = FindDifferences(original, modified);
  ASSERT_EQ(diffs.size(), 1);

  ASSERT_EQ(diffs[0].first, ChunkList({401}));
  ASSERT_EQ(diffs[0].second, ChunkList({393}));
}

void AssertPageLengthDifferences(const ColumnInfo& original, const ColumnInfo& modified,
                                 int32_t exact_number_of_equal_diffs,
                                 int32_t exact_number_of_larger_diffs,
                                 int32_t exact_number_of_smaller_diffs,
                                 const std::shared_ptr<ChunkedArray>& edit_array) {
  // Asserts that the differences between the original and modified page lengths
  // are as expected. A longest common subsequence diff is calculated on the original
  // and modified sequences of page lengths. The exact_number_of_equal_diffs,
  // exact_number_of_larger_diffs, and exact_number_of_smaller_diffs parameters specify
  // the expected number of differences with equal, larger, and smaller sums of the page
  // lengths respectively. The edit_length parameter is used to verify that the page
  // lenght differences are exactly equal to the edit_length.
  auto diffs = FindDifferences(original.page_lengths, modified.page_lengths);

  // Note, the assertion function assumes that all edits are made using the same edit
  // array, this could be improved by passing a list of edit arrays to the function
  // and calculating the edit length for each edit array.
  int64_t edit_length = edit_array->length();
  if (::arrow::is_list_like(edit_array->type()->id())) {
    // add null and empty lists to the edit length because the page length corresponds to
    // the number of def/rep levels rather than the number of elements in the array
    for (auto chunk : edit_array->chunks()) {
      auto list_array = checked_cast<const ::arrow::ListArray*>(chunk.get());
      for (int i = 0; i < list_array->length(); i++) {
        if (list_array->IsNull(i) || (list_array->value_length(i) == 0)) {
          edit_length++;
        }
      }
    }
  }

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

  int32_t equal_diffs = 0;
  int32_t larger_diffs = 0;
  int32_t smaller_diffs = 0;
  for (const auto& diff : diffs) {
    int64_t original_sum = 0, modified_sum = 0;
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
  }

  ASSERT_EQ(equal_diffs, exact_number_of_equal_diffs);
  ASSERT_EQ(larger_diffs, exact_number_of_larger_diffs);
  ASSERT_EQ(smaller_diffs, exact_number_of_smaller_diffs);
}

void AssertPageLengthDifferences(const ColumnInfo& original, const ColumnInfo& modified,
                                 int32_t max_number_of_equal_diffs) {
  // A less restrictive version of the above assertion function mainly used to
  // assert the update case.
  auto diffs = FindDifferences(original.page_lengths, modified.page_lengths);
  if (diffs.size() > static_cast<size_t>(max_number_of_equal_diffs)) {
    PrintDifferences(original.page_lengths, modified.page_lengths, diffs);
  }
  ASSERT_LE(diffs.size(), static_cast<size_t>(max_number_of_equal_diffs));

  for (const auto& diff : diffs) {
    int64_t left_sum = 0, right_sum = 0;
    for (const auto& val : diff.first) left_sum += val;
    for (const auto& val : diff.second) right_sum += val;
    // This is only used from the UpdateOnce and UpdateTwice test cases where the edit(s)
    // don't change the length of the original array, only update the value. This happens
    // to apply to the list types as well because of the consistent array data generation.
    ASSERT_EQ(left_sum, right_sum);
  }

  if (diffs.size() == 0) {
    // no differences found, the arrays are equal
    ASSERT_TRUE(original.page_lengths == modified.page_lengths);
  }
}

Result<int64_t> CalculateCdcSize(const std::shared_ptr<Array>& array, bool nullable) {
  // calculate the CDC chunk size based on the array elements belonging to a parquet page
  auto type_id = array->type()->id();

  int64_t result = 0;
  if (::arrow::is_fixed_width(type_id)) {
    int64_t element_size;
    if (array->type()->id() == ::arrow::Type::BOOL) {
      // the CDC chunker increments the chunk size by 1 for each boolean element
      element_size = 1;
    } else {
      element_size = array->type()->byte_width();
    }
    auto valid_elements = array->length() - array->null_count();
    result = valid_elements * element_size;
  } else if (::arrow::is_binary_like(type_id)) {
    auto binary_array = checked_cast<const ::arrow::BinaryArray*>(array.get());
    result += binary_array->total_values_length();
  } else if (::arrow::is_large_binary_like(type_id)) {
    auto binary_array = checked_cast<const ::arrow::LargeBinaryArray*>(array.get());
    result += binary_array->total_values_length();
  } else {
    return Status::NotImplemented("CDC size calculation for type ",
                                  array->type()->ToString(), " is not implemented");
  }

  if (nullable) {
    // in case of nullable types chunk size is calculated from def_levels and
    // the valid values
    return result + array->length() * sizeof(uint16_t);
  } else {
    // for non-nullable types the chunk size is calculated purely from the values
    return result;
  }
}

Result<int64_t> CalculateCdcSize(const std::shared_ptr<::arrow::ChunkedArray>& array,
                                 bool nullable) {
  int64_t result = 0;
  for (int i = 0; i < array->num_chunks(); i++) {
    ARROW_ASSIGN_OR_RAISE(auto chunk_size, CalculateCdcSize(array->chunk(i), nullable));
    result += chunk_size;
  }
  return result;
}

void AssertContentDefinedChunkSizes(const std::shared_ptr<::arrow::ChunkedArray>& array,
                                    const ColumnInfo& column_info, bool nullable,
                                    int64_t min_chunk_size, int64_t max_chunk_size,
                                    bool expect_dictionary_page) {
  // check that the chunk sizes are within the expected range

  // the test tables are combined in the test cases so we expect only a single chunk
  auto type_id = array->type()->id();

  // check for the dictionary page if expected
  if (type_id == ::arrow::Type::BOOL) {
    ASSERT_FALSE(column_info.has_dictionary_page);
  } else {
    ASSERT_EQ(column_info.has_dictionary_page, expect_dictionary_page);
    ASSERT_EQ(column_info.has_dictionary_page, expect_dictionary_page);
  }

  if (::arrow::is_fixed_width(type_id) || ::arrow::is_base_binary_like(type_id)) {
    int64_t offset = 0;

    auto page_lengths = column_info.page_lengths;
    for (size_t i = 0; i < page_lengths.size() - 1; i++) {
      // since CDC chunking is applied on the logical values before any parquet encoding
      // we first slice the array to get the logical array and then calculate the CDC
      // chunk size based on that
      auto page_length = page_lengths[i];
      auto array_chunk = array->Slice(offset, page_length);
      offset += page_length;

      ASSERT_OK_AND_ASSIGN(auto cdc_chunk_size, CalculateCdcSize(array_chunk, nullable));
      ASSERT_GE(cdc_chunk_size, min_chunk_size);
      ASSERT_LE(cdc_chunk_size, max_chunk_size);
    }

    auto last_page_length = page_lengths.back();
    auto last_array_chunk = array->Slice(offset, last_page_length);
    ASSERT_OK_AND_ASSIGN(auto last_cdc_chunk_size,
                         CalculateCdcSize(last_array_chunk, nullable));
    // min chunk size is not guaranteed for the last chunk, only check that it is not
    // larger than the max chunk size
    ASSERT_LE(last_cdc_chunk_size, max_chunk_size);

    // the sum of the page lengths should be equal to the length of the array
    offset += last_page_length;
    ASSERT_EQ(offset, array->length());
  }

  // TODO(kszucs): have approximate size assertions for variable length types because
  // we cannot calculate accurate CDC chunk sizes for list-like types without actually
  // scanning the data and reimplementing the logic from the CDC chunker
}

class TestCDC : public ::testing::Test {
 protected:
  static constexpr int64_t kMinChunkSize = 4 * 1024;
  static constexpr int64_t kMaxChunkSize = 16 * 1024;
  uint64_t GetRollingHashMask(const ContentDefinedChunker& cdc) const {
    return cdc.GetRollingHashMask();
  }
};

TEST_F(TestCDC, ChunkSizeParameterValidation) {
  // Test that constructor validates min/max chunk size parameters
  auto li = LevelInfo();

  ASSERT_NO_THROW(ContentDefinedChunker(li, 256 * 1024, 1024 * 1024));

  // with norm_level=0 the difference between min and max chunk size must be
  // at least 16
  ASSERT_THROW(ContentDefinedChunker(li, 0, -1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1024, 512), ParquetException);

  ASSERT_THROW(ContentDefinedChunker(li, -1, 0), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 0, 0), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 0, 16), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 32));
  ASSERT_THROW(ContentDefinedChunker(li, -16, -16), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 16, 0), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 32, 32), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 32, 48), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 32, 64));
  ASSERT_NO_THROW(ContentDefinedChunker(li, 1024 * 1024, 2 * 1024 * 1024));
  ASSERT_NO_THROW(
      ContentDefinedChunker(li, 1024 * 1024 * 1024L, 2LL * 1024 * 1024 * 1024L));

  // with norm_level=1 the difference between min and max chunk size must be
  // at least 64
  ASSERT_THROW(ContentDefinedChunker(li, 1, -1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, -1, 1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1, 1, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1, 32, 1), ParquetException);
  ASSERT_THROW(ContentDefinedChunker(li, 1, 33, 1), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 1, 65, 1));

  // with norm_level=2 the difference between min and max chunk size must be
  // at least 128
  ASSERT_THROW(ContentDefinedChunker(li, 0, 123, 2), ParquetException);
  ASSERT_NO_THROW(ContentDefinedChunker(li, 0, 128, 2));
}

TEST_F(TestCDC, RollingHashMaskCalculation) {
  auto le = LevelInfo();
  auto min_size = 256 * 1024;
  auto max_size = 1024 * 1024;

  auto cdc0 = ContentDefinedChunker(le, min_size, max_size, 0);
  ASSERT_EQ(GetRollingHashMask(cdc0), 0xFFFE000000000000);

  auto cdc1 = ContentDefinedChunker(le, min_size, max_size, 1);
  ASSERT_EQ(GetRollingHashMask(cdc1), 0xFFFC000000000000);

  auto cdc2 = ContentDefinedChunker(le, min_size, max_size, 2);
  ASSERT_EQ(GetRollingHashMask(cdc2), 0xFFF8000000000000);

  auto cdc3 = ContentDefinedChunker(le, min_size, max_size, 3);
  ASSERT_EQ(GetRollingHashMask(cdc3), 0xFFF0000000000000);

  auto cdc4 = ContentDefinedChunker(le, min_size, max_size, -1);
  ASSERT_EQ(GetRollingHashMask(cdc4), 0xFFFF000000000000);

  // check that mask bits are between 1 and 63 after adjusting with the 8 hash tables
  ASSERT_THROW(ContentDefinedChunker(le, 0, 16, 0), ParquetException);

  auto cdc5 = ContentDefinedChunker(le, 0, 32, 0);
  ASSERT_EQ(GetRollingHashMask(cdc5), 0x8000000000000000);
  ASSERT_THROW(ContentDefinedChunker(le, 0, 32, 1), ParquetException);

  auto cdc6 = ContentDefinedChunker(le, 0, 64, 0);
  ASSERT_EQ(GetRollingHashMask(cdc6), 0xC000000000000000);

  auto cdc7 = ContentDefinedChunker(le, 0, 16, -1);
  ASSERT_EQ(GetRollingHashMask(cdc7), 0x8000000000000000);

  // another unrealistic case, checking for the validation
  ASSERT_THROW(ContentDefinedChunker(le, 128, 384, -60), ParquetException);
  auto cdc8 = ContentDefinedChunker(le, 128, 384, -59);
  ASSERT_EQ(GetRollingHashMask(cdc8), 0xFFFFFFFFFFFFFFFE);
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

  auto expected_msg = ::testing::Property(
      &ParquetException::what,
      ::testing::HasSubstr("Content-defined chunking is not supported in WriteBatch() or "
                           "WriteBatchSpaced(), use WriteArrow() instead."));
  EXPECT_THROW_THAT(
      [&]() {
        int_column_writer.WriteBatch(numbers.size(), nullptr, nullptr, numbers.data());
      },
      ParquetException, expected_msg);
  EXPECT_THROW_THAT(
      [&]() {
        int_column_writer.WriteBatchSpaced(numbers.size(), nullptr, nullptr,
                                           valid_bits.data(), 0, numbers.data());
      },
      ParquetException, expected_msg);
}

TEST_F(TestCDC, LastChunkDoesntTriggerAddDataPage) {
  // Define the schema with a single column "number"
  auto schema = std::dynamic_pointer_cast<schema::GroupNode>(schema::GroupNode::Make(
      "root", Repetition::REQUIRED,
      {schema::PrimitiveNode::Make("number", Repetition::REQUIRED, Type::INT32)}));

  auto sink = CreateOutputStream();
  auto builder = WriterProperties::Builder();
  auto props = builder.enable_content_defined_chunking()
                   ->content_defined_chunking_options({kMinChunkSize, kMaxChunkSize, 0})
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

  auto info = GetColumnParquetInfo(buffer, /*column_index=*/0);
  ASSERT_EQ(info.size(), 1);

  ASSERT_OK_AND_ASSIGN(auto chunked_array, ChunkedArray::Make({array, array}));
  AssertContentDefinedChunkSizes(chunked_array, info.front(), /*nullable=*/false,
                                 kMinChunkSize, kMaxChunkSize,
                                 /*expect_dictionary_page=*/false);
}

struct CaseConfig {
  // Arrow data type to generate the testing data for
  std::shared_ptr<::arrow::DataType> dtype;
  // Whether the data type is nullable
  bool is_nullable;
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

class TestCDCSingleRowGroup : public ::testing::TestWithParam<CaseConfig> {
 protected:
  static constexpr int64_t kPartSize = 128 * 1024;
  static constexpr int64_t kEditSize = 128;
  static constexpr int64_t kMinChunkSize = 4 * 1024;
  static constexpr int64_t kMaxChunkSize = 16 * 1024;
  static constexpr int64_t kRowGroupLength = 1024 * 1024;

  // Column random table parts for testing
  std::shared_ptr<Field> field_;
  std::shared_ptr<Table> part1_, part2_, part3_, part4_, part5_, part6_, part7_;

  void SetUp() override {
    const auto& param = GetParam();
    auto field_ = ::arrow::field("f0", param.dtype, param.is_nullable);
    auto schema = ::arrow::schema({field_});

    // since the chunk sizes are constant we derive the number of records to generate
    // from the size of the data type unless it is nested or variable length where
    // we use a hand picked value to avoid generating too large tables
    int64_t bytes_per_record;
    if (param.dtype->byte_width() > 0) {
      bytes_per_record = param.dtype->byte_width();
      if (param.is_nullable) {
        bytes_per_record += sizeof(uint16_t);
      }
    } else {
      // for variable length types we use the size of the first element
      bytes_per_record = 16;
    }

    auto part_length = kPartSize / bytes_per_record;
    auto edit_length = kEditSize / bytes_per_record;

    ASSERT_OK_AND_ASSIGN(part1_, GenerateTable(schema, part_length, /*seed=*/0));
    ASSERT_OK_AND_ASSIGN(part2_, GenerateTable(schema, edit_length, /*seed=*/1));
    ASSERT_OK_AND_ASSIGN(part3_,
                         GenerateTable(schema, part_length, /*seed=*/part_length));
    ASSERT_OK_AND_ASSIGN(part4_, GenerateTable(schema, edit_length, /*seed=*/2));
    ASSERT_OK_AND_ASSIGN(part5_,
                         GenerateTable(schema, part_length, /*seed=*/2 * part_length));
    ASSERT_OK_AND_ASSIGN(part6_, GenerateTable(schema, edit_length, /*seed=*/3));
    ASSERT_OK_AND_ASSIGN(part7_, GenerateTable(schema, edit_length, /*seed=*/4));
  }
};

TEST_P(TestCDCSingleRowGroup, DeleteOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there is a single "diff" between the two page length sequences
    // and that the diff removes edit_length number of values, there should be no
    // other differences because we deal with a single row group (in case of multiple
    // row groups the first page of each subsequent row group would be different due
    // to shifting caused by the fixed sized row group length)
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/0,
                                /*exact_number_of_smaller_diffs=*/1, part2_->column(0));
  }
}

TEST_P(TestCDCSingleRowGroup, DeleteTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there are exactly two "diffs" between the two page length sequences
    // and those diffs remove edit_length number of values (part2 and part4 have the
    // same number of values), there should be no other differences because we have
    // a single row group
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/0,
                                /*exact_number_of_smaller_diffs=*/2, part2_->column(0));
  }
}

TEST_P(TestCDCSingleRowGroup, UpdateOnce) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part4_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there is a single "diff" between the two page length sequences
    // which doesn't change the length of the array, only the values are updated
    // there should be no other differences because we deal with a single row group
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
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there are exactly two "diffs" between the two page length sequences
    // which don't change the length of the array, only the values are updated
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
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there is a single "diff" between the two page length sequences
    // adding edit_length number of values, there should be no other differences
    // because we deal with a single row group and made a single modification
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/0, part2_->column(0));
  }
}

TEST_P(TestCDCSingleRowGroup, InsertTwice) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part3_, part5_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, part2_, part3_, part4_, part5_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    // check that there are exactly two "diffs" between the two page length sequences
    // which add edit_length number of values, there should be no other differences
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/2,
                                /*exact_number_of_smaller_diffs=*/0, part2_->column(0));
  }
}

TEST_P(TestCDCSingleRowGroup, Prepend) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part4_, part1_, part2_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    auto original_page_lengths = base_info.front().page_lengths;
    auto modified_page_lengths = modified_info.front().page_lengths;

    // we expect to have the same number or more pages at the beginning of the
    // modified file without increasing the size of any subsequent page
    ASSERT_LE(original_page_lengths.size(), modified_page_lengths.size());
    AssertPageLengthDifferences(base_info.front(), modified_info.front(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/0, part4_->column(0));
  }
}

TEST_P(TestCDCSingleRowGroup, Append) {
  const auto& param = GetParam();

  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_, part4_}));
  ASSERT_FALSE(base->Equals(*modified));

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto base_parquet,
        WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));
    ASSERT_OK_AND_ASSIGN(
        auto modified_parquet,
        WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/0);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/0);

    // assert that there is only one row group
    ASSERT_EQ(base_info.size(), 1);
    ASSERT_EQ(modified_info.size(), 1);

    // check that the chunk sizes are within the expected range
    AssertContentDefinedChunkSizes(base->column(0), base_info.front(), param.is_nullable,
                                   kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);
    AssertContentDefinedChunkSizes(modified->column(0), modified_info.front(),
                                   param.is_nullable, kMinChunkSize, kMaxChunkSize,
                                   /*expect_dictionary_page=*/enable_dictionary);

    auto original_page_lengths = base_info.front().page_lengths;
    auto modified_page_lengths = modified_info.front().page_lengths;

    // there are either additional pages and/or the last page is larger in the modified
    // than in the original file
    ASSERT_LE(original_page_lengths.size(), modified_page_lengths.size());
    // all pages must be identical except for the last one which can be larger
    for (size_t i = 0; i < original_page_lengths.size() - 1; i++) {
      ASSERT_EQ(original_page_lengths[i], modified_page_lengths[i]);
    }
    auto last_index = original_page_lengths.size() - 1;
    ASSERT_GE(modified_page_lengths[last_index], original_page_lengths[last_index]);
  }
}

TEST_P(TestCDCSingleRowGroup, EmptyTable) {
  const auto& param = GetParam();

  auto schema = ::arrow::schema({::arrow::field("f0", param.dtype, param.is_nullable)});
  ASSERT_OK_AND_ASSIGN(auto empty_table, GenerateTable(schema, 0, /*seed=*/0));
  ASSERT_EQ(empty_table->num_rows(), 0);

  for (bool enable_dictionary : {false, true}) {
    ASSERT_OK_AND_ASSIGN(
        auto parquet,
        WriteTableToBuffer(empty_table, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           enable_dictionary, param.data_page_version));

    auto info = GetColumnParquetInfo(parquet, /*column_index=*/0);

    // There is a single row group
    ASSERT_EQ(info.size(), 1);

    // An empty table should result in no data pages
    ASSERT_TRUE(info.front().page_lengths.empty());
    ASSERT_TRUE(info.front().page_sizes.empty());
  }
}

TEST_P(TestCDCSingleRowGroup, ArrayOffsets) {
  // check that the array offsets are respected in the chunker
  const auto& param = GetParam();
  ASSERT_OK_AND_ASSIGN(auto table, ConcatAndCombine({part1_, part2_, part3_}));

  for (auto offset : {0, 512, 1024}) {
    auto sliced_table = table->Slice(offset);

    // assert that the first column has a non-zero offset
    auto column = sliced_table->column(0);
    auto first_chunk = column->chunk(0);
    ASSERT_EQ(first_chunk->offset(), offset);

    // write out the sliced table, read it back and compare
    ASSERT_OK_AND_ASSIGN(
        auto sliced_parquet,
        WriteTableToBuffer(sliced_table, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                           true, param.data_page_version));

    auto sliced_info = GetColumnParquetInfo(sliced_parquet, /*column_index=*/0);
    ASSERT_EQ(sliced_info.size(), 1);
  }
}

#if defined(ADDRESS_SANITIZER) || defined(ARROW_VALGRIND)
// Instantiate the test suite with a reduced set of types to avoid slow tests
INSTANTIATE_TEST_SUITE_P(
    Types, TestCDCSingleRowGroup,
    testing::Values(
        CaseConfig{::arrow::boolean(), false}, CaseConfig{::arrow::int64(), true},
        // Binary-like
        CaseConfig{::arrow::utf8(), false},
        CaseConfig{::arrow::fixed_size_binary(16), true},
        // Nested types
        CaseConfig{::arrow::list(::arrow::int32()), false},
        CaseConfig{::arrow::list(::arrow::utf8()), true},
        CaseConfig{::arrow::struct_({::arrow::field("f0", ::arrow::float64())}), true}));
#else
INSTANTIATE_TEST_SUITE_P(
    Types, TestCDCSingleRowGroup,
    testing::Values(
        // Boolean
        CaseConfig{::arrow::boolean(), false},
        // Numeric
        CaseConfig{::arrow::uint8(), false}, CaseConfig{::arrow::uint16(), false},
        CaseConfig{::arrow::uint32(), false}, CaseConfig{::arrow::uint64(), true},
        CaseConfig{::arrow::int8(), false}, CaseConfig{::arrow::int16(), false},
        CaseConfig{::arrow::int32(), false}, CaseConfig{::arrow::int64(), true},
        CaseConfig{::arrow::float16(), false}, CaseConfig{::arrow::float32(), false},
        CaseConfig{::arrow::float64(), true},
        CaseConfig{::arrow::decimal128(18, 6), false},
        CaseConfig{::arrow::decimal256(40, 6), false},
        // Binary-like
        CaseConfig{::arrow::utf8(), false}, CaseConfig{::arrow::binary(), true},
        CaseConfig{::arrow::fixed_size_binary(16), true},
        // Temporal
        CaseConfig{::arrow::date32(), false},
        CaseConfig{::arrow::time32(::arrow::TimeUnit::MILLI), true},
        CaseConfig{::arrow::time64(::arrow::TimeUnit::NANO), false},
        CaseConfig{::arrow::timestamp(::arrow::TimeUnit::NANO), true},
        CaseConfig{::arrow::duration(::arrow::TimeUnit::NANO), false},
        // Nested types
        CaseConfig{::arrow::list(::arrow::int16()), false},
        CaseConfig{::arrow::list(::arrow::int32()), true},
        CaseConfig{::arrow::list(::arrow::utf8()), true},
        CaseConfig{::arrow::struct_({::arrow::field("f0", ::arrow::int32())}), false},
        CaseConfig{::arrow::struct_({::arrow::field("f0", ::arrow::float64())}), true},
        CaseConfig{
            ::arrow::list(::arrow::struct_({::arrow::field("f0", ::arrow::int32())})),
            false},
        // Extension type
        CaseConfig{::arrow::extension::json(), true},
        // Use ParquetDataPageVersion::V2
        CaseConfig{::arrow::large_binary(), false, ParquetDataPageVersion::V2},
        CaseConfig{::arrow::list(::arrow::utf8()), true, ParquetDataPageVersion::V2}));
#endif

class TestCDCMultipleRowGroups : public ::testing::Test {
 protected:
  static auto constexpr kPartLength = 128 * 1024;
  static auto constexpr kEditLength = 128;
  static auto constexpr kRowGroupLength = 64 * 1024;
  static auto constexpr kEnableDictionary = false;
  static auto constexpr kMinChunkSize = 4 * 1024;
  static auto constexpr kMaxChunkSize = 16 * 1024;

  // Column random table parts for testing

  std::shared_ptr<Table> part1_, part2_, part3_;
  std::shared_ptr<Table> edit1_, edit2_, edit3_;

  void SetUp() override {
    auto schema = ::arrow::schema({
        ::arrow::field("int32", ::arrow::int32(), true),
        ::arrow::field("float64", ::arrow::float64(), true),
        ::arrow::field("bool", ::arrow::boolean(), false),
    });

    ASSERT_OK_AND_ASSIGN(part1_, GenerateTable(schema, kPartLength, /*seed=*/0));
    ASSERT_OK_AND_ASSIGN(part2_, GenerateTable(schema, kPartLength, /*seed=*/2));
    ASSERT_OK_AND_ASSIGN(part3_, GenerateTable(schema, kPartLength, /*seed=*/4));

    ASSERT_OK_AND_ASSIGN(edit1_, GenerateTable(schema, kEditLength, /*seed=*/1));
    ASSERT_OK_AND_ASSIGN(edit2_, GenerateTable(schema, kEditLength, /*seed=*/3));
    ASSERT_OK_AND_ASSIGN(edit3_, GenerateTable(schema, kEditLength, /*seed=*/5));
  }
};

TEST_F(TestCDCMultipleRowGroups, InsertOnce) {
  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, edit1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, edit1_, edit2_, part2_, part3_}));
  ASSERT_FALSE(base->Equals(*modified));
  ASSERT_EQ(modified->num_rows(), base->num_rows() + edit2_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_parquet,
      WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));
  ASSERT_OK_AND_ASSIGN(
      auto modified_parquet,
      WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));

  for (int col = 0; col < base->num_columns(); col++) {
    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/col);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/col);

    // assert that there are 7 row groups
    ASSERT_EQ(base_info.size(), 7);
    ASSERT_EQ(modified_info.size(), 7);

    // the first two row groups should be identical, each part contains two row groups and
    // the first part is not modified
    ASSERT_EQ(base_info.at(0).page_lengths, modified_info.at(0).page_lengths);
    ASSERT_EQ(base_info.at(1).page_lengths, modified_info.at(1).page_lengths);
    // then there is an insertion which causes a larger "diff" somewhere in the row group
    // and a smaller "diff" at the end of the row group because the row group length is
    // fixed; this rule applies to the subsequent row groups as well because the values
    // are shifted by the insertion
    auto edit_array = edit2_->column(col);
    for (size_t i = 2; i < modified_info.size() - 1; i++) {
      AssertPageLengthDifferences(base_info.at(i), modified_info.at(i),
                                  /*exact_number_of_equal_diffs=*/0,
                                  /*exact_number_of_larger_diffs=*/1,
                                  /*exact_number_of_smaller_diffs=*/1, edit_array);
    }
    // the last row group will simply be larger because of the insertion
    AssertPageLengthDifferences(base_info.back(), modified_info.back(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/1,
                                /*exact_number_of_smaller_diffs=*/0, edit_array);
  }
}

TEST_F(TestCDCMultipleRowGroups, DeleteOnce) {
  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_OK_AND_ASSIGN(auto modified, ConcatAndCombine({part1_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*modified));
  ASSERT_EQ(modified->num_rows(), base->num_rows() - edit1_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_parquet,
      WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));
  ASSERT_OK_AND_ASSIGN(
      auto modified_parquet,
      WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));

  for (int col = 0; col < base->num_columns(); col++) {
    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/col);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/col);

    // assert that there are 7 row groups
    ASSERT_EQ(base_info.size(), 7);
    ASSERT_EQ(modified_info.size(), 7);

    // the first two row groups should be identical, each part contains two row groups and
    // the first part is not modified
    ASSERT_EQ(base_info.at(0).page_lengths, modified_info.at(0).page_lengths);
    ASSERT_EQ(base_info.at(1).page_lengths, modified_info.at(1).page_lengths);
    // because of the deletion values are shifted in the row group, we expect a smaller
    // "diff" at the beginning of the row group and a larger "diff" at the end of the
    // row group
    auto edit_array = edit2_->column(col);
    for (size_t i = 2; i < modified_info.size() - 1; i++) {
      AssertPageLengthDifferences(base_info.at(i), modified_info.at(i),
                                  /*exact_number_of_equal_diffs=*/0,
                                  /*exact_number_of_larger_diffs=*/1,
                                  /*exact_number_of_smaller_diffs=*/1, edit_array);
    }
    // the last row group will simply be smaller because of the deletion
    AssertPageLengthDifferences(base_info.back(), modified_info.back(),
                                /*exact_number_of_equal_diffs=*/0,
                                /*exact_number_of_larger_diffs=*/0,
                                /*exact_number_of_smaller_diffs=*/1, edit_array);
  }
}

TEST_F(TestCDCMultipleRowGroups, UpdateOnce) {
  ASSERT_OK_AND_ASSIGN(auto base,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, edit3_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*modified));

  ASSERT_OK_AND_ASSIGN(
      auto base_parquet,
      WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));
  ASSERT_OK_AND_ASSIGN(
      auto modified_parquet,
      WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));
  for (int col = 0; col < base->num_columns(); col++) {
    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/col);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/col);

    // assert that there are 7 row groups
    ASSERT_EQ(base_info.size(), 7);
    ASSERT_EQ(modified_info.size(), 7);

    // the first two row groups should be identical, each part contains two row groups and
    // the first part is not modified
    ASSERT_EQ(base_info.at(0).page_lengths, modified_info.at(0).page_lengths);
    ASSERT_EQ(base_info.at(1).page_lengths, modified_info.at(1).page_lengths);
    // then there is an update (without insertion or deletion so no shifting occurs) which
    // causes a "diff" with both sides having the same number of values but different ones
    AssertPageLengthDifferences(base_info.at(2), modified_info.at(2),
                                /*max_number_of_equal_diffs=*/1);
    for (size_t i = 2; i < modified_info.size(); i++) {
      // the rest of the row groups should be identical
      ASSERT_EQ(base_info.at(i).page_lengths, modified_info.at(i).page_lengths);
    }
  }
}

TEST_F(TestCDCMultipleRowGroups, Append) {
  ASSERT_OK_AND_ASSIGN(auto base, ConcatAndCombine({part1_, edit1_, part2_, part3_}));
  ASSERT_OK_AND_ASSIGN(auto modified,
                       ConcatAndCombine({part1_, edit1_, part2_, part3_, edit2_}));
  ASSERT_FALSE(base->Equals(*modified));
  ASSERT_EQ(modified->num_rows(), base->num_rows() + edit2_->num_rows());

  ASSERT_OK_AND_ASSIGN(
      auto base_parquet,
      WriteTableToBuffer(base, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));
  ASSERT_OK_AND_ASSIGN(
      auto modified_parquet,
      WriteTableToBuffer(modified, kMinChunkSize, kMaxChunkSize, kRowGroupLength,
                         kEnableDictionary, ParquetDataPageVersion::V1));

  for (int col = 0; col < base->num_columns(); col++) {
    auto base_info = GetColumnParquetInfo(base_parquet, /*column_index=*/col);
    auto modified_info = GetColumnParquetInfo(modified_parquet, /*column_index=*/col);

    // assert that there are 7 row groups
    ASSERT_EQ(base_info.size(), 7);
    ASSERT_EQ(modified_info.size(), 7);

    for (size_t i = 0; i < modified_info.size() - 1; i++) {
      ASSERT_EQ(base_info.at(i).page_lengths, modified_info.at(i).page_lengths);
    }
    // only the last row group should have more or equal number of pages
    auto original_page_lengths = base_info.back().page_lengths;
    auto modified_page_lengths = modified_info.back().page_lengths;

    // the last row group should be larger or equal in size
    ASSERT_GE(original_page_lengths.size(), modified_page_lengths.size());
    // all pages must be identical except for the last one which can be larger
    for (size_t i = 0; i < original_page_lengths.size() - 1; i++) {
      ASSERT_EQ(original_page_lengths[i], modified_page_lengths[i]);
    }
    ASSERT_GT(modified_page_lengths.back(), original_page_lengths.back());
  }
}

}  // namespace parquet::internal

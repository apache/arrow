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

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/extension/parquet_variant.h"
#include "arrow/extension_type.h"
#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/json/rapidjson_defs.h"  // IWYU pragma: keep
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/exception.h"
#include "parquet/variant/builder.h"
#include "parquet/variant/decoding.h"
#include "parquet/variant/unshred.h"
#include "parquet/variant/validate.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

namespace parquet::variant {

using ::arrow::internal::checked_pointer_cast;

namespace {

namespace rj = ::arrow::rapidjson;

using ExpectedCases = std::map<int, std::string_view>;

constexpr int kMissingParquetFileCaseNumber = 3;

const ExpectedCases kSkipUnshreddedCases = {
    {43, "testPartiallyShreddedObjectMissingFieldConflict"},
    {125, "testPartiallyShreddedObjectFieldConflict"},
};

const ExpectedCases kReadCompatOnlyCases = {
    {41, "testArrayMissingValueColumn"},
    {43, "testPartiallyShreddedObjectMissingFieldConflict"},
    {84, "testShreddedObjectWithOptionalFieldStructs"},
    {125, "testPartiallyShreddedObjectFieldConflict"},
    {131, "testMissingValueColumn"},
    {132, "testShreddedObjectMissingFieldValueColumn"},
    {138, "testShreddedObjectMissingValueColumn"},
};

const ExpectedCases kReadCompatErrorCases = {
    {40, "testArrayWithElementValueTypedValueConflict"},
    {42, "testValueAndTypedValueConflict"},
    {87, "testNonObjectWithNonNullShreddedFields"},
    {128, "testEmptyPartiallyShreddedObjectConflict"},
};

const ExpectedCases kUnsupportedTypedValueCases = {
    {127, "testUnsignedInteger"},
    {137, "testFixedLengthByteArray"},
};

std::optional<std::string> ParquetTestingSiblingDir(std::string_view name) {
  auto data = ::arrow::internal::GetEnvVar("PARQUET_TEST_DATA");
  if (!data.ok() || data->empty()) {
    return std::nullopt;
  }
  auto dir = *data + "/../" + std::string(name);
  PARQUET_ASSIGN_OR_THROW(auto dir_path,
                          ::arrow::internal::PlatformFilename::FromString(dir));
  PARQUET_ASSIGN_OR_THROW(auto exists, ::arrow::internal::FileExists(dir_path));
  if (!exists) {
    return std::nullopt;
  }
  return dir;
}

std::shared_ptr<Buffer> ReadFile(const std::string& path) {
  PARQUET_ASSIGN_OR_THROW(auto file, ::arrow::io::ReadableFile::Open(path));
  PARQUET_ASSIGN_OR_THROW(auto size, file->GetSize());
  PARQUET_ASSIGN_OR_THROW(auto data, file->Read(size));
  return data;
}

class TestVariantParquetTesting : public ::testing::Test {
 protected:
  using Case = std::pair<std::string, EncodedVariantValue>;

  static EncodedVariantValue LoadEncodedVariantFile(const std::string& dir,
                                                    const std::string& name) {
    auto metadata = ReadFile(dir + "/" + name + ".metadata");
    auto value = ReadFile(dir + "/" + name + ".value");
    return EncodedVariantValue{.metadata = std::move(metadata),
                               .value = std::move(value)};
  }

  static std::vector<Case> LoadEncodedVariantCases(const std::string& dir) {
    constexpr std::string_view kMetadataSuffix = ".metadata";
    PARQUET_ASSIGN_OR_THROW(auto dir_path,
                            ::arrow::internal::PlatformFilename::FromString(dir));
    PARQUET_ASSIGN_OR_THROW(auto entries, ::arrow::internal::ListDir(dir_path));

    std::vector<Case> cases;
    for (const auto& entry : entries) {
      auto filename = entry.ToString();
      if (filename.ends_with(kMetadataSuffix)) {
        auto name = filename.substr(0, filename.size() - kMetadataSuffix.size());
        auto value = LoadEncodedVariantFile(dir, name);
        cases.emplace_back(std::move(name), std::move(value));
      }
    }
    return cases;
  }

  static std::shared_ptr<::arrow::Table> RoundTripVariantArray(
      const std::shared_ptr<::arrow::extension::VariantArray>& array) {
    auto table = ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("variant", array->type())}), {array});
    auto arrow_writer_properties =
        ArrowWriterProperties::Builder().set_variant_validation_enabled(true)->build();
    PARQUET_ASSIGN_OR_THROW(auto sink, ::arrow::io::BufferOutputStream::Create());
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        *table, ::arrow::default_memory_pool(), sink, /*chunk_size=*/table->num_rows(),
        default_writer_properties(), std::move(arrow_writer_properties)));
    PARQUET_ASSIGN_OR_THROW(auto buffer, sink->Finish());

    std::optional<::arrow::ExtensionTypeGuard> guard;
    if (::arrow::GetExtensionType(
            std::string(::arrow::extension::kVariantExtensionName)) == nullptr) {
      guard.emplace(array->type());
    }
    ArrowReaderProperties reader_properties;
    reader_properties.set_arrow_extensions_enabled(true);
    reader_properties.set_variant_validation_enabled(true);
    parquet::arrow::FileReaderBuilder reader_builder;
    PARQUET_THROW_NOT_OK(reader_builder.Open(
        std::make_shared<::arrow::io::BufferReader>(std::move(buffer))));
    reader_builder.properties(reader_properties);
    PARQUET_ASSIGN_OR_THROW(auto reader, reader_builder.Build());
    PARQUET_ASSIGN_OR_THROW(auto read_table, reader->ReadTable());
    return read_table;
  }

  void SetUp() override {
    auto maybe_dir = ParquetTestingSiblingDir("variant");
    if (!maybe_dir.has_value()) {
      GTEST_SKIP() << "PARQUET_TEST_DATA not set or variant folder missing";
    }
    ASSERT_NO_THROW(cases_ = LoadEncodedVariantCases(*maybe_dir));
  }

  std::vector<Case> cases_;
};

class TestShreddedVariantParquetTesting : public ::testing::Test {
 protected:
  struct ShreddedVariantCase {
    std::string test_name;
    std::string parquet_file;
    std::vector<std::optional<std::string>> expected_files;
    std::optional<std::string> error_message;
  };

  std::shared_ptr<::arrow::Table> ReadVariantTestingTable(
      const std::string& parquet_file,
      ArrowReaderProperties reader_properties = ArrowReaderProperties()) const {
    auto registered_storage_type = ::arrow::struct_(
        {::arrow::field("metadata", ::arrow::binary(), /*nullable=*/false),
         ::arrow::field("value", ::arrow::binary(), /*nullable=*/false)});
    std::optional<::arrow::ExtensionTypeGuard> guard;
    if (::arrow::GetExtensionType(
            std::string(::arrow::extension::kVariantExtensionName)) == nullptr) {
      guard.emplace(::arrow::extension::variant(registered_storage_type));
    }

    parquet::arrow::FileReaderBuilder builder;
    PARQUET_THROW_NOT_OK(
        builder.OpenFile(dir_ + "/" + parquet_file, /*memory_map=*/false));
    reader_properties.set_arrow_extensions_enabled(true);
    builder.properties(reader_properties);
    PARQUET_ASSIGN_OR_THROW(auto reader, builder.Build());
    PARQUET_ASSIGN_OR_THROW(auto table, reader->ReadTable());
    return table;
  }

  EncodedVariantValue LoadEncodedVariantBinaryFile(const std::string& name) const {
    auto encoded_buffer = ReadFile(dir_ + "/" + name);
    size_t metadata_size;
    auto metadata_view = VariantMetadataView::ParsePrefix(
        std::string_view{*encoded_buffer}, &metadata_size);
    auto metadata =
        ::arrow::SliceBuffer(encoded_buffer, 0, static_cast<int64_t>(metadata_size));
    auto value =
        ::arrow::SliceBuffer(encoded_buffer, static_cast<int64_t>(metadata_size));
    VariantValueView::Validate(std::string_view{*value}, metadata_view);
    return EncodedVariantValue{.metadata = std::move(metadata),
                               .value = std::move(value)};
  }

  std::unordered_map<int, ShreddedVariantCase> LoadShreddedVariantCases() const {
    auto data = ReadFile(dir_ + "/cases.json");
    const auto data_view = std::string_view{*data};
    rj::Document document;
    document.Parse(data_view.data(), data_view.size());
    if (document.HasParseError()) {
      throw ParquetException("Cannot parse cases.json: ",
                             rj::GetParseError_En(document.GetParseError()));
    }
    if (!document.IsArray()) {
      throw ParquetException("Expected cases.json root array");
    }

    std::unordered_set<int> case_numbers;
    std::unordered_map<int, ShreddedVariantCase> cases;
    cases.reserve(document.Size());
    for (const auto& value : document.GetArray()) {
      if (!value.IsObject()) {
        throw ParquetException("Expected cases.json entry object");
      }
      auto case_number = value.FindMember("case_number");
      if (case_number == value.MemberEnd() || !case_number->value.IsInt()) {
        throw ParquetException("Expected case_number integer");
      }
      const int number = case_number->value.GetInt();
      if (!case_numbers.insert(number).second) {
        throw ParquetException("Duplicate case_number: ", number);
      }

      auto parquet_file = value.FindMember("parquet_file");
      if (parquet_file == value.MemberEnd()) {
        if (number != kMissingParquetFileCaseNumber || value.MemberCount() != 1) {
          throw ParquetException("Missing parquet_file for case ", number);
        }
        continue;
      }
      if (!parquet_file->value.IsString()) {
        throw ParquetException("Expected parquet_file string for case ", number);
      }
      auto test = value.FindMember("test");
      if (test == value.MemberEnd() || !test->value.IsString()) {
        throw ParquetException("Expected test string for case ", number);
      }

      ShreddedVariantCase test_case;
      test_case.test_name = test->value.GetString();
      test_case.parquet_file = parquet_file->value.GetString();

      auto variant_file = value.FindMember("variant_file");
      auto variant_files = value.FindMember("variant_files");
      if (variant_file != value.MemberEnd() && variant_files != value.MemberEnd()) {
        throw ParquetException("Both variant_file and variant_files are set for case ",
                               number);
      }
      if (variant_file != value.MemberEnd()) {
        if (!variant_file->value.IsString()) {
          throw ParquetException("Expected variant_file string for case ", number);
        }
        test_case.expected_files.emplace_back(variant_file->value.GetString());
      } else if (variant_files != value.MemberEnd()) {
        if (!variant_files->value.IsArray()) {
          throw ParquetException("Expected variant_files array for case ", number);
        }
        for (const auto& entry : variant_files->value.GetArray()) {
          if (entry.IsNull()) {
            test_case.expected_files.emplace_back(std::nullopt);
          } else if (entry.IsString()) {
            test_case.expected_files.emplace_back(entry.GetString());
          } else {
            throw ParquetException("Expected variant_files string or null for case ",
                                   number);
          }
        }
      }

      auto error_message = value.FindMember("error_message");
      if (error_message != value.MemberEnd()) {
        if (!error_message->value.IsString()) {
          throw ParquetException("Expected error_message string for case ", number);
        }
        test_case.error_message = std::string(error_message->value.GetString());
      } else if (test_case.expected_files.empty()) {
        throw ParquetException("Missing expected variant file for case ", number);
      }
      cases.emplace(number, std::move(test_case));
    }
    return cases;
  }

  const ShreddedVariantCase& FindCase(int case_number, std::string_view test_name) const {
    auto it = cases_.find(case_number);
    if (it == cases_.end()) {
      throw ParquetException("Missing case_number: ", case_number);
    }
    if (it->second.test_name != test_name) {
      throw ParquetException("Unexpected test name for case ", case_number, ": ",
                             it->second.test_name);
    }
    return it->second;
  }

  void SetUp() override {
    auto maybe_dir = ParquetTestingSiblingDir("shredded_variant");
    if (!maybe_dir.has_value()) {
      GTEST_SKIP() << "PARQUET_TEST_DATA not set or shredded variant folder missing";
    }
    dir_ = std::move(*maybe_dir);
    ASSERT_NO_THROW(cases_ = LoadShreddedVariantCases());
  }

  std::string dir_;
  std::unordered_map<int, ShreddedVariantCase> cases_;
};

}  // namespace

TEST_F(TestVariantParquetTesting, RoundTripsEncoded) {
  VariantArrayBuilder builder;
  for (const auto& [name, encoded] : cases_) {
    SCOPED_TRACE(name);
    builder.AppendEncoded(encoded);
  }
  auto input = builder.Finish();

  std::shared_ptr<::arrow::Table> table;
  ASSERT_NO_THROW(table = RoundTripVariantArray(input));
  ASSERT_NE(nullptr, table);
  auto column = table->GetColumnByName("variant");
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(1, column->num_chunks());
  const auto& array =
      checked_pointer_cast<::arrow::extension::VariantArray>(column->chunk(0));
  ASSERT_EQ(static_cast<int64_t>(cases_.size()), array->length());

  for (size_t row = 0; row < cases_.size(); ++row) {
    const auto& [name, expected] = cases_[row];
    SCOPED_TRACE(name);
    auto actual = UnshredVariantRow(*array, static_cast<int64_t>(row));
    ASSERT_EQ(std::string_view{*expected.metadata}, std::string_view{*actual.metadata});
    ASSERT_EQ(std::string_view{*expected.value}, std::string_view{*actual.value});
  }
}

TEST_F(TestShreddedVariantParquetTesting, UnshredsArrayWithMissingValueColumn) {
  // Case 131 is the simplest single-row fixture without a top-level value column.
  const auto& test_case = FindCase(131, "testMissingValueColumn");
  auto table = ReadVariantTestingTable(test_case.parquet_file);
  auto column = table->GetColumnByName("var");
  ASSERT_NE(nullptr, column);
  ASSERT_EQ(1, column->num_chunks());
  auto variant_array =
      checked_pointer_cast<::arrow::extension::VariantArray>(column->chunk(0));
  ASSERT_NE(nullptr, variant_array);

  auto unshredded = UnshredVariantArray(*variant_array);
  auto actual = UnshredVariantRow(*unshredded, /*row=*/0);
  ASSERT_EQ(1, test_case.expected_files.size());
  ASSERT_TRUE(test_case.expected_files[0].has_value());
  auto expected = LoadEncodedVariantBinaryFile(*test_case.expected_files[0]);
  ASSERT_EQ(std::string_view{*expected.metadata}, std::string_view{*actual.metadata});
  ASSERT_EQ(std::string_view{*expected.value}, std::string_view{*actual.value});
}

TEST_F(TestShreddedVariantParquetTesting, ReadsShreddedParquetTesting) {
  size_t skipped_cases = 0;
  for (const auto& [case_number, test_case] : cases_) {
    if (kReadCompatErrorCases.contains(case_number) ||
        kUnsupportedTypedValueCases.contains(case_number)) {
      continue;
    }
    ASSERT_FALSE(test_case.error_message.has_value());

    SCOPED_TRACE(::testing::Message()
                 << "case " << case_number << " (" << test_case.test_name << ")");
    auto table = ReadVariantTestingTable(test_case.parquet_file);
    auto column = table->GetColumnByName("var");
    ASSERT_NE(nullptr, column);
    ASSERT_EQ(1, column->num_chunks());
    auto variant_array =
        checked_pointer_cast<::arrow::extension::VariantArray>(column->chunk(0));
    ASSERT_NE(nullptr, variant_array);

    auto skip = kSkipUnshreddedCases.find(case_number);
    if (skip != kSkipUnshreddedCases.end()) {
      ASSERT_EQ(skip->second, test_case.test_name);
      ++skipped_cases;
      continue;
    }

    ASSERT_EQ(static_cast<int64_t>(test_case.expected_files.size()),
              variant_array->length());
    for (int64_t row = 0; row < variant_array->length(); ++row) {
      SCOPED_TRACE(row);
      if (!test_case.expected_files[row].has_value()) {
        ASSERT_TRUE(variant_array->IsNull(row));
        continue;
      }

      auto expected = LoadEncodedVariantBinaryFile(*test_case.expected_files[row]);
      auto actual = UnshredVariantRow(*variant_array, row);
      ASSERT_EQ(std::string_view{*expected.metadata}, std::string_view{*actual.metadata});
      ASSERT_EQ(std::string_view{*expected.value}, std::string_view{*actual.value});
    }
  }
  ASSERT_EQ(kSkipUnshreddedCases.size(), skipped_cases);
}

TEST_F(TestShreddedVariantParquetTesting, ReadCompatOnlyShapes) {
  ArrowReaderProperties reader_properties;
  reader_properties.set_variant_validation_enabled(false);
  for (const auto& [case_number, test_name] : kReadCompatOnlyCases) {
    const auto& test_case = FindCase(case_number, test_name);
    SCOPED_TRACE(case_number);

    auto table = ReadVariantTestingTable(test_case.parquet_file, reader_properties);
    auto column = table->GetColumnByName("var");
    ASSERT_NE(nullptr, column);
    ASSERT_THROW(ValidateVariants<true>(*column), ParquetInvalidOrCorruptedFileException);
    ASSERT_NO_THROW(ValidateVariants<false>(*column));
  }
}

TEST_F(TestShreddedVariantParquetTesting, RejectsUnsupportedTypedValues) {
  for (const auto& [case_number, test_name] : kUnsupportedTypedValueCases) {
    const auto& test_case = FindCase(case_number, test_name);
    SCOPED_TRACE(::testing::Message()
                 << "case " << case_number << " (" << test_case.test_name << ")");
    ASSERT_TRUE(test_case.error_message.has_value());
    ASSERT_FALSE(test_case.error_message->empty());
    ASSERT_THROW(ReadVariantTestingTable(test_case.parquet_file), ParquetException);
  }
}

TEST_F(TestShreddedVariantParquetTesting, RejectsErrorsInReadCompat) {
  ArrowReaderProperties reader_properties;
  reader_properties.set_variant_validation_enabled(false);
  for (const auto& [case_number, test_name] : kReadCompatErrorCases) {
    const auto& test_case = FindCase(case_number, test_name);
    SCOPED_TRACE(case_number);
    ASSERT_TRUE(test_case.error_message.has_value());
    ASSERT_FALSE(test_case.error_message->empty());

    auto table = ReadVariantTestingTable(test_case.parquet_file, reader_properties);
    auto column = table->GetColumnByName("var");
    ASSERT_NE(nullptr, column);
    ASSERT_THROW(ValidateVariants<false>(*column),
                 ParquetInvalidOrCorruptedFileException);
  }
}

}  // namespace parquet::variant

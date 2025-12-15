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

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <optional>
#include <cstdlib>
#include <map>
#include <filesystem>

#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encryption/encryption.h"
#include "parquet/encryption/external/test_utils.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/arrow/writer.h"
#include "parquet/arrow/reader.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/table.h"
#include "arrow/util/compression.h"
#include "arrow/util/secure_string.h"

using ::arrow::io::BufferReader;
using ::arrow::io::BufferOutputStream;
using parquet::Compression;
using parquet::Encoding;
using parquet::ParquetDataPageVersion;
using parquet::ParquetVersion;
using parquet::ReaderProperties;
using parquet::Repetition;
using parquet::Type;
using parquet::WriterProperties;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

//Integration test for External DBPA encryption and decryption.
// main functionality is written in DoRoundtrip() and BuildParams()
namespace parquet::encryption::test {

namespace {

struct TestParams {
  Type::type physical_type;
  bool dictionary_on;
  ParquetDataPageVersion dpv;
  Compression::type compression;
  std::optional<Encoding::type> encoding;
  std::optional<int32_t> data_type_length;
};

std::string TestParamsToString(const TestParams& p) {
  std::string type = parquet::TypeToString(p.physical_type);
  std::string dict = p.dictionary_on ? "dict_on" : "dict_off";
  std::string ver = (p.dpv == ParquetDataPageVersion::V1) ? "v1" : "v2";
  std::string comp = ::arrow::util::Codec::GetCodecAsString(p.compression);
  std::string enc = p.encoding.has_value() ? parquet::EncodingToString(p.encoding.value())
                                          : std::string("default");
  std::string len = (p.physical_type == Type::FIXED_LEN_BYTE_ARRAY && p.data_type_length.has_value())
                        ? (std::string("_len") + std::to_string(*p.data_type_length))
                        : std::string("");
  return type + "_" + dict + "_" + ver + "_" + comp + "_" + enc + len;
}

// In here, we build the test matrix. We combine parameters for these dimensions and values.
// - physical type: INT32, BYTE_ARRAY, BOOLEAN, INT64, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY
// - dictionary page enabled: false
// - page version: V1 and V2
// - compression: UNCOMPRESSED and GZIP
// - encoding: PLAIN
std::vector<TestParams> BuildParams() {

  // Cover an expanded set including FIXED_LEN_BYTE_ARRAY lengths; others can be added later.
  std::vector<Type::type> types = {
      Type::INT32,
      Type::BYTE_ARRAY,
      Type::BOOLEAN,
      Type::INT64,
      Type::FLOAT,
      Type::DOUBLE,
      Type::FIXED_LEN_BYTE_ARRAY};
  std::vector<bool> is_dictionary_page_enabled = {false}; // add true to cover dict paths
  std::vector<ParquetDataPageVersion> page_versions = {ParquetDataPageVersion::V1, ParquetDataPageVersion::V2};
  std::vector<Compression::type> compressions = {Compression::UNCOMPRESSED, Compression::GZIP};
  std::vector<int32_t> flba_lengths = {8, 16};

  std::vector<TestParams> all_test_params;

  for (auto type : types) {
    for (bool dict_on : is_dictionary_page_enabled) {
      for (auto page_version : page_versions) {
        for (auto compression : compressions) {
          if (type == Type::FIXED_LEN_BYTE_ARRAY) {
            for (auto len : flba_lengths) {
              if (dict_on) {
                all_test_params.push_back({type, true, page_version, compression, std::nullopt, len});
              } else {
                all_test_params.push_back({type, false, page_version, compression, Encoding::PLAIN, len});
              }
            }
          } 
          else if (dict_on) {
            all_test_params.push_back({type, true, page_version, compression, std::nullopt, std::nullopt});
          } 
          else {
            if (type == Type::INT32) {
              all_test_params.push_back({type, false, page_version, compression, Encoding::PLAIN, std::nullopt});
              all_test_params.push_back({type, false, page_version, compression, Encoding::DELTA_BINARY_PACKED, std::nullopt});
            } 
            else if (type == Type::INT64) {
              all_test_params.push_back({type, false, page_version, compression, Encoding::PLAIN, std::nullopt});
              all_test_params.push_back({type, false, page_version, compression, Encoding::DELTA_BINARY_PACKED, std::nullopt});
            } 
            else if (type == Type::BYTE_ARRAY) {
              all_test_params.push_back({type, false, page_version, compression, Encoding::PLAIN, std::nullopt});
              all_test_params.push_back({type, false, page_version, compression, Encoding::DELTA_LENGTH_BYTE_ARRAY, std::nullopt});
              all_test_params.push_back({type, false, page_version, compression, Encoding::DELTA_BYTE_ARRAY, std::nullopt});
            } 
            else if (type == Type::BOOLEAN) {
              all_test_params.push_back({type, false, page_version, compression, Encoding::RLE, std::nullopt});
            } 
            else {
              all_test_params.push_back({type, false, page_version, compression, std::nullopt, std::nullopt});
            }
          }
        }
      }
    }
  }
  return all_test_params;
}

// This is use to name/identify each of the test cases.
std::string ParamName(const testing::TestParamInfo<TestParams>& info) {
  return TestParamsToString(info.param);
}

class ExternalDbpaIntegrationTest : public ::testing::TestWithParam<TestParams> {
 protected:
  void SetUp() override {

    // Default library path, can be overridden by environment variable DBPA_LIBRARY_PATH
    library_path_ = "libdbpsRemoteAgent.so";
    if (const char* lib_env = std::getenv("DBPA_LIBRARY_PATH")) {
      if (*lib_env != '\0') {
        library_path_ = std::string(lib_env);
      }
    }

    // Build shared connection_config for both encryption and decryption
    std::map<std::string, std::string> algo_config;
    algo_config["agent_library_path"] = library_path_;
    // If using a remote agent, attach the connection config file path
    {
      std::string library_path_lower = library_path_;
      std::transform(library_path_lower.begin(), library_path_lower.end(),
                     library_path_lower.begin(), ::tolower);
      const bool is_remote_agent = library_path_lower.find("remote") != std::string::npos;
      if (is_remote_agent) {
        std::string config_file_name = "test_connection_config_file.json";
        if (const char* cfg_env = std::getenv("DBPA_CONFIG_FILE_NAME")) {
          if (*cfg_env != '\0') {
            config_file_name = std::string(cfg_env);
          }
        }

        if (!std::filesystem::exists(config_file_name)) {
          FAIL() << "Connection config [" << config_file_name << "] file not found";
        }
        algo_config["connection_config_file_path"] = config_file_name;
      }
    }

    connection_config_[parquet::ParquetCipher::EXTERNAL_DBPA_V1] = std::move(algo_config);

    // Default number of rows for test input tables
    num_rows_ = 10;
  }

  // Build a single-column Arrow table according to the given params.
  // Only a subset of types is supported for now.
  std::shared_ptr<::arrow::Table> MakeInputTable(Type::type physical_type,
                                                bool dictionary_on,
                                                const std::optional<int32_t>& data_type_length) {
    if (physical_type == Type::INT32) {
      return MakeInt32Table(dictionary_on);
    }
    else if (physical_type == Type::BYTE_ARRAY) {
      return MakeByteArrayTable(dictionary_on);
    }
    else if (physical_type == Type::BOOLEAN) {
      return MakeBooleanTable(dictionary_on);
    }
    else if (physical_type == Type::INT64) {
      return MakeInt64Table(dictionary_on);
    }
    else if (physical_type == Type::FLOAT) {
      return MakeFloatTable(dictionary_on);
    }
    else if (physical_type == Type::DOUBLE) {
      return MakeDoubleTable(dictionary_on);
    }
    else if (physical_type == Type::FIXED_LEN_BYTE_ARRAY) {
      if (!data_type_length.has_value()) {
        return nullptr;
      }
      return MakeFixedSizeBinaryTable(dictionary_on, *data_type_length);
    }
    else {
      return nullptr;
    }
  }

  std::shared_ptr<::arrow::Table> MakeInt32Table(bool dictionary_on) {
    ::arrow::Int32Builder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      int32_t value = dictionary_on ? static_cast<int32_t>(i % 16)
                                    : static_cast<int32_t>(i);
      auto st = builder.Append(value);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::int32())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeByteArrayTable(bool dictionary_on) {
    ::arrow::StringBuilder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      std::string s = dictionary_on ? std::string("k") + std::to_string(i % 8)
                                    : std::string("val_") + std::to_string(i);
      auto st = builder.Append(s);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::utf8())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeBooleanTable(bool dictionary_on) {
    ::arrow::BooleanBuilder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      bool value = dictionary_on ? (i % 2 == 0) : (i % 3 == 0);
      auto st = builder.Append(value);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::boolean())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeInt64Table(bool dictionary_on) {
    ::arrow::Int64Builder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      int64_t value = dictionary_on ? static_cast<int64_t>(i % 16) : static_cast<int64_t>(i);
      auto st = builder.Append(value);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::int64())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeFloatTable(bool dictionary_on) {
    ::arrow::FloatBuilder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      float value = dictionary_on ? static_cast<float>(i % 16) : static_cast<float>(i);
      auto st = builder.Append(value);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::float32())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeDoubleTable(bool dictionary_on) {
    ::arrow::DoubleBuilder builder;
    for (int64_t i = 0; i < num_rows_; ++i) {
      double value = dictionary_on ? static_cast<double>(i % 16) : static_cast<double>(i);
      auto st = builder.Append(value);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::float64())}), {array});
  }

  std::shared_ptr<::arrow::Table> MakeFixedSizeBinaryTable(bool dictionary_on, int32_t byte_width) {
    auto type = ::arrow::fixed_size_binary(byte_width);
    ::arrow::FixedSizeBinaryBuilder builder(type);
    for (int64_t i = 0; i < num_rows_; ++i) {
      std::string s;
      if (dictionary_on) {
        int64_t k = i % 8;
        s = std::string(byte_width, static_cast<char>('A' + (k % 26)));
      } else {
        s.resize(byte_width);
        for (int32_t j = 0; j < byte_width; ++j) {
          s[j] = static_cast<char>('a' + ((i + j) % 26));
        }
      }
      auto st = builder.Append(reinterpret_cast<const uint8_t*>(s.data()));
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Array> array;
    {
      auto st = builder.Finish(&array);
      EXPECT_TRUE(st.ok()) << st.ToString();
    }
    return ::arrow::Table::Make(
        ::arrow::schema({::arrow::field("col", ::arrow::fixed_size_binary(byte_width))}), {array});
  }

  // Build ExternalFileEncryptionProperties using EXTERNAL_DBPA_V1 for the column.
  std::shared_ptr<parquet::ExternalFileEncryptionProperties> MakeExternalEncryptionProperties(
      const std::string& column_path) {
    std::map<std::string, std::shared_ptr<parquet::ColumnEncryptionProperties>> cols;
    parquet::ColumnEncryptionProperties::Builder col_builder(column_path);
    col_builder.key(kColumnEncryptionKey2)->key_id("kc2");
    col_builder.parquet_cipher(parquet::ParquetCipher::EXTERNAL_DBPA_V1);
    cols[column_path] = col_builder.build();

    parquet::ExternalFileEncryptionProperties::Builder feb(kFooterEncryptionKey);
    feb.footer_key_metadata("kf")
       ->encrypted_columns(cols)
       ->algorithm(parquet::ParquetCipher::AES_GCM_V1)
       ->app_context(app_context_)
       // Use shared connection_config built in SetUp()
       ->connection_config(connection_config_);
    return feb.build_external();
  }

  std::shared_ptr<parquet::ExternalFileDecryptionProperties> MakeExternalDecryptionProperties() {
    auto kr = std::make_shared<parquet::StringKeyIdRetriever>();
    kr->PutKey("kf", kFooterEncryptionKey);
    kr->PutKey("kc2", kColumnEncryptionKey2);
    parquet::ExternalFileDecryptionProperties::Builder fdb;
    fdb.key_retriever(kr)
       ->app_context(app_context_)
       // Use shared connection_config built in SetUp()
       ->connection_config(connection_config_);
    return fdb.build_external();
  }

  void DoRoundtrip(const TestParams& p) {

    // Build Arrow table
    std::shared_ptr<::arrow::Table> input_table = MakeInputTable(
      p.physical_type, p.dictionary_on, p.data_type_length);
    if (!input_table) {
      GTEST_SKIP() << "Type not covered in this test variant";
    }

    // Writer properties
    WriterProperties::Builder wpb;
    wpb.version(ParquetVersion::PARQUET_2_6)
       ->data_page_version(p.dpv)
       ->data_pagesize(16 * 1024)
       ->compression(p.compression);
    // Configure dictionary usage at the writer level:
    // - enable_dictionary(): allow dictionary encoding; combined with low-cardinality data
    //   above, this yields a dictionary page.
    // - disable_dictionary(): force non-dictionary encoding; if an explicit Encoding was
    //   provided in the test params, set it for column "col" to exercise different
    //   non-dictionary encodings.
    if (p.dictionary_on) {
      wpb.enable_dictionary();
      wpb.dictionary_pagesize_limit(8 * 1024);
    }
    else {
      wpb.disable_dictionary();
      if (p.encoding.has_value()) {
        wpb.encoding("col", p.encoding.value());
      }
    }
    auto enc_props = MakeExternalEncryptionProperties("col");
    wpb.encryption(enc_props);
    auto writer_props = wpb.build();

    // Write using high-level parquet::arrow API
    auto sink_res = BufferOutputStream::Create();
    ASSERT_TRUE(sink_res.ok()) << sink_res.status().ToString();
    auto sink = *sink_res;
    {
      auto st = parquet::arrow::WriteTable(*input_table, ::arrow::default_memory_pool(), sink,
                                           /*chunk_size=*/num_rows_, writer_props);
      ASSERT_TRUE(st.ok()) << st.ToString();
    }
    auto buffer_res = sink->Finish();
    ASSERT_TRUE(buffer_res.ok()) << buffer_res.status().ToString();
    auto buffer = *buffer_res;

    // Read back using parquet::arrow API
    parquet::ReaderProperties rp = parquet::default_reader_properties();
    rp.file_decryption_properties(MakeExternalDecryptionProperties());
    parquet::arrow::FileReaderBuilder frb;
    {
      auto st = frb.Open(std::make_shared<BufferReader>(buffer), rp);
      ASSERT_TRUE(st.ok()) << st.ToString();
    }
    std::unique_ptr<parquet::arrow::FileReader> fr;
    {
      auto st = frb.Build(&fr);
      ASSERT_TRUE(st.ok()) << st.ToString();
    }
    std::shared_ptr<::arrow::Table> output_table;
    {
      auto st = fr->ReadTable(&output_table);
      ASSERT_TRUE(st.ok()) << st.ToString();
    }

    // Assert equality
    ASSERT_TRUE(output_table->Equals(*input_table));
  }

  std::string app_context_ =
      "{\"user_id\": \"abc123\", \"location\": {\"lat\": 9.7489, \"lon\": -83.7534}}";
  std::string library_path_;
  std::map<parquet::ParquetCipher::type, std::map<std::string, std::string>> connection_config_;
  //::arrow::util::SecureString kFooterEncryptionKey_(kFooterEncryptionKey);
  //::arrow::util::SecureString kColumnEncryptionKey2_(kColumnEncryptionKey2);
  int64_t num_rows_;
};

TEST_P(ExternalDbpaIntegrationTest, Roundtrip) { DoRoundtrip(GetParam()); }

INSTANTIATE_TEST_SUITE_P(DBPAIntegration,
                         ExternalDbpaIntegrationTest,
                         ::testing::ValuesIn(BuildParams()), ParamName);
}  // namespace

}  // namespace parquet::encryption::test

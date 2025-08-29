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

#include <string>

#include "parquet/encryption/encryption.h"
#include "parquet/encryption/test_encryption_util.h"

namespace parquet::encryption::test {

TEST(TestColumnEncryptionProperties, ColumnEncryptedWithOwnKey) {
  std::string column_path_1 = "column_1";
  ColumnEncryptionProperties::Builder column_builder_1(column_path_1);
  column_builder_1.key(kColumnEncryptionKey1);
  column_builder_1.key_id("kc1");
  std::shared_ptr<ColumnEncryptionProperties> column_props_1 = column_builder_1.build();

  ASSERT_EQ(column_path_1, column_props_1->column_path());
  ASSERT_EQ(true, column_props_1->is_encrypted());
  ASSERT_EQ(false, column_props_1->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, column_props_1->key());
  ASSERT_EQ("kc1", column_props_1->key_metadata());
}

TEST(TestColumnEncryptionProperties, ColumnEncryptedWithFooterKey) {
  std::string column_path_1 = "column_1";
  ColumnEncryptionProperties::Builder column_builder_1(column_path_1);
  std::shared_ptr<ColumnEncryptionProperties> column_props_1 = column_builder_1.build();

  ASSERT_EQ(column_path_1, column_props_1->column_path());
  ASSERT_EQ(true, column_props_1->is_encrypted());
  ASSERT_EQ(true, column_props_1->is_encrypted_with_footer_key());
}

TEST(TestColumnEncryptionProperties, ColumnParquetCipherNotSpecified) {
  std::string column_path = "column_path";
  ColumnEncryptionProperties::Builder column_builder(column_path);
  std::shared_ptr<ColumnEncryptionProperties> properties = column_builder.build();

  ASSERT_EQ(column_path, properties->column_path());
  ASSERT_EQ(false, properties->parquet_cipher().has_value());
}

TEST(TestColumnEncryptionProperties, ColumnParquetCipherSpecified) {
  std::string column_path = "column_path";
  ColumnEncryptionProperties::Builder column_builder(column_path);
  column_builder.parquet_cipher(ParquetCipher::AES_GCM_CTR_V1);
  std::shared_ptr<ColumnEncryptionProperties> properties = column_builder.build();

  ASSERT_EQ(column_path, properties->column_path());
  ASSERT_EQ(true, properties->parquet_cipher().has_value());
  ASSERT_EQ(ParquetCipher::AES_GCM_CTR_V1, properties->parquet_cipher().value());
}

TEST(TestColumnDecryptionProperties, ColumnParquetCipherNotSpecified) {
  std::string column_path = "column_path";
  ColumnDecryptionProperties::Builder column_builder(column_path);
  std::shared_ptr<ColumnDecryptionProperties> properties = column_builder.build();

  ASSERT_EQ(column_path, properties->column_path());
  ASSERT_EQ(false, properties->parquet_cipher().has_value());
}

TEST(TestColumnDecryptionProperties, ColumnParquetCipherSpecified) {
  std::string column_path = "column_path";
  ColumnDecryptionProperties::Builder column_builder(column_path);
  column_builder.parquet_cipher(ParquetCipher::AES_GCM_CTR_V1);
  std::shared_ptr<ColumnDecryptionProperties> properties = column_builder.build();

  ASSERT_EQ(column_path, properties->column_path());
  ASSERT_EQ(true, properties->parquet_cipher().has_value());
  ASSERT_EQ(ParquetCipher::AES_GCM_CTR_V1, properties->parquet_cipher().value());
}

// Encrypt all columns and the footer with the same key.
// (uniform encryption)
TEST(TestEncryptionProperties, UniformEncryption) {
  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.footer_key_metadata("kf");
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ(true, props->encrypted_footer());
  ASSERT_EQ(kDefaultEncryptionAlgorithm, props->algorithm().algorithm);
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());
  ASSERT_EQ("kf", props->footer_key_metadata());

  std::shared_ptr<parquet::schema::ColumnPath> column_path =
      parquet::schema::ColumnPath::FromDotString("a_column");
  std::shared_ptr<ColumnEncryptionProperties> out_col_props =
      props->column_encryption_properties(column_path->ToDotString());

  ASSERT_EQ(true, out_col_props->is_encrypted());
  ASSERT_EQ(true, out_col_props->is_encrypted_with_footer_key());
}

// Encrypt two columns with their own keys and the same key for
// the footer and other columns
TEST(TestEncryptionProperties, EncryptFooterAndTwoColumns) {
  std::shared_ptr<parquet::schema::ColumnPath> column_path_1 =
      parquet::schema::ColumnPath::FromDotString("column_1");
  ColumnEncryptionProperties::Builder column_builder_1(column_path_1->ToDotString());
  column_builder_1.key(kColumnEncryptionKey1);
  column_builder_1.key_id("kc1");

  std::shared_ptr<parquet::schema::ColumnPath> column_path_2 =
      parquet::schema::ColumnPath::FromDotString("column_2");
  ColumnEncryptionProperties::Builder column_builder_2(column_path_2->ToDotString());
  column_builder_2.key(kColumnEncryptionKey2);
  column_builder_2.key_id("kc2");

  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encrypted_columns;
  encrypted_columns[column_path_1->ToDotString()] = column_builder_1.build();
  encrypted_columns[column_path_2->ToDotString()] = column_builder_2.build();

  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.footer_key_metadata("kf");
  builder.encrypted_columns(encrypted_columns);
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ(true, props->encrypted_footer());
  ASSERT_EQ(kDefaultEncryptionAlgorithm, props->algorithm().algorithm);
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_1 =
      props->column_encryption_properties(column_path_1->ToDotString());

  ASSERT_EQ(column_path_1->ToDotString(), out_col_props_1->column_path());
  ASSERT_EQ(true, out_col_props_1->is_encrypted());
  ASSERT_EQ(false, out_col_props_1->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, out_col_props_1->key());
  ASSERT_EQ("kc1", out_col_props_1->key_metadata());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_2 =
      props->column_encryption_properties(column_path_2->ToDotString());

  ASSERT_EQ(column_path_2->ToDotString(), out_col_props_2->column_path());
  ASSERT_EQ(true, out_col_props_2->is_encrypted());
  ASSERT_EQ(false, out_col_props_2->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey2, out_col_props_2->key());
  ASSERT_EQ("kc2", out_col_props_2->key_metadata());

  std::shared_ptr<parquet::schema::ColumnPath> column_path_3 =
      parquet::schema::ColumnPath::FromDotString("column_3");
  std::shared_ptr<ColumnEncryptionProperties> out_col_props_3 =
      props->column_encryption_properties(column_path_3->ToDotString());

  ASSERT_EQ(NULLPTR, out_col_props_3);
}

// Encryption configuration 3: Encrypt two columns, don’t encrypt footer.
// (plaintext footer mode, readable by legacy readers)
TEST(TestEncryptionProperties, EncryptTwoColumnsNotFooter) {
  std::shared_ptr<parquet::schema::ColumnPath> column_path_1 =
      parquet::schema::ColumnPath::FromDotString("column_1");
  ColumnEncryptionProperties::Builder column_builder_1(column_path_1);
  column_builder_1.key(kColumnEncryptionKey1);
  column_builder_1.key_id("kc1");

  std::shared_ptr<parquet::schema::ColumnPath> column_path_2 =
      parquet::schema::ColumnPath::FromDotString("column_2");
  ColumnEncryptionProperties::Builder column_builder_2(column_path_2);
  column_builder_2.key(kColumnEncryptionKey2);
  column_builder_2.key_id("kc2");

  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encrypted_columns;
  encrypted_columns[column_path_1->ToDotString()] = column_builder_1.build();
  encrypted_columns[column_path_2->ToDotString()] = column_builder_2.build();

  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.footer_key_metadata("kf");
  builder.set_plaintext_footer();
  builder.encrypted_columns(encrypted_columns);
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ(false, props->encrypted_footer());
  ASSERT_EQ(kDefaultEncryptionAlgorithm, props->algorithm().algorithm);
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_1 =
      props->column_encryption_properties(column_path_1->ToDotString());

  ASSERT_EQ(column_path_1->ToDotString(), out_col_props_1->column_path());
  ASSERT_EQ(true, out_col_props_1->is_encrypted());
  ASSERT_EQ(false, out_col_props_1->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, out_col_props_1->key());
  ASSERT_EQ("kc1", out_col_props_1->key_metadata());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_2 =
      props->column_encryption_properties(column_path_2->ToDotString());

  ASSERT_EQ(column_path_2->ToDotString(), out_col_props_2->column_path());
  ASSERT_EQ(true, out_col_props_2->is_encrypted());
  ASSERT_EQ(false, out_col_props_2->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey2, out_col_props_2->key());
  ASSERT_EQ("kc2", out_col_props_2->key_metadata());

  // other columns: encrypted with footer, footer is not encrypted
  // so column is not encrypted as well
  std::string column_path_3 = "column_3";
  std::shared_ptr<ColumnEncryptionProperties> out_col_props_3 =
      props->column_encryption_properties(column_path_3);

  ASSERT_EQ(NULLPTR, out_col_props_3);
}

// Use aad_prefix
TEST(TestEncryptionProperties, UseAadPrefix) {
  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.aad_prefix(kFileName);
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ(kFileName, props->algorithm().aad.aad_prefix);
  ASSERT_EQ(false, props->algorithm().aad.supply_aad_prefix);
}

// Use aad_prefix and
// disable_aad_prefix_storage.
TEST(TestEncryptionProperties, UseAadPrefixNotStoreInFile) {
  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.aad_prefix(kFileName);
  builder.disable_aad_prefix_storage();
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ("", props->algorithm().aad.aad_prefix);
  ASSERT_EQ(true, props->algorithm().aad.supply_aad_prefix);
}

// Use AES_GCM_CTR_V1 algorithm
TEST(TestEncryptionProperties, UseAES_GCM_CTR_V1Algorithm) {
  FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.algorithm(ParquetCipher::AES_GCM_CTR_V1);
  std::shared_ptr<FileEncryptionProperties> props = builder.build();

  ASSERT_EQ(ParquetCipher::AES_GCM_CTR_V1, props->algorithm().algorithm);
}

TEST(TestExternalFileEncryptionProperties, SuperClassFieldsSetCorrectly) {
  std::string column_name_1 = "column_1";
  ColumnEncryptionProperties::Builder column_builder_1(column_name_1);
  column_builder_1.key(kColumnEncryptionKey1);
  column_builder_1.key_id("kc1");

  std::string column_name_2 = "column_2";
  ColumnEncryptionProperties::Builder column_builder_2(column_name_2);
  column_builder_2.key(kColumnEncryptionKey2);
  column_builder_2.key_id("kc2");

  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encrypted_columns;
  encrypted_columns[column_name_1] = column_builder_1.build();
  encrypted_columns[column_name_2] = column_builder_2.build();

  ExternalFileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.footer_key_metadata("kf");
  builder.encrypted_columns(encrypted_columns);
  std::shared_ptr<ExternalFileEncryptionProperties> props = builder.build_external();

  ASSERT_EQ(true, props->encrypted_footer());
  ASSERT_EQ(kDefaultEncryptionAlgorithm, props->algorithm().algorithm);
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_1 =
      props->column_encryption_properties(column_name_1);

  ASSERT_EQ(column_name_1, out_col_props_1->column_path());
  ASSERT_EQ(true, out_col_props_1->is_encrypted());
  ASSERT_EQ(false, out_col_props_1->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, out_col_props_1->key());
  ASSERT_EQ("kc1", out_col_props_1->key_metadata());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_2 =
      props->column_encryption_properties(column_name_2);

  ASSERT_EQ(column_name_2, out_col_props_2->column_path());
  ASSERT_EQ(true, out_col_props_2->is_encrypted());
  ASSERT_EQ(false, out_col_props_2->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey2, out_col_props_2->key());
  ASSERT_EQ("kc2", out_col_props_2->key_metadata());

  std::string column_name_3 = "column_3";
  std::shared_ptr<ColumnEncryptionProperties> out_col_props_3 =
      props->column_encryption_properties(column_name_3);

  ASSERT_EQ(NULLPTR, out_col_props_3);
  ASSERT_EQ(true, props->app_context().empty());
  ASSERT_EQ(true, props->connection_config().size() == 0);
}

// The subclass adds two additional fields
TEST(TestExternalFileEncryptionProperties, SetExternalContextAndConfig) {
  std::string app_context = "{\n"
                   "  \"user_id\": \"abc123\",\n"
                   "  \"location\": {\n"
                   "    \"lat\": 10.0,\n"
                   "    \"lon\": -84.0\n"
                   "  }\n"
                   "}";
  std::map<ParquetCipher::type, std::map<std::string, std::string>> connection_config;
  connection_config[ParquetCipher::AES_GCM_V1]["lib_location"] = "path/to/lib.so";
  connection_config[ParquetCipher::AES_GCM_V1]["config_file"] = "path/to/config/file";

  ExternalFileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.app_context(app_context);
  builder.connection_config(connection_config);
  std::shared_ptr<ExternalFileEncryptionProperties> props = builder.build_external();

  ASSERT_EQ(false, props->app_context().empty());
  ASSERT_EQ(app_context, props->app_context());
  ASSERT_EQ(false, props->connection_config().size() == 0);
  ASSERT_EQ(connection_config, props->connection_config());
}

TEST(TestExternalFileEncryptionProperties, EncryptTwoColumnsWithDifferentAlgorithms) {
  std::string column_name_1 = "column_1";
  ColumnEncryptionProperties::Builder column_builder_1(column_name_1);
  column_builder_1.key(kColumnEncryptionKey1);
  column_builder_1.key_id("kc1");
  column_builder_1.parquet_cipher(ParquetCipher::AES_GCM_V1);

  std::string column_name_2 = "column_2";
  ColumnEncryptionProperties::Builder column_builder_2(column_name_2);
  column_builder_2.key(kColumnEncryptionKey2);
  column_builder_2.key_id("kc2");
  column_builder_2.parquet_cipher(ParquetCipher::AES_GCM_CTR_V1);

  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encrypted_columns;
  encrypted_columns[column_name_1] = column_builder_1.build();
  encrypted_columns[column_name_2] = column_builder_2.build();

  ExternalFileEncryptionProperties::Builder builder(kFooterEncryptionKey);
  builder.footer_key_metadata("kf");
  builder.encrypted_columns(encrypted_columns);
  std::shared_ptr<ExternalFileEncryptionProperties> props = builder.build_external();

  ASSERT_EQ(true, props->encrypted_footer());
  ASSERT_EQ(kDefaultEncryptionAlgorithm, props->algorithm().algorithm);
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_1 =
      props->column_encryption_properties(column_name_1);

  ASSERT_EQ(column_name_1, out_col_props_1->column_path());
  ASSERT_EQ(true, out_col_props_1->is_encrypted());
  ASSERT_EQ(false, out_col_props_1->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, out_col_props_1->key());
  ASSERT_EQ(ParquetCipher::AES_GCM_V1, out_col_props_1->parquet_cipher());
  ASSERT_EQ("kc1", out_col_props_1->key_metadata());

  std::shared_ptr<ColumnEncryptionProperties> out_col_props_2 =
      props->column_encryption_properties(column_name_2);

  ASSERT_EQ(column_name_2, out_col_props_2->column_path());
  ASSERT_EQ(true, out_col_props_2->is_encrypted());
  ASSERT_EQ(false, out_col_props_2->is_encrypted_with_footer_key());
  ASSERT_EQ(kColumnEncryptionKey2, out_col_props_2->key());
  ASSERT_EQ(ParquetCipher::AES_GCM_CTR_V1, out_col_props_2->parquet_cipher());
  ASSERT_EQ("kc2", out_col_props_2->key_metadata());
}

TEST(TestDecryptionProperties, UseKeyRetriever) {
  std::shared_ptr<parquet::StringKeyIdRetriever> string_kr1 =
      std::make_shared<parquet::StringKeyIdRetriever>();
  string_kr1->PutKey("kf", kFooterEncryptionKey);
  string_kr1->PutKey("kc1", kColumnEncryptionKey1);
  string_kr1->PutKey("kc2", kColumnEncryptionKey2);
  std::shared_ptr<parquet::DecryptionKeyRetriever> kr1 =
      std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr1);

  parquet::FileDecryptionProperties::Builder builder;
  builder.key_retriever(kr1);
  std::shared_ptr<parquet::FileDecryptionProperties> props = builder.build();

  auto out_key_retriever = props->key_retriever();
  ASSERT_EQ(kFooterEncryptionKey, out_key_retriever->GetKey("kf"));
  ASSERT_EQ(kColumnEncryptionKey1, out_key_retriever->GetKey("kc1"));
  ASSERT_EQ(kColumnEncryptionKey2, out_key_retriever->GetKey("kc2"));
}

TEST(TestDecryptionProperties, SupplyAadPrefix) {
  parquet::FileDecryptionProperties::Builder builder;
  builder.footer_key(kFooterEncryptionKey);
  builder.aad_prefix(kFileName);
  std::shared_ptr<parquet::FileDecryptionProperties> props = builder.build();

  ASSERT_EQ(kFileName, props->aad_prefix());
}

TEST(ColumnDecryptionProperties, SetKey) {
  std::shared_ptr<parquet::schema::ColumnPath> column_path_1 =
      parquet::schema::ColumnPath::FromDotString("column_1");
  ColumnDecryptionProperties::Builder col_builder_1(column_path_1);
  col_builder_1.key(kColumnEncryptionKey1);

  auto props = col_builder_1.build();
  ASSERT_EQ(kColumnEncryptionKey1, props->key());
}

TEST(TestDecryptionProperties, UsingExplicitFooterAndColumnKeys) {
  std::string column_path_1 = "column_1";
  std::string column_path_2 = "column_2";
  std::map<std::string, std::shared_ptr<parquet::ColumnDecryptionProperties>>
      decryption_cols;
  parquet::ColumnDecryptionProperties::Builder col_builder_1(column_path_1);
  parquet::ColumnDecryptionProperties::Builder col_builder_2(column_path_2);

  decryption_cols[column_path_1] = col_builder_1.key(kColumnEncryptionKey1)->build();
  decryption_cols[column_path_2] = col_builder_2.key(kColumnEncryptionKey2)->build();

  parquet::FileDecryptionProperties::Builder builder;
  builder.footer_key(kFooterEncryptionKey);
  builder.column_keys(decryption_cols);
  std::shared_ptr<parquet::FileDecryptionProperties> props = builder.build();

  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());
  ASSERT_EQ(kColumnEncryptionKey1, props->column_key(column_path_1));
  ASSERT_EQ(kColumnEncryptionKey2, props->column_key(column_path_2));
}

TEST(TestExternalFileDecryptionProperties, SuperClassFieldsSetCorrectly) {
  std::shared_ptr<parquet::StringKeyIdRetriever> string_kr1 =
      std::make_shared<parquet::StringKeyIdRetriever>();
  string_kr1->PutKey("kf", kFooterEncryptionKey);
  string_kr1->PutKey("kc1", kColumnEncryptionKey1);
  string_kr1->PutKey("kc2", kColumnEncryptionKey2);
  std::shared_ptr<parquet::DecryptionKeyRetriever> kr1 =
      std::static_pointer_cast<parquet::StringKeyIdRetriever>(string_kr1);
  

  auto builder = parquet::ExternalFileDecryptionProperties::Builder();
  builder.footer_key(kFooterEncryptionKey);
  builder.plaintext_files_allowed();
  builder.key_retriever(kr1);
  std::shared_ptr<parquet::ExternalFileDecryptionProperties> props = builder.build_external();

  ASSERT_EQ(true, props->plaintext_files_allowed());
  ASSERT_EQ(kFooterEncryptionKey, props->footer_key());
  ASSERT_EQ(true, props->app_context().empty());
  ASSERT_EQ(true, props->connection_config().size() == 0);

  auto out_key_retriever = props->key_retriever();
  ASSERT_EQ(kFooterEncryptionKey, out_key_retriever->GetKey("kf"));
  ASSERT_EQ(kColumnEncryptionKey1, out_key_retriever->GetKey("kc1"));
  ASSERT_EQ(kColumnEncryptionKey2, out_key_retriever->GetKey("kc2"));
}

TEST(TestExternalFileDecryptionProperties, SetExternalContextAndConfig) {
  std::shared_ptr<parquet::StringKeyIdRetriever> string_kr1 =
      std::make_shared<parquet::StringKeyIdRetriever>();
  std::string app_context = "{\n"
                   "  \"user_id\": \"abc123\",\n"
                   "  \"location\": {\n"
                   "    \"lat\": 10.0,\n"
                   "    \"lon\": -84.0\n"
                   "  }\n"
                   "}";
  std::map<ParquetCipher::type, std::map<std::string, std::string>> connection_config;
  std::map<std::string, std::string> inner_config;
  inner_config["lib_location"] = "path/to/lib.so";
  inner_config["config_file"] = "path/to/config/file";
  connection_config[ParquetCipher::AES_GCM_CTR_V1] = inner_config;

  auto builder = parquet::ExternalFileDecryptionProperties::Builder();
  builder.footer_key(kFooterEncryptionKey);
  builder.app_context(app_context);
  builder.connection_config(connection_config);
  std::shared_ptr<parquet::ExternalFileDecryptionProperties> props = builder.build_external();
  
  ASSERT_EQ(false, props->app_context().empty());
  ASSERT_EQ(app_context, props->app_context());
  ASSERT_EQ(false, props->connection_config().size() == 0);
  ASSERT_EQ(connection_config, props->connection_config());
}

TEST(TestExternalFileDecryptionProperties, SetInvalidAppContext) {
  std::string invalid_app_context = "invalid_app_context";
  parquet::ExternalFileDecryptionProperties::Builder builder;
  builder.footer_key(kFooterEncryptionKey);
  ASSERT_THROW(builder.app_context(invalid_app_context), parquet::ParquetException);
}

}  // namespace parquet::encryption::test

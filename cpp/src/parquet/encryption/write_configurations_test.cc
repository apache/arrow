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

#include <stdio.h>

#include <stdexcept>
#include <tuple>
#include <vector>

#include <arrow/io/file.h>

#include "arrow/util/endian.h"
#include "arrow/util/ubsan.h"

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/file_reader.h"
#include "parquet/file_writer.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"

/*
 * This file contains unit-tests for writing encrypted Parquet files with
 * different encryption configurations.
 * The files are saved in temporary folder and will be deleted after reading
 * them in read_configurations_test.cc test.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * Each unit-test creates a single parquet file with eight columns using one of the
 * following encryption configurations:
 *
 *  - Encryption configuration 1:   Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  - Encryption configuration 2:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - Encryption configuration 3:   Encrypt two columns, with different keys.
 *                                  Don’t encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - Encryption configuration 4:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  - Encryption configuration 5:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  - Encryption configuration 6:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 */

namespace parquet::encryption::test {

using FileClass = ::arrow::io::FileOutputStream;

std::unique_ptr<TemporaryDir> temp_dir;

class TestEncryptionConfiguration : public ::testing::Test {
 public:
  static void SetUpTestCase();

 protected:
  FileEncryptor encryptor_;

  std::string path_to_double_field_ = kDoubleFieldName;
  std::string path_to_float_field_ = kFloatFieldName;
  std::string file_name_;
  SecureString kFooterEncryptionKey_ = kFooterEncryptionKey;
  SecureString kColumnEncryptionKey1_ = kColumnEncryptionKey1;
  SecureString kColumnEncryptionKey2_ = kColumnEncryptionKey2;
  std::string kFileName_ = std::string(kFileName);

  void EncryptFile(std::shared_ptr<FileEncryptionProperties> encryption_configurations,
                   std::string file_name) {
    std::string file = temp_dir->path().ToString() + file_name;
    encryptor_.EncryptFile(file, encryption_configurations);
  }
};

// Encryption configuration 1: Encrypt all columns and the footer with the same key.
// (uniform encryption)
TEST_F(TestEncryptionConfiguration, UniformEncryption) {
  FileEncryptionProperties::Builder file_encryption_builder_1(kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_1.footer_key_metadata("kf")->build(),
                    "tmp_uniform_encryption.parquet.encrypted");
}

// Encryption configuration 2: Encrypt two columns and the footer, with different keys.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndTheFooter) {
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encryption_cols2;
  encryption_cols2[path_to_double_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1_, "kc1");
  encryption_cols2[path_to_float_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2_, "kc2");
  encryption_cols2[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

  FileEncryptionProperties::Builder file_encryption_builder_2(kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_2.footer_key_metadata("kf")
                        ->encrypted_columns(std::move(encryption_cols2))
                        ->build(),
                    "tmp_encrypt_columns_and_footer.parquet.encrypted");
}

// Encryption configuration 3: Encrypt two columns, with different keys.
// Don’t encrypt footer.
// (plaintext footer mode, readable by legacy readers)
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsWithPlaintextFooter) {
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encryption_cols3;
  encryption_cols3[path_to_double_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1_, "kc1");
  encryption_cols3[path_to_float_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2_, "kc2");
  encryption_cols3[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

  FileEncryptionProperties::Builder file_encryption_builder_3(kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_3.footer_key_metadata("kf")
                        ->encrypted_columns(std::move(encryption_cols3))
                        ->set_plaintext_footer()
                        ->build(),
                    "tmp_encrypt_columns_plaintext_footer.parquet.encrypted");
}

// Encryption configuration 4: Encrypt two columns and the footer, with different keys.
// Use aad_prefix.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndFooterWithAadPrefix) {
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encryption_cols4;
  encryption_cols4[path_to_double_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1_, "kc1");
  encryption_cols4[path_to_float_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2_, "kc2");
  encryption_cols4[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

  FileEncryptionProperties::Builder file_encryption_builder_4(kFooterEncryptionKey_);

  this->EncryptFile(file_encryption_builder_4.footer_key_metadata("kf")
                        ->encrypted_columns(std::move(encryption_cols4))
                        ->aad_prefix(kFileName_)
                        ->build(),
                    "tmp_encrypt_columns_and_footer_aad.parquet.encrypted");
}

// Encryption configuration 5: Encrypt two columns and the footer, with different keys.
// Use aad_prefix and disable_aad_prefix_storage.
TEST_F(TestEncryptionConfiguration,
       EncryptTwoColumnsAndFooterWithAadPrefixDisable_aad_prefix_storage) {
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encryption_cols5;
  encryption_cols5[path_to_double_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1_, "kc1");
  encryption_cols5[path_to_float_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2_, "kc2");
  encryption_cols5[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

  FileEncryptionProperties::Builder file_encryption_builder_5(kFooterEncryptionKey_);

  this->EncryptFile(
      file_encryption_builder_5.encrypted_columns(std::move(encryption_cols5))
          ->footer_key_metadata("kf")
          ->aad_prefix(kFileName_)
          ->disable_aad_prefix_storage()
          ->build(),
      "tmp_encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted");
}

// Encryption configuration 6: Encrypt two columns and the footer, with different keys.
// Use AES_GCM_CTR_V1 algorithm.
TEST_F(TestEncryptionConfiguration, EncryptTwoColumnsAndFooterUseAES_GCM_CTR) {
  std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> encryption_cols6;
  encryption_cols6[path_to_double_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1_, "kc1");
  encryption_cols6[path_to_float_field_] =
      ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2_, "kc2");
  encryption_cols6[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

  FileEncryptionProperties::Builder file_encryption_builder_6(kFooterEncryptionKey_);

  EXPECT_NO_THROW(
      this->EncryptFile(file_encryption_builder_6.footer_key_metadata("kf")
                            ->encrypted_columns(std::move(encryption_cols6))
                            ->algorithm(parquet::ParquetCipher::AES_GCM_CTR_V1)
                            ->build(),
                        "tmp_encrypt_columns_and_footer_ctr.parquet.encrypted"));
}

enum class AadPrefixMode { None, Stored, External };

class TestGeneratedEncryptionConfiguration
    : public ::testing::TestWithParam<
          std::tuple<ParquetCipher::type, bool, AadPrefixMode>> {
 public:
  void SetUp() override {
    PARQUET_ASSIGN_OR_THROW(temp_dir_, temp_data_dir());
    file_path_ = temp_dir_->path().ToString() + "algorithm_metadata.parquet.encrypted";

    std::map<std::string, std::shared_ptr<ColumnEncryptionProperties>> columns;
    columns[kDoubleFieldName] =
        ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey1, "kc1");
    columns[kFloatFieldName] =
        ColumnEncryptionProperties::WithColumnKey(kColumnEncryptionKey2, "kc2");
    columns[kBooleanFieldName] = ColumnEncryptionProperties::Unencrypted();

    FileEncryptionProperties::Builder builder(kFooterEncryptionKey);
    builder.footer_key_metadata("kf")
        ->encrypted_columns(std::move(columns))
        ->algorithm(std::get<0>(GetParam()));
    if (std::get<1>(GetParam())) {
      builder.set_plaintext_footer();
    }
    if (std::get<2>(GetParam()) != AadPrefixMode::None) {
      builder.aad_prefix(kFileName);
    }
    if (std::get<2>(GetParam()) == AadPrefixMode::External) {
      builder.disable_aad_prefix_storage();
    }
    auto properties = builder.build();
    file_aad_ = properties->file_aad();
    FileEncryptor().EncryptFile(file_path_, std::move(properties));
  }

 protected:
  enum class Keys { All, MissingColumn, WrongFooter };
  enum class AadPrefix { Match, Missing, Wrong };

  std::shared_ptr<FileDecryptionProperties> MakeDecryptionProperties(
      Keys keys = Keys::All, AadPrefix prefix = AadPrefix::Match) {
    auto retriever = std::make_shared<StringKeyIdRetriever>();
    retriever->PutKey("kf", keys == Keys::WrongFooter ? SecureString(kNewFooterMasterKey)
                                                      : kFooterEncryptionKey);
    if (keys != Keys::MissingColumn) {
      retriever->PutKey("kc1", kColumnEncryptionKey1);
    }
    retriever->PutKey("kc2", kColumnEncryptionKey2);

    FileDecryptionProperties::Builder builder;
    builder.key_retriever(std::move(retriever));
    if (std::get<2>(GetParam()) != AadPrefixMode::None && prefix != AadPrefix::Missing) {
      builder.aad_prefix(prefix == AadPrefix::Wrong ? "incorrect-prefix" : kFileName);
    }
    return builder.build();
  }

  std::unique_ptr<ParquetFileReader> OpenReader(
      std::shared_ptr<FileDecryptionProperties> decryption_properties) {
    auto reader_properties = default_reader_properties();
    reader_properties.file_decryption_properties(std::move(decryption_properties));
    return ParquetFileReader::OpenFile(file_path_, false, reader_properties);
  }

  void CheckPlaintextFooterSignature() {
    PARQUET_ASSIGN_OR_THROW(auto file, ::arrow::io::ReadableFile::Open(file_path_));
    PARQUET_ASSIGN_OR_THROW(auto file_size, file->GetSize());
    ASSERT_GT(file_size, 8);
    PARQUET_ASSIGN_OR_THROW(auto trailer, file->ReadAt(file_size - 8, 8));
    ASSERT_EQ(trailer->size(), 8);
    const auto footer_size = ::arrow::bit_util::FromLittleEndian(
        ::arrow::util::SafeLoadAs<uint32_t>(trailer->data()));
    constexpr auto signature_size = kNonceLength + kGcmTagLength;
    ASSERT_GT(footer_size, signature_size);
    ASSERT_LE(footer_size, file_size - 8);
    PARQUET_ASSIGN_OR_THROW(auto footer,
                            file->ReadAt(file_size - 8 - footer_size, footer_size));
    ASSERT_EQ(footer->size(), footer_size);
    const auto footer_span = footer->span_as<uint8_t>();
    const auto metadata = footer_span.first(footer_size - signature_size);
    const auto nonce = footer_span.subspan(metadata.size(), kNonceLength);
    const auto tag = footer_span.last(kGcmTagLength);
    // Footer AAD is the file AAD followed by the footer module type byte.
    const auto aad = file_aad_ + static_cast<char>(kFooter);

    // Verify the on-disk tag with GCM explicitly, independently of reader selection.
    auto encryptor = AesEncryptor::Make(ParquetCipher::AES_GCM_V1,
                                        static_cast<int32_t>(kFooterEncryptionKey.size()),
                                        /*metadata=*/true,
                                        /*write_length=*/false);
    std::vector<uint8_t> encrypted(encryptor->CiphertextLength(metadata.size()));
    encryptor->SignedFooterEncrypt(metadata, kFooterEncryptionKey.as_span(),
                                   str2span(aad), nonce, encrypted);
    EXPECT_EQ(std::vector<uint8_t>(tag.begin(), tag.end()),
              std::vector<uint8_t>(encrypted.end() - kGcmTagLength, encrypted.end()));
  }

  void ReadFirstSecretValue(std::shared_ptr<FileDecryptionProperties> properties) {
    auto reader = OpenReader(std::move(properties));
    auto column = reader->RowGroup(0)->Column(5);
    auto* double_reader = static_cast<DoubleReader*>(column.get());
    double value;
    int64_t values_read;
    double_reader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
  }

  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string file_path_;
  std::string file_aad_;
};

TEST_P(TestGeneratedEncryptionConfiguration, AlgorithmMetadataAndRoundTrip) {
  const auto cipher = std::get<0>(GetParam());
  const bool plaintext_footer = std::get<1>(GetParam());
  const auto prefix_mode = std::get<2>(GetParam());
  if (plaintext_footer) {
    CheckPlaintextFooterSignature();
  }
  auto reader = OpenReader(MakeDecryptionProperties());
  const auto metadata = reader->metadata();
  if (plaintext_footer) {
    ASSERT_TRUE(metadata->is_encryption_algorithm_set());
    const auto algorithm = metadata->encryption_algorithm();
    ASSERT_EQ(cipher, algorithm.algorithm);
    EXPECT_FALSE(algorithm.aad.aad_file_unique.empty());
    EXPECT_EQ(prefix_mode == AadPrefixMode::External, algorithm.aad.supply_aad_prefix);
    EXPECT_EQ(prefix_mode == AadPrefixMode::Stored ? kFileName : "",
              algorithm.aad.aad_prefix);
  } else {
    // An encrypted footer declares the algorithm in FileCryptoMetaData instead.
    EXPECT_FALSE(metadata->is_encryption_algorithm_set());
  }
  reader->Close();

  FileDecryptor decryptor;
  // Verify every physical column and all row groups through sync and async readers.
  ASSERT_NO_THROW(decryptor.DecryptFile(file_path_, MakeDecryptionProperties()));
  if (prefix_mode == AadPrefixMode::Stored) {
    ASSERT_NO_THROW(decryptor.DecryptFile(
        file_path_, MakeDecryptionProperties(Keys::All, AadPrefix::Missing)));
  }
  if (plaintext_footer) {
    // This reads only public columns and does not authenticate the keyless footer.
    ASSERT_NO_THROW(decryptor.DecryptFile(file_path_, nullptr));
  }
  EXPECT_THROW(ReadFirstSecretValue(MakeDecryptionProperties(Keys::MissingColumn)),
               std::out_of_range);
  EXPECT_THROW(OpenReader(MakeDecryptionProperties(Keys::WrongFooter)), ParquetException);
  if (prefix_mode == AadPrefixMode::External) {
    EXPECT_THROW(OpenReader(MakeDecryptionProperties(Keys::All, AadPrefix::Missing)),
                 ParquetException);
    EXPECT_THROW(OpenReader(MakeDecryptionProperties(Keys::All, AadPrefix::Wrong)),
                 ParquetException);
  }
}

INSTANTIATE_TEST_SUITE_P(
    GeneratedFiles, TestGeneratedEncryptionConfiguration,
    ::testing::Combine(::testing::Values(ParquetCipher::AES_GCM_V1,
                                         ParquetCipher::AES_GCM_CTR_V1),
                       ::testing::Bool(),
                       ::testing::Values(AadPrefixMode::None, AadPrefixMode::Stored,
                                         AadPrefixMode::External)));

// Set temp_dir before running the write/read tests. The encrypted files will
// be written/read from this directory.
void TestEncryptionConfiguration::SetUpTestCase() {
  temp_dir = temp_data_dir().ValueOrDie();
}

}  // namespace parquet::encryption::test

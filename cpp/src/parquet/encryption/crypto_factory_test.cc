#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "arrow/util/secure_string.h"

#include "parquet/encryption/crypto_factory.h"
#include "parquet/encryption/file_key_material_store.h"
#include "parquet/encryption/test_encryption_util.h"
#include "parquet/encryption/test_in_memory_kms.h"

using ::testing::_;
using ::testing::HasSubstr;
using ::testing::Return;
using ::testing::StrEq;

namespace parquet::encryption::test {

class CryptoFactoryTest : public ::testing::Test {

  void SetUp() {
    key_list_ = BuildKeyMap(kColumnMasterKeyIds, kColumnMasterKeys, kFooterMasterKeyId,
                            kFooterMasterKey);
    crypto_factory_.RegisterKmsClientFactory(std::make_shared<TestOnlyInMemoryKmsClientFactory>(
            true, key_list_));
  }

  protected:
    std::unordered_map<std::string, ::arrow::util::SecureString> key_list_;
    KmsConnectionConfig kms_config_;
    CryptoFactory crypto_factory_;
};

TEST_F(CryptoFactoryTest, UniformEncryptionAndColumnKeysThrowsException) {
    ExternalEncryptionConfiguration config("kf");
    config.uniform_encryption = true;
    config.column_keys = "kc1:col1,col2";

    try {
        auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(xcp.what(), HasSubstr("Cannot set both column encryption and uniform"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, UniformEncryptionAndPerColumnEncryptionThrowsException) {
    ExternalEncryptionConfiguration config("kf");
    config.uniform_encryption = true;

    std::unordered_map<std::string, ColumnEncryptionAttributes> per_column_encryption;
    ColumnEncryptionAttributes attributes;
    attributes.parquet_cipher = ParquetCipher::AES_GCM_CTR_V1;
    attributes.key_id = "kc1";
    per_column_encryption["col1"] = attributes;
    config.per_column_encryption = per_column_encryption;

    try {
        auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(xcp.what(), HasSubstr("Cannot set both column encryption and uniform"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, NoUniformEncryptionAndNoColumnsThrowsException) {
    ExternalEncryptionConfiguration config("kf");

    try {
        auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(xcp.what(), HasSubstr(
            "uniform_encryption must be set or column encryption must be specified in either"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, BasicEncryptionConfig) {
    ExternalEncryptionConfiguration config("kf");
    config.plaintext_footer = true;
    config.column_keys = "kc1:col1,col2;kc2:col3,col4";

    auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
    EXPECT_EQ(16, properties->footer_key().size());
    EXPECT_TRUE(properties->footer_key_metadata().size() > 0);
    EXPECT_EQ(ParquetCipher::AES_GCM_V1, properties->algorithm().algorithm);
    EXPECT_FALSE(properties->encrypted_footer());
    EXPECT_TRUE(properties->encrypted_columns().size() == 4);

    auto column_properties_1 = properties->column_encryption_properties("col1");
    EXPECT_EQ("col1", column_properties_1->column_path());
    EXPECT_TRUE(column_properties_1->is_encrypted());
    EXPECT_FALSE(column_properties_1->is_encrypted_with_footer_key());
    EXPECT_THAT(column_properties_1->key_metadata(), HasSubstr("kc1"));
    EXPECT_FALSE(column_properties_1->parquet_cipher().has_value());

    auto column_properties_2 = properties->column_encryption_properties("col4");
    EXPECT_EQ("col4", column_properties_2->column_path());
    EXPECT_TRUE(column_properties_2->is_encrypted());
    EXPECT_FALSE(column_properties_2->is_encrypted_with_footer_key());
    EXPECT_THAT(column_properties_2->key_metadata(), HasSubstr("kc2"));
    EXPECT_FALSE(column_properties_2->parquet_cipher().has_value());
}

TEST_F(CryptoFactoryTest, EncryptionConfigWithExternalDbpaAlgorithmThrowsException) {
    EncryptionConfiguration config("kf");
    config.encryption_algorithm = ParquetCipher::EXTERNAL_DBPA_V1;

    try {
        auto properties = crypto_factory_.GetFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(
            xcp.what(),
            HasSubstr("EXTERNAL_DBPA_V1 algorithm is not supported for file level encryption"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, ExternalEncryptionConfig) {
    ExternalEncryptionConfiguration config("kf");
    config.plaintext_footer = true;
    config.column_keys = "kc3:col3,col4";

    std::unordered_map<std::string, ColumnEncryptionAttributes> per_column_encryption;
    ColumnEncryptionAttributes attributes_1;
    attributes_1.parquet_cipher = ParquetCipher::AES_GCM_CTR_V1;
    attributes_1.key_id = "kc1";
    per_column_encryption["col1"] = attributes_1;

    ColumnEncryptionAttributes attributes_2;
    attributes_2.parquet_cipher = ParquetCipher::AES_GCM_V1;
    attributes_2.key_id = "kc2";
    per_column_encryption["col2"] = attributes_2;

    config.per_column_encryption = per_column_encryption;
    config.app_context = 
        "{\"user_id\": \"abc123\", \"location\": {\"lat\": 9.7489, \"lon\": -83.7534}}";
    config.connection_config = {
        {ParquetCipher::EXTERNAL_DBPA_V1, {{"file_path", "path/to/file"}}}
    };

    auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
    EXPECT_EQ(16, properties->footer_key().size());
    EXPECT_TRUE(properties->footer_key_metadata().size() > 0);
    EXPECT_EQ(ParquetCipher::AES_GCM_V1, properties->algorithm().algorithm);
    EXPECT_FALSE(properties->encrypted_footer());
    EXPECT_TRUE(properties->encrypted_columns().size() == 4);

    auto column_properties_1 = properties->column_encryption_properties("col1");
    EXPECT_EQ("col1", column_properties_1->column_path());
    EXPECT_TRUE(column_properties_1->is_encrypted());
    EXPECT_FALSE(column_properties_1->is_encrypted_with_footer_key());
    EXPECT_THAT(column_properties_1->key_metadata(), HasSubstr("kc1"));
    EXPECT_TRUE(column_properties_1->parquet_cipher().has_value());
    EXPECT_EQ(ParquetCipher::AES_GCM_CTR_V1, column_properties_1->parquet_cipher().value());

    auto column_properties_2 = properties->column_encryption_properties("col2");
    EXPECT_EQ("col2", column_properties_2->column_path());
    EXPECT_TRUE(column_properties_2->is_encrypted());
    EXPECT_FALSE(column_properties_2->is_encrypted_with_footer_key());
    EXPECT_THAT(column_properties_2->key_metadata(), HasSubstr("kc2"));
    EXPECT_TRUE(column_properties_2->parquet_cipher().has_value());
    EXPECT_EQ(ParquetCipher::AES_GCM_V1, column_properties_2->parquet_cipher().value());

    auto column_properties_3 = properties->column_encryption_properties("col3");
    EXPECT_EQ("col3", column_properties_3->column_path());
    EXPECT_TRUE(column_properties_3->is_encrypted());
    EXPECT_FALSE(column_properties_3->is_encrypted_with_footer_key());
    EXPECT_THAT(column_properties_3->key_metadata(), HasSubstr("kc3"));
    EXPECT_FALSE(column_properties_3->parquet_cipher().has_value());

    EXPECT_FALSE(properties->app_context().empty());
    EXPECT_FALSE(properties->connection_config().empty());
    EXPECT_EQ(properties->app_context(), config.app_context);
    EXPECT_NE(properties->connection_config().find(ParquetCipher::EXTERNAL_DBPA_V1),
              properties->connection_config().end());
    EXPECT_EQ(properties->connection_config().find(ParquetCipher::AES_GCM_CTR_V1),
              properties->connection_config().end());
    EXPECT_NE(properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).find("file_path"),
              properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).end());
    EXPECT_EQ(properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).at("file_path"),
              "path/to/file");
}

TEST_F(CryptoFactoryTest, ExternalEncryptionConfigWithExternalDbpaAlgorithmThrowsException) {
    ExternalEncryptionConfiguration config("kf");
    config.plaintext_footer = true;
    config.column_keys = "kc3:col3,col4";
    config.encryption_algorithm = ParquetCipher::EXTERNAL_DBPA_V1;

    try {
        auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(
            xcp.what(),
            HasSubstr("EXTERNAL_DBPA_V1 algorithm is not supported for file level encryption"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, ColumnRepeatedInMapsThrowsException) {
    ExternalEncryptionConfiguration config("kf");
    config.plaintext_footer = true;
    config.column_keys = "kc3:col3,col2";

    std::unordered_map<std::string, ColumnEncryptionAttributes> per_column_encryption;
    ColumnEncryptionAttributes attributes_1;
    attributes_1.parquet_cipher = ParquetCipher::AES_GCM_CTR_V1;
    attributes_1.key_id = "kc1";
    per_column_encryption["col1"] = attributes_1;

    ColumnEncryptionAttributes attributes_2;
    attributes_2.parquet_cipher = ParquetCipher::AES_GCM_V1;
    attributes_2.key_id = "kc2";
    per_column_encryption["col2"] = attributes_2;

    config.per_column_encryption = per_column_encryption;

    try {
        auto properties = crypto_factory_.GetExternalFileEncryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(xcp.what(), HasSubstr("Multiple keys defined for column [col2]"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

TEST_F(CryptoFactoryTest, BasicDecryptionConfig) {
    ExternalDecryptionConfiguration config;
    config.cache_lifetime_seconds = 600;

    auto properties = crypto_factory_.GetExternalFileDecryptionProperties(kms_config_, config);
    EXPECT_TRUE(properties->check_plaintext_footer_integrity());
    EXPECT_TRUE(properties->plaintext_files_allowed());
    EXPECT_THAT(properties->key_retriever(), testing::NotNull());
    EXPECT_TRUE(properties->app_context().empty());
    EXPECT_TRUE(properties->connection_config().empty());
}

TEST_F(CryptoFactoryTest, ExternalDecryptionConfig) {
    ExternalDecryptionConfiguration config;
    config.cache_lifetime_seconds = 600;
    config.app_context = 
        "{\"user_id\": \"abc123\", \"location\": {\"lat\": 9.7489, \"lon\": -83.7534}}";
    config.connection_config = {
        {ParquetCipher::EXTERNAL_DBPA_V1, {{"file_path", "path/to/file"}}}
    };

    auto properties = crypto_factory_.GetExternalFileDecryptionProperties(kms_config_, config);
    EXPECT_TRUE(properties->check_plaintext_footer_integrity());
    EXPECT_TRUE(properties->plaintext_files_allowed());
    EXPECT_THAT(properties->key_retriever(), testing::NotNull());
    EXPECT_FALSE(properties->app_context().empty());
    EXPECT_FALSE(properties->connection_config().empty());
    EXPECT_EQ(properties->app_context(), config.app_context);
    EXPECT_NE(properties->connection_config().find(ParquetCipher::EXTERNAL_DBPA_V1),
              properties->connection_config().end());
    EXPECT_EQ(properties->connection_config().find(ParquetCipher::AES_GCM_CTR_V1),
              properties->connection_config().end());
    EXPECT_NE(properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).find("file_path"),
              properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).end());
    EXPECT_EQ(properties->connection_config().at(ParquetCipher::EXTERNAL_DBPA_V1).at("file_path"),
              "path/to/file");
}

TEST_F(CryptoFactoryTest, ExternalDecryptionConfigWithInvalidAppContextThrowsException) {
    ExternalDecryptionConfiguration config;
    config.cache_lifetime_seconds = 600;
    config.app_context = "invalid_json";

    try {
        auto properties = crypto_factory_.GetExternalFileDecryptionProperties(kms_config_, config);
        FAIL() << "ParquetException should have been raised";
    } catch (const ParquetException& xcp) {
        EXPECT_THAT(xcp.what(), HasSubstr("App context is not a valid JSON string"));
    } catch (...) {
        FAIL() << "Caught unexpected exception type";
    }
}

}  // namespace parquet::encryption::test
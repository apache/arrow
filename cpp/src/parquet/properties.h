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

#ifndef PARQUET_COLUMN_PROPERTIES_H
#define PARQUET_COLUMN_PROPERTIES_H

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <random>
#include <openssl/rand.h>

#include "arrow/type.h"

#include "parquet/encryption.h"
#include "parquet/exception.h"
#include "parquet/parquet_version.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "arrow/util/logging.h"
#include "arrow/util/utf8.h"
namespace parquet {

struct ParquetVersion {
  enum type { PARQUET_1_0, PARQUET_2_0 };
};

static int64_t DEFAULT_BUFFER_SIZE = 1024;
static bool DEFAULT_USE_BUFFERED_STREAM = false;
static constexpr bool DEFAULT_CHECK_SIGNATURE = true;
static const std::string NULL_STRING = "";

class PARQUET_EXPORT ColumnEncryptionProperties {
 public:
  class Builder {
   public:
    // Convenience builder for regular (not nested) columns.
    Builder(const std::string name) {
      Builder(schema::ColumnPath::FromDotString(name), true);
    }

    // Convenience builder for encrypted columns.
    Builder(const std::shared_ptr<schema::ColumnPath>& path)
      : Builder(path, true) {}

    // Set a column-specific key.
    // If key is not set on an encrypted column, the column will
    // be encrypted with the footer key.
    // keyBytes Key length must be either 16, 24 or 32 bytes.
    Builder* withKey(const std::string& key) {
      if (key.empty ())
        return this;

      DCHECK(!key.empty());
      key_ = key;
      return this;
    }

    // Set a key retrieval metadata.
    // use either withKeyMetaData or withKeyID, not both
    Builder* withKeyMetaData(const std::string& key_metadata) {
      DCHECK(!key_metadata.empty());
      DCHECK(key_metadata_.empty());
      key_metadata_ = key_metadata;
      return this;
    }

    // Set a key retrieval metadata (converted from String).
    // use either withKeyMetaData or withKeyID, not both
    // key_id will be converted to metadata (UTF-8 array).
    Builder* withKeyID(std::string key_id) {
      //key_id is expected to be in UTF8 encoding
      ::arrow::util::InitializeUTF8();
      const uint8_t *data = reinterpret_cast<const uint8_t*>(key_id.c_str());
      if (!::arrow::util::ValidateUTF8(data, key_id.size()))
        throw ParquetException("key id should be in UTF8 encoding");

      DCHECK(!key_id.empty());
      this->withKeyMetaData(key_id);
      return this;
    }

    std::shared_ptr<ColumnEncryptionProperties> build() {
      return std::shared_ptr<ColumnEncryptionProperties>(
          new ColumnEncryptionProperties(encrypted_,
                                         column_path_,
                                         key_,
                                         key_metadata_));
    }

   private:
    const std::shared_ptr<schema::ColumnPath> column_path_;
    bool encrypted_;
    std::string key_;
    std::string key_metadata_;

    Builder(const std::shared_ptr<schema::ColumnPath>& path, bool encrypted)
      : column_path_(path), encrypted_(encrypted) {}
  };
  const std::shared_ptr<schema::ColumnPath>& getPath() { return column_path_; }
  bool isEncrypted() const { return encrypted_; }
  bool isEncryptedWithFooterKey() const { return encrypted_with_footer_key_; }
  const std::string& getKey() const { return key_; }
  const std::string& getKeyMetaData() const { return key_metadata_; }

  ColumnEncryptionProperties() = default;
  ColumnEncryptionProperties(const ColumnEncryptionProperties& other) = default;
  ColumnEncryptionProperties(ColumnEncryptionProperties&& other) = default;

 private:
  const std::shared_ptr<schema::ColumnPath> column_path_;
  bool encrypted_;
  bool encrypted_with_footer_key_;
  std::string key_;
  std::string key_metadata_;
  explicit ColumnEncryptionProperties(
      bool encrypted,
      const std::shared_ptr<schema::ColumnPath>& column_path,
      const std::string& key,
      const std::string& key_metadata):column_path_(column_path){
    DCHECK(column_path != nullptr);
    if (!encrypted)
      DCHECK(key.empty() && key_metadata.empty());

    if (!key.empty())
      DCHECK(key.length() == 16 || key.length() == 24 || key.length() == 32);

    encrypted_with_footer_key_ = (encrypted && key.empty());
    if (encrypted_with_footer_key_)
      DCHECK(key_metadata.empty());

    encrypted_ = encrypted;
    key_metadata_ = key_metadata;
    key_ = key;
  }
};

class PARQUET_EXPORT ColumnDecryptionProperties {
 public:
  class Builder {
   public:
    // convenience builder for regular (not nested) columns.
    Builder(const std::string name) {
      Builder(schema::ColumnPath::FromDotString(name));
    }

    Builder(const std::shared_ptr<schema::ColumnPath>& path)
      : column_path_(path) {}

    // Set an explicit column key. If applied on a file that contains
    // key metadata for this column the metadata will be ignored,
    // the column will be decrypted with this key.
    // key length must be either 16, 24 or 32 bytes.
    Builder* withKey(const std::string& key) {
      if (key.empty ())
        return this;

      DCHECK(!key.empty());
      key_ = key;
      return this;
    }

    std::shared_ptr<ColumnDecryptionProperties> build() {
      return std::shared_ptr<ColumnDecryptionProperties>(
          new ColumnDecryptionProperties(column_path_, key_));
    }

  private:
    const std::shared_ptr<schema::ColumnPath> column_path_;
    std::string key_;
  };

  ColumnDecryptionProperties() = default;
  ColumnDecryptionProperties(const ColumnDecryptionProperties& other) = default;
  ColumnDecryptionProperties(ColumnDecryptionProperties&& other) = default;

  const std::shared_ptr<schema::ColumnPath>& getPath() { return column_path_; }
  const std::string& getKey() const { return key_; }

 private:
  const std::shared_ptr<schema::ColumnPath> column_path_;
  std::string key_;

  // This class is only required for setting explicit column decryption keys -
  // to override key retriever (or to provide keys when key metadata and/or
  // key retriever are not available)
  explicit ColumnDecryptionProperties(
      const std::shared_ptr<schema::ColumnPath>& column_path,
      const std::string& key):column_path_(column_path){
    DCHECK(column_path != nullptr);

    if (!key.empty())
      DCHECK(key.length() == 16 || key.length() == 24 || key.length() == 32);

    key_ = key;
  }
};

class PARQUET_EXPORT AADPrefixVerifier {
 public:
  // Verifies identity (AAD Prefix) of individual file,
  // or of file collection in a data set.
  // Throws exception if an AAD prefix is wrong.
  // In a data set, AAD Prefixes should be collected,
  // and then checked for missing files.
  virtual void check(std::string aad_prefix) = 0;
};

class PARQUET_EXPORT FileDecryptionProperties {
 public:
  class Builder {
  public:
    Builder(){
      check_plaintext_footer_integrity_ = DEFAULT_CHECK_SIGNATURE;
    }

    // Set an explicit footer key. If applied on a file that contains
    // footer key metadata the metadata will be ignored, the footer
    // will be decrypted/verified with this key.
    // If explicit key is not set, footer key will be fetched from
    // key retriever.
    // param footerKey Key length must be either 16, 24 or 32 bytes.
    Builder* withFooterKey(std::string footer_key) {
      if (footer_key.empty ()) {
        return this;
      }
      DCHECK(!footer_key.empty());
      footer_key_ = footer_key;
      return this;
    }

    // Set explicit column keys (decryption properties).
    // Its also possible to set a key retriever on this property object.
    // Upon file decryption, availability of explicit keys is checked before
    // invocation of the retriever callback.
    // If an explicit key is available for a footer or a column,
    // its key metadata will be ignored.
    Builder* withColumnKeys(const std::map<std::shared_ptr<schema::ColumnPath>,
                            std::shared_ptr<ColumnDecryptionProperties>,
                            schema::ColumnPath::CmpColumnPath>&
                            column_properties) {
      if (column_properties.size () == 0)
        return this;

      if (column_property_map_.size () != 0)
        throw ParquetException("Column properties already set");

      column_property_map_ = column_properties;
      return this;
    }

    // Set a key retriever callback. Its also possible to
    // set explicit footer or column keys on this file property object.
    // Upon file decryption, availability of explicit keys is checked before
    // invocation of the retriever callback.
    // If an explicit key is available for a footer or a column,
    // its key metadata will be ignored.
    Builder* withKeyRetriever(const std::shared_ptr<DecryptionKeyRetriever>&
                              key_retriever) {
      if (key_retriever == NULLPTR)
        return this;

      DCHECK(key_retriever_ == NULLPTR);
      key_retriever_ = key_retriever;
      return this;
    }

    // Skip integrity verification of plaintext footers.
    // If not called, integrity of plaintext footers will be checked in runtime,
    // and an exception will be thrown in the following situations:
    // - footer signing key is not available
    // (not passed, or not found by key retriever)
    // - footer content and signature don't match
    Builder* withoutFooterSignatureVerification() {
      check_plaintext_footer_integrity_ = false;
      return this;
    }

    // Explicitly supply the file AAD prefix.
    // A must when a prefix is used for file encryption, but not stored in file.
    // If AAD prefix is stored in file, it will be compared to the explicitly
    // supplied value and an exception will be thrown if they differ.
    Builder* withAADPrefix(std::string aad_prefix) {
      if (aad_prefix.empty()) {
        return this;
      }
      DCHECK(aad_prefix_.empty());
      aad_prefix_ = aad_prefix;
      return this;
    }

    // Set callback for verification of AAD Prefixes stored in file.
    Builder* withAADPrefixVerifier(
        std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier) {
      if (aad_prefix_verifier == NULLPTR)
        return this;

      DCHECK(aad_prefix_verifier_ == NULLPTR);
      aad_prefix_verifier_ = aad_prefix_verifier;
      return this;
    }

    std::shared_ptr<FileDecryptionProperties> build() {
      return std::shared_ptr<FileDecryptionProperties>(
          new FileDecryptionProperties(footer_key_,
                                       key_retriever_,
                                       check_plaintext_footer_integrity_,
                                       aad_prefix_,
                                       aad_prefix_verifier_,
                                       column_property_map_));
    }

  private:
    std::string footer_key_;
    std::string aad_prefix_;
    std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier_;

    std::map<std::shared_ptr<schema::ColumnPath>,
      std::shared_ptr<ColumnDecryptionProperties>,
      schema::ColumnPath::CmpColumnPath> column_property_map_;

    std::shared_ptr<DecryptionKeyRetriever> key_retriever_;
    bool check_plaintext_footer_integrity_;
  };

  const std::string& getColumnKey(
      const std::shared_ptr<schema::ColumnPath>& column_path) {
    if (column_property_map_.find(column_path) != column_property_map_.end()) {
      auto column_prop = column_property_map_[column_path];
      if (column_prop != nullptr)
        return column_prop->getKey();
    }
    return NULL_STRING;
  }

  const std::string& getFooterKey() {
    return footer_key_;
  }

  const std::string& getAADPrefix() { return aad_prefix_; }

  std::shared_ptr<DecryptionKeyRetriever> getKeyRetriever() {
    return key_retriever_;
  }

  bool checkFooterIntegrity() {
    return check_plaintext_footer_integrity_;
  }

  const std::shared_ptr<AADPrefixVerifier> &getAADPrefixVerifier() {
    return aad_prefix_verifier_;
  }

 private:
  std::string footer_key_;
  std::string aad_prefix_;
  std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier_;

  std::map<std::shared_ptr<schema::ColumnPath>,
    std::shared_ptr<ColumnDecryptionProperties>,
    schema::ColumnPath::CmpColumnPath> column_property_map_;

  std::shared_ptr<DecryptionKeyRetriever> key_retriever_;
  bool check_plaintext_footer_integrity_;


  FileDecryptionProperties(
      const std::string& footer_key,
      const std::shared_ptr<DecryptionKeyRetriever>& key_retriever,
      bool check_plaintext_footer_integrity,
      std::string aad_prefix,
      std::shared_ptr<AADPrefixVerifier> aad_prefix_verifier,
      std::map<std::shared_ptr<schema::ColumnPath>,
      std::shared_ptr<ColumnDecryptionProperties>,
      schema::ColumnPath::CmpColumnPath> column_property_map) {
    DCHECK(!footer_key.empty() ||
           NULLPTR != key_retriever ||
           0 != column_property_map.size());
    if (!footer_key.empty())
      DCHECK(footer_key.length() == 16 || footer_key.length() == 24 ||
             footer_key.length() == 32);
    if (footer_key.empty() && check_plaintext_footer_integrity)
      DCHECK(NULLPTR != key_retriever);
    aad_prefix_verifier_ = aad_prefix_verifier;
    footer_key_ = footer_key;
    check_plaintext_footer_integrity_ = check_plaintext_footer_integrity;
    key_retriever_ = key_retriever;
    aad_prefix_ = aad_prefix;
    column_property_map_ = column_property_map;
  }

};

class PARQUET_EXPORT ReaderProperties {
 public:
  explicit ReaderProperties(MemoryPool* pool = ::arrow::default_memory_pool())
      : pool_(pool) {
    buffered_stream_enabled_ = DEFAULT_USE_BUFFERED_STREAM;
    buffer_size_ = DEFAULT_BUFFER_SIZE;
    column_map_ = std::shared_ptr<std::map<std::shared_ptr<schema::ColumnPath>,
      std::string, parquet::schema::ColumnPath::CmpColumnPath>>(new std::map<std::shared_ptr<schema::ColumnPath>,
                                                                std::string,
                                                                schema::ColumnPath::CmpColumnPath>());
  }

  MemoryPool* memory_pool() const { return pool_; }

  std::shared_ptr<ArrowInputStream> GetStream(std::shared_ptr<ArrowInputFile> source,
                                              int64_t start, int64_t num_bytes);

  bool is_buffered_stream_enabled() const { return buffered_stream_enabled_; }

  void enable_buffered_stream() { buffered_stream_enabled_ = true; }

  void disable_buffered_stream() { buffered_stream_enabled_ = false; }

  void set_buffer_size(int64_t buf_size) { buffer_size_ = buf_size; }

  int64_t buffer_size() const { return buffer_size_; }

  std::shared_ptr<std::map<std::shared_ptr<schema::ColumnPath>,
    std::string, parquet::schema::ColumnPath::CmpColumnPath>> column_map () {
    return column_map_;
  }

  const std::string& fileAAD() { return fileAAD_; }

  void set_fileAAD (std::string fileAAD) { fileAAD_ = fileAAD; }

  void file_decryption(const std::shared_ptr<FileDecryptionProperties>& decryption) {
    file_decryption_ = decryption;
  }

  FileDecryptionProperties* file_decryption() { return file_decryption_.get(); }

 private:
  MemoryPool* pool_;
  int64_t buffer_size_;
  bool buffered_stream_enabled_;
  std::shared_ptr<FileDecryptionProperties> file_decryption_;
  std::shared_ptr<std::map<std::shared_ptr<schema::ColumnPath>,
    std::string, parquet::schema::ColumnPath::CmpColumnPath>> column_map_; // a map between
  //ColumnPath and their encryption keys
  std::string fileAAD_;
};

ReaderProperties PARQUET_EXPORT default_reader_properties();

static constexpr int64_t kDefaultDataPageSize = 1024 * 1024;
static constexpr bool DEFAULT_IS_DICTIONARY_ENABLED = true;
static constexpr int64_t DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = kDefaultDataPageSize;
static constexpr int64_t DEFAULT_WRITE_BATCH_SIZE = 1024;
static constexpr int64_t DEFAULT_MAX_ROW_GROUP_LENGTH = 64 * 1024 * 1024;
static constexpr bool DEFAULT_ARE_STATISTICS_ENABLED = true;
static constexpr int64_t DEFAULT_MAX_STATISTICS_SIZE = 4096;
static constexpr Encoding::type DEFAULT_ENCODING = Encoding::PLAIN;
static constexpr ParquetVersion::type DEFAULT_WRITER_VERSION =
    ParquetVersion::PARQUET_1_0;
static const char DEFAULT_CREATED_BY[] = CREATED_BY_VERSION;
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;
static constexpr ParquetCipher::type DEFAULT_ENCRYPTION_ALGORITHM = ParquetCipher::AES_GCM_V1;
static constexpr int32_t MAXIMAL_AAD_METADATA_LENGTH = 256;
static constexpr bool DEFAULT_ENCRYPTED_FOOTER = true;
static constexpr int32_t AAD_FILE_UNIQUE_LENGTH = 8;

class PARQUET_EXPORT ColumnProperties {
 public:
  ColumnProperties(Encoding::type encoding = DEFAULT_ENCODING,
                   Compression::type codec = DEFAULT_COMPRESSION_TYPE,
                   bool dictionary_enabled = DEFAULT_IS_DICTIONARY_ENABLED,
                   bool statistics_enabled = DEFAULT_ARE_STATISTICS_ENABLED,
                   size_t max_stats_size = DEFAULT_MAX_STATISTICS_SIZE)
      : encoding_(encoding),
        codec_(codec),
        dictionary_enabled_(dictionary_enabled),
        statistics_enabled_(statistics_enabled),
        max_stats_size_(max_stats_size) {}

  void set_encoding(Encoding::type encoding) { encoding_ = encoding; }

  void set_compression(Compression::type codec) { codec_ = codec; }

  void set_dictionary_enabled(bool dictionary_enabled) {
    dictionary_enabled_ = dictionary_enabled;
  }

  void set_statistics_enabled(bool statistics_enabled) {
    statistics_enabled_ = statistics_enabled;
  }

  void set_max_statistics_size(size_t max_stats_size) {
    max_stats_size_ = max_stats_size;
  }

  Encoding::type encoding() const { return encoding_; }

  Compression::type compression() const { return codec_; }

  bool dictionary_enabled() const { return dictionary_enabled_; }

  bool statistics_enabled() const { return statistics_enabled_; }

  size_t max_statistics_size() const { return max_stats_size_; }

 private:
  Encoding::type encoding_;
  Compression::type codec_;
  bool dictionary_enabled_;
  bool statistics_enabled_;
  size_t max_stats_size_;
};

class PARQUET_EXPORT FileEncryptionProperties {
 public:
  class Builder {
  public:
   Builder(const std::string& footer_key)
     : parquet_cipher_(DEFAULT_ENCRYPTION_ALGORITHM),
      encrypted_footer_(DEFAULT_ENCRYPTED_FOOTER) {
      footer_key_ = footer_key;
      store_aad_prefix_in_file_ = false;
    }

    // Create files with plaintext footer.
    // If not called, the files will be created with encrypted footer (default).
    Builder* withPlaintextFooter() {
      encrypted_footer_ = false;
      return this;
    }

    // Set encryption algorithm.
    // If not called, files will be encrypted with AES_GCM_V1 (default).
    Builder* withAlgorithm(ParquetCipher::type parquet_cipher) {
      parquet_cipher_ = parquet_cipher;
      return this;
    }

    // Set a key retrieval metadata (converted from String).
    // use either withFooterKeyMetaData or withFooterKeyID, not both.
    Builder* withFooterKeyID(std::string key_id) {
      //key_id is expected to be in UTF8 encoding
      ::arrow::util::InitializeUTF8();
      const uint8_t* data = reinterpret_cast<const uint8_t*>(key_id.c_str());
      if (!::arrow::util::ValidateUTF8(data, key_id.size()))
        throw ParquetException("footer key id should be in UTF8 encoding");

      if (key_id.empty())
        return this;

      return withFooterKeyMetadata(key_id);
    }

    // Set a key retrieval metadata.
    // use either withFooterKeyMetaData or withFooterKeyID, not both.
    Builder* withFooterKeyMetadata(const std::string& footer_key_metadata) {
      if (footer_key_metadata.empty())
        return this;

      DCHECK(footer_key_metadata_.empty());
      footer_key_metadata_ = footer_key_metadata;
      return this;
    }

    // Set the file AAD Prefix.
    Builder* withAADPrefix(const std::string& aad_prefix) {
      if (aad_prefix.empty())
        return this;

      DCHECK(aad_prefix_.empty());
      aad_prefix_ = aad_prefix;
      store_aad_prefix_in_file_ = true;
      return this;
    }

    // Skip storing AAD Prefix in file.
    // If not called, and if AAD Prefix is set, it will be stored.
    Builder* withoutAADPrefixStorage() {
      store_aad_prefix_in_file_ = false;
      return this;
    }

    // Set the list of encrypted columns and their properties (keys etc).
    // If not called, all columns will be encrypted with the footer key.
    // If called, the file columns not in the list will be left unencrypted.
    Builder* withEncryptedColumns(
        const std::map<std::shared_ptr<schema::ColumnPath>,
        std::shared_ptr<ColumnEncryptionProperties>,
        schema::ColumnPath::CmpColumnPath>&
        encryptedColumns){
      if (encryptedColumns.size () == 0)
        return this;

      if (column_property_map_.size () != 0)
        throw ParquetException("Column properties already set");

      column_property_map_ = encryptedColumns;
      return this;
    }

    std::shared_ptr<FileEncryptionProperties> build() {
      return std::shared_ptr<FileEncryptionProperties>(
          new FileEncryptionProperties(parquet_cipher_,
                                       footer_key_,
                                       footer_key_metadata_,
                                       encrypted_footer_,
                                       aad_prefix_,
                                       store_aad_prefix_in_file_,
                                       column_property_map_));
    }

  private:
    ParquetCipher::type parquet_cipher_;
    bool encrypted_footer_;
    std::string footer_key_;
    std::string footer_key_metadata_;

    std::string aad_prefix_;
    bool store_aad_prefix_in_file_;
    std::map<std::shared_ptr<schema::ColumnPath>,
      std::shared_ptr<ColumnEncryptionProperties>,
      schema::ColumnPath::CmpColumnPath> column_property_map_;
  };

  bool encryptedFooter() const { return encrypted_footer_; }

  const EncryptionAlgorithm getAlgorithm() {
    return algorithm_;
  }

  const std::string& getFooterEncryptionKey() {
    return (encrypted_footer_? footer_key_ : NULL_STRING);
  }

  const std::string& getFooterEncryptionKeyMetadata() {
    return (encrypted_footer_? footer_key_metadata_ : NULL_STRING);
  }

  const std::string& getFooterSigningKey() {
    return (encrypted_footer_? NULL_STRING : footer_key_);
  }

  const std::string& getFooterSigningKeyMetadata() {
    return (encrypted_footer_? NULL_STRING : footer_key_metadata_);
  }

  const std::string& getFileAAD() const { return file_AAD_; }

  std::shared_ptr<ColumnEncryptionProperties> getColumnProperties(
      const std::shared_ptr<schema::ColumnPath>& column_path) {
    if (column_property_map_.size () == 0){
      auto builder = std::shared_ptr<ColumnEncryptionProperties::Builder>(
          new ColumnEncryptionProperties::Builder (column_path));
      return builder->build();
    }
    if (column_property_map_.find(column_path) != column_property_map_.end())
      return column_property_map_[column_path];

    return NULLPTR;
  }

 private:
  EncryptionAlgorithm algorithm_;  // encryption algorithm
  std::string footer_key_;
  std::string footer_key_metadata_;
  bool encrypted_footer_;
  std::string file_AAD_;

  std::map<std::shared_ptr<schema::ColumnPath>,
    std::shared_ptr<ColumnEncryptionProperties>,
    schema::ColumnPath::CmpColumnPath> column_property_map_;

  FileEncryptionProperties(ParquetCipher::type cipher,
                           std::string footer_key,
                           std::string footer_key_metadata,
                           bool encrypted_footer,
                           const std::string& aad_prefix,
                           bool store_aad_prefix_in_file,
                           const std::map<std::shared_ptr<schema::ColumnPath>,
                           std::shared_ptr<ColumnEncryptionProperties>,
                           schema::ColumnPath::CmpColumnPath>&
                           column_property_map)
    : footer_key_(footer_key),
    footer_key_metadata_(footer_key_metadata),
    encrypted_footer_(encrypted_footer),
    column_property_map_(column_property_map){
      DCHECK(!footer_key.empty());
      // footer_key must be either 16, 24 or 32 bytes.
      DCHECK(footer_key.length() == 16
             || footer_key.length() == 24
             || footer_key.length() == 32);

      uint8_t aad_file_unique[AAD_FILE_UNIQUE_LENGTH];
      memset(aad_file_unique, 0, AAD_FILE_UNIQUE_LENGTH);
      RAND_bytes(aad_file_unique, sizeof(AAD_FILE_UNIQUE_LENGTH));
      std::string aad_file_unique_str(
          reinterpret_cast<char const*>(aad_file_unique),
          AAD_FILE_UNIQUE_LENGTH) ;

      bool supply_aad_prefix = false;
      if (aad_prefix.empty())
        file_AAD_ = aad_file_unique_str;
      else {
        file_AAD_ = aad_prefix + aad_file_unique_str;
        if (!store_aad_prefix_in_file) supply_aad_prefix = true;
      }
      algorithm_.algorithm = cipher;
      algorithm_.aad.aad_file_unique = aad_file_unique_str;
      algorithm_.aad.supply_aad_prefix = supply_aad_prefix;
      if (!aad_prefix.empty() && store_aad_prefix_in_file) {
        algorithm_.aad.aad_prefix = aad_prefix;
      }
    }
};

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : pool_(::arrow::default_memory_pool()),
          dictionary_pagesize_limit_(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT),
          write_batch_size_(DEFAULT_WRITE_BATCH_SIZE),
          max_row_group_length_(DEFAULT_MAX_ROW_GROUP_LENGTH),
          pagesize_(kDefaultDataPageSize),
          version_(DEFAULT_WRITER_VERSION),
          created_by_(DEFAULT_CREATED_BY) {}
    virtual ~Builder() {}

    Builder* memory_pool(MemoryPool* pool) {
      pool_ = pool;
      return this;
    }

    Builder* enable_dictionary() {
      default_column_properties_.set_dictionary_enabled(true);
      return this;
    }

    Builder* disable_dictionary() {
      default_column_properties_.set_dictionary_enabled(false);
      return this;
    }

    Builder* enable_dictionary(const std::string& path) {
      dictionary_enabled_[path] = true;
      return this;
    }

    Builder* enable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->enable_dictionary(path->ToDotString());
    }

    Builder* disable_dictionary(const std::string& path) {
      dictionary_enabled_[path] = false;
      return this;
    }

    Builder* disable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->disable_dictionary(path->ToDotString());
    }

    Builder* dictionary_pagesize_limit(int64_t dictionary_psize_limit) {
      dictionary_pagesize_limit_ = dictionary_psize_limit;
      return this;
    }

    Builder* write_batch_size(int64_t write_batch_size) {
      write_batch_size_ = write_batch_size;
      return this;
    }

    Builder* max_row_group_length(int64_t max_row_group_length) {
      max_row_group_length_ = max_row_group_length;
      return this;
    }

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    Builder* version(ParquetVersion::type version) {
      version_ = version;
      return this;
    }

    Builder* created_by(const std::string& created_by) {
      created_by_ = created_by;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      default_column_properties_.set_encoding(encoding_type);
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(const std::string& path, Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      encodings_[path] = encoding_type;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(const std::shared_ptr<schema::ColumnPath>& path,
                      Encoding::type encoding_type) {
      return this->encoding(path->ToDotString(), encoding_type);
    }

    Builder* compression(Compression::type codec) {
      default_column_properties_.set_compression(codec);
      return this;
    }

    Builder* max_statistics_size(size_t max_stats_sz) {
      default_column_properties_.set_max_statistics_size(max_stats_sz);
      return this;
    }

    Builder* compression(const std::string& path, Compression::type codec) {
      codecs_[path] = codec;
      return this;
    }

    Builder* compression(const std::shared_ptr<schema::ColumnPath>& path,
                         Compression::type codec) {
      return this->compression(path->ToDotString(), codec);
    }

    Builder* encryption(
        const std::shared_ptr<FileEncryptionProperties>& file_encryption) {
      file_encryption_ = file_encryption;
      return this;
    }

    Builder* enable_statistics() {
      default_column_properties_.set_statistics_enabled(true);
      return this;
    }

    Builder* disable_statistics() {
      default_column_properties_.set_statistics_enabled(false);
      return this;
    }

    Builder* enable_statistics(const std::string& path) {
      statistics_enabled_[path] = true;
      return this;
    }

    Builder* enable_statistics(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->enable_statistics(path->ToDotString());
    }

    Builder* disable_statistics(const std::string& path) {
      statistics_enabled_[path] = false;
      return this;
    }

    Builder* disable_statistics(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->disable_statistics(path->ToDotString());
    }

    std::shared_ptr<WriterProperties> build() {
      std::unordered_map<std::string, ColumnProperties> column_properties;
      auto get = [&](const std::string& key) -> ColumnProperties& {
        auto it = column_properties.find(key);
        if (it == column_properties.end())
          return column_properties[key] = default_column_properties_;
        else
          return it->second;
      };

      for (const auto& item : encodings_) get(item.first).set_encoding(item.second);
      for (const auto& item : codecs_) get(item.first).set_compression(item.second);
      for (const auto& item : dictionary_enabled_)
        get(item.first).set_dictionary_enabled(item.second);
      for (const auto& item : statistics_enabled_)
        get(item.first).set_statistics_enabled(item.second);

      return std::shared_ptr<WriterProperties>(new WriterProperties(
          pool_, dictionary_pagesize_limit_, write_batch_size_, max_row_group_length_,
          pagesize_, version_, created_by_, std::move(file_encryption_),
          default_column_properties_, column_properties));
    }

   private:
    MemoryPool* pool_;
    int64_t dictionary_pagesize_limit_;
    int64_t write_batch_size_;
    int64_t max_row_group_length_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    std::string created_by_;
    std::shared_ptr<FileEncryptionProperties> file_encryption_;

    // Settings used for each column unless overridden in any of the maps below
    ColumnProperties default_column_properties_;
    std::unordered_map<std::string, Encoding::type> encodings_;
    std::unordered_map<std::string, Compression::type> codecs_;
    std::unordered_map<std::string, bool> dictionary_enabled_;
    std::unordered_map<std::string, bool> statistics_enabled_;
  };

  inline MemoryPool* memory_pool() const { return pool_; }

  inline int64_t dictionary_pagesize_limit() const { return dictionary_pagesize_limit_; }

  inline int64_t write_batch_size() const { return write_batch_size_; }

  inline int64_t max_row_group_length() const { return max_row_group_length_; }

  inline int64_t data_pagesize() const { return pagesize_; }

  inline ParquetVersion::type version() const { return parquet_version_; }

  inline std::string created_by() const { return parquet_created_by_; }

  inline FileEncryptionProperties* file_encryption() const {
    return parquet_file_encryption_.get();
  }

  inline std::shared_ptr<EncryptionProperties> footer_encryption() const {
    if (parquet_file_encryption_ == NULLPTR) {
      return NULLPTR;
    } else {
      std::string footer_key = parquet_file_encryption_->getFooterEncryptionKey ();
      if (footer_key.empty())
        footer_key = parquet_file_encryption_->getFooterSigningKey ();
      return std::make_shared<EncryptionProperties>(parquet_file_encryption_->getAlgorithm().algorithm,
                                                    footer_key, parquet_file_encryption_->getFileAAD());

    }
  }

  inline Encoding::type dictionary_index_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::RLE_DICTIONARY;
    }
  }

  inline Encoding::type dictionary_page_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::PLAIN;
    }
  }

  const ColumnProperties& column_properties(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = column_properties_.find(path->ToDotString());
    if (it != column_properties_.end()) return it->second;
    return default_column_properties_;
  }

  Encoding::type encoding(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).encoding();
  }

  Compression::type compression(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).compression();
  }

  bool dictionary_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).dictionary_enabled();
  }

  bool statistics_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).statistics_enabled();
  }

  size_t max_statistics_size(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).max_statistics_size();
  }

  std::shared_ptr<ColumnEncryptionProperties> column_encryption_props(const
      std::shared_ptr<schema::ColumnPath>& path) const {
    if (parquet_file_encryption_) {
      return parquet_file_encryption_->getColumnProperties(path);
    } else {
      return NULLPTR;
    }
  }

  std::shared_ptr<EncryptionProperties> encryption(
                                                   const std::shared_ptr<schema::ColumnPath>& path) const {
    if (parquet_file_encryption_) {
      auto column_prop = parquet_file_encryption_->getColumnProperties(path);
      if (column_prop == NULLPTR)
        return NULLPTR;
      if (column_prop->isEncryptedWithFooterKey()) {
        if (parquet_file_encryption_->encryptedFooter ()) {
          return std::make_shared<EncryptionProperties>(parquet_file_encryption_->getAlgorithm().algorithm,
                                                        parquet_file_encryption_->getFooterEncryptionKey(),
                                                        parquet_file_encryption_->getFileAAD());
        } else {
          return std::make_shared<EncryptionProperties>(parquet_file_encryption_->getAlgorithm().algorithm,
                                                        parquet_file_encryption_->getFooterSigningKey(),
                                                        parquet_file_encryption_->getFileAAD());
        }
      }

      return std::make_shared<EncryptionProperties>(parquet_file_encryption_->getAlgorithm().algorithm,
                                                    column_prop->getKey(),
                                                    parquet_file_encryption_->getFileAAD());
    } else {
      return NULLPTR;
    }
  }

 private:
  explicit WriterProperties(
      MemoryPool* pool, int64_t dictionary_pagesize_limit, int64_t write_batch_size,
      int64_t max_row_group_length, int64_t pagesize, ParquetVersion::type version,
      const std::string& created_by,
      std::shared_ptr<FileEncryptionProperties> file_encryption,
      const ColumnProperties& default_column_properties,
      const std::unordered_map<std::string, ColumnProperties>& column_properties)
      : pool_(pool),
        dictionary_pagesize_limit_(dictionary_pagesize_limit),
        write_batch_size_(write_batch_size),
        max_row_group_length_(max_row_group_length),
        pagesize_(pagesize),
        parquet_version_(version),
        parquet_created_by_(created_by),
        parquet_file_encryption_(file_encryption),
        default_column_properties_(default_column_properties),
        column_properties_(column_properties) {}

  MemoryPool* pool_;
  int64_t dictionary_pagesize_limit_;
  int64_t write_batch_size_;
  int64_t max_row_group_length_;
  int64_t pagesize_;
  ParquetVersion::type parquet_version_;
  std::string parquet_created_by_;
  std::shared_ptr<FileEncryptionProperties> parquet_file_encryption_;
  ColumnProperties default_column_properties_;
  std::unordered_map<std::string, ColumnProperties> column_properties_;
};

std::shared_ptr<WriterProperties> PARQUET_EXPORT default_writer_properties();

// ----------------------------------------------------------------------
// Properties specific to Apache Arrow columnar read and write

static constexpr bool kArrowDefaultUseThreads = false;

// Default number of rows to read when using ::arrow::RecordBatchReader
static constexpr int64_t kArrowDefaultBatchSize = 64 * 1024;

/// EXPERIMENTAL: Properties for configuring FileReader behavior.
class PARQUET_EXPORT ArrowReaderProperties {
 public:
  explicit ArrowReaderProperties(bool use_threads = kArrowDefaultUseThreads)
      : use_threads_(use_threads),
        read_dict_indices_(),
        batch_size_(kArrowDefaultBatchSize) {}

  void set_use_threads(bool use_threads) { use_threads_ = use_threads; }

  bool use_threads() const { return use_threads_; }

  void set_read_dictionary(int column_index, bool read_dict) {
    if (read_dict) {
      read_dict_indices_.insert(column_index);
    } else {
      read_dict_indices_.erase(column_index);
    }
  }
  bool read_dictionary(int column_index) const {
    if (read_dict_indices_.find(column_index) != read_dict_indices_.end()) {
      return true;
    } else {
      return false;
    }
  }

  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }

  int64_t batch_size() const { return batch_size_; }

 private:
  bool use_threads_;
  std::unordered_set<int> read_dict_indices_;
  int64_t batch_size_;
};

/// EXPERIMENTAL: Constructs the default ArrowReaderProperties
PARQUET_EXPORT
ArrowReaderProperties default_arrow_reader_properties();

class PARQUET_EXPORT ArrowWriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : write_timestamps_as_int96_(false),
          coerce_timestamps_enabled_(false),
          coerce_timestamps_unit_(::arrow::TimeUnit::SECOND),
          truncated_timestamps_allowed_(false),
          store_schema_(false) {}
    virtual ~Builder() {}

    Builder* disable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = false;
      return this;
    }

    Builder* enable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = true;
      return this;
    }

    Builder* coerce_timestamps(::arrow::TimeUnit::type unit) {
      coerce_timestamps_enabled_ = true;
      coerce_timestamps_unit_ = unit;
      return this;
    }

    Builder* allow_truncated_timestamps() {
      truncated_timestamps_allowed_ = true;
      return this;
    }

    Builder* disallow_truncated_timestamps() {
      truncated_timestamps_allowed_ = false;
      return this;
    }

    /// \brief EXPERIMENTAL: Write binary serialized Arrow schema to the file,
    /// to enable certain read options (like "read_dictionary") to be set
    /// automatically
    Builder* store_schema() {
      store_schema_ = true;
      return this;
    }

    std::shared_ptr<ArrowWriterProperties> build() {
      return std::shared_ptr<ArrowWriterProperties>(new ArrowWriterProperties(
          write_timestamps_as_int96_, coerce_timestamps_enabled_, coerce_timestamps_unit_,
          truncated_timestamps_allowed_, store_schema_));
    }

   private:
    bool write_timestamps_as_int96_;

    bool coerce_timestamps_enabled_;
    ::arrow::TimeUnit::type coerce_timestamps_unit_;
    bool truncated_timestamps_allowed_;

    bool store_schema_;
  };

  bool support_deprecated_int96_timestamps() const { return write_timestamps_as_int96_; }

  bool coerce_timestamps_enabled() const { return coerce_timestamps_enabled_; }
  ::arrow::TimeUnit::type coerce_timestamps_unit() const {
    return coerce_timestamps_unit_;
  }

  bool truncated_timestamps_allowed() const { return truncated_timestamps_allowed_; }

  bool store_schema() const { return store_schema_; }

 private:
  explicit ArrowWriterProperties(bool write_nanos_as_int96,
                                 bool coerce_timestamps_enabled,
                                 ::arrow::TimeUnit::type coerce_timestamps_unit,
                                 bool truncated_timestamps_allowed, bool store_schema)
      : write_timestamps_as_int96_(write_nanos_as_int96),
        coerce_timestamps_enabled_(coerce_timestamps_enabled),
        coerce_timestamps_unit_(coerce_timestamps_unit),
        truncated_timestamps_allowed_(truncated_timestamps_allowed),
        store_schema_(store_schema) {}

  const bool write_timestamps_as_int96_;
  const bool coerce_timestamps_enabled_;
  const ::arrow::TimeUnit::type coerce_timestamps_unit_;
  const bool truncated_timestamps_allowed_;
  const bool store_schema_;
};

/// \brief State object used for writing Arrow data directly to a Parquet
/// column chunk. API possibly not stable
struct ArrowWriteContext {
  ArrowWriteContext(MemoryPool* memory_pool, ArrowWriterProperties* properties)
      : memory_pool(memory_pool),
        properties(properties),
        data_buffer(AllocateBuffer(memory_pool)),
        def_levels_buffer(AllocateBuffer(memory_pool)) {}

  template <typename T>
  ::arrow::Status GetScratchData(const int64_t num_values, T** out) {
    ARROW_RETURN_NOT_OK(this->data_buffer->Resize(num_values * sizeof(T), false));
    *out = reinterpret_cast<T*>(this->data_buffer->mutable_data());
    return ::arrow::Status::OK();
  }

  MemoryPool* memory_pool;
  const ArrowWriterProperties* properties;

  // Buffer used for storing the data of an array converted to the physical type
  // as expected by parquet-cpp.
  std::shared_ptr<ResizableBuffer> data_buffer;

  // We use the shared ownership of this buffer
  std::shared_ptr<ResizableBuffer> def_levels_buffer;
};

PARQUET_EXPORT
std::shared_ptr<ArrowWriterProperties> default_arrow_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H

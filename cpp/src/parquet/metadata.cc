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

#include "parquet/metadata.h"

#include <algorithm>
#include <cinttypes>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/io/memory.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/schema_internal.h"
#include "parquet/statistics.h"
#include "parquet/thrift_internal.h"

namespace parquet {

const ApplicationVersion& ApplicationVersion::PARQUET_251_FIXED_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 8, 0);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_816_FIXED_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 2, 9);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_CPP_FIXED_STATS_VERSION() {
  static ApplicationVersion version("parquet-cpp", 1, 3, 0);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_MR_FIXED_STATS_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 10, 0);
  return version;
}

std::string ParquetVersionToString(ParquetVersion::type ver) {
  switch (ver) {
    case ParquetVersion::PARQUET_1_0:
      return "1.0";
    case ParquetVersion::PARQUET_2_0:
      return "2.0";
  }

  // This should be unreachable
  return "UNKNOWN";
}

template <typename DType>
static std::shared_ptr<Statistics> MakeTypedColumnStats(
    const format::ColumnMetaData& metadata, const ColumnDescriptor* descr) {
  // If ColumnOrder is defined, return max_value and min_value
  if (descr->column_order().get_order() == ColumnOrder::TYPE_DEFINED_ORDER) {
    return MakeStatistics<DType>(
        descr, metadata.statistics.min_value, metadata.statistics.max_value,
        metadata.num_values - metadata.statistics.null_count,
        metadata.statistics.null_count, metadata.statistics.distinct_count,
        metadata.statistics.__isset.max_value || metadata.statistics.__isset.min_value,
        metadata.statistics.__isset.null_count,
        metadata.statistics.__isset.distinct_count);
  }
  // Default behavior
  return MakeStatistics<DType>(
      descr, metadata.statistics.min, metadata.statistics.max,
      metadata.num_values - metadata.statistics.null_count,
      metadata.statistics.null_count, metadata.statistics.distinct_count,
      metadata.statistics.__isset.max || metadata.statistics.__isset.min,
      metadata.statistics.__isset.null_count, metadata.statistics.__isset.distinct_count);
}

std::shared_ptr<Statistics> MakeColumnStats(const format::ColumnMetaData& meta_data,
                                            const ColumnDescriptor* descr) {
  switch (static_cast<Type::type>(meta_data.type)) {
    case Type::BOOLEAN:
      return MakeTypedColumnStats<BooleanType>(meta_data, descr);
    case Type::INT32:
      return MakeTypedColumnStats<Int32Type>(meta_data, descr);
    case Type::INT64:
      return MakeTypedColumnStats<Int64Type>(meta_data, descr);
    case Type::INT96:
      return MakeTypedColumnStats<Int96Type>(meta_data, descr);
    case Type::DOUBLE:
      return MakeTypedColumnStats<DoubleType>(meta_data, descr);
    case Type::FLOAT:
      return MakeTypedColumnStats<FloatType>(meta_data, descr);
    case Type::BYTE_ARRAY:
      return MakeTypedColumnStats<ByteArrayType>(meta_data, descr);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return MakeTypedColumnStats<FLBAType>(meta_data, descr);
    case Type::UNDEFINED:
      break;
  }
  throw ParquetException("Can't decode page statistics for selected column type");
}

// MetaData Accessor

// ColumnCryptoMetaData
class ColumnCryptoMetaData::ColumnCryptoMetaDataImpl {
 public:
  explicit ColumnCryptoMetaDataImpl(const format::ColumnCryptoMetaData* crypto_metadata)
      : crypto_metadata_(crypto_metadata) {}

  bool encrypted_with_footer_key() const {
    return crypto_metadata_->__isset.ENCRYPTION_WITH_FOOTER_KEY;
  }
  bool encrypted_with_column_key() const {
    return crypto_metadata_->__isset.ENCRYPTION_WITH_COLUMN_KEY;
  }
  std::shared_ptr<schema::ColumnPath> path_in_schema() const {
    return std::make_shared<schema::ColumnPath>(
        crypto_metadata_->ENCRYPTION_WITH_COLUMN_KEY.path_in_schema);
  }
  const std::string& key_metadata() const {
    return crypto_metadata_->ENCRYPTION_WITH_COLUMN_KEY.key_metadata;
  }

 private:
  const format::ColumnCryptoMetaData* crypto_metadata_;
};

std::unique_ptr<ColumnCryptoMetaData> ColumnCryptoMetaData::Make(
    const uint8_t* metadata) {
  return std::unique_ptr<ColumnCryptoMetaData>(new ColumnCryptoMetaData(metadata));
}

ColumnCryptoMetaData::ColumnCryptoMetaData(const uint8_t* metadata)
    : impl_(new ColumnCryptoMetaDataImpl(
          reinterpret_cast<const format::ColumnCryptoMetaData*>(metadata))) {}

ColumnCryptoMetaData::~ColumnCryptoMetaData() = default;

std::shared_ptr<schema::ColumnPath> ColumnCryptoMetaData::path_in_schema() const {
  return impl_->path_in_schema();
}
bool ColumnCryptoMetaData::encrypted_with_footer_key() const {
  return impl_->encrypted_with_footer_key();
}
const std::string& ColumnCryptoMetaData::key_metadata() const {
  return impl_->key_metadata();
}

// ColumnChunk metadata
class ColumnChunkMetaData::ColumnChunkMetaDataImpl {
 public:
  explicit ColumnChunkMetaDataImpl(const format::ColumnChunk* column,
                                   const ColumnDescriptor* descr,
                                   int16_t row_group_ordinal, int16_t column_ordinal,
                                   const ApplicationVersion* writer_version,
                                   std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : column_(column), descr_(descr), writer_version_(writer_version) {
    column_metadata_ = &column->meta_data;
    if (column->__isset.crypto_metadata) {  // column metadata is encrypted
      format::ColumnCryptoMetaData ccmd = column->crypto_metadata;

      if (ccmd.__isset.ENCRYPTION_WITH_COLUMN_KEY) {
        if (file_decryptor != nullptr && file_decryptor->properties() != nullptr) {
          // should decrypt metadata
          std::shared_ptr<schema::ColumnPath> path = std::make_shared<schema::ColumnPath>(
              ccmd.ENCRYPTION_WITH_COLUMN_KEY.path_in_schema);
          std::string key_metadata = ccmd.ENCRYPTION_WITH_COLUMN_KEY.key_metadata;

          std::string aad_column_metadata = encryption::CreateModuleAad(
              file_decryptor->file_aad(), encryption::kColumnMetaData, row_group_ordinal,
              column_ordinal, static_cast<int16_t>(-1));
          auto decryptor = file_decryptor->GetColumnMetaDecryptor(
              path->ToDotString(), key_metadata, aad_column_metadata);
          auto len = static_cast<uint32_t>(column->encrypted_column_metadata.size());
          DeserializeThriftMsg(
              reinterpret_cast<const uint8_t*>(column->encrypted_column_metadata.c_str()),
              &len, &decrypted_metadata_, decryptor);
          column_metadata_ = &decrypted_metadata_;
        } else {
          throw ParquetException(
              "Cannot decrypt ColumnMetadata."
              " FileDecryption is not setup correctly");
        }
      }
    }
    for (const auto& encoding : column_metadata_->encodings) {
      encodings_.push_back(LoadEnumSafe(&encoding));
    }
    for (const auto& encoding_stats : column_metadata_->encoding_stats) {
      encoding_stats_.push_back({LoadEnumSafe(&encoding_stats.page_type),
                                 LoadEnumSafe(&encoding_stats.encoding),
                                 encoding_stats.count});
    }
    possible_stats_ = nullptr;
  }

  bool Equals(const ColumnChunkMetaDataImpl& other) const {
    return *column_metadata_ == *other.column_metadata_;
  }

  // column chunk
  inline int64_t file_offset() const { return column_->file_offset; }
  inline const std::string& file_path() const { return column_->file_path; }

  inline Type::type type() const { return LoadEnumSafe(&column_metadata_->type); }

  inline int64_t num_values() const { return column_metadata_->num_values; }

  std::shared_ptr<schema::ColumnPath> path_in_schema() {
    return std::make_shared<schema::ColumnPath>(column_metadata_->path_in_schema);
  }

  // Check if statistics are set and are valid
  // 1) Must be set in the metadata
  // 2) Statistics must not be corrupted
  inline bool is_stats_set() const {
    DCHECK(writer_version_ != nullptr);
    // If the column statistics don't exist or column sort order is unknown
    // we cannot use the column stats
    if (!column_metadata_->__isset.statistics ||
        descr_->sort_order() == SortOrder::UNKNOWN) {
      return false;
    }
    if (possible_stats_ == nullptr) {
      possible_stats_ = MakeColumnStats(*column_metadata_, descr_);
    }
    EncodedStatistics encodedStatistics = possible_stats_->Encode();
    return writer_version_->HasCorrectStatistics(type(), encodedStatistics,
                                                 descr_->sort_order());
  }

  inline std::shared_ptr<Statistics> statistics() const {
    return is_stats_set() ? possible_stats_ : nullptr;
  }

  inline Compression::type compression() const {
    return LoadEnumSafe(&column_metadata_->codec);
  }

  const std::vector<Encoding::type>& encodings() const { return encodings_; }

  const std::vector<PageEncodingStats>& encoding_stats() const { return encoding_stats_; }

  inline bool has_dictionary_page() const {
    return column_metadata_->__isset.dictionary_page_offset;
  }

  inline int64_t dictionary_page_offset() const {
    return column_metadata_->dictionary_page_offset;
  }

  inline int64_t data_page_offset() const { return column_metadata_->data_page_offset; }

  inline bool has_index_page() const {
    return column_metadata_->__isset.index_page_offset;
  }

  inline int64_t index_page_offset() const { return column_metadata_->index_page_offset; }

  inline int64_t total_compressed_size() const {
    return column_metadata_->total_compressed_size;
  }

  inline int64_t total_uncompressed_size() const {
    return column_metadata_->total_uncompressed_size;
  }

  inline std::unique_ptr<ColumnCryptoMetaData> crypto_metadata() const {
    if (column_->__isset.crypto_metadata) {
      return ColumnCryptoMetaData::Make(
          reinterpret_cast<const uint8_t*>(&column_->crypto_metadata));
    } else {
      return nullptr;
    }
  }

 private:
  mutable std::shared_ptr<Statistics> possible_stats_;
  std::vector<Encoding::type> encodings_;
  std::vector<PageEncodingStats> encoding_stats_;
  const format::ColumnChunk* column_;
  const format::ColumnMetaData* column_metadata_;
  format::ColumnMetaData decrypted_metadata_;
  const ColumnDescriptor* descr_;
  const ApplicationVersion* writer_version_;
};

std::unique_ptr<ColumnChunkMetaData> ColumnChunkMetaData::Make(
    const void* metadata, const ColumnDescriptor* descr,
    const ApplicationVersion* writer_version, int16_t row_group_ordinal,
    int16_t column_ordinal, std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  return std::unique_ptr<ColumnChunkMetaData>(
      new ColumnChunkMetaData(metadata, descr, row_group_ordinal, column_ordinal,
                              writer_version, std::move(file_decryptor)));
}

ColumnChunkMetaData::ColumnChunkMetaData(
    const void* metadata, const ColumnDescriptor* descr, int16_t row_group_ordinal,
    int16_t column_ordinal, const ApplicationVersion* writer_version,
    std::shared_ptr<InternalFileDecryptor> file_decryptor)
    : impl_{new ColumnChunkMetaDataImpl(
          reinterpret_cast<const format::ColumnChunk*>(metadata), descr,
          row_group_ordinal, column_ordinal, writer_version, std::move(file_decryptor))} {
}

ColumnChunkMetaData::~ColumnChunkMetaData() = default;

// column chunk
int64_t ColumnChunkMetaData::file_offset() const { return impl_->file_offset(); }

const std::string& ColumnChunkMetaData::file_path() const { return impl_->file_path(); }

Type::type ColumnChunkMetaData::type() const { return impl_->type(); }

int64_t ColumnChunkMetaData::num_values() const { return impl_->num_values(); }

std::shared_ptr<schema::ColumnPath> ColumnChunkMetaData::path_in_schema() const {
  return impl_->path_in_schema();
}

std::shared_ptr<Statistics> ColumnChunkMetaData::statistics() const {
  return impl_->statistics();
}

bool ColumnChunkMetaData::is_stats_set() const { return impl_->is_stats_set(); }

bool ColumnChunkMetaData::has_dictionary_page() const {
  return impl_->has_dictionary_page();
}

int64_t ColumnChunkMetaData::dictionary_page_offset() const {
  return impl_->dictionary_page_offset();
}

int64_t ColumnChunkMetaData::data_page_offset() const {
  return impl_->data_page_offset();
}

bool ColumnChunkMetaData::has_index_page() const { return impl_->has_index_page(); }

int64_t ColumnChunkMetaData::index_page_offset() const {
  return impl_->index_page_offset();
}

Compression::type ColumnChunkMetaData::compression() const {
  return impl_->compression();
}

bool ColumnChunkMetaData::can_decompress() const {
  return ::arrow::util::Codec::IsAvailable(compression());
}

const std::vector<Encoding::type>& ColumnChunkMetaData::encodings() const {
  return impl_->encodings();
}

const std::vector<PageEncodingStats>& ColumnChunkMetaData::encoding_stats() const {
  return impl_->encoding_stats();
}

int64_t ColumnChunkMetaData::total_uncompressed_size() const {
  return impl_->total_uncompressed_size();
}

int64_t ColumnChunkMetaData::total_compressed_size() const {
  return impl_->total_compressed_size();
}

std::unique_ptr<ColumnCryptoMetaData> ColumnChunkMetaData::crypto_metadata() const {
  return impl_->crypto_metadata();
}

bool ColumnChunkMetaData::Equals(const ColumnChunkMetaData& other) const {
  return impl_->Equals(*other.impl_);
}

// row-group metadata
class RowGroupMetaData::RowGroupMetaDataImpl {
 public:
  explicit RowGroupMetaDataImpl(const format::RowGroup* row_group,
                                const SchemaDescriptor* schema,
                                const ApplicationVersion* writer_version,
                                std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : row_group_(row_group),
        schema_(schema),
        writer_version_(writer_version),
        file_decryptor_(std::move(file_decryptor)) {}

  bool Equals(const RowGroupMetaDataImpl& other) const {
    return *row_group_ == *other.row_group_;
  }

  inline int num_columns() const { return static_cast<int>(row_group_->columns.size()); }

  inline int64_t num_rows() const { return row_group_->num_rows; }

  inline int64_t total_byte_size() const { return row_group_->total_byte_size; }

  inline int64_t total_compressed_size() const {
    return row_group_->total_compressed_size;
  }

  inline int64_t file_offset() const { return row_group_->file_offset; }

  inline const SchemaDescriptor* schema() const { return schema_; }

  std::unique_ptr<ColumnChunkMetaData> ColumnChunk(int i) {
    if (i < num_columns()) {
      return ColumnChunkMetaData::Make(&row_group_->columns[i], schema_->Column(i),
                                       writer_version_, row_group_->ordinal,
                                       static_cast<int16_t>(i), file_decryptor_);
    }
    throw ParquetException("The file only has ", num_columns(),
                           " columns, requested metadata for column: ", i);
  }

 private:
  const format::RowGroup* row_group_;
  const SchemaDescriptor* schema_;
  const ApplicationVersion* writer_version_;
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;
};

std::unique_ptr<RowGroupMetaData> RowGroupMetaData::Make(
    const void* metadata, const SchemaDescriptor* schema,
    const ApplicationVersion* writer_version,
    std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  return std::unique_ptr<RowGroupMetaData>(
      new RowGroupMetaData(metadata, schema, writer_version, std::move(file_decryptor)));
}

RowGroupMetaData::RowGroupMetaData(const void* metadata, const SchemaDescriptor* schema,
                                   const ApplicationVersion* writer_version,
                                   std::shared_ptr<InternalFileDecryptor> file_decryptor)
    : impl_{new RowGroupMetaDataImpl(reinterpret_cast<const format::RowGroup*>(metadata),
                                     schema, writer_version, std::move(file_decryptor))} {
}

RowGroupMetaData::~RowGroupMetaData() = default;

bool RowGroupMetaData::Equals(const RowGroupMetaData& other) const {
  return impl_->Equals(*other.impl_);
}

int RowGroupMetaData::num_columns() const { return impl_->num_columns(); }

int64_t RowGroupMetaData::num_rows() const { return impl_->num_rows(); }

int64_t RowGroupMetaData::total_byte_size() const { return impl_->total_byte_size(); }

int64_t RowGroupMetaData::total_compressed_size() const {
  return impl_->total_compressed_size();
}

int64_t RowGroupMetaData::file_offset() const { return impl_->file_offset(); }

const SchemaDescriptor* RowGroupMetaData::schema() const { return impl_->schema(); }

std::unique_ptr<ColumnChunkMetaData> RowGroupMetaData::ColumnChunk(int i) const {
  return impl_->ColumnChunk(i);
}

bool RowGroupMetaData::can_decompress() const {
  int n_columns = num_columns();
  for (int i = 0; i < n_columns; i++) {
    if (!ColumnChunk(i)->can_decompress()) {
      return false;
    }
  }
  return true;
}

// file metadata
class FileMetaData::FileMetaDataImpl {
 public:
  FileMetaDataImpl() = default;

  explicit FileMetaDataImpl(
      const void* metadata, uint32_t* metadata_len,
      std::shared_ptr<InternalFileDecryptor> file_decryptor = nullptr)
      : file_decryptor_(file_decryptor) {
    metadata_.reset(new format::FileMetaData);

    auto footer_decryptor =
        file_decryptor_ != nullptr ? file_decryptor->GetFooterDecryptor() : nullptr;

    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(metadata), metadata_len,
                         metadata_.get(), footer_decryptor);
    metadata_len_ = *metadata_len;

    if (metadata_->__isset.created_by) {
      writer_version_ = ApplicationVersion(metadata_->created_by);
    } else {
      writer_version_ = ApplicationVersion("unknown 0.0.0");
    }

    InitSchema();
    InitColumnOrders();
    InitKeyValueMetadata();
  }

  bool VerifySignature(const void* signature) {
    // verify decryption properties are set
    if (file_decryptor_ == nullptr) {
      throw ParquetException("Decryption not set properly. cannot verify signature");
    }
    // serialize the footer
    uint8_t* serialized_data;
    uint32_t serialized_len = metadata_len_;
    ThriftSerializer serializer;
    serializer.SerializeToBuffer(metadata_.get(), &serialized_len, &serialized_data);

    // encrypt with nonce
    auto nonce = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(signature));
    auto tag = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(signature)) +
               encryption::kNonceLength;

    std::string key = file_decryptor_->GetFooterKey();
    std::string aad = encryption::CreateFooterAad(file_decryptor_->file_aad());

    auto aes_encryptor = encryption::AesEncryptor::Make(
        file_decryptor_->algorithm(), static_cast<int>(key.size()), true, nullptr);

    std::shared_ptr<Buffer> encrypted_buffer = std::static_pointer_cast<ResizableBuffer>(
        AllocateBuffer(file_decryptor_->pool(),
                       aes_encryptor->CiphertextSizeDelta() + serialized_len));
    uint32_t encrypted_len = aes_encryptor->SignedFooterEncrypt(
        serialized_data, serialized_len, str2bytes(key), static_cast<int>(key.size()),
        str2bytes(aad), static_cast<int>(aad.size()), nonce,
        encrypted_buffer->mutable_data());
    // Delete AES encryptor object. It was created only to verify the footer signature.
    aes_encryptor->WipeOut();
    delete aes_encryptor;
    return 0 ==
           memcmp(encrypted_buffer->data() + encrypted_len - encryption::kGcmTagLength,
                  tag, encryption::kGcmTagLength);
  }

  inline uint32_t size() const { return metadata_len_; }
  inline int num_columns() const { return schema_.num_columns(); }
  inline int64_t num_rows() const { return metadata_->num_rows; }
  inline int num_row_groups() const {
    return static_cast<int>(metadata_->row_groups.size());
  }
  inline int32_t version() const { return metadata_->version; }
  inline const std::string& created_by() const { return metadata_->created_by; }
  inline int num_schema_elements() const {
    return static_cast<int>(metadata_->schema.size());
  }

  inline bool is_encryption_algorithm_set() const {
    return metadata_->__isset.encryption_algorithm;
  }
  inline EncryptionAlgorithm encryption_algorithm() {
    return FromThrift(metadata_->encryption_algorithm);
  }
  inline const std::string& footer_signing_key_metadata() {
    return metadata_->footer_signing_key_metadata;
  }

  const ApplicationVersion& writer_version() const { return writer_version_; }

  void WriteTo(::arrow::io::OutputStream* dst,
               const std::shared_ptr<Encryptor>& encryptor) const {
    ThriftSerializer serializer;
    // Only in encrypted files with plaintext footers the
    // encryption_algorithm is set in footer
    if (is_encryption_algorithm_set()) {
      uint8_t* serialized_data;
      uint32_t serialized_len;
      serializer.SerializeToBuffer(metadata_.get(), &serialized_len, &serialized_data);

      // encrypt the footer key
      std::vector<uint8_t> encrypted_data(encryptor->CiphertextSizeDelta() +
                                          serialized_len);
      unsigned encrypted_len =
          encryptor->Encrypt(serialized_data, serialized_len, encrypted_data.data());

      // write unencrypted footer
      PARQUET_THROW_NOT_OK(dst->Write(serialized_data, serialized_len));
      // Write signature (nonce and tag)
      PARQUET_THROW_NOT_OK(
          dst->Write(encrypted_data.data() + 4, encryption::kNonceLength));
      PARQUET_THROW_NOT_OK(
          dst->Write(encrypted_data.data() + encrypted_len - encryption::kGcmTagLength,
                     encryption::kGcmTagLength));
    } else {  // either plaintext file (when encryptor is null)
      // or encrypted file with encrypted footer
      serializer.Serialize(metadata_.get(), dst, encryptor);
    }
  }

  std::unique_ptr<RowGroupMetaData> RowGroup(int i) {
    if (!(i < num_row_groups())) {
      std::stringstream ss;
      ss << "The file only has " << num_row_groups()
         << " row groups, requested metadata for row group: " << i;
      throw ParquetException(ss.str());
    }
    return RowGroupMetaData::Make(&metadata_->row_groups[i], &schema_, &writer_version_,
                                  file_decryptor_);
  }

  bool Equals(const FileMetaDataImpl& other) const {
    return *metadata_ == *other.metadata_;
  }

  const SchemaDescriptor* schema() const { return &schema_; }

  const std::shared_ptr<const KeyValueMetadata>& key_value_metadata() const {
    return key_value_metadata_;
  }

  void set_file_path(const std::string& path) {
    for (format::RowGroup& row_group : metadata_->row_groups) {
      for (format::ColumnChunk& chunk : row_group.columns) {
        chunk.__set_file_path(path);
      }
    }
  }

  format::RowGroup& row_group(int i) {
    DCHECK_LT(i, num_row_groups());
    return metadata_->row_groups[i];
  }

  void AppendRowGroups(const std::unique_ptr<FileMetaDataImpl>& other) {
    if (!schema()->Equals(*other->schema())) {
      throw ParquetException("AppendRowGroups requires equal schemas.");
    }

    format::RowGroup other_rg;
    for (int i = 0; i < other->num_row_groups(); i++) {
      other_rg = other->row_group(i);
      metadata_->row_groups.push_back(other_rg);
      metadata_->num_rows += other_rg.num_rows;
    }
  }

  std::shared_ptr<FileMetaData> Subset(const std::vector<int>& row_groups) {
    for (int i : row_groups) {
      if (i < num_row_groups()) continue;

      throw ParquetException(
          "The file only has ", num_row_groups(),
          " row groups, but requested a subset including row group: ", i);
    }

    std::shared_ptr<FileMetaData> out(new FileMetaData());
    out->impl_.reset(new FileMetaDataImpl());
    out->impl_->metadata_.reset(new format::FileMetaData());

    auto metadata = out->impl_->metadata_.get();
    metadata->version = metadata_->version;
    metadata->schema = metadata_->schema;

    metadata->row_groups.resize(row_groups.size());
    int i = 0;
    for (int selected_index : row_groups) {
      metadata->num_rows += row_group(selected_index).num_rows;
      metadata->row_groups[i++] = row_group(selected_index);
    }

    metadata->key_value_metadata = metadata_->key_value_metadata;
    metadata->created_by = metadata_->created_by;
    metadata->column_orders = metadata_->column_orders;
    metadata->encryption_algorithm = metadata_->encryption_algorithm;
    metadata->footer_signing_key_metadata = metadata_->footer_signing_key_metadata;
    metadata->__isset = metadata_->__isset;

    out->impl_->schema_ = schema_;
    out->impl_->writer_version_ = writer_version_;
    out->impl_->key_value_metadata_ = key_value_metadata_;
    out->impl_->file_decryptor_ = file_decryptor_;

    return out;
  }

  void set_file_decryptor(std::shared_ptr<InternalFileDecryptor> file_decryptor) {
    file_decryptor_ = file_decryptor;
  }

 private:
  friend FileMetaDataBuilder;
  uint32_t metadata_len_ = 0;
  std::unique_ptr<format::FileMetaData> metadata_;
  SchemaDescriptor schema_;
  ApplicationVersion writer_version_;
  std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
  std::shared_ptr<InternalFileDecryptor> file_decryptor_;

  void InitSchema() {
    if (metadata_->schema.empty()) {
      throw ParquetException("Empty file schema (no root)");
    }
    schema_.Init(schema::Unflatten(&metadata_->schema[0],
                                   static_cast<int>(metadata_->schema.size())));
  }

  void InitColumnOrders() {
    // update ColumnOrder
    std::vector<parquet::ColumnOrder> column_orders;
    if (metadata_->__isset.column_orders) {
      for (auto column_order : metadata_->column_orders) {
        if (column_order.__isset.TYPE_ORDER) {
          column_orders.push_back(ColumnOrder::type_defined_);
        } else {
          column_orders.push_back(ColumnOrder::undefined_);
        }
      }
    } else {
      column_orders.resize(schema_.num_columns(), ColumnOrder::undefined_);
    }

    schema_.updateColumnOrders(column_orders);
  }

  void InitKeyValueMetadata() {
    std::shared_ptr<KeyValueMetadata> metadata = nullptr;
    if (metadata_->__isset.key_value_metadata) {
      metadata = std::make_shared<KeyValueMetadata>();
      for (const auto& it : metadata_->key_value_metadata) {
        metadata->Append(it.key, it.value);
      }
    }
    key_value_metadata_ = std::move(metadata);
  }
};

std::shared_ptr<FileMetaData> FileMetaData::Make(
    const void* metadata, uint32_t* metadata_len,
    std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  // This FileMetaData ctor is private, not compatible with std::make_shared
  return std::shared_ptr<FileMetaData>(
      new FileMetaData(metadata, metadata_len, file_decryptor));
}

FileMetaData::FileMetaData(const void* metadata, uint32_t* metadata_len,
                           std::shared_ptr<InternalFileDecryptor> file_decryptor)
    : impl_{std::unique_ptr<FileMetaDataImpl>(
          new FileMetaDataImpl(metadata, metadata_len, file_decryptor))} {}

FileMetaData::FileMetaData()
    : impl_{std::unique_ptr<FileMetaDataImpl>(new FileMetaDataImpl())} {}

FileMetaData::~FileMetaData() = default;

bool FileMetaData::Equals(const FileMetaData& other) const {
  return impl_->Equals(*other.impl_);
}

std::unique_ptr<RowGroupMetaData> FileMetaData::RowGroup(int i) const {
  return impl_->RowGroup(i);
}

bool FileMetaData::VerifySignature(const void* signature) {
  return impl_->VerifySignature(signature);
}

uint32_t FileMetaData::size() const { return impl_->size(); }

int FileMetaData::num_columns() const { return impl_->num_columns(); }

int64_t FileMetaData::num_rows() const { return impl_->num_rows(); }

int FileMetaData::num_row_groups() const { return impl_->num_row_groups(); }

bool FileMetaData::can_decompress() const {
  int n_row_groups = num_row_groups();
  for (int i = 0; i < n_row_groups; i++) {
    if (!RowGroup(i)->can_decompress()) {
      return false;
    }
  }
  return true;
}

bool FileMetaData::is_encryption_algorithm_set() const {
  return impl_->is_encryption_algorithm_set();
}

EncryptionAlgorithm FileMetaData::encryption_algorithm() const {
  return impl_->encryption_algorithm();
}

const std::string& FileMetaData::footer_signing_key_metadata() const {
  return impl_->footer_signing_key_metadata();
}

void FileMetaData::set_file_decryptor(
    std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  impl_->set_file_decryptor(file_decryptor);
}

ParquetVersion::type FileMetaData::version() const {
  switch (impl_->version()) {
    case 1:
      return ParquetVersion::PARQUET_1_0;
    case 2:
      return ParquetVersion::PARQUET_2_0;
    default:
      // Improperly set version, assuming Parquet 1.0
      break;
  }
  return ParquetVersion::PARQUET_1_0;
}

const ApplicationVersion& FileMetaData::writer_version() const {
  return impl_->writer_version();
}

const std::string& FileMetaData::created_by() const { return impl_->created_by(); }

int FileMetaData::num_schema_elements() const { return impl_->num_schema_elements(); }

const SchemaDescriptor* FileMetaData::schema() const { return impl_->schema(); }

const std::shared_ptr<const KeyValueMetadata>& FileMetaData::key_value_metadata() const {
  return impl_->key_value_metadata();
}

void FileMetaData::set_file_path(const std::string& path) { impl_->set_file_path(path); }

void FileMetaData::AppendRowGroups(const FileMetaData& other) {
  impl_->AppendRowGroups(other.impl_);
}

std::shared_ptr<FileMetaData> FileMetaData::Subset(
    const std::vector<int>& row_groups) const {
  return impl_->Subset(row_groups);
}

void FileMetaData::WriteTo(::arrow::io::OutputStream* dst,
                           const std::shared_ptr<Encryptor>& encryptor) const {
  return impl_->WriteTo(dst, encryptor);
}

class FileCryptoMetaData::FileCryptoMetaDataImpl {
 public:
  FileCryptoMetaDataImpl() = default;

  explicit FileCryptoMetaDataImpl(const uint8_t* metadata, uint32_t* metadata_len) {
    metadata_.reset(new format::FileCryptoMetaData);
    DeserializeThriftMsg(metadata, metadata_len, metadata_.get());
    metadata_len_ = *metadata_len;
  }

  EncryptionAlgorithm encryption_algorithm() {
    return FromThrift(metadata_->encryption_algorithm);
  }
  const std::string& key_metadata() { return metadata_->key_metadata; }
  void WriteTo(::arrow::io::OutputStream* dst) const {
    ThriftSerializer serializer;
    serializer.Serialize(metadata_.get(), dst);
  }

 private:
  friend FileMetaDataBuilder;
  std::unique_ptr<format::FileCryptoMetaData> metadata_;
  uint32_t metadata_len_;
};

EncryptionAlgorithm FileCryptoMetaData::encryption_algorithm() const {
  return impl_->encryption_algorithm();
}

const std::string& FileCryptoMetaData::key_metadata() const {
  return impl_->key_metadata();
}

std::shared_ptr<FileCryptoMetaData> FileCryptoMetaData::Make(
    const uint8_t* serialized_metadata, uint32_t* metadata_len) {
  return std::shared_ptr<FileCryptoMetaData>(
      new FileCryptoMetaData(serialized_metadata, metadata_len));
}

FileCryptoMetaData::FileCryptoMetaData(const uint8_t* serialized_metadata,
                                       uint32_t* metadata_len)
    : impl_(new FileCryptoMetaDataImpl(serialized_metadata, metadata_len)) {}

FileCryptoMetaData::FileCryptoMetaData() : impl_(new FileCryptoMetaDataImpl()) {}

FileCryptoMetaData::~FileCryptoMetaData() = default;

void FileCryptoMetaData::WriteTo(::arrow::io::OutputStream* dst) const {
  impl_->WriteTo(dst);
}

std::string FileMetaData::SerializeToString() const {
  // We need to pass in an initial size. Since it will automatically
  // increase the buffer size to hold the metadata, we just leave it 0.
  PARQUET_ASSIGN_OR_THROW(auto serializer, ::arrow::io::BufferOutputStream::Create(0));
  WriteTo(serializer.get());
  PARQUET_ASSIGN_OR_THROW(auto metadata_buffer, serializer->Finish());
  return metadata_buffer->ToString();
}

ApplicationVersion::ApplicationVersion(std::string application, int major, int minor,
                                       int patch)
    : application_(std::move(application)), version{major, minor, patch, "", "", ""} {}

namespace {
// Parse the application version format and set parsed values to
// ApplicationVersion.
//
// The application version format must be compatible parquet-mr's
// one. See also:
//   * https://github.com/apache/parquet-mr/blob/master/parquet-common/src/main/java/org/apache/parquet/VersionParser.java
//   * https://github.com/apache/parquet-mr/blob/master/parquet-common/src/main/java/org/apache/parquet/SemanticVersion.java
//
// The application version format:
//   "${APPLICATION_NAME}"
//   "${APPLICATION_NAME} version ${VERSION}"
//   "${APPLICATION_NAME} version ${VERSION} (build ${BUILD_NAME})"
//
// Eg:
//   parquet-cpp
//   parquet-cpp version 1.5.0ab-xyz5.5.0+cd
//   parquet-cpp version 1.5.0ab-xyz5.5.0+cd (build abcd)
//
// The VERSION format:
//   "${MAJOR}"
//   "${MAJOR}.${MINOR}"
//   "${MAJOR}.${MINOR}.${PATCH}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}-${PRE_RELEASE}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}-${PRE_RELEASE}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}${UNKNOWN}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}-${PRE_RELEASE}"
//   "${MAJOR}.${MINOR}.${PATCH}-${PRE_RELEASE}+${BUILD_INFO}"
//   "${MAJOR}.${MINOR}.${PATCH}+${BUILD_INFO}"
//
// Eg:
//   1
//   1.5
//   1.5.0
//   1.5.0ab
//   1.5.0ab-cdh5.5.0
//   1.5.0ab-cdh5.5.0+cd
//   1.5.0ab+cd
//   1.5.0-cdh5.5.0
//   1.5.0-cdh5.5.0+cd
//   1.5.0+cd
class ApplicationVersionParser {
 public:
  ApplicationVersionParser(const std::string& created_by,
                           ApplicationVersion& application_version)
      : created_by_(created_by),
        application_version_(application_version),
        spaces_(" \t\v\r\n\f"),
        digits_("0123456789") {}

  void Parse() {
    application_version_.application_ = "unknown";
    application_version_.version = {0, 0, 0, "", "", ""};

    if (!ParseApplicationName()) {
      return;
    }
    if (!ParseVersion()) {
      return;
    }
    if (!ParseBuildName()) {
      return;
    }
  }

 private:
  bool IsSpace(const std::string& string, const size_t& offset) {
    auto target = ::arrow::util::string_view(string).substr(offset, 1);
    return target.find_first_of(spaces_) != ::arrow::util::string_view::npos;
  }

  void RemovePrecedingSpaces(const std::string& string, size_t& start,
                             const size_t& end) {
    while (start < end && IsSpace(string, start)) {
      ++start;
    }
  }

  void RemoveTrailingSpaces(const std::string& string, const size_t& start, size_t& end) {
    while (start < (end - 1) && (end - 1) < string.size() && IsSpace(string, end - 1)) {
      --end;
    }
  }

  bool ParseApplicationName() {
    std::string version_mark(" version ");
    auto version_mark_position = created_by_.find(version_mark);
    size_t application_name_end;
    // No VERSION and BUILD_NAME.
    if (version_mark_position == std::string::npos) {
      version_start_ = std::string::npos;
      application_name_end = created_by_.size();
    } else {
      version_start_ = version_mark_position + version_mark.size();
      application_name_end = version_mark_position;
    }

    size_t application_name_start = 0;
    RemovePrecedingSpaces(created_by_, application_name_start, application_name_end);
    RemoveTrailingSpaces(created_by_, application_name_start, application_name_end);
    application_version_.application_ = created_by_.substr(
        application_name_start, application_name_end - application_name_start);

    return true;
  }

  bool ParseVersion() {
    // No VERSION.
    if (version_start_ == std::string::npos) {
      return false;
    }

    RemovePrecedingSpaces(created_by_, version_start_, created_by_.size());
    version_end_ = created_by_.find(" (", version_start_);
    // No BUILD_NAME.
    if (version_end_ == std::string::npos) {
      version_end_ = created_by_.size();
    }
    RemoveTrailingSpaces(created_by_, version_start_, version_end_);
    // No VERSION.
    if (version_start_ == version_end_) {
      return false;
    }
    version_string_ = created_by_.substr(version_start_, version_end_ - version_start_);

    if (!ParseVersionMajor()) {
      return false;
    }
    if (!ParseVersionMinor()) {
      return false;
    }
    if (!ParseVersionPatch()) {
      return false;
    }
    if (!ParseVersionUnknown()) {
      return false;
    }
    if (!ParseVersionPreRelease()) {
      return false;
    }
    if (!ParseVersionBuildInfo()) {
      return false;
    }

    return true;
  }

  bool ParseVersionMajor() {
    size_t version_major_start = 0;
    auto version_major_end = version_string_.find_first_not_of(digits_);
    // MAJOR only.
    if (version_major_end == std::string::npos) {
      version_major_end = version_string_.size();
      version_parsing_position_ = version_major_end;
    } else {
      // No ".".
      if (version_string_[version_major_end] != '.') {
        return false;
      }
      // No MAJOR.
      if (version_major_end == version_major_start) {
        return false;
      }
      version_parsing_position_ = version_major_end + 1;  // +1 is for '.'.
    }
    auto version_major_string = version_string_.substr(
        version_major_start, version_major_end - version_major_start);
    application_version_.version.major = atoi(version_major_string.c_str());
    return true;
  }

  bool ParseVersionMinor() {
    auto version_minor_start = version_parsing_position_;
    auto version_minor_end =
        version_string_.find_first_not_of(digits_, version_minor_start);
    // MAJOR.MINOR only.
    if (version_minor_end == std::string::npos) {
      version_minor_end = version_string_.size();
      version_parsing_position_ = version_minor_end;
    } else {
      // No ".".
      if (version_string_[version_minor_end] != '.') {
        return false;
      }
      // No MINOR.
      if (version_minor_end == version_minor_start) {
        return false;
      }
      version_parsing_position_ = version_minor_end + 1;  // +1 is for '.'.
    }
    auto version_minor_string = version_string_.substr(
        version_minor_start, version_minor_end - version_minor_start);
    application_version_.version.minor = atoi(version_minor_string.c_str());
    return true;
  }

  bool ParseVersionPatch() {
    auto version_patch_start = version_parsing_position_;
    auto version_patch_end =
        version_string_.find_first_not_of(digits_, version_patch_start);
    // No UNKNOWN, PRE_RELEASE and BUILD_INFO.
    if (version_patch_end == std::string::npos) {
      version_patch_end = version_string_.size();
    }
    // No PATCH.
    if (version_patch_end == version_patch_start) {
      return false;
    }
    auto version_patch_string = version_string_.substr(
        version_patch_start, version_patch_end - version_patch_start);
    application_version_.version.patch = atoi(version_patch_string.c_str());
    version_parsing_position_ = version_patch_end;
    return true;
  }

  bool ParseVersionUnknown() {
    // No UNKNOWN.
    if (version_parsing_position_ == version_string_.size()) {
      return true;
    }
    auto version_unknown_start = version_parsing_position_;
    auto version_unknown_end = version_string_.find_first_of("-+", version_unknown_start);
    // No PRE_RELEASE and BUILD_INFO
    if (version_unknown_end == std::string::npos) {
      version_unknown_end = version_string_.size();
    }
    application_version_.version.unknown = version_string_.substr(
        version_unknown_start, version_unknown_end - version_unknown_start);
    version_parsing_position_ = version_unknown_end;
    return true;
  }

  bool ParseVersionPreRelease() {
    // No PRE_RELEASE.
    if (version_parsing_position_ == version_string_.size() ||
        version_string_[version_parsing_position_] != '-') {
      return true;
    }

    auto version_pre_release_start = version_parsing_position_ + 1;  // +1 is for '-'.
    auto version_pre_release_end =
        version_string_.find_first_of("+", version_pre_release_start);
    // No BUILD_INFO
    if (version_pre_release_end == std::string::npos) {
      version_pre_release_end = version_string_.size();
    }
    application_version_.version.pre_release = version_string_.substr(
        version_pre_release_start, version_pre_release_end - version_pre_release_start);
    version_parsing_position_ = version_pre_release_end;
    return true;
  }

  bool ParseVersionBuildInfo() {
    // No BUILD_INFO.
    if (version_parsing_position_ == version_string_.size() ||
        version_string_[version_parsing_position_] != '+') {
      return true;
    }

    auto version_build_info_start = version_parsing_position_ + 1;  // +1 is for '+'.
    application_version_.version.build_info =
        version_string_.substr(version_build_info_start);
    return true;
  }

  bool ParseBuildName() {
    std::string build_mark(" (build ");
    auto build_mark_position = created_by_.find(build_mark, version_end_);
    // No BUILD_NAME.
    if (build_mark_position == std::string::npos) {
      return false;
    }
    auto build_name_start = build_mark_position + build_mark.size();
    RemovePrecedingSpaces(created_by_, build_name_start, created_by_.size());
    auto build_name_end = created_by_.find_first_of(")", build_name_start);
    // No end ")".
    if (build_name_end == std::string::npos) {
      return false;
    }
    RemoveTrailingSpaces(created_by_, build_name_start, build_name_end);
    application_version_.build_ =
        created_by_.substr(build_name_start, build_name_end - build_name_start);

    return true;
  }

  const std::string& created_by_;
  ApplicationVersion& application_version_;

  // For parsing.
  std::string spaces_;
  std::string digits_;
  size_t version_parsing_position_;
  size_t version_start_;
  size_t version_end_;
  std::string version_string_;
};
}  // namespace

ApplicationVersion::ApplicationVersion(const std::string& created_by) {
  ApplicationVersionParser parser(created_by, *this);
  parser.Parse();
}

bool ApplicationVersion::VersionLt(const ApplicationVersion& other_version) const {
  if (application_ != other_version.application_) return false;

  if (version.major < other_version.version.major) return true;
  if (version.major > other_version.version.major) return false;
  DCHECK_EQ(version.major, other_version.version.major);
  if (version.minor < other_version.version.minor) return true;
  if (version.minor > other_version.version.minor) return false;
  DCHECK_EQ(version.minor, other_version.version.minor);
  return version.patch < other_version.version.patch;
}

bool ApplicationVersion::VersionEq(const ApplicationVersion& other_version) const {
  return application_ == other_version.application_ &&
         version.major == other_version.version.major &&
         version.minor == other_version.version.minor &&
         version.patch == other_version.version.patch;
}

// Reference:
// parquet-mr/parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java
// PARQUET-686 has more discussion on statistics
bool ApplicationVersion::HasCorrectStatistics(Type::type col_type,
                                              EncodedStatistics& statistics,
                                              SortOrder::type sort_order) const {
  // parquet-cpp version 1.3.0 and parquet-mr 1.10.0 onwards stats are computed
  // correctly for all types
  if ((application_ == "parquet-cpp" && VersionLt(PARQUET_CPP_FIXED_STATS_VERSION())) ||
      (application_ == "parquet-mr" && VersionLt(PARQUET_MR_FIXED_STATS_VERSION()))) {
    // Only SIGNED are valid unless max and min are the same
    // (in which case the sort order does not matter)
    bool max_equals_min = statistics.has_min && statistics.has_max
                              ? statistics.min() == statistics.max()
                              : false;
    if (SortOrder::SIGNED != sort_order && !max_equals_min) {
      return false;
    }

    // Statistics of other types are OK
    if (col_type != Type::FIXED_LEN_BYTE_ARRAY && col_type != Type::BYTE_ARRAY) {
      return true;
    }
  }
  // created_by is not populated, which could have been caused by
  // parquet-mr during the same time as PARQUET-251, see PARQUET-297
  if (application_ == "unknown") {
    return true;
  }

  // Unknown sort order has incorrect stats
  if (SortOrder::UNKNOWN == sort_order) {
    return false;
  }

  // PARQUET-251
  if (VersionLt(PARQUET_251_FIXED_VERSION())) {
    return false;
  }

  return true;
}

// MetaData Builders
// row-group metadata
class ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilderImpl {
 public:
  explicit ColumnChunkMetaDataBuilderImpl(std::shared_ptr<WriterProperties> props,
                                          const ColumnDescriptor* column)
      : owned_column_chunk_(new format::ColumnChunk),
        properties_(std::move(props)),
        column_(column) {
    Init(owned_column_chunk_.get());
  }

  explicit ColumnChunkMetaDataBuilderImpl(std::shared_ptr<WriterProperties> props,
                                          const ColumnDescriptor* column,
                                          format::ColumnChunk* column_chunk)
      : properties_(std::move(props)), column_(column) {
    Init(column_chunk);
  }

  const void* contents() const { return column_chunk_; }

  // column chunk
  void set_file_path(const std::string& val) { column_chunk_->__set_file_path(val); }

  // column metadata
  void SetStatistics(const EncodedStatistics& val) {
    column_chunk_->meta_data.__set_statistics(ToThrift(val));
  }

  void Finish(int64_t num_values, int64_t dictionary_page_offset,
              int64_t index_page_offset, int64_t data_page_offset,
              int64_t compressed_size, int64_t uncompressed_size, bool has_dictionary,
              bool dictionary_fallback,
              const std::map<Encoding::type, int32_t>& dict_encoding_stats,
              const std::map<Encoding::type, int32_t>& data_encoding_stats,
              const std::shared_ptr<Encryptor>& encryptor) {
    if (dictionary_page_offset > 0) {
      column_chunk_->meta_data.__set_dictionary_page_offset(dictionary_page_offset);
      column_chunk_->__set_file_offset(dictionary_page_offset + compressed_size);
    } else {
      column_chunk_->__set_file_offset(data_page_offset + compressed_size);
    }
    column_chunk_->__isset.meta_data = true;
    column_chunk_->meta_data.__set_num_values(num_values);
    if (index_page_offset >= 0) {
      column_chunk_->meta_data.__set_index_page_offset(index_page_offset);
    }
    column_chunk_->meta_data.__set_data_page_offset(data_page_offset);
    column_chunk_->meta_data.__set_total_uncompressed_size(uncompressed_size);
    column_chunk_->meta_data.__set_total_compressed_size(compressed_size);

    std::vector<format::Encoding::type> thrift_encodings;
    if (has_dictionary) {
      thrift_encodings.push_back(ToThrift(properties_->dictionary_index_encoding()));
      if (properties_->version() == ParquetVersion::PARQUET_1_0) {
        thrift_encodings.push_back(ToThrift(Encoding::PLAIN));
      } else {
        thrift_encodings.push_back(ToThrift(properties_->dictionary_page_encoding()));
      }
    } else {  // Dictionary not enabled
      thrift_encodings.push_back(ToThrift(properties_->encoding(column_->path())));
    }
    thrift_encodings.push_back(ToThrift(Encoding::RLE));
    // Only PLAIN encoding is supported for fallback in V1
    // TODO(majetideepak): Use user specified encoding for V2
    if (dictionary_fallback) {
      thrift_encodings.push_back(ToThrift(Encoding::PLAIN));
    }
    column_chunk_->meta_data.__set_encodings(thrift_encodings);
    std::vector<format::PageEncodingStats> thrift_encoding_stats;
    // Add dictionary page encoding stats
    for (const auto& entry : dict_encoding_stats) {
      format::PageEncodingStats dict_enc_stat;
      dict_enc_stat.__set_page_type(format::PageType::DICTIONARY_PAGE);
      dict_enc_stat.__set_encoding(ToThrift(entry.first));
      dict_enc_stat.__set_count(entry.second);
      thrift_encoding_stats.push_back(dict_enc_stat);
    }
    // Add data page encoding stats
    for (const auto& entry : data_encoding_stats) {
      format::PageEncodingStats data_enc_stat;
      data_enc_stat.__set_page_type(format::PageType::DATA_PAGE);
      data_enc_stat.__set_encoding(ToThrift(entry.first));
      data_enc_stat.__set_count(entry.second);
      thrift_encoding_stats.push_back(data_enc_stat);
    }
    column_chunk_->meta_data.__set_encoding_stats(thrift_encoding_stats);

    const auto& encrypt_md =
        properties_->column_encryption_properties(column_->path()->ToDotString());
    // column is encrypted
    if (encrypt_md != nullptr && encrypt_md->is_encrypted()) {
      column_chunk_->__isset.crypto_metadata = true;
      format::ColumnCryptoMetaData ccmd;
      if (encrypt_md->is_encrypted_with_footer_key()) {
        // encrypted with footer key
        ccmd.__isset.ENCRYPTION_WITH_FOOTER_KEY = true;
        ccmd.__set_ENCRYPTION_WITH_FOOTER_KEY(format::EncryptionWithFooterKey());
      } else {  // encrypted with column key
        format::EncryptionWithColumnKey eck;
        eck.__set_key_metadata(encrypt_md->key_metadata());
        eck.__set_path_in_schema(column_->path()->ToDotVector());
        ccmd.__isset.ENCRYPTION_WITH_COLUMN_KEY = true;
        ccmd.__set_ENCRYPTION_WITH_COLUMN_KEY(eck);
      }
      column_chunk_->__set_crypto_metadata(ccmd);

      bool encrypted_footer =
          properties_->file_encryption_properties()->encrypted_footer();
      bool encrypt_metadata =
          !encrypted_footer || !encrypt_md->is_encrypted_with_footer_key();
      if (encrypt_metadata) {
        ThriftSerializer serializer;
        // Serialize and encrypt ColumnMetadata separately
        // Thrift-serialize the ColumnMetaData structure,
        // encrypt it with the column key, and write to encrypted_column_metadata
        uint8_t* serialized_data;
        uint32_t serialized_len;

        serializer.SerializeToBuffer(&column_chunk_->meta_data, &serialized_len,
                                     &serialized_data);

        std::vector<uint8_t> encrypted_data(encryptor->CiphertextSizeDelta() +
                                            serialized_len);
        unsigned encrypted_len =
            encryptor->Encrypt(serialized_data, serialized_len, encrypted_data.data());

        const char* temp =
            const_cast<const char*>(reinterpret_cast<char*>(encrypted_data.data()));
        std::string encrypted_column_metadata(temp, encrypted_len);
        column_chunk_->__set_encrypted_column_metadata(encrypted_column_metadata);

        if (encrypted_footer) {
          column_chunk_->__isset.meta_data = false;
        } else {
          // Keep redacted metadata version for old readers
          column_chunk_->__isset.meta_data = true;
          column_chunk_->meta_data.__isset.statistics = false;
          column_chunk_->meta_data.__isset.encoding_stats = false;
        }
      }
    }
  }

  void WriteTo(::arrow::io::OutputStream* sink) {
    ThriftSerializer serializer;
    serializer.Serialize(column_chunk_, sink);
  }

  const ColumnDescriptor* descr() const { return column_; }
  int64_t total_compressed_size() const {
    return column_chunk_->meta_data.total_compressed_size;
  }

 private:
  void Init(format::ColumnChunk* column_chunk) {
    column_chunk_ = column_chunk;

    column_chunk_->meta_data.__set_type(ToThrift(column_->physical_type()));
    column_chunk_->meta_data.__set_path_in_schema(column_->path()->ToDotVector());
    column_chunk_->meta_data.__set_codec(
        ToThrift(properties_->compression(column_->path())));
  }

  format::ColumnChunk* column_chunk_;
  std::unique_ptr<format::ColumnChunk> owned_column_chunk_;
  const std::shared_ptr<WriterProperties> properties_;
  const ColumnDescriptor* column_;
};

std::unique_ptr<ColumnChunkMetaDataBuilder> ColumnChunkMetaDataBuilder::Make(
    std::shared_ptr<WriterProperties> props, const ColumnDescriptor* column,
    void* contents) {
  return std::unique_ptr<ColumnChunkMetaDataBuilder>(
      new ColumnChunkMetaDataBuilder(std::move(props), column, contents));
}

std::unique_ptr<ColumnChunkMetaDataBuilder> ColumnChunkMetaDataBuilder::Make(
    std::shared_ptr<WriterProperties> props, const ColumnDescriptor* column) {
  return std::unique_ptr<ColumnChunkMetaDataBuilder>(
      new ColumnChunkMetaDataBuilder(std::move(props), column));
}

ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilder(
    std::shared_ptr<WriterProperties> props, const ColumnDescriptor* column)
    : impl_{std::unique_ptr<ColumnChunkMetaDataBuilderImpl>(
          new ColumnChunkMetaDataBuilderImpl(std::move(props), column))} {}

ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilder(
    std::shared_ptr<WriterProperties> props, const ColumnDescriptor* column,
    void* contents)
    : impl_{std::unique_ptr<ColumnChunkMetaDataBuilderImpl>(
          new ColumnChunkMetaDataBuilderImpl(
              std::move(props), column,
              reinterpret_cast<format::ColumnChunk*>(contents)))} {}

ColumnChunkMetaDataBuilder::~ColumnChunkMetaDataBuilder() = default;

const void* ColumnChunkMetaDataBuilder::contents() const { return impl_->contents(); }

void ColumnChunkMetaDataBuilder::set_file_path(const std::string& path) {
  impl_->set_file_path(path);
}

void ColumnChunkMetaDataBuilder::Finish(
    int64_t num_values, int64_t dictionary_page_offset, int64_t index_page_offset,
    int64_t data_page_offset, int64_t compressed_size, int64_t uncompressed_size,
    bool has_dictionary, bool dictionary_fallback,
    const std::map<Encoding::type, int32_t>& dict_encoding_stats,
    const std::map<Encoding::type, int32_t>& data_encoding_stats,
    const std::shared_ptr<Encryptor>& encryptor) {
  impl_->Finish(num_values, dictionary_page_offset, index_page_offset, data_page_offset,
                compressed_size, uncompressed_size, has_dictionary, dictionary_fallback,
                dict_encoding_stats, data_encoding_stats, encryptor);
}

void ColumnChunkMetaDataBuilder::WriteTo(::arrow::io::OutputStream* sink) {
  impl_->WriteTo(sink);
}

const ColumnDescriptor* ColumnChunkMetaDataBuilder::descr() const {
  return impl_->descr();
}

void ColumnChunkMetaDataBuilder::SetStatistics(const EncodedStatistics& result) {
  impl_->SetStatistics(result);
}

int64_t ColumnChunkMetaDataBuilder::total_compressed_size() const {
  return impl_->total_compressed_size();
}

class RowGroupMetaDataBuilder::RowGroupMetaDataBuilderImpl {
 public:
  explicit RowGroupMetaDataBuilderImpl(std::shared_ptr<WriterProperties> props,
                                       const SchemaDescriptor* schema, void* contents)
      : properties_(std::move(props)), schema_(schema), next_column_(0) {
    row_group_ = reinterpret_cast<format::RowGroup*>(contents);
    InitializeColumns(schema->num_columns());
  }

  ColumnChunkMetaDataBuilder* NextColumnChunk() {
    if (!(next_column_ < num_columns())) {
      std::stringstream ss;
      ss << "The schema only has " << num_columns()
         << " columns, requested metadata for column: " << next_column_;
      throw ParquetException(ss.str());
    }
    auto column = schema_->Column(next_column_);
    auto column_builder = ColumnChunkMetaDataBuilder::Make(
        properties_, column, &row_group_->columns[next_column_++]);
    auto column_builder_ptr = column_builder.get();
    column_builders_.push_back(std::move(column_builder));
    return column_builder_ptr;
  }

  int current_column() { return next_column_ - 1; }

  void Finish(int64_t total_bytes_written, int16_t row_group_ordinal) {
    if (!(next_column_ == schema_->num_columns())) {
      std::stringstream ss;
      ss << "Only " << next_column_ - 1 << " out of " << schema_->num_columns()
         << " columns are initialized";
      throw ParquetException(ss.str());
    }

    int64_t file_offset = 0;
    int64_t total_compressed_size = 0;
    for (int i = 0; i < schema_->num_columns(); i++) {
      if (!(row_group_->columns[i].file_offset >= 0)) {
        std::stringstream ss;
        ss << "Column " << i << " is not complete.";
        throw ParquetException(ss.str());
      }
      if (i == 0) {
        file_offset = row_group_->columns[0].file_offset;
      }
      // sometimes column metadata is encrypted and not available to read,
      // so we must get total_compressed_size from column builder
      total_compressed_size += column_builders_[i]->total_compressed_size();
    }

    row_group_->__set_file_offset(file_offset);
    row_group_->__set_total_compressed_size(total_compressed_size);
    row_group_->__set_total_byte_size(total_bytes_written);
    row_group_->__set_ordinal(row_group_ordinal);
  }

  void set_num_rows(int64_t num_rows) { row_group_->num_rows = num_rows; }

  int num_columns() { return static_cast<int>(row_group_->columns.size()); }

  int64_t num_rows() { return row_group_->num_rows; }

 private:
  void InitializeColumns(int ncols) { row_group_->columns.resize(ncols); }

  format::RowGroup* row_group_;
  const std::shared_ptr<WriterProperties> properties_;
  const SchemaDescriptor* schema_;
  std::vector<std::unique_ptr<ColumnChunkMetaDataBuilder>> column_builders_;
  int next_column_;
};

std::unique_ptr<RowGroupMetaDataBuilder> RowGroupMetaDataBuilder::Make(
    std::shared_ptr<WriterProperties> props, const SchemaDescriptor* schema_,
    void* contents) {
  return std::unique_ptr<RowGroupMetaDataBuilder>(
      new RowGroupMetaDataBuilder(std::move(props), schema_, contents));
}

RowGroupMetaDataBuilder::RowGroupMetaDataBuilder(std::shared_ptr<WriterProperties> props,
                                                 const SchemaDescriptor* schema_,
                                                 void* contents)
    : impl_{new RowGroupMetaDataBuilderImpl(std::move(props), schema_, contents)} {}

RowGroupMetaDataBuilder::~RowGroupMetaDataBuilder() = default;

ColumnChunkMetaDataBuilder* RowGroupMetaDataBuilder::NextColumnChunk() {
  return impl_->NextColumnChunk();
}

int RowGroupMetaDataBuilder::current_column() const { return impl_->current_column(); }

int RowGroupMetaDataBuilder::num_columns() { return impl_->num_columns(); }

int64_t RowGroupMetaDataBuilder::num_rows() { return impl_->num_rows(); }

void RowGroupMetaDataBuilder::set_num_rows(int64_t num_rows) {
  impl_->set_num_rows(num_rows);
}

void RowGroupMetaDataBuilder::Finish(int64_t total_bytes_written,
                                     int16_t row_group_ordinal) {
  impl_->Finish(total_bytes_written, row_group_ordinal);
}

// file metadata
// TODO(PARQUET-595) Support key_value_metadata
class FileMetaDataBuilder::FileMetaDataBuilderImpl {
 public:
  explicit FileMetaDataBuilderImpl(
      const SchemaDescriptor* schema, std::shared_ptr<WriterProperties> props,
      std::shared_ptr<const KeyValueMetadata> key_value_metadata)
      : metadata_(new format::FileMetaData()),
        properties_(std::move(props)),
        schema_(schema),
        key_value_metadata_(std::move(key_value_metadata)) {
    if (properties_->file_encryption_properties() != nullptr &&
        properties_->file_encryption_properties()->encrypted_footer()) {
      crypto_metadata_.reset(new format::FileCryptoMetaData());
    }
  }

  RowGroupMetaDataBuilder* AppendRowGroup() {
    row_groups_.emplace_back();
    current_row_group_builder_ =
        RowGroupMetaDataBuilder::Make(properties_, schema_, &row_groups_.back());
    return current_row_group_builder_.get();
  }

  std::unique_ptr<FileMetaData> Finish() {
    int64_t total_rows = 0;
    for (auto row_group : row_groups_) {
      total_rows += row_group.num_rows;
    }
    metadata_->__set_num_rows(total_rows);
    metadata_->__set_row_groups(row_groups_);

    if (key_value_metadata_) {
      metadata_->key_value_metadata.clear();
      metadata_->key_value_metadata.reserve(key_value_metadata_->size());
      for (int64_t i = 0; i < key_value_metadata_->size(); ++i) {
        format::KeyValue kv_pair;
        kv_pair.__set_key(key_value_metadata_->key(i));
        kv_pair.__set_value(key_value_metadata_->value(i));
        metadata_->key_value_metadata.push_back(kv_pair);
      }
      metadata_->__isset.key_value_metadata = true;
    }

    int32_t file_version = 0;
    switch (properties_->version()) {
      case ParquetVersion::PARQUET_1_0:
        file_version = 1;
        break;
      case ParquetVersion::PARQUET_2_0:
        file_version = 2;
        break;
      default:
        break;
    }
    metadata_->__set_version(file_version);
    metadata_->__set_created_by(properties_->created_by());

    // Users cannot set the `ColumnOrder` since we donot not have user defined sort order
    // in the spec yet.
    // We always default to `TYPE_DEFINED_ORDER`. We can expose it in
    // the API once we have user defined sort orders in the Parquet format.
    // TypeDefinedOrder implies choose SortOrder based on ConvertedType/PhysicalType
    format::TypeDefinedOrder type_defined_order;
    format::ColumnOrder column_order;
    column_order.__set_TYPE_ORDER(type_defined_order);
    column_order.__isset.TYPE_ORDER = true;
    metadata_->column_orders.resize(schema_->num_columns(), column_order);
    metadata_->__isset.column_orders = true;

    // if plaintext footer, set footer signing algorithm
    auto file_encryption_properties = properties_->file_encryption_properties();
    if (file_encryption_properties && !file_encryption_properties->encrypted_footer()) {
      EncryptionAlgorithm signing_algorithm;
      EncryptionAlgorithm algo = file_encryption_properties->algorithm();
      signing_algorithm.aad.aad_file_unique = algo.aad.aad_file_unique;
      signing_algorithm.aad.supply_aad_prefix = algo.aad.supply_aad_prefix;
      if (!algo.aad.supply_aad_prefix) {
        signing_algorithm.aad.aad_prefix = algo.aad.aad_prefix;
      }
      signing_algorithm.algorithm = ParquetCipher::AES_GCM_V1;

      metadata_->__set_encryption_algorithm(ToThrift(signing_algorithm));
      const std::string& footer_signing_key_metadata =
          file_encryption_properties->footer_key_metadata();
      if (footer_signing_key_metadata.size() > 0) {
        metadata_->__set_footer_signing_key_metadata(footer_signing_key_metadata);
      }
    }

    ToParquet(static_cast<parquet::schema::GroupNode*>(schema_->schema_root().get()),
              &metadata_->schema);
    auto file_meta_data = std::unique_ptr<FileMetaData>(new FileMetaData());
    file_meta_data->impl_->metadata_ = std::move(metadata_);
    file_meta_data->impl_->InitSchema();
    file_meta_data->impl_->InitKeyValueMetadata();
    return file_meta_data;
  }

  std::unique_ptr<FileCryptoMetaData> BuildFileCryptoMetaData() {
    if (crypto_metadata_ == nullptr) {
      return nullptr;
    }

    auto file_encryption_properties = properties_->file_encryption_properties();

    crypto_metadata_->__set_encryption_algorithm(
        ToThrift(file_encryption_properties->algorithm()));
    std::string key_metadata = file_encryption_properties->footer_key_metadata();

    if (!key_metadata.empty()) {
      crypto_metadata_->__set_key_metadata(key_metadata);
    }

    std::unique_ptr<FileCryptoMetaData> file_crypto_metadata =
        std::unique_ptr<FileCryptoMetaData>(new FileCryptoMetaData());
    file_crypto_metadata->impl_->metadata_ = std::move(crypto_metadata_);

    return file_crypto_metadata;
  }

 protected:
  std::unique_ptr<format::FileMetaData> metadata_;
  std::unique_ptr<format::FileCryptoMetaData> crypto_metadata_;

 private:
  const std::shared_ptr<WriterProperties> properties_;
  std::vector<format::RowGroup> row_groups_;

  std::unique_ptr<RowGroupMetaDataBuilder> current_row_group_builder_;
  const SchemaDescriptor* schema_;
  std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
};

std::unique_ptr<FileMetaDataBuilder> FileMetaDataBuilder::Make(
    const SchemaDescriptor* schema, std::shared_ptr<WriterProperties> props,
    std::shared_ptr<const KeyValueMetadata> key_value_metadata) {
  return std::unique_ptr<FileMetaDataBuilder>(
      new FileMetaDataBuilder(schema, std::move(props), std::move(key_value_metadata)));
}

FileMetaDataBuilder::FileMetaDataBuilder(
    const SchemaDescriptor* schema, std::shared_ptr<WriterProperties> props,
    std::shared_ptr<const KeyValueMetadata> key_value_metadata)
    : impl_{std::unique_ptr<FileMetaDataBuilderImpl>(new FileMetaDataBuilderImpl(
          schema, std::move(props), std::move(key_value_metadata)))} {}

FileMetaDataBuilder::~FileMetaDataBuilder() = default;

RowGroupMetaDataBuilder* FileMetaDataBuilder::AppendRowGroup() {
  return impl_->AppendRowGroup();
}

std::unique_ptr<FileMetaData> FileMetaDataBuilder::Finish() { return impl_->Finish(); }

std::unique_ptr<FileCryptoMetaData> FileMetaDataBuilder::GetCryptoMetaData() {
  return impl_->BuildFileCryptoMetaData();
}

}  // namespace parquet

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

#pragma once

#include "parquet/windows_compatibility.h"

#include <cstdint>
#include <limits>

#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "arrow/util/logging.h"

#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/encryption/internal_file_encryptor.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

#include "generated/parquet_types.h"  // IYWU pragma: export

namespace parquet {

// ----------------------------------------------------------------------
// Convert Thrift enums to Parquet enums

// Unsafe enum converters (input is not checked for validity)

static inline Type::type FromThriftUnsafe(format::Type::type type) {
  return static_cast<Type::type>(type);
}

static inline ConvertedType::type FromThriftUnsafe(format::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<ConvertedType::type>(static_cast<int>(type) + 1);
}

static inline Repetition::type FromThriftUnsafe(format::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

static inline Encoding::type FromThriftUnsafe(format::Encoding::type type) {
  return static_cast<Encoding::type>(type);
}

static inline PageType::type FromThriftUnsafe(format::PageType::type type) {
  return static_cast<PageType::type>(type);
}

static inline Compression::type FromThriftUnsafe(format::CompressionCodec::type type) {
  switch (type) {
    case format::CompressionCodec::UNCOMPRESSED:
      return Compression::UNCOMPRESSED;
    case format::CompressionCodec::SNAPPY:
      return Compression::SNAPPY;
    case format::CompressionCodec::GZIP:
      return Compression::GZIP;
    case format::CompressionCodec::LZO:
      return Compression::LZO;
    case format::CompressionCodec::BROTLI:
      return Compression::BROTLI;
    case format::CompressionCodec::LZ4:
      return Compression::LZ4_HADOOP;
    case format::CompressionCodec::LZ4_RAW:
      return Compression::LZ4;
    case format::CompressionCodec::ZSTD:
      return Compression::ZSTD;
    default:
      DCHECK(false) << "Cannot reach here";
      return Compression::UNCOMPRESSED;
  }
}

static inline BoundaryOrder::type FromThriftUnsafe(format::BoundaryOrder::type type) {
  return static_cast<BoundaryOrder::type>(type);
}

namespace internal {

template <typename T>
struct ThriftEnumTypeTraits {};

template <>
struct ThriftEnumTypeTraits<::parquet::format::Type::type> {
  using ParquetEnum = Type;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::ConvertedType::type> {
  using ParquetEnum = ConvertedType;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::FieldRepetitionType::type> {
  using ParquetEnum = Repetition;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::Encoding::type> {
  using ParquetEnum = Encoding;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::PageType::type> {
  using ParquetEnum = PageType;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::BoundaryOrder::type> {
  using ParquetEnum = BoundaryOrder;
};

// If the parquet file is corrupted it is possible the enum value decoded
// will not be in the range of defined values, which is undefined behaviour.
// This facility prevents this by loading the value as the underlying type
// and checking to make sure it is in range.

template <typename EnumType,
          typename EnumTypeRaw = typename std::underlying_type<EnumType>::type>
inline static EnumTypeRaw LoadEnumRaw(const EnumType* in) {
  EnumTypeRaw raw_value;
  // Use memcpy(), as a regular cast would be undefined behaviour on invalid values
  memcpy(&raw_value, in, sizeof(EnumType));
  return raw_value;
}

template <typename ApiType>
struct SafeLoader {
  using ApiTypeEnum = typename ApiType::type;
  using ApiTypeRawEnum = typename std::underlying_type<ApiTypeEnum>::type;

  template <typename ThriftType>
  inline static ApiTypeRawEnum LoadRaw(const ThriftType* in) {
    static_assert(sizeof(ApiTypeEnum) == sizeof(ThriftType),
                  "parquet type should always be the same size as thrift type");
    return static_cast<ApiTypeRawEnum>(LoadEnumRaw(in));
  }

  template <typename ThriftType, bool IsUnsigned = true>
  inline static ApiTypeEnum LoadChecked(
      const typename std::enable_if<IsUnsigned, ThriftType>::type* in) {
    auto raw_value = LoadRaw(in);
    if (ARROW_PREDICT_FALSE(raw_value >=
                            static_cast<ApiTypeRawEnum>(ApiType::UNDEFINED))) {
      return ApiType::UNDEFINED;
    }
    return FromThriftUnsafe(static_cast<ThriftType>(raw_value));
  }

  template <typename ThriftType, bool IsUnsigned = false>
  inline static ApiTypeEnum LoadChecked(
      const typename std::enable_if<!IsUnsigned, ThriftType>::type* in) {
    auto raw_value = LoadRaw(in);
    if (ARROW_PREDICT_FALSE(raw_value >=
                                static_cast<ApiTypeRawEnum>(ApiType::UNDEFINED) ||
                            raw_value < 0)) {
      return ApiType::UNDEFINED;
    }
    return FromThriftUnsafe(static_cast<ThriftType>(raw_value));
  }

  template <typename ThriftType>
  inline static ApiTypeEnum Load(const ThriftType* in) {
    return LoadChecked<ThriftType, std::is_unsigned<ApiTypeRawEnum>::value>(in);
  }
};

}  // namespace internal

// Safe enum loader: will check for invalid enum value before converting

template <typename ThriftType,
          typename ParquetEnum =
              typename internal::ThriftEnumTypeTraits<ThriftType>::ParquetEnum>
inline typename ParquetEnum::type LoadEnumSafe(const ThriftType* in) {
  return internal::SafeLoader<ParquetEnum>::Load(in);
}

inline typename Compression::type LoadEnumSafe(const format::CompressionCodec::type* in) {
  const auto raw_value = internal::LoadEnumRaw(in);
  // Check bounds manually, as Compression::type doesn't have the same values
  // as format::CompressionCodec.
  const auto min_value =
      static_cast<decltype(raw_value)>(format::CompressionCodec::UNCOMPRESSED);
  const auto max_value =
      static_cast<decltype(raw_value)>(format::CompressionCodec::LZ4_RAW);
  if (raw_value < min_value || raw_value > max_value) {
    return Compression::UNCOMPRESSED;
  }
  return FromThriftUnsafe(*in);
}

// Safe non-enum converters

static inline AadMetadata FromThrift(format::AesGcmV1 aesGcmV1) {
  return AadMetadata{aesGcmV1.aad_prefix, aesGcmV1.aad_file_unique,
                     aesGcmV1.supply_aad_prefix};
}

static inline AadMetadata FromThrift(format::AesGcmCtrV1 aesGcmCtrV1) {
  return AadMetadata{aesGcmCtrV1.aad_prefix, aesGcmCtrV1.aad_file_unique,
                     aesGcmCtrV1.supply_aad_prefix};
}

static inline EncryptionAlgorithm FromThrift(format::EncryptionAlgorithm encryption) {
  EncryptionAlgorithm encryption_algorithm;

  if (encryption.__isset.AES_GCM_V1) {
    encryption_algorithm.algorithm = ParquetCipher::AES_GCM_V1;
    encryption_algorithm.aad = FromThrift(encryption.AES_GCM_V1);
  } else if (encryption.__isset.AES_GCM_CTR_V1) {
    encryption_algorithm.algorithm = ParquetCipher::AES_GCM_CTR_V1;
    encryption_algorithm.aad = FromThrift(encryption.AES_GCM_CTR_V1);
  } else {
    throw ParquetException("Unsupported algorithm");
  }
  return encryption_algorithm;
}

static inline SortingColumn FromThrift(format::SortingColumn thrift_sorting_column) {
  SortingColumn sorting_column;
  sorting_column.column_idx = thrift_sorting_column.column_idx;
  sorting_column.nulls_first = thrift_sorting_column.nulls_first;
  sorting_column.descending = thrift_sorting_column.descending;
  return sorting_column;
}

// ----------------------------------------------------------------------
// Convert Thrift enums from Parquet enums

static inline format::Type::type ToThrift(Type::type type) {
  return static_cast<format::Type::type>(type);
}

static inline format::ConvertedType::type ToThrift(ConvertedType::type type) {
  // item 0 is NONE
  DCHECK_NE(type, ConvertedType::NONE);
  // it is forbidden to emit "NA" (PARQUET-1990)
  DCHECK_NE(type, ConvertedType::NA);
  DCHECK_NE(type, ConvertedType::UNDEFINED);
  return static_cast<format::ConvertedType::type>(static_cast<int>(type) - 1);
}

static inline format::FieldRepetitionType::type ToThrift(Repetition::type type) {
  return static_cast<format::FieldRepetitionType::type>(type);
}

static inline format::Encoding::type ToThrift(Encoding::type type) {
  return static_cast<format::Encoding::type>(type);
}

static inline format::CompressionCodec::type ToThrift(Compression::type type) {
  switch (type) {
    case Compression::UNCOMPRESSED:
      return format::CompressionCodec::UNCOMPRESSED;
    case Compression::SNAPPY:
      return format::CompressionCodec::SNAPPY;
    case Compression::GZIP:
      return format::CompressionCodec::GZIP;
    case Compression::LZO:
      return format::CompressionCodec::LZO;
    case Compression::BROTLI:
      return format::CompressionCodec::BROTLI;
    case Compression::LZ4:
      return format::CompressionCodec::LZ4_RAW;
    case Compression::LZ4_HADOOP:
      // Deprecated "LZ4" Parquet compression has Hadoop-specific framing
      return format::CompressionCodec::LZ4;
    case Compression::ZSTD:
      return format::CompressionCodec::ZSTD;
    default:
      DCHECK(false) << "Cannot reach here";
      return format::CompressionCodec::UNCOMPRESSED;
  }
}

static inline format::BoundaryOrder::type ToThrift(BoundaryOrder::type type) {
  switch (type) {
    case BoundaryOrder::Unordered:
    case BoundaryOrder::Ascending:
    case BoundaryOrder::Descending:
      return static_cast<format::BoundaryOrder::type>(type);
    default:
      DCHECK(false) << "Cannot reach here";
      return format::BoundaryOrder::UNORDERED;
  }
}

static inline format::SortingColumn ToThrift(SortingColumn sorting_column) {
  format::SortingColumn thrift_sorting_column;
  thrift_sorting_column.column_idx = sorting_column.column_idx;
  thrift_sorting_column.descending = sorting_column.descending;
  thrift_sorting_column.nulls_first = sorting_column.nulls_first;
  return thrift_sorting_column;
}

static inline format::Statistics ToThrift(const EncodedStatistics& stats) {
  format::Statistics statistics;
  if (stats.has_min) {
    statistics.__set_min_value(stats.min());
    // If the order is SIGNED, then the old min value must be set too.
    // This for backward compatibility
    if (stats.is_signed()) {
      statistics.__set_min(stats.min());
    }
  }
  if (stats.has_max) {
    statistics.__set_max_value(stats.max());
    // If the order is SIGNED, then the old max value must be set too.
    // This for backward compatibility
    if (stats.is_signed()) {
      statistics.__set_max(stats.max());
    }
  }
  if (stats.has_null_count) {
    statistics.__set_null_count(stats.null_count);
  }
  if (stats.has_distinct_count) {
    statistics.__set_distinct_count(stats.distinct_count);
  }

  return statistics;
}

static inline format::AesGcmV1 ToAesGcmV1Thrift(AadMetadata aad) {
  format::AesGcmV1 aesGcmV1;
  // aad_file_unique is always set
  aesGcmV1.__set_aad_file_unique(aad.aad_file_unique);
  aesGcmV1.__set_supply_aad_prefix(aad.supply_aad_prefix);
  if (!aad.aad_prefix.empty()) {
    aesGcmV1.__set_aad_prefix(aad.aad_prefix);
  }
  return aesGcmV1;
}

static inline format::AesGcmCtrV1 ToAesGcmCtrV1Thrift(AadMetadata aad) {
  format::AesGcmCtrV1 aesGcmCtrV1;
  // aad_file_unique is always set
  aesGcmCtrV1.__set_aad_file_unique(aad.aad_file_unique);
  aesGcmCtrV1.__set_supply_aad_prefix(aad.supply_aad_prefix);
  if (!aad.aad_prefix.empty()) {
    aesGcmCtrV1.__set_aad_prefix(aad.aad_prefix);
  }
  return aesGcmCtrV1;
}

static inline format::EncryptionAlgorithm ToThrift(EncryptionAlgorithm encryption) {
  format::EncryptionAlgorithm encryption_algorithm;
  if (encryption.algorithm == ParquetCipher::AES_GCM_V1) {
    encryption_algorithm.__set_AES_GCM_V1(ToAesGcmV1Thrift(encryption.aad));
  } else {
    encryption_algorithm.__set_AES_GCM_CTR_V1(ToAesGcmCtrV1Thrift(encryption.aad));
  }
  return encryption_algorithm;
}

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

class ThriftDeserializer {
 public:
  explicit ThriftDeserializer(const ReaderProperties& properties)
      : ThriftDeserializer(properties.thrift_string_size_limit(),
                           properties.thrift_container_size_limit()) {}

  ThriftDeserializer(int32_t string_size_limit, int32_t container_size_limit)
      : string_size_limit_(string_size_limit),
        container_size_limit_(container_size_limit) {}

  // Deserialize a thrift message from buf/len.  buf/len must at least contain
  // all the bytes needed to store the thrift message.  On return, len will be
  // set to the actual length of the header.
  template <class T>
  void DeserializeMessage(const uint8_t* buf, uint32_t* len, T* deserialized_msg,
                          const std::shared_ptr<Decryptor>& decryptor = NULLPTR) {
    if (decryptor == NULLPTR) {
      // thrift message is not encrypted
      DeserializeUnencryptedMessage(buf, len, deserialized_msg);
    } else {
      // thrift message is encrypted
      uint32_t clen;
      clen = *len;
      // decrypt
      auto decrypted_buffer = std::static_pointer_cast<ResizableBuffer>(
          AllocateBuffer(decryptor->pool(),
                         static_cast<int64_t>(clen - decryptor->CiphertextSizeDelta())));
      const uint8_t* cipher_buf = buf;
      uint32_t decrypted_buffer_len =
          decryptor->Decrypt(cipher_buf, 0, decrypted_buffer->mutable_data());
      if (decrypted_buffer_len <= 0) {
        throw ParquetException("Couldn't decrypt buffer\n");
      }
      *len = decrypted_buffer_len + decryptor->CiphertextSizeDelta();
      DeserializeUnencryptedMessage(decrypted_buffer->data(), &decrypted_buffer_len,
                                    deserialized_msg);
    }
  }

 private:
  // On Thrift 0.14.0+, we want to use TConfiguration to raise the max message size
  // limit (ARROW-13655).  If we wanted to protect against huge messages, we could
  // do it ourselves since we know the message size up front.
  std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t* buf, uint32_t len) {
#if PARQUET_THRIFT_VERSION_MAJOR > 0 || PARQUET_THRIFT_VERSION_MINOR >= 14
    auto conf = std::make_shared<apache::thrift::TConfiguration>();
    conf->setMaxMessageSize(std::numeric_limits<int>::max());
    return std::shared_ptr<ThriftBuffer>(
        new ThriftBuffer(buf, len, ThriftBuffer::OBSERVE, conf));
#else
    return std::make_shared<ThriftBuffer>(buf, len);
#endif
  }

  template <class T>
  void DeserializeUnencryptedMessage(const uint8_t* buf, uint32_t* len,
                                     T* deserialized_msg) {
    // Deserialize msg bytes into c++ thrift msg using memory transport.
    auto tmem_transport = CreateReadOnlyMemoryBuffer(const_cast<uint8_t*>(buf), *len);
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
    // Protect against CPU and memory bombs
    tproto_factory.setStringSizeLimit(string_size_limit_);
    tproto_factory.setContainerSizeLimit(container_size_limit_);
    auto tproto = tproto_factory.getProtocol(tmem_transport);
    try {
      deserialized_msg->read(tproto.get());
    } catch (std::exception& e) {
      std::stringstream ss;
      ss << "Couldn't deserialize thrift: " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
  }

  const int32_t string_size_limit_;
  const int32_t container_size_limit_;
};

/// Utility class to serialize thrift objects to a binary format.  This object
/// should be reused if possible to reuse the underlying memory.
/// Note: thrift will encode NULLs into the serialized buffer so it is not valid
/// to treat it as a string.
class ThriftSerializer {
 public:
  explicit ThriftSerializer(int initial_buffer_size = 1024)
      : mem_buffer_(new ThriftBuffer(initial_buffer_size)) {
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  }

  /// Serialize obj into a memory buffer.  The result is returned in buffer/len.  The
  /// memory returned is owned by this object and will be invalid when another object
  /// is serialized.
  template <class T>
  void SerializeToBuffer(const T* obj, uint32_t* len, uint8_t** buffer) {
    SerializeObject(obj);
    mem_buffer_->getBuffer(buffer, len);
  }

  template <class T>
  void SerializeToString(const T* obj, std::string* result) {
    SerializeObject(obj);
    *result = mem_buffer_->getBufferAsString();
  }

  template <class T>
  int64_t Serialize(const T* obj, ArrowOutputStream* out,
                    const std::shared_ptr<Encryptor>& encryptor = NULLPTR) {
    uint8_t* out_buffer;
    uint32_t out_length;
    SerializeToBuffer(obj, &out_length, &out_buffer);

    // obj is not encrypted
    if (encryptor == NULLPTR) {
      PARQUET_THROW_NOT_OK(out->Write(out_buffer, out_length));
      return static_cast<int64_t>(out_length);
    } else {  // obj is encrypted
      return SerializeEncryptedObj(out, out_buffer, out_length, encryptor);
    }
  }

 private:
  template <class T>
  void SerializeObject(const T* obj) {
    try {
      mem_buffer_->resetBuffer();
      obj->write(protocol_.get());
    } catch (std::exception& e) {
      std::stringstream ss;
      ss << "Couldn't serialize thrift: " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
  }

  int64_t SerializeEncryptedObj(ArrowOutputStream* out, uint8_t* out_buffer,
                                uint32_t out_length,
                                const std::shared_ptr<Encryptor>& encryptor) {
    auto cipher_buffer = std::static_pointer_cast<ResizableBuffer>(AllocateBuffer(
        encryptor->pool(),
        static_cast<int64_t>(encryptor->CiphertextSizeDelta() + out_length)));
    int cipher_buffer_len =
        encryptor->Encrypt(out_buffer, out_length, cipher_buffer->mutable_data());

    PARQUET_THROW_NOT_OK(out->Write(cipher_buffer->data(), cipher_buffer_len));
    return static_cast<int64_t>(cipher_buffer_len);
  }

  std::shared_ptr<ThriftBuffer> mem_buffer_;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
};

}  // namespace parquet

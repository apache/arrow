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

#include "arrow/util/windows_compatibility.h"

#include <cstdint>
// Check if thrift version < 0.11.0
// or if FORCE_BOOST_SMART_PTR is defined. Ref: https://thrift.apache.org/lib/cpp
#if defined(PARQUET_THRIFT_USE_BOOST) || defined(FORCE_BOOST_SMART_PTR)
#include <boost/shared_ptr.hpp>
#else
#include <memory>
#endif
#include <string>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <sstream>

#include "arrow/util/logging.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/statistics.h"

#include "parquet/parquet_types.h"  // IYWU pragma: export

namespace parquet {

// Check if thrift version < 0.11.0
// or if FORCE_BOOST_SMART_PTR is defined. Ref: https://thrift.apache.org/lib/cpp
#if defined(PARQUET_THRIFT_USE_BOOST) || defined(FORCE_BOOST_SMART_PTR)
using ::boost::shared_ptr;
#else
using ::std::shared_ptr;
#endif

// ----------------------------------------------------------------------
// Convert Thrift enums to / from parquet enums

static inline Type::type FromThrift(format::Type::type type) {
  return static_cast<Type::type>(type);
}

static inline ConvertedType::type FromThrift(format::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<ConvertedType::type>(static_cast<int>(type) + 1);
}

static inline Repetition::type FromThrift(format::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

static inline Encoding::type FromThrift(format::Encoding::type type) {
  return static_cast<Encoding::type>(type);
}

static inline format::Type::type ToThrift(Type::type type) {
  return static_cast<format::Type::type>(type);
}

static inline format::ConvertedType::type ToThrift(ConvertedType::type type) {
  // item 0 is NONE
  DCHECK_NE(type, ConvertedType::NONE);
  return static_cast<format::ConvertedType::type>(static_cast<int>(type) - 1);
}

static inline format::FieldRepetitionType::type ToThrift(Repetition::type type) {
  return static_cast<format::FieldRepetitionType::type>(type);
}

static inline format::Encoding::type ToThrift(Encoding::type type) {
  return static_cast<format::Encoding::type>(type);
}

static inline Compression::type FromThrift(format::CompressionCodec::type type) {
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
      return Compression::LZ4;
    case format::CompressionCodec::ZSTD:
      return Compression::ZSTD;
    default:
      DCHECK(false) << "Cannot reach here";
      return Compression::UNCOMPRESSED;
  }
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
      return format::CompressionCodec::LZ4;
    case Compression::ZSTD:
      return format::CompressionCodec::ZSTD;
    default:
      DCHECK(false) << "Cannot reach here";
      return format::CompressionCodec::UNCOMPRESSED;
  }
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

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  shared_ptr<ThriftBuffer> tmem_transport(
      new ThriftBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> tproto_factory;
  shared_ptr<apache::thrift::protocol::TProtocol> tproto =  //
      tproto_factory.getProtocol(tmem_transport);
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
  int64_t Serialize(const T* obj, ArrowOutputStream* out) {
    uint8_t* out_buffer;
    uint32_t out_length;
    SerializeToBuffer(obj, &out_length, &out_buffer);
    PARQUET_THROW_NOT_OK(out->Write(out_buffer, out_length));
    return static_cast<int64_t>(out_length);
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

  shared_ptr<ThriftBuffer> mem_buffer_;
  shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
};

}  // namespace parquet

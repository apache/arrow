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

#ifndef PARQUET_THRIFT_UTIL_H
#define PARQUET_THRIFT_UTIL_H

#include <cstdint>

// Needed for thrift
#include <boost/shared_ptr.hpp>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "parquet/exception.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/util/logging.h"
#include "parquet/util/memory.h"

namespace parquet {

// ----------------------------------------------------------------------
// Convert Thrift enums to / from parquet enums

static inline Type::type FromThrift(format::Type::type type) {
  return static_cast<Type::type>(type);
}

static inline LogicalType::type FromThrift(format::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<LogicalType::type>(static_cast<int>(type) + 1);
}

static inline Repetition::type FromThrift(format::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

static inline Encoding::type FromThrift(format::Encoding::type type) {
  return static_cast<Encoding::type>(type);
}

static inline Compression::type FromThrift(format::CompressionCodec::type type) {
  return static_cast<Compression::type>(type);
}

static inline format::Type::type ToThrift(Type::type type) {
  return static_cast<format::Type::type>(type);
}

static inline format::ConvertedType::type ToThrift(LogicalType::type type) {
  // item 0 is NONE
  DCHECK_NE(type, LogicalType::NONE);
  return static_cast<format::ConvertedType::type>(static_cast<int>(type) - 1);
}

static inline format::FieldRepetitionType::type ToThrift(Repetition::type type) {
  return static_cast<format::FieldRepetitionType::type>(type);
}

static inline format::Encoding::type ToThrift(Encoding::type type) {
  return static_cast<format::Encoding::type>(type);
}

static inline format::CompressionCodec::type ToThrift(Compression::type type) {
  return static_cast<format::CompressionCodec::type>(type);
}

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer>
      tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
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

// Serialize obj into a buffer. The result is returned as a string.
// The arguments are the object to be serialized and
// the expected size of the serialized object
template <class T>
inline int64_t SerializeThriftMsg(T* obj, uint32_t len, OutputStream* out) {
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer(
      new apache::thrift::transport::TMemoryBuffer(len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer>
      tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(mem_buffer);
  try {
    mem_buffer->resetBuffer();
    obj->write(tproto.get());
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Couldn't serialize thrift: " << e.what() << "\n";
    throw ParquetException(ss.str());
  }

  uint8_t* out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  out->Write(out_buffer, out_length);
  return out_length;
}

}  // namespace parquet

#endif  // PARQUET_THRIFT_UTIL_H

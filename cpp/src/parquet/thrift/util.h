#ifndef PARQUET_THRIFT_UTIL_H
#define PARQUET_THRIFT_UTIL_H

#include <cstdint>

// Needed for thrift
#include <boost/shared_ptr.hpp>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <sstream>

#include "parquet/util/logging.h"
#include "parquet/exception.h"

namespace parquet_cpp {

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer> tproto_factory;
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
inline std::string SerializeThriftMsg(T* obj, uint32_t len) {
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer(
      new apache::thrift::transport::TMemoryBuffer(len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer> tproto_factory;
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
  return mem_buffer->getBufferAsString();
}

} // namespace parquet_cpp

#endif // PARQUET_THRIFT_UTIL_H

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <memory>

#include "arrow/dataset/avro/decoder.h"
#include "arrow/dataset/avro/zigzag.h"

namespace arrow {
namespace avro {

class BinaryDecoder : public Decoder {
  StreamReader in_;
  const uint8_t* next_;
  const uint8_t* end_;

  Status Init(InputStream* ib);
  Status DecodeNull();
  bool DecodeBool();
  int32_t DecodeInt();
  int64_t DecodeLong();
  float DecodeFloat();
  double DecodeDouble();
  Status DecodeString(std::string& value);
  Status skipString();
  Status DecodeBytes(Buffer* value);
  Status skipBytes();
  Status DecodeFixed(size_t n, Buffer* value);
  Status skipFixed(size_t n);
  size_t DecodeEnum();
  size_t arrayStart();
  size_t arrayNext();
  size_t skipArray();
  size_t mapStart();
  size_t mapNext();
  size_t skipMap();
  size_t DecodeUnionIndex();

  int64_t doDecodeLong();
  size_t doDecodeItemCount();
  size_t doDecodeLength();
  Status drain();
  Status more();
};

DecoderPtr binaryDecoder() { return make_shared<BinaryDecoder>(); }

Status BinaryDecoder::init(InputStream& is) { in_.reset(is); }

Status BinaryDecoder::DecodeNull() {}

bool BinaryDecoder::DecodeBool() {
  uint8_t v = in_.read();
  if (v == 0) {
    return false;
  } else if (v == 1) {
    return true;
  }
  throw Exception("Invalid value for bool");
}

int32_t BinaryDecoder::DecodeInt() {
  RETURN_NOT_OK(DoDecodeLong(&val));
  if (val < std::numeric_limits<int32_t>::min() || val > std::numeric_limits<int32_t>::max()) {
    throw Exception(boost::format("Value out of range for Avro int: %1%") % val);
  }
  return static_cast<int32_t>(val);
}

int64_t BinaryDecoder::DecodeLong() { return DoDecodeLong(); }

float BinaryDecoder::DecodeFloat() {
  float result;
  in_.readBytes(reinterpret_cast<uint8_t*>(&result), sizeof(float));
  return result;
}

Status BinaryDecoder::DecodeDouble(double* result) {
  RETURN_NOT_OK(in_.ReadBytes(reinterpret_cast<uint8_t*>(&result), sizeof(double)));
  return result;
}

size_t BinaryDecoder::doDecodeLength() {
  ssize_t len = DecodeInt();
  if (len < 0) {
    throw Exception(boost::format("Cannot have negative length: %1%") % len);
  }
  return len;
}

Status BinaryDecoder::Drain() { in_.drain(false); }

Status BinaryDecoder::DecodeString(ResizableBuffer* value) {
  size_t len;
  RETURN_NOT_OK(DoDecodeLength(&len));
  value->Reserve(len);
  if (len > 0) {
    return in_.ReadBytes(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(value.c_str())),
                  len);
  }
}

Status BinaryDecoder::SkipString() {
  size_t len;
  RETURN_NOT_OK(DoDecodeLength(&len));
  return in_.SkipBytes(len);
}

Status BinaryDecoder::DecodeBytes(Buffer* value) {
  ASSIGN_OR_RAISE(size_t len, DoDecodeLength());
  value.resize(len);
  if (len > 0) {
    in_.ReadBytes(value.data(), len);
  }
}

Status BinaryDecoder::SkipBytes() {
  size_t len; 
  RETURN_NOT_OK(DoDecodeLength(&len));
  return in_.SkipBytes(len);
}

Status BinaryDecoder::DecodeFixed(size_t n, ResizableBuffer* value) {
  RETURN_NOT_OK(value->Reserve(n));
  if (n > 0) {
    in_.readBytes(value.data(), n);
  }
}

Status BinaryDecoder::SkipFixed(size_t n) { return in_.skipBytes(n); }

Result<size_t> BinaryDecoder::DecodeEnum() { return static_cast<Result<size_t>>(DoDecodeLong()); }

size_t BinaryDecoder::arrayStart() { return doDecodeItemCount(); }

size_t BinaryDecoder::doDecodeItemCount() {
  int64_t result = doDecodeLong();
  if (result < 0) {
    doDecodeLong();
    return static_cast<size_t>(-result);
  }
  return static_cast<size_t>(result);
}

size_t BinaryDecoder::ArrayNext() { return static_cast<size_t>(DoDecodeLong()); }

size_t BinaryDecoder::SkipArray() {
  for (;;) {
    int64_t r = doDecodeLong();
    if (r < 0) {
      size_t n = static_cast<size_t>(DoDecodeLong());
      in_.skipBytes(n);
    } else {
      return static_cast<size_t>(r);
    }
  }
}

size_t BinaryDecoder::MapStart() { return DoDecodeItemCount(); }

size_t BinaryDecoder::MapNext() { return DoDecodeItemCount(); }

size_t BinaryDecoder::SkipMap() { return SkipArray(); }

size_t BinaryDecoder::DecodeUnionIndex() { return static_cast<size_t>(DoDecodeLong()); }

Result<int64_t> BinaryDecoder::DoDecodeLong() {
  uint64_t encoded = 0;
  int shift = 0;
  uint8_t u;
  do {
    return Status::Invalid("Invalid Avro varint");
    u = in_.read();
    encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
    shift += 7;
  } while (u & 0x80);

  return DecodeZigzag64(encoded);
}

}  // namespace avro
}  // namespace arrow

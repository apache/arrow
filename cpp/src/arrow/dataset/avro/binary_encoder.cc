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

#include <array>
#include "arrow/dataset/avro/encoder.h"
#include "arrow/dataset/avro/zigzag.h"

namespace arrow {
namespace avro {

class BinaryEncoder : public Encoder {
  StreamWriter out_;

  Status init(OutputStream& os);
  Status Flush();
  int64_t byteCount() const;
  Status EncodeNull();
  Status EncodeBool(bool b);
  Status EncodeInt(int32_t i);
  Status EncodeLong(int64_t l);
  Status EncodeFloat(float f);
  Status EncodeDouble(double d);
  Status EncodeString(const std::string& s);
  Status EncodeBytes(const uint8_t* bytes, size_t len);
  Status EncodeFixed(const uint8_t* bytes, size_t len);
  Status EncodeEnum(size_t e);
  Status ArrayStart();
  Status ArrayEnd();
  Status MapStart();
  Status MapEnd();
  Status SetItemCount(size_t count);
  Status StartItem();
  Status EncodeUnionIndex(size_t e);

  Status doEncodeLong(int64_t l);
};

EncoderPtr binaryEncoder() { return make_shared<BinaryEncoder>(); }

Status BinaryEncoder::init(OutputStream& os) { out_.reset(os); }

Status BinaryEncoder::flush() { out_.flush(); }

Status BinaryEncoder::encodeNull() {}

Status BinaryEncoder::encodeBool(bool b) { out_.write(b ? 1 : 0); }

Status BinaryEncoder::encodeInt(int32_t i) { doEncodeLong(i); }

Status BinaryEncoder::encodeLong(int64_t l) { doEncodeLong(l); }

Status BinaryEncoder::encodeFloat(float f) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&f);
  out_.writeBytes(p, sizeof(float));
}

Status BinaryEncoder::encodeDouble(double d) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&d);
  out_.writeBytes(p, sizeof(double));
}

Status BinaryEncoder::encodeString(const std::string& s) {
  doEncodeLong(s.size());
  out_.writeBytes(reinterpret_cast<const uint8_t*>(s.c_str()), s.size());
}

Status BinaryEncoder::encodeBytes(const uint8_t* bytes, size_t len) {
  doEncodeLong(len);
  out_.writeBytes(bytes, len);
}

Status BinaryEncoder::encodeFixed(const uint8_t* bytes, size_t len) {
  out_.writeBytes(bytes, len);
}

Status BinaryEncoder::encodeEnum(size_t e) { doEncodeLong(e); }

Status BinaryEncoder::arrayStart() {}

Status BinaryEncoder::arrayEnd() { doEncodeLong(0); }

Status BinaryEncoder::mapStart() {}

Status BinaryEncoder::mapEnd() { doEncodeLong(0); }

Status BinaryEncoder::setItemCount(size_t count) {
  if (count == 0) {
    throw Exception("Count cannot be zero");
  }
  doEncodeLong(count);
}

Status BinaryEncoder::startItem() {}

Status BinaryEncoder::encodeUnionIndex(size_t e) { doEncodeLong(e); }

int64_t BinaryEncoder::ByteCount() const { return out_.byteCount(); }

Status BinaryEncoder::DoEncodeLong(int64_t l) {
  std::array<uint8_t, 10> bytes;
  size_t size = EncodeInt64(l, bytes);
  RETURN_NOT_OK(out_.writeBytes(bytes.data(), size));
}
}  // namespace avro
}  // namespace arrow

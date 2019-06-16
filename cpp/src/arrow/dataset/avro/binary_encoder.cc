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
#include "Encoder.hh"
#include "Zigzag.hh"

namespace avro {

using std::make_shared;

class BinaryEncoder : public Encoder {
  StreamWriter out_;

  void init(OutputStream& os);
  void flush();
  int64_t byteCount() const;
  void encodeNull();
  void encodeBool(bool b);
  void encodeInt(int32_t i);
  void encodeLong(int64_t l);
  void encodeFloat(float f);
  void encodeDouble(double d);
  void encodeString(const std::string& s);
  void encodeBytes(const uint8_t* bytes, size_t len);
  void encodeFixed(const uint8_t* bytes, size_t len);
  void encodeEnum(size_t e);
  void arrayStart();
  void arrayEnd();
  void mapStart();
  void mapEnd();
  void setItemCount(size_t count);
  void startItem();
  void encodeUnionIndex(size_t e);

  void doEncodeLong(int64_t l);
};

EncoderPtr binaryEncoder() { return make_shared<BinaryEncoder>(); }

void BinaryEncoder::init(OutputStream& os) { out_.reset(os); }

void BinaryEncoder::flush() { out_.flush(); }

void BinaryEncoder::encodeNull() {}

void BinaryEncoder::encodeBool(bool b) { out_.write(b ? 1 : 0); }

void BinaryEncoder::encodeInt(int32_t i) { doEncodeLong(i); }

void BinaryEncoder::encodeLong(int64_t l) { doEncodeLong(l); }

void BinaryEncoder::encodeFloat(float f) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&f);
  out_.writeBytes(p, sizeof(float));
}

void BinaryEncoder::encodeDouble(double d) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(&d);
  out_.writeBytes(p, sizeof(double));
}

void BinaryEncoder::encodeString(const std::string& s) {
  doEncodeLong(s.size());
  out_.writeBytes(reinterpret_cast<const uint8_t*>(s.c_str()), s.size());
}

void BinaryEncoder::encodeBytes(const uint8_t* bytes, size_t len) {
  doEncodeLong(len);
  out_.writeBytes(bytes, len);
}

void BinaryEncoder::encodeFixed(const uint8_t* bytes, size_t len) {
  out_.writeBytes(bytes, len);
}

void BinaryEncoder::encodeEnum(size_t e) { doEncodeLong(e); }

void BinaryEncoder::arrayStart() {}

void BinaryEncoder::arrayEnd() { doEncodeLong(0); }

void BinaryEncoder::mapStart() {}

void BinaryEncoder::mapEnd() { doEncodeLong(0); }

void BinaryEncoder::setItemCount(size_t count) {
  if (count == 0) {
    throw Exception("Count cannot be zero");
  }
  doEncodeLong(count);
}

void BinaryEncoder::startItem() {}

void BinaryEncoder::encodeUnionIndex(size_t e) { doEncodeLong(e); }

int64_t BinaryEncoder::byteCount() const { return out_.byteCount(); }

void BinaryEncoder::doEncodeLong(int64_t l) {
  std::array<uint8_t, 10> bytes;
  size_t size = encodeInt64(l, bytes);
  out_.writeBytes(bytes.data(), size);
}
}  // namespace avro

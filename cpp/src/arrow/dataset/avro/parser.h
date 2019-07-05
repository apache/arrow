/*
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
#pragma once

#include "Config.hh"
#include "Reader.hh"

#include <array>

namespace arrow {
namespace avro {

///
/// Class that wraps a reader or ValidatingReader with an interface that uses
/// explicit get* names instead of getValue
///

template <class Reader>
class Parser {
 ARROW_DISALLOW_COPY_AND_ASSIGN(Parser);

 public:
  // Constructor only works with Writer
  explicit Parser(const InputBuffer& in) : reader_(in) {}

  /// Constructor only works with ValidatingWriter
  Parser(const ValidSchema& schema, const InputBuffer& in) : reader_(schema, in) {}

  // Note Result<T> not used here because this is expected to be in an inner loop.

  Status ReadNull() {
    Null null;
    return reader_.ReadValue(null);
  }

  Status ReadBool(bool* out) {
    bool val;
    reader_.ReadValue(val);
    return val;
  }

  Status ReadInt(int32_t* val) {
    return reader_.ReadValue(val);
  }

  Status ReadLong(int64_t* val) {
    return reader_.ReadValue(val);
  }

  Status ReadFloat(float* val) {
    return reader_.ReadValue(val);
  }

  Status ReadDouble(double* val) {
    return reader_.ReadValue(val);
  }

  Status ReadString(std::string* val) { reader_.ReadValue(val); }

  Status ReadBytes(std::vector<uint8_t>* val) { return reader_.ReadBytes(val); }

  template <size_t N>
  Status ReadFixed(uint8_t* val) {
    return reader_.ReadFixed(val);
  }

  template <size_t N>
  Status readFixed(std::array<uint8_t, N>* val) {
    return reader_.ReadFixed(val);
  }

  Status ReadRecord() { reader_.ReadRecord(); }

  Status ReadRecordEnd() { reader_.ReadRecordEnd(); }

  Status ReadArrayBlockSize(int64_t* val) { return reader_.ReadArrayBlockSize(val); }

  Status ReadUnion(int64_t* val) { return reader_.ReadUnion(val); }

  Status ReadEnum(int64_t* val) { return reader_.ReadEnum(val); }

  Status ReadMapBlockSize(int64_t* val) { return reader_.ReadMapBlockSize(val); }

 private:
  friend Type NextType(Parser<ValidatingReader>& p);
  friend bool CurrentRecordName(Parser<ValidatingReader>& p, std::string& name);
  friend bool NextFieldName(Parser<ValidatingReader>& p, std::string& name);

  Reader reader_;
};

inline Type NextType(Parser<ValidatingReader>& p) { return p.reader_.nextType(); }

inline bool CurrentRecordName(Parser<ValidatingReader>& p, std::string& name) {
  return p.reader_.CurrentRecordName(name);
}

inline bool NextFieldName(Parser<ValidatingReader>& p, std::string& name) {
  return p.reader_.NextFieldName(name);
}

}  // namespace avro
}  // namespace arrow

#endif

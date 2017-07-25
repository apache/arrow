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

#include "arrow/util/compression_snappy.h"

// Work around warning caused by Snappy include
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include <snappy.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// Snappy implementation

Status SnappyCodec::Decompress(int64_t input_len, const uint8_t* input,
                               int64_t output_len, uint8_t* output_buffer) {
  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
                             static_cast<size_t>(input_len),
                             reinterpret_cast<char*>(output_buffer))) {
    return Status::IOError("Corrupt snappy compressed data.");
  }
  return Status::OK();
}

int64_t SnappyCodec::MaxCompressedLen(int64_t input_len, const uint8_t* input) {
  return snappy::MaxCompressedLength(input_len);
}

Status SnappyCodec::Compress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer,
                             int64_t* output_length) {
  size_t output_len;
  snappy::RawCompress(reinterpret_cast<const char*>(input),
                      static_cast<size_t>(input_len),
                      reinterpret_cast<char*>(output_buffer), &output_len);
  *output_length = static_cast<int64_t>(output_len);
  return Status::OK();
}

}  // namespace arrow

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

#include "arrow/util/compression.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {

Codec::~Codec() {}

Status Codec::Create(Compression::type codec_type, std::unique_ptr<Codec>* result) {
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      break;
    case Compression::SNAPPY:
      result->reset(new SnappyCodec());
      break;
    case Compression::GZIP:
      result->reset(new GZipCodec());
      break;
    case Compression::LZO:
      return Status::NotImplemented("LZO codec not implemented");
    case Compression::BROTLI:
      result->reset(new BrotliCodec());
      break;
    default:
      return Status::Invalid("Unrecognized codec");
  }
  return Status::OK();
}

}  // namespace arrow

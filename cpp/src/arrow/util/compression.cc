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

#include <memory>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/compression_internal.h"

namespace arrow {
namespace util {

Compressor::~Compressor() {}

Decompressor::~Decompressor() {}

Codec::~Codec() {}

int Codec::UseDefaultCompressionLevel() { return kUseDefaultCompressionLevel; }

Status Codec::Init() { return Status::OK(); }

std::string Codec::GetCodecAsString(Compression::type t) {
  switch (t) {
    case Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
    case Compression::SNAPPY:
      return "SNAPPY";
    case Compression::GZIP:
      return "GZIP";
    case Compression::LZO:
      return "LZO";
    case Compression::BROTLI:
      return "BROTLI";
    case Compression::LZ4:
      return "LZ4_RAW";
    case Compression::LZ4_FRAME:
      return "LZ4";
    case Compression::ZSTD:
      return "ZSTD";
    case Compression::BZ2:
      return "BZ2";
    default:
      return "UNKNOWN";
  }
}

Result<Compression::type> Codec::GetCompressionType(const std::string& name) {
  if (name == "UNCOMPRESSED") {
    return Compression::UNCOMPRESSED;
  } else if (name == "GZIP") {
    return Compression::GZIP;
  } else if (name == "SNAPPY") {
    return Compression::SNAPPY;
  } else if (name == "LZO") {
    return Compression::LZO;
  } else if (name == "BROTLI") {
    return Compression::BROTLI;
  } else if (name == "LZ4_RAW") {
    return Compression::LZ4;
  } else if (name == "LZ4") {
    return Compression::LZ4_FRAME;
  } else if (name == "ZSTD") {
    return Compression::ZSTD;
  } else if (name == "BZ2") {
    return Compression::BZ2;
  } else {
    return Status::Invalid("Unrecognized compression type: ", name);
  }
}

Result<std::unique_ptr<Codec>> Codec::Create(Compression::type codec_type,
                                             int compression_level) {
  std::unique_ptr<Codec> codec;
  const bool compression_level_set{compression_level != kUseDefaultCompressionLevel};
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      if (compression_level_set) {
        return Status::Invalid("Compression level cannot be specified for UNCOMPRESSED.");
      }
      return nullptr;
    case Compression::SNAPPY:
#ifdef ARROW_WITH_SNAPPY
      if (compression_level_set) {
        return Status::Invalid("Snappy doesn't support setting a compression level.");
      }
      codec = internal::MakeSnappyCodec();
      break;
#else
      return Status::NotImplemented("Snappy codec support not built");
#endif
    case Compression::GZIP:
#ifdef ARROW_WITH_ZLIB
      codec = internal::MakeGZipCodec(compression_level);
      break;
#else
      return Status::NotImplemented("Gzip codec support not built");
#endif
    case Compression::LZO:
      if (compression_level_set) {
        return Status::Invalid("LZ0 doesn't support setting a compression level.");
      }
      return Status::NotImplemented("LZO codec not implemented");
    case Compression::BROTLI:
#ifdef ARROW_WITH_BROTLI
      codec = internal::MakeBrotliCodec(compression_level);
      break;
#else
      return Status::NotImplemented("Brotli codec support not built");
#endif
    case Compression::LZ4:
#ifdef ARROW_WITH_LZ4
      if (compression_level_set) {
        return Status::Invalid("LZ4 doesn't support setting a compression level.");
      }
      codec = internal::MakeLz4RawCodec();
      break;
#else
      return Status::NotImplemented("LZ4 codec support not built");
#endif
    case Compression::LZ4_FRAME:
#ifdef ARROW_WITH_LZ4
      if (compression_level_set) {
        return Status::Invalid("LZ4 doesn't support setting a compression level.");
      }
      codec = internal::MakeLz4FrameCodec();
      break;
#else
      return Status::NotImplemented("LZ4 codec support not built");
#endif
    case Compression::ZSTD:
#ifdef ARROW_WITH_ZSTD
      codec = internal::MakeZSTDCodec(compression_level);
      break;
#else
      return Status::NotImplemented("ZSTD codec support not built");
#endif
    case Compression::BZ2:
#ifdef ARROW_WITH_BZ2
      codec = internal::MakeBZ2Codec(compression_level);
      break;
#else
      return Status::NotImplemented("BZ2 codec support not built");
#endif
    default:
      return Status::Invalid("Unrecognized codec");
  }

  RETURN_NOT_OK(codec->Init());
  return std::move(codec);
}

bool Codec::IsAvailable(Compression::type codec_type) {
  switch (codec_type) {
    case Compression::UNCOMPRESSED:
      return true;
    case Compression::SNAPPY:
#ifdef ARROW_WITH_SNAPPY
      return true;
#else
      return false;
#endif
    case Compression::GZIP:
#ifdef ARROW_WITH_ZLIB
      return true;
#else
      return false;
#endif
    case Compression::LZO:
      return false;
    case Compression::BROTLI:
#ifdef ARROW_WITH_BROTLI
      return true;
#else
      return false;
#endif
    case Compression::LZ4:
    case Compression::LZ4_FRAME:
#ifdef ARROW_WITH_LZ4
      return true;
#else
      return false;
#endif
    case Compression::ZSTD:
#ifdef ARROW_WITH_ZSTD
      return true;
#else
      return false;
#endif
    case Compression::BZ2:
#ifdef ARROW_WITH_BZ2
      return true;
#else
      return false;
#endif
    default:
      return false;
  }
}

}  // namespace util
}  // namespace arrow

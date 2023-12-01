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

#include "arrow/util/compression_internal.h"

#include <cstdint>
#include <cstring>
#include <memory>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"

#ifndef LZ4F_HEADER_SIZE_MAX
#define LZ4F_HEADER_SIZE_MAX 19
#endif

namespace arrow {
namespace util {
namespace internal {

namespace {

constexpr int kLz4MinCompressionLevel = 1;

// ----------------------------------------------------------------------
// Lz4 frame decompressor implementation

class LZ4Decompressor : public Decompressor {
 public:
  LZ4Decompressor() { cjformat_ = cramjam::StreamingLz4; };
};

// ----------------------------------------------------------------------
// Lz4 frame compressor implementation

class LZ4Compressor : public Compressor {
 public:
  explicit LZ4Compressor(int compression_level) {
    compression_level_ = compression_level;
    cjformat_ = cramjam::StreamingLz4;
  }
};

// ----------------------------------------------------------------------
// Lz4 frame codec implementation

class Lz4FrameCodec : public Codec {
 public:
  explicit Lz4FrameCodec(int compression_level) {
    cjformat_ = cramjam::Lz4;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kLz4DefaultCompressionLevel
                             : compression_level;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    uintptr_t len = cramjam::lz4_frame_max_compressed_len(input_len, compression_level_);
    return static_cast<int64_t>(len);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<LZ4Compressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<LZ4Decompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::LZ4_FRAME; }
  int minimum_compression_level() const override { return kLz4MinCompressionLevel; }
  int maximum_compression_level() const override {
    return cramjam::lz4_frame_max_compression_level();
  }
  int default_compression_level() const override { return kLz4DefaultCompressionLevel; }

  int compression_level() const override { return compression_level_; }
};

// ----------------------------------------------------------------------
// Lz4 "raw" codec implementation

class Lz4Codec : public Codec {
 public:
  explicit Lz4Codec(int compression_level) {
    cjformat_ = cramjam::Lz4Block;
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kLz4DefaultCompressionLevel
                             : compression_level;
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return cramjam::lz4_block_max_compressed_len(input_len, nullptr);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented(
        "Streaming compression unsupported with LZ4 raw format. "
        "Try using LZ4 frame format instead.");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented(
        "Streaming decompression unsupported with LZ4 raw format. "
        "Try using LZ4 frame format instead.");
  }

  Compression::type compression_type() const override { return Compression::LZ4; }
  int minimum_compression_level() const override { return kLz4MinCompressionLevel; }
  int maximum_compression_level() const override {
    return cramjam::lz4_frame_max_compression_level();
  }
  int default_compression_level() const override { return kLz4DefaultCompressionLevel; }
};

// ----------------------------------------------------------------------
// Lz4 Hadoop "raw" codec implementation

class Lz4HadoopCodec : public Lz4Codec {
 public:
  Lz4HadoopCodec() : Lz4Codec(kUseDefaultCompressionLevel) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    const int64_t decompressed_size =
        TryDecompressHadoop(input_len, input, output_buffer_len, output_buffer);
    if (decompressed_size != kNotHadoop) {
      return decompressed_size;
    }
    // Fall back on raw LZ4 codec (for files produces by earlier versions of Parquet C++)
    return Lz4Codec::Decompress(input_len, input, output_buffer_len, output_buffer);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    return kPrefixLength + Lz4Codec::MaxCompressedLen(input_len, nullptr);
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    if (output_buffer_len < kPrefixLength) {
      return Status::Invalid("Output buffer too small for Lz4HadoopCodec compression");
    }

    ARROW_ASSIGN_OR_RAISE(
        int64_t output_len,
        Lz4Codec::Compress(input_len, input, output_buffer_len - kPrefixLength,
                           output_buffer + kPrefixLength));

    // Prepend decompressed size in bytes and compressed size in bytes
    // to be compatible with Hadoop Lz4Codec
    const uint32_t decompressed_size =
        bit_util::ToBigEndian(static_cast<uint32_t>(input_len));
    const uint32_t compressed_size =
        bit_util::ToBigEndian(static_cast<uint32_t>(output_len));
    SafeStore(output_buffer, decompressed_size);
    SafeStore(output_buffer + sizeof(uint32_t), compressed_size);

    return kPrefixLength + output_len;
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented(
        "Streaming compression unsupported with LZ4 Hadoop raw format. "
        "Try using LZ4 frame format instead.");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented(
        "Streaming decompression unsupported with LZ4 Hadoop raw format. "
        "Try using LZ4 frame format instead.");
  }

  Compression::type compression_type() const override { return Compression::LZ4_HADOOP; }

 protected:
  // Offset starting at which page data can be read/written
  static const int64_t kPrefixLength = sizeof(uint32_t) * 2;

  static const int64_t kNotHadoop = -1;

  int64_t TryDecompressHadoop(int64_t input_len, const uint8_t* input,
                              int64_t output_buffer_len, uint8_t* output_buffer) {
    // Parquet files written with the Hadoop Lz4Codec use their own framing.
    // The input buffer can contain an arbitrary number of "frames", each
    // with the following structure:
    // - bytes 0..3: big-endian uint32_t representing the frame decompressed size
    // - bytes 4..7: big-endian uint32_t representing the frame compressed size
    // - bytes 8...: frame compressed data
    //
    // The Hadoop Lz4Codec source code can be found here:
    // https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/src/main/native/src/codec/Lz4Codec.cc
    int64_t total_decompressed_size = 0;

    while (input_len >= kPrefixLength) {
      const uint32_t expected_decompressed_size =
          bit_util::FromBigEndian(SafeLoadAs<uint32_t>(input));
      const uint32_t expected_compressed_size =
          bit_util::FromBigEndian(SafeLoadAs<uint32_t>(input + sizeof(uint32_t)));
      input += kPrefixLength;
      input_len -= kPrefixLength;

      if (input_len < expected_compressed_size) {
        // Not enough bytes for Hadoop "frame"
        return kNotHadoop;
      }
      if (output_buffer_len < expected_decompressed_size) {
        // Not enough bytes to hold advertised output => probably not Hadoop
        return kNotHadoop;
      }
      // Try decompressing and compare with expected decompressed length
      auto maybe_decompressed_size = Lz4Codec::Decompress(
          expected_compressed_size, input, output_buffer_len, output_buffer);
      if (!maybe_decompressed_size.ok() ||
          *maybe_decompressed_size != expected_decompressed_size) {
        return kNotHadoop;
      }
      input += expected_compressed_size;
      input_len -= expected_compressed_size;
      output_buffer += expected_decompressed_size;
      output_buffer_len -= expected_decompressed_size;
      total_decompressed_size += expected_decompressed_size;
    }

    if (input_len == 0) {
      return total_decompressed_size;
    } else {
      return kNotHadoop;
    }
  }

  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeLz4FrameCodec(int compression_level) {
  return std::make_unique<Lz4FrameCodec>(compression_level);
}

std::unique_ptr<Codec> MakeLz4HadoopRawCodec() {
  return std::make_unique<Lz4HadoopCodec>();
}

std::unique_ptr<Codec> MakeLz4RawCodec(int compression_level) {
  return std::make_unique<Lz4Codec>(compression_level);
}

}  // namespace internal

}  // namespace util
}  // namespace arrow

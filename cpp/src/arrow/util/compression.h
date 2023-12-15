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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

constexpr int kUseDefaultCompressionLevel = std::numeric_limits<int>::min();

/// \brief Streaming compressor interface
///
class ARROW_EXPORT Compressor {
 public:
  virtual ~Compressor() { cramjam::free_compressor(cjformat_, &compressor_); };

  struct CompressResult {
    int64_t bytes_read;
    int64_t bytes_written;
  };
  struct FlushResult {
    int64_t bytes_written;
    bool should_retry;
  };
  struct EndResult {
    int64_t bytes_written;
    bool should_retry;
  };

  virtual Status Init() {
    char* error = nullptr;
    compressor_ = cramjam::compressor_init(cjformat_, compression_level_, &error);
    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }
    return Status::OK();
  }

  /// \brief Compress some input.
  ///
  /// If bytes_read is 0 on return, then a larger output buffer should be supplied.
  virtual Result<CompressResult> Compress(int64_t input_len, const uint8_t* input) {
    char* error = nullptr;
    size_t nbytes_read = 0, nbytes_written = 0;
    cramjam::compressor_compress(cjformat_, &compressor_, input, input_len, &nbytes_read,
                                 &nbytes_written, &error);
    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }
    return CompressResult{static_cast<int64_t>(nbytes_read),
                          static_cast<int64_t>(nbytes_written)};
  };

  virtual std::shared_ptr<Buffer> Inner() {
    if (finished_) {
      return compressed_;
    }
    auto buf = cramjam::compressor_inner(cjformat_, &compressor_);
    return Buffer::FromCramjamBuffer(buf);
  }

  /// \brief Flush part of the compressed output.
  ///
  /// If should_retry is true on return, Flush() should be called again
  /// with a larger buffer.
  virtual Result<FlushResult> Flush() {
    char* error = nullptr;
    // todo: need to have nbytes written set during flush
    size_t nbytes_written = 0;
    cramjam::compressor_flush(cjformat_, &compressor_, &error);
    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }
    return FlushResult{static_cast<int64_t>(nbytes_written), false};
  };

  /// \brief End compressing, doing whatever is necessary to end the stream.
  ///
  /// If should_retry is true on return, End() should be called again
  /// with a larger buffer.  Otherwise, the Compressor should not be used anymore.
  ///
  /// End() implies Flush().
  virtual Result<EndResult> End(int64_t output_len, uint8_t* output) {
    ARROW_RETURN_NOT_OK(Finish());
    return EndResult{static_cast<int64_t>(compressed_->size()), false};
  };

  /// \bFinish and take underlying buffer
  virtual Result<std::shared_ptr<Buffer>> Finish() {
    if (!finished_) {
      char* error = nullptr;
      auto buffer = cramjam::compressor_finish(cjformat_, &compressor_, &error);
      compressed_ = Buffer::FromCramjamBuffer(buffer);
      if (error) {
        std::string msg(error);
        cramjam::free_string(error);
        return Status::IOError(error);
      }
      finished_ = true;
    }
    return compressed_;
  };

  // XXX add methods for buffer size heuristics?
 protected:
  bool finished_ = false;
  void* compressor_;
  int32_t compression_level_ = 0;
  cramjam::StreamingCodec cjformat_;
  std::shared_ptr<Buffer> compressed_;
};

/// \brief Streaming decompressor interface
///
class ARROW_EXPORT Decompressor {
 public:
  virtual ~Decompressor() { cramjam::free_decompressor(cjformat_, &decompressor_); };

  struct DecompressResult {
    int64_t bytes_read;
    int64_t bytes_written;
  };

  virtual Status Init() {
    decompressor_ = cramjam::decompressor_init(cjformat_);
    return Status::OK();
  }

  /// \brief Decompress some input.
  ///
  virtual Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input) {
    char* error = nullptr;
    size_t nbytes_written = 0, nbytes_read = 0;
    cramjam::decompressor_decompress(cjformat_, &decompressor_, input, input_len,
                                     &nbytes_read, &nbytes_written, &error);
    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }

    if (static_cast<int64_t>(nbytes_read) < input_len) {
      finished_ = true;  // didn't read all, so end of stream was encountered
    }

    return DecompressResult{static_cast<int64_t>(nbytes_read),
                            static_cast<int64_t>(nbytes_written)};
  };

  /// \brief Return whether the compressed stream is finished.
  ///
  /// This is a heuristic.  If true is returned, then it is guaranteed
  /// that the stream is finished.  If false is returned, however, it may
  /// simply be that the underlying library isn't able to provide the information.
  virtual bool IsFinished() { return finished_; };

  /// \brief Reinitialize decompressor, making it ready for a new compressed stream.
  virtual Status Reset() {
    cramjam::free_decompressor(cjformat_, &decompressor_);
    finished_ = false;
    return Init();
  };

  virtual std::shared_ptr<Buffer> Inner() {
    if (finished_) {
      return decompressed_;
    }
    auto buf = cramjam::decompressor_inner(cjformat_, &decompressor_);
    return Buffer::FromCramjamBuffer(buf);
  }

  /// \bFinish and take underlying buffer
  virtual Result<std::shared_ptr<Buffer>> Finish() {
    if (!finished_) {
      char* error = nullptr;
      auto buffer = cramjam::decompressor_finish(cjformat_, &decompressor_, &error);
      decompressed_ = Buffer::FromCramjamBuffer(buffer);
      if (error) {
        std::string msg(error);
        cramjam::free_string(error);
        return Status::IOError(error);
      }
      finished_ = true;
    }
    return decompressed_;
  };

  // XXX add methods for buffer size heuristics?
 protected:
  bool finished_ = false;
  void* decompressor_;
  std::shared_ptr<Buffer> decompressed_;
  cramjam::StreamingCodec cjformat_;
};

/// \brief Compression codec options
class ARROW_EXPORT CodecOptions {
 public:
  explicit CodecOptions(int32_t compression_level = kUseDefaultCompressionLevel)
      : compression_level(compression_level) {}

  virtual ~CodecOptions() = default;

  int32_t compression_level = 0;
};

// ----------------------------------------------------------------------
// GZip codec options implementation

enum class GZipFormat {
  ZLIB,
  DEFLATE,
  GZIP,
};

class ARROW_EXPORT GZipCodecOptions : public CodecOptions {
 public:
  GZipFormat gzip_format = GZipFormat::GZIP;
  std::optional<int> window_bits;
};

// ----------------------------------------------------------------------
// brotli codec options implementation

class ARROW_EXPORT BrotliCodecOptions : public CodecOptions {
 public:
  std::optional<int> window_bits;
};

/// \brief Compression codec
class ARROW_EXPORT Codec {
 public:
  virtual ~Codec() = default;

  /// \brief Return special value to indicate that a codec implementation
  /// should use its default compression level
  static int UseDefaultCompressionLevel();

  /// \brief Return a string name for compression type
  static const std::string& GetCodecAsString(Compression::type t);

  /// \brief Return compression type for name (all lower case)
  static Result<Compression::type> GetCompressionType(const std::string& name);

  /// \brief Create a codec for the given compression algorithm with CodecOptions
  static Result<std::unique_ptr<Codec>> Create(
      Compression::type codec, const CodecOptions& codec_options = CodecOptions{});

  /// \brief Create a codec for the given compression algorithm
  static Result<std::unique_ptr<Codec>> Create(Compression::type codec,
                                               int32_t compression_level);

  /// \brief Return true if support for indicated codec has been enabled
  static bool IsAvailable(Compression::type codec);

  /// \brief Return true if indicated codec supports setting a compression level
  static bool SupportsCompressionLevel(Compression::type codec);

  /// \brief Return the smallest supported compression level for the codec
  /// Note: This function creates a temporary Codec instance
  static Result<int> MinimumCompressionLevel(Compression::type codec);

  /// \brief Return the largest supported compression level for the codec
  /// Note: This function creates a temporary Codec instance
  static Result<int> MaximumCompressionLevel(Compression::type codec);

  /// \brief Return the default compression level
  /// Note: This function creates a temporary Codec instance
  static Result<int> DefaultCompressionLevel(Compression::type codec);

  /// \brief Return the smallest supported compression level
  virtual int minimum_compression_level() const = 0;

  /// \brief Return the largest supported compression level
  virtual int maximum_compression_level() const = 0;

  /// \brief Return the default compression level
  virtual int default_compression_level() const = 0;

  /// \brief One-shot decompression function
  ///
  /// output_buffer_len must be correct and therefore be obtained in advance.
  /// The actual decompressed length is returned.
  ///
  /// \note One-shot decompression is not always compatible with streaming
  /// compression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                                     int64_t output_buffer_len, uint8_t* output_buffer) {
    uintptr_t nbytes_read = 0, nbytes_written = 0;
    char* error = nullptr;
    cramjam::decompress_into(cjformat_, input, input_len, output_buffer,
                             output_buffer_len, &nbytes_read, &nbytes_written, &error);
    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }
    return nbytes_written;
  };

  /// \brief One-shot compression function
  ///
  /// output_buffer_len must first have been computed using MaxCompressedLen().
  /// The actual compressed length is returned.
  ///
  /// \note One-shot compression is not always compatible with streaming
  /// decompression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                                   int64_t output_buffer_len, uint8_t* output_buffer) {
    uintptr_t nbytes_read = 0, nbytes_written = 0;
    char* error = nullptr;
    cramjam::compress_into(cjformat_, compression_level_, input, input_len, output_buffer,
                           output_buffer_len, &nbytes_read, &nbytes_written, &error);

    if (error) {
      std::string msg(error);
      cramjam::free_string(error);
      return Status::IOError(msg);
    }
    return nbytes_written;
  };

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) = 0;

  /// \brief Create a streaming compressor instance
  virtual Result<std::shared_ptr<Compressor>> MakeCompressor() = 0;

  /// \brief Create a streaming compressor instance
  virtual Result<std::shared_ptr<Decompressor>> MakeDecompressor() = 0;

  /// \brief This Codec's compression type
  virtual Compression::type compression_type() const = 0;

  /// \brief The name of this Codec's compression type
  const std::string& name() const { return GetCodecAsString(compression_type()); }

  /// \brief This Codec's compression level, if applicable
  virtual int compression_level() const { return UseDefaultCompressionLevel(); }

 protected:
  cramjam::Codec cjformat_;
  int32_t compression_level_ = 0;

 private:
  /// \brief Initializes the codec's resources.
  virtual Status Init();
};

}  // namespace util
}  // namespace arrow

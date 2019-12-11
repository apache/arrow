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
#include <string>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {

struct Compression {
  /// \brief Compression algorithm
  enum type { UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, LZ4, LZO, BZ2 };
};

namespace util {

constexpr int kUseDefaultCompressionLevel = std::numeric_limits<int>::min();

/// \brief Streaming compressor interface
///
class ARROW_EXPORT Compressor {
 public:
  virtual ~Compressor();

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

  /// \brief Compress some input.
  ///
  /// If bytes_read is 0 on return, then a larger output buffer should be supplied.
  virtual Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                          int64_t output_len, uint8_t* output) = 0;

  /// \brief Flush part of the compressed output.
  ///
  /// If should_retry is true on return, Flush() should be called again
  /// with a larger buffer.
  virtual Result<FlushResult> Flush(int64_t output_len, uint8_t* output) = 0;

  /// \brief End compressing, doing whatever is necessary to end the stream.
  ///
  /// If should_retry is true on return, End() should be called again
  /// with a larger buffer.  Otherwise, the Compressor should not be used anymore.
  ///
  /// End() implies Flush().
  virtual Result<EndResult> End(int64_t output_len, uint8_t* output) = 0;

  // XXX add methods for buffer size heuristics?
};

/// \brief Streaming decompressor interface
///
class ARROW_EXPORT Decompressor {
 public:
  virtual ~Decompressor();

  struct DecompressResult {
    // XXX is need_more_output necessary? (Brotli?)
    int64_t bytes_read;
    int64_t bytes_written;
    bool need_more_output;
  };

  /// \brief Decompress some input.
  ///
  /// If need_more_output is true on return, a larger output buffer needs
  /// to be supplied.
  virtual Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                              int64_t output_len, uint8_t* output) = 0;

  /// \brief Return whether the compressed stream is finished.
  ///
  /// This is a heuristic.  If true is returned, then it is guaranteed
  /// that the stream is finished.  If false is returned, however, it may
  /// simply be that the underlying library isn't able to provide the information.
  virtual bool IsFinished() = 0;

  /// \brief Reinitialize decompressor, making it ready for a new compressed stream.
  virtual Status Reset() = 0;

  // XXX add methods for buffer size heuristics?
};

/// \brief Compression codec
class ARROW_EXPORT Codec {
 public:
  virtual ~Codec();

  /// \brief Return special value to indicate that a codec implementation
  /// should use its default compression level
  static int UseDefaultCompressionLevel();

  /// \brief Return a string name for compression type
  static std::string GetCodecAsString(Compression::type t);

  /// \brief Create a codec for the given compression algorithm
  static Result<std::unique_ptr<Codec>> Create(
      Compression::type codec, int compression_level = kUseDefaultCompressionLevel);

  /// \brief Return true if support for indicated codec has been enabled
  static bool IsAvailable(Compression::type codec);

  /// \brief One-shot decompression function
  ///
  /// output_buffer_len must be correct and therefore be obtained in advance.
  /// The actual decompressed length is returned.
  ///
  /// \note One-shot decompression is not always compatible with streaming
  /// compression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                                     int64_t output_buffer_len,
                                     uint8_t* output_buffer) = 0;

  /// \brief One-shot compression function
  ///
  /// output_buffer_len must first have been computed using MaxCompressedLen().
  /// The actual compressed length is returned.
  ///
  /// \note One-shot compression is not always compatible with streaming
  /// decompression.  Depending on the codec (e.g. LZ4), different formats may
  /// be used.
  virtual Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                                   int64_t output_buffer_len, uint8_t* output_buffer) = 0;

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) = 0;

  /// \brief Create a streaming compressor instance
  virtual Result<std::shared_ptr<Compressor>> MakeCompressor() = 0;

  /// \brief Create a streaming compressor instance
  virtual Result<std::shared_ptr<Decompressor>> MakeDecompressor() = 0;

  virtual const char* name() const = 0;

  // Deprecated APIs

  /// \brief Create a codec for the given compression algorithm
  ARROW_DEPRECATED("Use Result-returning version")
  static Status Create(Compression::type codec, std::unique_ptr<Codec>* out);

  /// \brief Create a codec for the given compression algorithm and level
  ARROW_DEPRECATED("Use Result-returning version")
  static Status Create(Compression::type codec, int compression_level,
                       std::unique_ptr<Codec>* out);

  /// \brief One-shot decompression function
  ARROW_DEPRECATED("Use Result-returning version")
  Status Decompress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len,
                    uint8_t* output_buffer, int64_t* output_len);

  /// \brief One-shot compression function
  ARROW_DEPRECATED("Use Result-returning version")
  Status Compress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len,
                  uint8_t* output_buffer, int64_t* output_len);

  /// \brief Create a streaming compressor instance
  ARROW_DEPRECATED("Use Result-returning version")
  virtual Status MakeCompressor(std::shared_ptr<Compressor>* out);

  /// \brief Create a streaming decompressor instance
  ARROW_DEPRECATED("Use Result-returning version")
  virtual Status MakeDecompressor(std::shared_ptr<Decompressor>* out);

 private:
  /// \brief Initializes the codec's resources.
  virtual Status Init();
};

}  // namespace util
}  // namespace arrow

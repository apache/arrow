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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>

#include <zconf.h>
#include <zlib.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// gzip implementation

// These are magic numbers from zlib.h.  Not clear why they are not defined
// there.

// Maximum window size
constexpr int WINDOW_BITS = 15;

// Output Gzip.
constexpr int GZIP_CODEC = 16;

// Determine if this is libz or gzip from header.
constexpr int DETECT_CODEC = 32;

constexpr int kGZipMinCompressionLevel = 1;
constexpr int kGZipMaxCompressionLevel = 9;

int CompressionWindowBitsForFormat(GZipFormat::type format) {
  int window_bits = WINDOW_BITS;
  switch (format) {
    case GZipFormat::DEFLATE:
      window_bits = -window_bits;
      break;
    case GZipFormat::GZIP:
      window_bits += GZIP_CODEC;
      break;
    case GZipFormat::ZLIB:
      break;
  }
  return window_bits;
}

int DecompressionWindowBitsForFormat(GZipFormat::type format) {
  if (format == GZipFormat::DEFLATE) {
    return -WINDOW_BITS;
  } else {
    /* If not deflate, autodetect format from header */
    return WINDOW_BITS | DETECT_CODEC;
  }
}

Status ZlibErrorPrefix(const char* prefix_msg, const char* msg) {
  return Status::IOError(prefix_msg, (msg) ? msg : "(unknown error)");
}

// ----------------------------------------------------------------------
// gzip decompressor implementation

class GZipDecompressor : public Decompressor {
 public:
  explicit GZipDecompressor(GZipFormat::type format)
      : format_(format), initialized_(false), finished_(false) {}

  ~GZipDecompressor() override {
    if (initialized_) {
      inflateEnd(&stream_);
    }
  }

  Status Init() {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));
    finished_ = false;

    int ret;
    int window_bits = DecompressionWindowBitsForFormat(format_);
    if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
      return ZlibError("zlib inflateInit failed: ");
    } else {
      initialized_ = true;
      return Status::OK();
    }
  }

  Status Reset() override {
    DCHECK(initialized_);
    finished_ = false;
    int ret;
    if ((ret = inflateReset(&stream_)) != Z_OK) {
      return ZlibError("zlib inflateReset failed: ");
    } else {
      return Status::OK();
    }
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    static constexpr auto input_limit =
        static_cast<int64_t>(std::numeric_limits<uInt>::max());
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = static_cast<uInt>(std::min(input_len, input_limit));
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));
    int ret;

    ret = inflate(&stream_, Z_SYNC_FLUSH);
    if (ret == Z_DATA_ERROR || ret == Z_STREAM_ERROR || ret == Z_MEM_ERROR) {
      return ZlibError("zlib inflate failed: ");
    }
    if (ret == Z_NEED_DICT) {
      return ZlibError("zlib inflate failed (need preset dictionary): ");
    }
    finished_ = (ret == Z_STREAM_END);
    if (ret == Z_BUF_ERROR) {
      // No progress was possible
      return DecompressResult{0, 0, true};
    } else {
      ARROW_CHECK(ret == Z_OK || ret == Z_STREAM_END);
      // Some progress has been made
      return DecompressResult{input_len - stream_.avail_in,
                              output_len - stream_.avail_out, false};
    }
    return Status::OK();
  }

  bool IsFinished() override { return finished_; }

 protected:
  Status ZlibError(const char* prefix_msg) {
    return ZlibErrorPrefix(prefix_msg, stream_.msg);
  }

  z_stream stream_;
  GZipFormat::type format_;
  bool initialized_;
  bool finished_;
};

// ----------------------------------------------------------------------
// gzip compressor implementation

class GZipCompressor : public Compressor {
 public:
  explicit GZipCompressor(int compression_level)
      : initialized_(false), compression_level_(compression_level) {}

  ~GZipCompressor() override {
    if (initialized_) {
      deflateEnd(&stream_);
    }
  }

  Status Init(GZipFormat::type format) {
    DCHECK(!initialized_);
    memset(&stream_, 0, sizeof(stream_));

    int ret;
    // Initialize to run specified format
    int window_bits = CompressionWindowBitsForFormat(format);
    if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits,
                            compression_level_, Z_DEFAULT_STRATEGY)) != Z_OK) {
      return ZlibError("zlib deflateInit failed: ");
    } else {
      initialized_ = true;
      return Status::OK();
    }
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";

    static constexpr auto input_limit =
        static_cast<int64_t>(std::numeric_limits<uInt>::max());

    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = static_cast<uInt>(std::min(input_len, input_limit));
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

    int64_t ret = 0;
    ret = deflate(&stream_, Z_NO_FLUSH);
    if (ret == Z_STREAM_ERROR) {
      return ZlibError("zlib compress failed: ");
    }
    if (ret == Z_OK) {
      // Some progress has been made
      return CompressResult{input_len - stream_.avail_in, output_len - stream_.avail_out};
    } else {
      // No progress was possible
      ARROW_CHECK_EQ(ret, Z_BUF_ERROR);
      return CompressResult{0, 0};
    }
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";

    static constexpr auto input_limit =
        static_cast<int64_t>(std::numeric_limits<uInt>::max());

    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

    int64_t ret = 0;
    ret = deflate(&stream_, Z_SYNC_FLUSH);
    if (ret == Z_STREAM_ERROR) {
      return ZlibError("zlib flush failed: ");
    }
    int64_t bytes_written;
    if (ret == Z_OK) {
      bytes_written = output_len - stream_.avail_out;
    } else {
      ARROW_CHECK_EQ(ret, Z_BUF_ERROR);
      bytes_written = 0;
    }
    // "If deflate returns with avail_out == 0, this function must be called
    //  again with the same value of the flush parameter and more output space
    //  (updated avail_out), until the flush is complete (deflate returns
    //  with non-zero avail_out)."
    // "Note that Z_BUF_ERROR is not fatal, and deflate() can be called again
    //  with more input and more output space to continue compressing."
    return FlushResult{bytes_written, stream_.avail_out == 0};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    DCHECK(initialized_) << "Called on non-initialized stream";

    static constexpr auto input_limit =
        static_cast<int64_t>(std::numeric_limits<uInt>::max());

    stream_.avail_in = 0;
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(std::min(output_len, input_limit));

    int64_t ret = 0;
    ret = deflate(&stream_, Z_FINISH);
    if (ret == Z_STREAM_ERROR) {
      return ZlibError("zlib flush failed: ");
    }
    int64_t bytes_written = output_len - stream_.avail_out;
    if (ret == Z_STREAM_END) {
      // Flush complete, we can now end the stream
      initialized_ = false;
      ret = deflateEnd(&stream_);
      if (ret == Z_OK) {
        return EndResult{bytes_written, false};
      } else {
        return ZlibError("zlib end failed: ");
      }
    } else {
      // Not everything could be flushed,
      return EndResult{bytes_written, true};
    }
  }

 protected:
  Status ZlibError(const char* prefix_msg) {
    return ZlibErrorPrefix(prefix_msg, stream_.msg);
  }

  z_stream stream_;
  bool initialized_;
  int compression_level_;
};

// ----------------------------------------------------------------------
// gzip codec implementation

class GZipCodec : public Codec {
 public:
  explicit GZipCodec(int compression_level, GZipFormat::type format)
      : format_(format),
        compressor_initialized_(false),
        decompressor_initialized_(false) {
    compression_level_ = compression_level == kUseDefaultCompressionLevel
                             ? kGZipDefaultCompressionLevel
                             : compression_level;
  }

  ~GZipCodec() override {
    EndCompressor();
    EndDecompressor();
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<GZipCompressor>(compression_level_);
    RETURN_NOT_OK(ptr->Init(format_));
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<GZipDecompressor>(format_);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Status InitCompressor() {
    EndDecompressor();
    memset(&stream_, 0, sizeof(stream_));

    int ret;
    // Initialize to run specified format
    int window_bits = CompressionWindowBitsForFormat(format_);
    if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits,
                            compression_level_, Z_DEFAULT_STRATEGY)) != Z_OK) {
      return ZlibErrorPrefix("zlib deflateInit failed: ", stream_.msg);
    }
    compressor_initialized_ = true;
    return Status::OK();
  }

  void EndCompressor() {
    if (compressor_initialized_) {
      (void)deflateEnd(&stream_);
    }
    compressor_initialized_ = false;
  }

  Status InitDecompressor() {
    EndCompressor();
    memset(&stream_, 0, sizeof(stream_));
    int ret;

    // Initialize to run either deflate or zlib/gzip format
    int window_bits = DecompressionWindowBitsForFormat(format_);
    if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
      return ZlibErrorPrefix("zlib inflateInit failed: ", stream_.msg);
    }
    decompressor_initialized_ = true;
    return Status::OK();
  }

  void EndDecompressor() {
    if (decompressor_initialized_) {
      (void)inflateEnd(&stream_);
    }
    decompressor_initialized_ = false;
  }

  Result<int64_t> Decompress(int64_t input_length, const uint8_t* input,
                             int64_t output_buffer_length, uint8_t* output) override {
    if (!decompressor_initialized_) {
      RETURN_NOT_OK(InitDecompressor());
    }
    if (output_buffer_length == 0) {
      // The zlib library does not allow *output to be NULL, even when
      // output_buffer_length is 0 (inflate() will return Z_STREAM_ERROR). We don't
      // consider this an error, so bail early if no output is expected. Note that we
      // don't signal an error if the input actually contains compressed data.
      return 0;
    }

    // Reset the stream for this block
    if (inflateReset(&stream_) != Z_OK) {
      return ZlibErrorPrefix("zlib inflateReset failed: ", stream_.msg);
    }

    int ret = 0;
    // gzip can run in streaming mode or non-streaming mode.  We only
    // support the non-streaming use case where we present it the entire
    // compressed input and a buffer big enough to contain the entire
    // compressed output.  In the case where we don't know the output,
    // we just make a bigger buffer and try the non-streaming mode
    // from the beginning again.
    while (ret != Z_STREAM_END) {
      stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
      stream_.avail_in = static_cast<uInt>(input_length);
      stream_.next_out = reinterpret_cast<Bytef*>(output);
      stream_.avail_out = static_cast<uInt>(output_buffer_length);

      // We know the output size.  In this case, we can use Z_FINISH
      // which is more efficient.
      ret = inflate(&stream_, Z_FINISH);
      if (ret == Z_STREAM_END || ret != Z_OK) break;

      // Failure, buffer was too small
      return Status::IOError("Too small a buffer passed to GZipCodec. InputLength=",
                             input_length, " OutputLength=", output_buffer_length);
    }

    // Failure for some other reason
    if (ret != Z_STREAM_END) {
      return ZlibErrorPrefix("GZipCodec failed: ", stream_.msg);
    }

    return stream_.total_out;
  }

  int64_t MaxCompressedLen(int64_t input_length,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    // Must be in compression mode
    if (!compressor_initialized_) {
      Status s = InitCompressor();
      ARROW_CHECK_OK(s);
    }
    int64_t max_len = deflateBound(&stream_, static_cast<uLong>(input_length));
    // ARROW-3514: return a more pessimistic estimate to account for bugs
    // in old zlib versions.
    return max_len + 12;
  }

  Result<int64_t> Compress(int64_t input_length, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output) override {
    if (!compressor_initialized_) {
      RETURN_NOT_OK(InitCompressor());
    }
    stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
    stream_.avail_in = static_cast<uInt>(input_length);
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = static_cast<uInt>(output_buffer_len);

    int64_t ret = 0;
    if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
      if (ret == Z_OK) {
        // Will return Z_OK (and stream.msg NOT set) if stream.avail_out is too
        // small
        return Status::IOError("zlib deflate failed, output buffer too small");
      }

      return ZlibErrorPrefix("zlib deflate failed: ", stream_.msg);
    }

    if (deflateReset(&stream_) != Z_OK) {
      return ZlibErrorPrefix("zlib deflateReset failed: ", stream_.msg);
    }

    // Actual output length
    return output_buffer_len - stream_.avail_out;
  }

  Status Init() override {
    const Status init_compressor_status = InitCompressor();
    if (!init_compressor_status.ok()) {
      return init_compressor_status;
    }
    return InitDecompressor();
  }

  Compression::type compression_type() const override { return Compression::GZIP; }

  int compression_level() const override { return compression_level_; }
  int minimum_compression_level() const override { return kGZipMinCompressionLevel; }
  int maximum_compression_level() const override { return kGZipMaxCompressionLevel; }
  int default_compression_level() const override { return kGZipDefaultCompressionLevel; }

 private:
  // zlib is stateful and the z_stream state variable must be initialized
  // before
  z_stream stream_;

  // Realistically, this will always be GZIP, but we leave the option open to
  // configure
  GZipFormat::type format_;

  // These variables are mutually exclusive. When the codec is in "compressor"
  // state, compressor_initialized_ is true while decompressor_initialized_ is
  // false. When it's decompressing, the opposite is true.
  //
  // Indeed, this is slightly hacky, but the alternative is having separate
  // Compressor and Decompressor classes. If this ever becomes an issue, we can
  // perform the refactoring then
  bool compressor_initialized_;
  bool decompressor_initialized_;
  int compression_level_;
};

}  // namespace

std::unique_ptr<Codec> MakeGZipCodec(int compression_level, GZipFormat::type format) {
  return std::unique_ptr<Codec>(new GZipCodec(compression_level, format));
}

}  // namespace internal
}  // namespace util
}  // namespace arrow

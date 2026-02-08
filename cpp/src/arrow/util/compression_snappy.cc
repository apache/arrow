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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include <snappy.h>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/crc32c.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"

using std::size_t;

namespace arrow {
namespace util {
namespace internal {

namespace {

// ----------------------------------------------------------------------
// Snappy framing constants

constexpr uint8_t kChunkTypeCompressedData = 0x00;
constexpr uint8_t kChunkTypeUncompressedData = 0x01;
constexpr uint8_t kChunkTypePadding = 0xFEu;
constexpr uint8_t kChunkTypeStreamIdentifier = 0xFFu;

constexpr size_t kMaxUncompressedChunkSize = 64 * 1024;  // 64 KiB
constexpr size_t kChunkHeaderSize = 4;                   // 1 byte type + 3 bytes length
constexpr size_t kChunkChecksumSize = 4;                 // masked CRC32C

constexpr uint8_t kStreamIdentifierPayload[] = {'s', 'N', 'a', 'P', 'p', 'Y'};
constexpr size_t kStreamIdentifierPayloadSize = sizeof(kStreamIdentifierPayload);

constexpr uint8_t kStreamHeader[] = {kChunkTypeStreamIdentifier,
                                     static_cast<uint8_t>(kStreamIdentifierPayloadSize & 0xFF),
                                     static_cast<uint8_t>((kStreamIdentifierPayloadSize >> 8) & 0xFF),
                                     static_cast<uint8_t>((kStreamIdentifierPayloadSize >> 16) & 0xFF),
                                     's', 'N', 'a', 'P', 'p', 'Y'};
constexpr size_t kStreamHeaderSize = sizeof(kStreamHeader);

inline uint32_t LoadLittleEndian32(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

inline void StoreLittleEndian32(uint32_t value, uint8_t* out) {
  out[0] = static_cast<uint8_t>(value & 0xFF);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xFF);
  out[2] = static_cast<uint8_t>((value >> 16) & 0xFF);
  out[3] = static_cast<uint8_t>((value >> 24) & 0xFF);
}

inline uint32_t LoadLittleEndian24(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16);
}

// ----------------------------------------------------------------------
// Snappy framed compressor implementation

class SnappyFramedCompressor : public Compressor {
 public:
  SnappyFramedCompressor() = default;

  Status Init() {
    header_emitted_ = false;
    return Status::OK();
  }

  Result<CompressResult> Compress(int64_t input_len, const uint8_t* input,
                                  int64_t output_len, uint8_t* output) override {
    int64_t bytes_read = 0;
    int64_t bytes_written = 0;

    uint8_t* dst = output;
    int64_t dst_remaining = output_len;

    // Emit stream header once at the beginning of the stream.
    if (!header_emitted_) {
      if (dst_remaining < static_cast<int64_t>(kStreamHeaderSize)) {
        // Need a larger output buffer.
        return CompressResult{0, 0};
      }
      std::memcpy(dst, kStreamHeader, kStreamHeaderSize);
      dst += kStreamHeaderSize;
      dst_remaining -= static_cast<int64_t>(kStreamHeaderSize);
      bytes_written += static_cast<int64_t>(kStreamHeaderSize);
      header_emitted_ = true;
    }

    while (input_len > 0) {
      const int64_t chunk_uncompressed =
          std::min<int64_t>(input_len, static_cast<int64_t>(kMaxUncompressedChunkSize));
      const size_t max_compressed =
          snappy::MaxCompressedLength(static_cast<size_t>(chunk_uncompressed));
      const int64_t required = static_cast<int64_t>(kChunkHeaderSize + kChunkChecksumSize +
                                                    static_cast<int64_t>(max_compressed));
      if (dst_remaining < required) {
        // Not enough space to compress even a single full chunk.
        break;
      }

      uint8_t* header = dst;
      uint8_t* checksum_ptr = dst + kChunkHeaderSize;
      char* compressed_data = reinterpret_cast<char*>(dst + kChunkHeaderSize +
                                                      kChunkChecksumSize);

      size_t compressed_size = 0;
      snappy::RawCompress(reinterpret_cast<const char*>(input),
                          static_cast<size_t>(chunk_uncompressed), compressed_data,
                          &compressed_size);

      const uint32_t masked_crc =
          crc32c_masked(input, static_cast<std::size_t>(chunk_uncompressed));
      StoreLittleEndian32(masked_crc, checksum_ptr);

      const uint32_t chunk_data_length =
          kChunkChecksumSize + static_cast<uint32_t>(compressed_size);
      header[0] = kChunkTypeCompressedData;
      header[1] = static_cast<uint8_t>(chunk_data_length & 0xFF);
      header[2] = static_cast<uint8_t>((chunk_data_length >> 8) & 0xFF);
      header[3] = static_cast<uint8_t>((chunk_data_length >> 16) & 0xFF);

      const int64_t total_chunk_size =
          static_cast<int64_t>(kChunkHeaderSize + chunk_data_length);
      dst += total_chunk_size;
      dst_remaining -= total_chunk_size;
      bytes_written += total_chunk_size;

      input += chunk_uncompressed;
      input_len -= chunk_uncompressed;
      bytes_read += chunk_uncompressed;
    }

    return CompressResult{bytes_read, bytes_written};
  }

  Result<FlushResult> Flush(int64_t output_len, uint8_t* output) override {
    // There is no internal buffering other than the stream header.
    if (!header_emitted_) {
      if (output_len < static_cast<int64_t>(kStreamHeaderSize)) {
        return FlushResult{0, true};
      }
      std::memcpy(output, kStreamHeader, kStreamHeaderSize);
      header_emitted_ = true;
      return FlushResult{static_cast<int64_t>(kStreamHeaderSize), false};
    }
    return FlushResult{0, false};
  }

  Result<EndResult> End(int64_t output_len, uint8_t* output) override {
    // For Snappy framed streams there is no explicit end-of-stream marker.
    // We only ensure the stream header is emitted for an empty stream.
    if (!header_emitted_) {
      if (output_len < static_cast<int64_t>(kStreamHeaderSize)) {
        return EndResult{0, true};
      }
      std::memcpy(output, kStreamHeader, kStreamHeaderSize);
      header_emitted_ = true;
      return EndResult{static_cast<int64_t>(kStreamHeaderSize), false};
    }
    return EndResult{0, false};
  }

 private:
  bool header_emitted_ = false;
};

// ----------------------------------------------------------------------
// Snappy framed decompressor implementation

class SnappyFramedDecompressor : public Decompressor {
 public:
  SnappyFramedDecompressor() = default;

  Status Init() {
    finished_ = false;
    input_offset_ = 0;
    saw_eof_ = false;
    saw_stream_identifier_ = false;
    return Status::OK();
  }

  Result<DecompressResult> Decompress(int64_t input_len, const uint8_t* input,
                                      int64_t output_len, uint8_t* output) override {
    // Accept all input for this call and keep it in the internal buffer.
    if (input_len > 0) {
      finished_ = false;
      const auto old_size = static_cast<int64_t>(input_buffer_.size());
      input_buffer_.resize(static_cast<std::size_t>(old_size + input_len));
      std::memcpy(input_buffer_.data() + old_size, input, static_cast<size_t>(input_len));
    } else if (input == nullptr) {
      // The Snappy framed format has no explicit end-of-stream marker.
      // Treat a call with zero input and a null pointer as an explicit
      // end-of-input signal.
      saw_eof_ = true;
    }

    int64_t bytes_read = input_len;
    int64_t bytes_written = 0;

    // First, exhaust any pending uncompressed data from a previous chunk.
    while (!uncompressed_buffer_.empty() && bytes_written < output_len) {
      const int64_t remaining =
          static_cast<int64_t>(uncompressed_buffer_.size()) - uncompressed_offset_;
      const int64_t to_copy = std::min<int64_t>(remaining, output_len - bytes_written);
      if (to_copy > 0) {
        std::memcpy(output + bytes_written,
                    uncompressed_buffer_.data() + uncompressed_offset_,
                    static_cast<size_t>(to_copy));
        bytes_written += to_copy;
        uncompressed_offset_ += to_copy;
      }
      if (uncompressed_offset_ < static_cast<int64_t>(uncompressed_buffer_.size())) {
        // Output buffer is full, but the current chunk is not completely written.
        return DecompressResult{bytes_read, bytes_written, true};
      }
      // Current chunk finished.
      uncompressed_buffer_.clear();
      uncompressed_offset_ = 0;
    }

    if (!uncompressed_buffer_.empty()) {
      // Output buffer is full but we still have pending decoded data.
      return DecompressResult{bytes_read, bytes_written, true};
    }

    if (saw_eof_ && input_buffer_.empty()) {
      if (!saw_stream_identifier_) {
        return Status::IOError(
            "Invalid Snappy framed stream: missing stream identifier");
      }
      finished_ = true;
      return DecompressResult{bytes_read, bytes_written, false};
    }

    // Now parse as many chunks as possible from the input buffer.
    bool need_more_input = false;
    while (true) {
      const int64_t available =
          static_cast<int64_t>(input_buffer_.size()) - static_cast<int64_t>(input_offset_);
      if (available == 0) {
        break;
      }
      if (available < static_cast<int64_t>(kChunkHeaderSize)) {
        need_more_input = true;
        break;  // Need more input for a complete header.
      }

      const uint8_t* header = input_buffer_.data() + input_offset_;
      const uint8_t chunk_type = header[0];
      // Length is stored as a 24-bit little-endian value.
      const uint32_t chunk_length = LoadLittleEndian24(header + 1);

      if (available < static_cast<int64_t>(kChunkHeaderSize + chunk_length)) {
        // Wait for more input.
        need_more_input = true;
        break;
      }

      const uint8_t* chunk_data = header + kChunkHeaderSize;

      if (chunk_type == kChunkTypeStreamIdentifier) {
        if (saw_stream_identifier_) {
          return Status::IOError(
              "Invalid Snappy framed stream: duplicate stream identifier");
        }
        if (chunk_length != kStreamIdentifierPayloadSize ||
            std::memcmp(chunk_data, kStreamIdentifierPayload,
                        kStreamIdentifierPayloadSize) != 0) {
          return Status::IOError("Invalid Snappy framed stream identifier");
        }
        saw_stream_identifier_ = true;
        ConsumeFromInputBuffer(kChunkHeaderSize + chunk_length);
        continue;
      }

      if (chunk_type == kChunkTypePadding || (chunk_type >= 0x80u && chunk_type <= 0xFDu)) {
        // Skippable chunk types.
        ConsumeFromInputBuffer(kChunkHeaderSize + chunk_length);
        continue;
      }

      if (!saw_stream_identifier_) {
        return Status::IOError(
            "Invalid Snappy framed stream: missing stream identifier");
      }

      if (chunk_type >= 0x02u && chunk_type <= 0x7Fu) {
        return Status::IOError("Encountered reserved unskippable Snappy framed chunk");
      }

      if (chunk_type != kChunkTypeCompressedData &&
          chunk_type != kChunkTypeUncompressedData) {
        return Status::IOError("Unknown Snappy framed chunk type");
      }

      if (chunk_length < kChunkChecksumSize) {
        return Status::IOError("Snappy framed chunk too small for checksum");
      }

      const uint8_t* checksum_ptr = chunk_data;
      const uint8_t* payload = chunk_data + kChunkChecksumSize;
      const uint32_t payload_length = chunk_length - static_cast<uint32_t>(kChunkChecksumSize);
      const uint32_t expected_masked_crc = LoadLittleEndian32(checksum_ptr);

      // Decode chunk payload into the uncompressed buffer.
      if (chunk_type == kChunkTypeCompressedData) {
        size_t uncompressed_size = 0;
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(payload),
                                           static_cast<size_t>(payload_length),
                                           &uncompressed_size)) {
          return Status::IOError("Corrupt Snappy framed compressed chunk");
        }
        if (uncompressed_size > kMaxUncompressedChunkSize) {
          return Status::IOError("Snappy framed chunk exceeds maximum uncompressed size");
        }
        uncompressed_buffer_.resize(uncompressed_size);
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(payload),
                                   static_cast<size_t>(payload_length),
                                   reinterpret_cast<char*>(uncompressed_buffer_.data()))) {
          return Status::IOError("Corrupt Snappy framed compressed chunk");
        }
      } else {
        // Uncompressed data.
        if (payload_length > kMaxUncompressedChunkSize) {
          return Status::IOError("Snappy framed chunk exceeds maximum uncompressed size");
        }
        uncompressed_buffer_.resize(payload_length);
        if (payload_length > 0) {
          std::memcpy(uncompressed_buffer_.data(), payload, payload_length);
        }
      }

      const uint32_t actual_masked_crc =
          crc32c_masked(uncompressed_buffer_.data(), uncompressed_buffer_.size());
      if (actual_masked_crc != expected_masked_crc) {
        return Status::IOError("Snappy framed chunk failed CRC32C check");
      }

      // Consume this chunk from the input buffer now that it has been decoded.
      ConsumeFromInputBuffer(kChunkHeaderSize + chunk_length);

      // Emit as much of the decoded data as fits into the caller's output buffer.
      const int64_t to_copy =
          std::min<int64_t>(static_cast<int64_t>(uncompressed_buffer_.size()),
                            output_len - bytes_written);
      if (to_copy > 0) {
        std::memcpy(output + bytes_written, uncompressed_buffer_.data(),
                    static_cast<size_t>(to_copy));
        bytes_written += to_copy;
        uncompressed_offset_ = to_copy;
      } else {
        uncompressed_offset_ = 0;
      }

      if (uncompressed_offset_ < static_cast<int64_t>(uncompressed_buffer_.size())) {
        // Output buffer is full, but the chunk is only partially written.
        return DecompressResult{bytes_read, bytes_written, true};
      }

      // Entire chunk emitted.
      uncompressed_buffer_.clear();
      uncompressed_offset_ = 0;
    }

    if (saw_eof_ && need_more_input) {
      return Status::IOError("Truncated Snappy framed stream");
    }

    finished_ = saw_eof_ && input_buffer_.empty() && uncompressed_buffer_.empty();

    const bool need_more_output =
        (!uncompressed_buffer_.empty() && bytes_written == output_len);

    return DecompressResult{bytes_read, bytes_written, need_more_output};
  }

  bool IsFinished() override { return finished_; }

  Status Reset() override {
    input_buffer_.clear();
    input_offset_ = 0;
    uncompressed_buffer_.clear();
    uncompressed_offset_ = 0;
    finished_ = false;
    saw_eof_ = false;
    saw_stream_identifier_ = false;
    return Status::OK();
  }

 private:
  void ConsumeFromInputBuffer(size_t nbytes) {
    input_offset_ += nbytes;
    if (input_offset_ >= input_buffer_.size()) {
      input_buffer_.clear();
      input_offset_ = 0;
    } else if (input_offset_ > input_buffer_.size() / 2) {
      // Compact the buffer to avoid unbounded growth.
      const auto remaining = input_buffer_.size() - input_offset_;
      std::memmove(input_buffer_.data(), input_buffer_.data() + input_offset_, remaining);
      input_buffer_.resize(remaining);
      input_offset_ = 0;
    }
  }

  std::vector<uint8_t> input_buffer_;
  size_t input_offset_ = 0;
  std::vector<uint8_t> uncompressed_buffer_;
  int64_t uncompressed_offset_ = 0;
  bool finished_ = false;
  bool saw_eof_ = false;
  bool saw_stream_identifier_ = false;
};

// ----------------------------------------------------------------------
// Snappy codec (one-shot raw Snappy bitstream)

class SnappyCodec : public Codec {
 public:
  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    size_t decompressed_size;
    if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(input),
                                       static_cast<size_t>(input_len),
                                       &decompressed_size)) {
      return Status::IOError("Corrupt snappy compressed data.");
    }
    if (output_buffer_len < static_cast<int64_t>(decompressed_size)) {
      return Status::Invalid("Output buffer size (", output_buffer_len, ") must be ",
                             decompressed_size, " or larger.");
    }
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
                               static_cast<size_t>(input_len),
                               reinterpret_cast<char*>(output_buffer))) {
      return Status::IOError("Corrupt snappy compressed data.");
    }
    return static_cast<int64_t>(decompressed_size);
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return snappy::MaxCompressedLength(static_cast<size_t>(input_len));
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t ARROW_ARG_UNUSED(output_buffer_len),
                           uint8_t* output_buffer) override {
    size_t output_size;
    snappy::RawCompress(reinterpret_cast<const char*>(input),
                        static_cast<size_t>(input_len),
                        reinterpret_cast<char*>(output_buffer), &output_size);
    return static_cast<int64_t>(output_size);
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    auto ptr = std::make_shared<SnappyFramedCompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    auto ptr = std::make_shared<SnappyFramedDecompressor>();
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Compression::type compression_type() const override { return Compression::SNAPPY; }
  int minimum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int maximum_compression_level() const override { return kUseDefaultCompressionLevel; }
  int default_compression_level() const override { return kUseDefaultCompressionLevel; }
};

}  // namespace

std::unique_ptr<Codec> MakeSnappyCodec() { return std::make_unique<SnappyCodec>(); }

}  // namespace internal
}  // namespace util
}  // namespace arrow

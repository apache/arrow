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

#include "arrow/io/compressed.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"

namespace arrow {

using util::Codec;
using util::Compressor;
using util::Decompressor;

namespace io {

// ----------------------------------------------------------------------
// CompressedOutputStream implementation

class CompressedOutputStream::Impl {
 public:
  Impl(MemoryPool* pool, const std::shared_ptr<OutputStream>& raw)
      : pool_(pool), raw_(raw), is_open_(false), compressed_pos_(0), total_pos_(0) {}

  Status Init(Codec* codec) {
    ARROW_ASSIGN_OR_RAISE(compressor_, codec->MakeCompressor());
    ARROW_ASSIGN_OR_RAISE(compressed_, AllocateResizableBuffer(kChunkSize, pool_));
    compressed_pos_ = 0;
    is_open_ = true;
    return Status::OK();
  }

  Result<int64_t> Tell() const {
    std::lock_guard<std::mutex> guard(lock_);
    return total_pos_;
  }

  std::shared_ptr<OutputStream> raw() const { return raw_; }

  Status FlushCompressed() {
    if (compressed_pos_ > 0) {
      RETURN_NOT_OK(raw_->Write(compressed_->data(), compressed_pos_));
      compressed_pos_ = 0;
    }
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);

    auto input = reinterpret_cast<const uint8_t*>(data);
    while (nbytes > 0) {
      int64_t input_len = nbytes;
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      ARROW_ASSIGN_OR_RAISE(auto result,
                            compressor_->Compress(input_len, input, output_len, output));
      compressed_pos_ += result.bytes_written;

      if (result.bytes_read == 0) {
        // Not enough output, try to flush it and retry
        if (compressed_pos_ > 0) {
          RETURN_NOT_OK(FlushCompressed());
          output_len = compressed_->size() - compressed_pos_;
          output = compressed_->mutable_data() + compressed_pos_;
          ARROW_ASSIGN_OR_RAISE(
              result, compressor_->Compress(input_len, input, output_len, output));
          compressed_pos_ += result.bytes_written;
        }
      }
      input += result.bytes_read;
      nbytes -= result.bytes_read;
      total_pos_ += result.bytes_read;
      if (compressed_pos_ == compressed_->size()) {
        // Output buffer full, flush it
        RETURN_NOT_OK(FlushCompressed());
      }
      if (result.bytes_read == 0) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      }
    }
    return Status::OK();
  }

  Status Flush() {
    std::lock_guard<std::mutex> guard(lock_);

    while (true) {
      // Flush compressor
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      ARROW_ASSIGN_OR_RAISE(auto result, compressor_->Flush(output_len, output));
      compressed_pos_ += result.bytes_written;

      // Flush compressed output
      RETURN_NOT_OK(FlushCompressed());

      if (result.should_retry) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      } else {
        break;
      }
    }
    return Status::OK();
  }

  Status FinalizeCompression() {
    while (true) {
      // Try to end compressor
      int64_t output_len = compressed_->size() - compressed_pos_;
      uint8_t* output = compressed_->mutable_data() + compressed_pos_;
      ARROW_ASSIGN_OR_RAISE(auto result, compressor_->End(output_len, output));
      compressed_pos_ += result.bytes_written;

      // Flush compressed output
      RETURN_NOT_OK(FlushCompressed());

      if (result.should_retry) {
        // Need to enlarge output buffer
        RETURN_NOT_OK(compressed_->Resize(compressed_->size() * 2));
      } else {
        // Done
        break;
      }
    }
    return Status::OK();
  }

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);

    if (is_open_) {
      is_open_ = false;
      RETURN_NOT_OK(FinalizeCompression());
      return raw_->Close();
    } else {
      return Status::OK();
    }
  }

  Status Abort() {
    std::lock_guard<std::mutex> guard(lock_);

    if (is_open_) {
      is_open_ = false;
      return raw_->Abort();
    } else {
      return Status::OK();
    }
  }

  bool closed() {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

 private:
  // Write 64 KB compressed data at a time
  static const int64_t kChunkSize = 64 * 1024;

  MemoryPool* pool_;
  std::shared_ptr<OutputStream> raw_;
  bool is_open_;
  std::shared_ptr<Compressor> compressor_;
  std::shared_ptr<ResizableBuffer> compressed_;
  int64_t compressed_pos_;
  // Total number of bytes compressed
  int64_t total_pos_;

  mutable std::mutex lock_;
};

Result<std::shared_ptr<CompressedOutputStream>> CompressedOutputStream::Make(
    util::Codec* codec, const std::shared_ptr<OutputStream>& raw, MemoryPool* pool) {
  // CAUTION: codec is not owned
  std::shared_ptr<CompressedOutputStream> res(new CompressedOutputStream);
  res->impl_.reset(new Impl(pool, raw));
  RETURN_NOT_OK(res->impl_->Init(codec));
  return res;
}

CompressedOutputStream::~CompressedOutputStream() { internal::CloseFromDestructor(this); }

Status CompressedOutputStream::Close() { return impl_->Close(); }

Status CompressedOutputStream::Abort() { return impl_->Abort(); }

bool CompressedOutputStream::closed() const { return impl_->closed(); }

Result<int64_t> CompressedOutputStream::Tell() const { return impl_->Tell(); }

Status CompressedOutputStream::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status CompressedOutputStream::Flush() { return impl_->Flush(); }

std::shared_ptr<OutputStream> CompressedOutputStream::raw() const { return impl_->raw(); }

// ----------------------------------------------------------------------
// CompressedInputStream implementation

class CompressedInputStream::Impl {
 public:
  Impl(MemoryPool* pool, const std::shared_ptr<InputStream>& raw)
      : pool_(pool),
        raw_(raw),
        is_open_(true),
        supports_zero_copy_from_raw_(raw_->supports_zero_copy()),
        compressed_pos_(0),
        decompressed_pos_(0),
        fresh_decompressor_(false),
        total_pos_(0) {}

  Status Init(Codec* codec) {
    ARROW_ASSIGN_OR_RAISE(decompressor_, codec->MakeDecompressor());
    fresh_decompressor_ = true;
    return Status::OK();
  }

  Status Close() {
    if (is_open_) {
      is_open_ = false;
      return raw_->Close();
    } else {
      return Status::OK();
    }
  }

  Status Abort() {
    if (is_open_) {
      is_open_ = false;
      return raw_->Abort();
    } else {
      return Status::OK();
    }
  }

  bool closed() const { return !is_open_; }

  Result<int64_t> Tell() const { return total_pos_; }

  // Read compressed data if necessary
  Status EnsureCompressedData() {
    int64_t compressed_avail = compressed_buffer_available();
    if (compressed_avail == 0) {
      // Ensure compressed_ buffer is allocated with kChunkSize.
      if (!supports_zero_copy_from_raw_) {
        if (compressed_for_non_zero_copy_ == nullptr) {
          ARROW_ASSIGN_OR_RAISE(compressed_for_non_zero_copy_,
                                AllocateResizableBuffer(kChunkSize, pool_));
        } else if (compressed_for_non_zero_copy_->size() != kChunkSize) {
          RETURN_NOT_OK(
              compressed_for_non_zero_copy_->Resize(kChunkSize, /*shrink_to_fit=*/false));
        }
        ARROW_ASSIGN_OR_RAISE(
            int64_t read_size,
            raw_->Read(kChunkSize,
                       compressed_for_non_zero_copy_->mutable_data_as<void>()));
        if (read_size != compressed_for_non_zero_copy_->size()) {
          RETURN_NOT_OK(
              compressed_for_non_zero_copy_->Resize(read_size, /*shrink_to_fit=*/false));
        }
        compressed_ = compressed_for_non_zero_copy_;
      } else {
        ARROW_ASSIGN_OR_RAISE(compressed_, raw_->Read(kChunkSize));
      }
      compressed_pos_ = 0;
    }
    return Status::OK();
  }

  // Decompress some data from the compressed_ buffer into decompressor_.
  // Call this function only if the decompressed_ buffer is fully consumed.
  Status DecompressData() {
    // compressed_buffer_available() could be 0 here because there might
    // still be some decompressed data left to emit even though the compressed
    // data was entirely consumed (especially if the expansion factor is large)
    DCHECK_NE(compressed_->data(), nullptr);
    DCHECK_EQ(0, decompressed_buffer_available());

    int64_t decompress_size = kDecompressSize;

    while (true) {
      if (decompressed_ == nullptr) {
        ARROW_ASSIGN_OR_RAISE(decompressed_,
                              AllocateResizableBuffer(decompress_size, pool_));
      } else {
        // Shrinking the buffer if it's already large enough
        RETURN_NOT_OK(decompressed_->Resize(decompress_size, /*shrink_to_fit=*/true));
      }
      decompressed_pos_ = 0;

      int64_t input_len = compressed_->size() - compressed_pos_;
      const uint8_t* input = compressed_->data() + compressed_pos_;
      int64_t output_len = decompressed_->size();
      uint8_t* output = decompressed_->mutable_data();

      ARROW_ASSIGN_OR_RAISE(
          auto result, decompressor_->Decompress(input_len, input, output_len, output));
      compressed_pos_ += result.bytes_read;
      if (result.bytes_read > 0) {
        fresh_decompressor_ = false;
      }
      if (result.bytes_written > 0 || !result.need_more_output || input_len == 0) {
        // Not calling shrink_to_fit here because we're likely to reusing the buffer.
        RETURN_NOT_OK(
            decompressed_->Resize(result.bytes_written, /*shrink_to_fit=*/false));
        break;
      }
      DCHECK_EQ(result.bytes_written, 0);
      // Need to enlarge output buffer
      decompress_size *= 2;
    }
    return Status::OK();
  }

  // Copying a given number of bytes from the decompressed_ buffer.
  int64_t ReadFromDecompressed(int64_t nbytes, uint8_t* out) {
    int64_t readable = decompressed_ ? (decompressed_->size() - decompressed_pos_) : 0;
    int64_t read_bytes = std::min(readable, nbytes);

    if (read_bytes > 0) {
      memcpy(out, decompressed_->data() + decompressed_pos_, read_bytes);
      decompressed_pos_ += read_bytes;
    }

    return read_bytes;
  }

  // Try to feed more data into the decompressed_ buffer.
  // Returns whether there is more data to read.
  Result<bool> RefillDecompressed() {
    // First try to read data from the decompressor, unless we haven't read any
    // compressed data yet.
    if (compressed_ && compressed_->size() != 0) {
      if (decompressor_->IsFinished()) {
        // We just went over the end of a previous compressed stream.
        RETURN_NOT_OK(decompressor_->Reset());
        fresh_decompressor_ = true;
      }
      RETURN_NOT_OK(DecompressData());
    }
    int64_t decompress_avail = decompressed_buffer_available();
    if (decompress_avail == 0) {
      // Got nothing from existing `compressed_` and `decompressor_`,
      // need to read more compressed data.
      RETURN_NOT_OK(EnsureCompressedData());
      if (compressed_buffer_available() == 0) {
        // No more data to decompress
        if (!fresh_decompressor_ && !decompressor_->IsFinished()) {
          return Status::IOError("Truncated compressed stream");
        }
        return false;
      }
      RETURN_NOT_OK(DecompressData());
    }
    return true;
  }

  Result<int64_t> Read(int64_t nbytes, void* out) {
    auto* out_data = reinterpret_cast<uint8_t*>(out);

    int64_t total_read = 0;
    bool decompressor_has_data = true;

    while (nbytes - total_read > 0 && decompressor_has_data) {
      total_read += ReadFromDecompressed(nbytes - total_read, out_data + total_read);

      if (nbytes == total_read) {
        break;
      }

      // At this point, no more decompressed data remains, so we need to
      // decompress more
      ARROW_ASSIGN_OR_RAISE(decompressor_has_data, RefillDecompressed());
    }

    total_pos_ += total_read;
    return total_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes, pool_));
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buf->mutable_data()));
    RETURN_NOT_OK(buf->Resize(bytes_read));
    // R build with openSUSE155 requires an explicit shared_ptr construction
    return std::shared_ptr<Buffer>(std::move(buf));
  }

  const std::shared_ptr<InputStream>& raw() const { return raw_; }

 private:
  int64_t compressed_buffer_available() const {
    return compressed_ ? compressed_->size() - compressed_pos_ : 0;
  }

  int64_t decompressed_buffer_available() const {
    return decompressed_ ? decompressed_->size() - decompressed_pos_ : 0;
  }

 private:
  // Read 64 KB compressed data at a time
  static const int64_t kChunkSize = 64 * 1024;
  // Decompress 1 MB at a time
  static const int64_t kDecompressSize = 1024 * 1024;

  MemoryPool* pool_;
  std::shared_ptr<InputStream> raw_;
  bool is_open_;
  const bool supports_zero_copy_from_raw_;
  std::shared_ptr<Decompressor> decompressor_;
  // If `raw_->supports_zero_copy()`, this buffer would not allocate memory.
  // Otherwise, this buffer would allocate `kChunkSize` memory and read data from
  // `raw_`.
  std::shared_ptr<ResizableBuffer> compressed_for_non_zero_copy_;
  std::shared_ptr<Buffer> compressed_;
  // Position in compressed buffer
  int64_t compressed_pos_;
  std::shared_ptr<ResizableBuffer> decompressed_;
  // Position in decompressed buffer
  int64_t decompressed_pos_;
  // True if the decompressor hasn't read any data yet.
  bool fresh_decompressor_;
  // Total number of bytes decompressed
  int64_t total_pos_;
};

Result<std::shared_ptr<CompressedInputStream>> CompressedInputStream::Make(
    Codec* codec, const std::shared_ptr<InputStream>& raw, MemoryPool* pool) {
  // CAUTION: codec is not owned
  std::shared_ptr<CompressedInputStream> res(new CompressedInputStream);
  res->impl_.reset(new Impl(pool, raw));
  RETURN_NOT_OK(res->impl_->Init(codec));
  return res;
}

CompressedInputStream::~CompressedInputStream() { internal::CloseFromDestructor(this); }

Status CompressedInputStream::DoClose() { return impl_->Close(); }

Status CompressedInputStream::DoAbort() { return impl_->Abort(); }

bool CompressedInputStream::closed() const { return impl_->closed(); }

Result<int64_t> CompressedInputStream::DoTell() const { return impl_->Tell(); }

Result<int64_t> CompressedInputStream::DoRead(int64_t nbytes, void* out) {
  return impl_->Read(nbytes, out);
}

Result<std::shared_ptr<Buffer>> CompressedInputStream::DoRead(int64_t nbytes) {
  return impl_->Read(nbytes);
}

std::shared_ptr<InputStream> CompressedInputStream::raw() const { return impl_->raw(); }

Result<std::shared_ptr<const KeyValueMetadata>> CompressedInputStream::ReadMetadata() {
  return impl_->raw()->ReadMetadata();
}

Future<std::shared_ptr<const KeyValueMetadata>> CompressedInputStream::ReadMetadataAsync(
    const IOContext& io_context) {
  return impl_->raw()->ReadMetadataAsync(io_context);
}

}  // namespace io
}  // namespace arrow

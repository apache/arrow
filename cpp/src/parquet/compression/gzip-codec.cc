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

#include <cstring>
#include <sstream>
#include <string>

#include "parquet/compression/codec.h"
#include "parquet/exception.h"

namespace parquet {

// These are magic numbers from zlib.h.  Not clear why they are not defined
// there.

// Maximum window size
static constexpr int WINDOW_BITS = 15;

// Output Gzip.
static constexpr int GZIP_CODEC = 16;

// Determine if this is libz or gzip from header.
static constexpr int DETECT_CODEC = 32;

GZipCodec::GZipCodec(Format format)
    : format_(format), compressor_initialized_(false), decompressor_initialized_(false) {}

GZipCodec::~GZipCodec() {
  EndCompressor();
  EndDecompressor();
}

void GZipCodec::InitCompressor() {
  EndDecompressor();
  memset(&stream_, 0, sizeof(stream_));

  int ret;
  // Initialize to run specified format
  int window_bits = WINDOW_BITS;
  if (format_ == DEFLATE) {
    window_bits = -window_bits;
  } else if (format_ == GZIP) {
    window_bits += GZIP_CODEC;
  }
  if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED, window_bits, 9,
           Z_DEFAULT_STRATEGY)) != Z_OK) {
    throw ParquetException("zlib deflateInit failed: " + std::string(stream_.msg));
  }

  compressor_initialized_ = true;
}

void GZipCodec::EndCompressor() {
  if (compressor_initialized_) { (void)deflateEnd(&stream_); }
  compressor_initialized_ = false;
}

void GZipCodec::InitDecompressor() {
  EndCompressor();
  memset(&stream_, 0, sizeof(stream_));
  int ret;

  // Initialize to run either deflate or zlib/gzip format
  int window_bits = format_ == DEFLATE ? -WINDOW_BITS : WINDOW_BITS | DETECT_CODEC;
  if ((ret = inflateInit2(&stream_, window_bits)) != Z_OK) {
    throw ParquetException("zlib inflateInit failed: " + std::string(stream_.msg));
  }
  decompressor_initialized_ = true;
}

void GZipCodec::EndDecompressor() {
  if (decompressor_initialized_) { (void)inflateEnd(&stream_); }
  decompressor_initialized_ = false;
}

void GZipCodec::Decompress(
    int64_t input_length, const uint8_t* input, int64_t output_length, uint8_t* output) {
  if (!decompressor_initialized_) { InitDecompressor(); }
  if (output_length == 0) {
    // The zlib library does not allow *output to be NULL, even when output_length
    // is 0 (inflate() will return Z_STREAM_ERROR). We don't consider this an
    // error, so bail early if no output is expected. Note that we don't signal
    // an error if the input actually contains compressed data.
    return;
  }

  // Reset the stream for this block
  if (inflateReset(&stream_) != Z_OK) {
    throw ParquetException("zlib inflateReset failed: " + std::string(stream_.msg));
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
    stream_.avail_in = input_length;
    stream_.next_out = reinterpret_cast<Bytef*>(output);
    stream_.avail_out = output_length;

    // We know the output size.  In this case, we can use Z_FINISH
    // which is more efficient.
    ret = inflate(&stream_, Z_FINISH);
    if (ret == Z_STREAM_END || ret != Z_OK) break;

    // Failure, buffer was too small
    std::stringstream ss;
    ss << "Too small a buffer passed to GZipCodec. InputLength=" << input_length
       << " OutputLength=" << output_length;
    throw ParquetException(ss.str());
  }

  // Failure for some other reason
  if (ret != Z_STREAM_END) {
    std::stringstream ss;
    ss << "GZipCodec failed: ";
    if (stream_.msg != NULL) ss << stream_.msg;
    throw ParquetException(ss.str());
  }
}

int64_t GZipCodec::MaxCompressedLen(int64_t input_length, const uint8_t* input) {
  // Most be in compression mode
  if (!compressor_initialized_) { InitCompressor(); }
  // TODO(wesm): deal with zlib < 1.2.3 (see Impala codebase)
  return deflateBound(&stream_, static_cast<uLong>(input_length));
}

int64_t GZipCodec::Compress(
    int64_t input_length, const uint8_t* input, int64_t output_length, uint8_t* output) {
  if (!compressor_initialized_) { InitCompressor(); }
  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = output_length;

  int64_t ret = 0;
  if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
    if (ret == Z_OK) {
      // will return Z_OK (and stream.msg NOT set) if stream.avail_out is too
      // small
      throw ParquetException("zlib deflate failed, output buffer to small");
    }
    std::stringstream ss;
    ss << "zlib deflate failed: " << stream_.msg;
    throw ParquetException(ss.str());
  }

  if (deflateReset(&stream_) != Z_OK) {
    throw ParquetException("zlib deflateReset failed: " + std::string(stream_.msg));
  }

  // Actual output length
  return output_length - stream_.avail_out;
}

}  // namespace parquet

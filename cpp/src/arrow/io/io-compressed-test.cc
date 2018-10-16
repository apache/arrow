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

#include <memory>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/io/test-common.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace io {

using ::arrow::util::Codec;

std::vector<uint8_t> MakeRandomData(int data_size) {
  std::vector<uint8_t> data(data_size);
  random_bytes(data_size, 1234, data.data());
  return data;
}

std::vector<uint8_t> MakeCompressibleData(int data_size) {
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  std::vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);
  return data;
}

void CheckCompressedInputStream(Codec* codec, const std::vector<uint8_t>& data) {
  // Create compressed data
  int64_t max_compressed_len, compressed_len;
  max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  std::shared_ptr<ResizableBuffer> compressed;
  ASSERT_OK(AllocateResizableBuffer(max_compressed_len, &compressed));
  ASSERT_OK(codec->Compress(data.size(), data.data(), max_compressed_len,
                            compressed->mutable_data(), &compressed_len));
  ASSERT_OK(compressed->Resize(compressed_len));

  // Create compressed input stream
  auto buffer_reader = std::make_shared<BufferReader>(compressed);
  std::shared_ptr<CompressedInputStream> stream;
  ASSERT_OK(CompressedInputStream::Make(codec, buffer_reader, &stream));

  std::vector<uint8_t> decompressed;
  int64_t decompressed_size = 0;
  const int64_t chunk_size = 1111;
  while (true) {
    std::shared_ptr<Buffer> buf;
    ASSERT_OK(stream->Read(chunk_size, &buf));
    ASSERT_LE(buf->size(), chunk_size);
    if (buf->size() == 0) {
      // EOF
      break;
    }
    decompressed.resize(decompressed_size + buf->size());
    memcpy(decompressed.data() + decompressed_size, buf->data(), buf->size());
    decompressed_size += buf->size();
  }

  ASSERT_EQ(decompressed_size, data.size());
  ASSERT_EQ(decompressed, data);
}

void CheckCompressedOutputStream(Codec* codec, const std::vector<uint8_t>& data) {
  // Create compressed output stream
  std::shared_ptr<BufferOutputStream> buffer_writer;
  ASSERT_OK(BufferOutputStream::Create(1024, default_memory_pool(), &buffer_writer));
  std::shared_ptr<CompressedOutputStream> stream;
  ASSERT_OK(CompressedOutputStream::Make(codec, buffer_writer, &stream));

  const uint8_t* input = data.data();
  int64_t input_len = data.size();
  const int64_t chunk_size = 1111;
  while (input_len > 0) {
    int64_t nbytes = std::min(chunk_size, input_len);
    ASSERT_OK(stream->Write(input, nbytes));
    input += nbytes;
    input_len -= nbytes;
  }
  ASSERT_OK(stream->Close());

  // Get compressed data and decompress it
  std::shared_ptr<Buffer> compressed;
  ASSERT_OK(buffer_writer->Finish(&compressed));
  std::vector<uint8_t> decompressed(data.size());
  ASSERT_OK(codec->Decompress(compressed->size(), compressed->data(), decompressed.size(),
                              decompressed.data()));
  ASSERT_EQ(decompressed, data);
}

class CompressedInputStreamTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() {
    std::unique_ptr<Codec> codec;
    ABORT_NOT_OK(Codec::Create(GetCompression(), &codec));
    return codec;
  }
};

TEST_P(CompressedInputStreamTest, CompressibleData) {
  auto codec = MakeCodec();
  // The data must be large enough to exercise internal buffers
  auto data = MakeCompressibleData(8 * 1024 * 1024);

  CheckCompressedInputStream(codec.get(), data);
}

TEST_P(CompressedInputStreamTest, RandomData) {
  auto codec = MakeCodec();
  // The data must be large enough to exercise internal buffers
  auto data = MakeRandomData(3 * 1024 * 1024);

  CheckCompressedInputStream(codec.get(), data);
}

// NOTE: Snappy doesn't support streaming decompression

// NOTE: LZ4 streaming decompression uses the LZ4 framing format,
// which must be tested against a streaming compressor

INSTANTIATE_TEST_CASE_P(TestGZipInputStream, CompressedInputStreamTest,
                        ::testing::Values(Compression::GZIP));

INSTANTIATE_TEST_CASE_P(TestZSTDInputStream, CompressedInputStreamTest,
                        ::testing::Values(Compression::ZSTD));

INSTANTIATE_TEST_CASE_P(TestBrotliInputStream, CompressedInputStreamTest,
                        ::testing::Values(Compression::BROTLI));

class CompressedOutputStreamTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() {
    std::unique_ptr<Codec> codec;
    ABORT_NOT_OK(Codec::Create(GetCompression(), &codec));
    return codec;
  }
};

TEST_P(CompressedOutputStreamTest, CompressibleData) {
  auto codec = MakeCodec();
  auto data = MakeCompressibleData(8 * 1024 * 1024);

  CheckCompressedOutputStream(codec.get(), data);
}

TEST_P(CompressedOutputStreamTest, RandomData) {
  auto codec = MakeCodec();
  auto data = MakeRandomData(3 * 1024 * 1024);

  CheckCompressedOutputStream(codec.get(), data);
}

INSTANTIATE_TEST_CASE_P(TestGZipOutputStream, CompressedOutputStreamTest,
                        ::testing::Values(Compression::GZIP));

INSTANTIATE_TEST_CASE_P(TestZSTDOutputStream, CompressedOutputStreamTest,
                        ::testing::Values(Compression::ZSTD));

INSTANTIATE_TEST_CASE_P(TestBrotliOutputStream, CompressedOutputStreamTest,
                        ::testing::Values(Compression::BROTLI));

}  // namespace io
}  // namespace arrow

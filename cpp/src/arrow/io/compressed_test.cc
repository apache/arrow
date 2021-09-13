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

#include <algorithm>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace io {

using ::arrow::util::Codec;

#ifdef ARROW_VALGRIND
// Avoid slowing down tests too much with Valgrind
static constexpr int64_t RANDOM_DATA_SIZE = 50 * 1024;
static constexpr int64_t COMPRESSIBLE_DATA_SIZE = 120 * 1024;
#else
// The data should be large enough to exercise internal buffers
static constexpr int64_t RANDOM_DATA_SIZE = 3 * 1024 * 1024;
static constexpr int64_t COMPRESSIBLE_DATA_SIZE = 8 * 1024 * 1024;
#endif

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

std::shared_ptr<Buffer> CompressDataOneShot(Codec* codec,
                                            const std::vector<uint8_t>& data) {
  int64_t max_compressed_len, compressed_len;
  max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  auto compressed = *AllocateResizableBuffer(max_compressed_len);
  compressed_len = *codec->Compress(data.size(), data.data(), max_compressed_len,
                                    compressed->mutable_data());
  ABORT_NOT_OK(compressed->Resize(compressed_len));
  return std::move(compressed);
}

Status RunCompressedInputStream(Codec* codec, std::shared_ptr<Buffer> compressed,
                                int64_t* stream_pos, std::vector<uint8_t>* out) {
  // Create compressed input stream
  auto buffer_reader = std::make_shared<BufferReader>(compressed);
  ARROW_ASSIGN_OR_RAISE(auto stream, CompressedInputStream::Make(codec, buffer_reader));

  std::vector<uint8_t> decompressed;
  int64_t decompressed_size = 0;
  const int64_t chunk_size = 1111;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto buf, stream->Read(chunk_size));
    if (buf->size() == 0) {
      // EOF
      break;
    }
    decompressed.resize(decompressed_size + buf->size());
    memcpy(decompressed.data() + decompressed_size, buf->data(), buf->size());
    decompressed_size += buf->size();
  }
  if (stream_pos != nullptr) {
    RETURN_NOT_OK(stream->Tell().Value(stream_pos));
  }
  *out = std::move(decompressed);
  return Status::OK();
}

Status RunCompressedInputStream(Codec* codec, std::shared_ptr<Buffer> compressed,
                                std::vector<uint8_t>* out) {
  return RunCompressedInputStream(codec, compressed, nullptr, out);
}

void CheckCompressedInputStream(Codec* codec, const std::vector<uint8_t>& data) {
  // Create compressed data
  auto compressed = CompressDataOneShot(codec, data);

  std::vector<uint8_t> decompressed;
  int64_t stream_pos = -1;
  ASSERT_OK(RunCompressedInputStream(codec, compressed, &stream_pos, &decompressed));

  ASSERT_EQ(decompressed.size(), data.size());
  ASSERT_EQ(decompressed, data);
  ASSERT_EQ(stream_pos, static_cast<int64_t>(decompressed.size()));
}

void CheckCompressedOutputStream(Codec* codec, const std::vector<uint8_t>& data,
                                 bool do_flush) {
  // Create compressed output stream
  ASSERT_OK_AND_ASSIGN(auto buffer_writer, BufferOutputStream::Create());
  ASSERT_OK_AND_ASSIGN(auto stream, CompressedOutputStream::Make(codec, buffer_writer));
  ASSERT_OK_AND_EQ(0, stream->Tell());

  const uint8_t* input = data.data();
  int64_t input_len = data.size();
  const int64_t chunk_size = 1111;
  while (input_len > 0) {
    int64_t nbytes = std::min(chunk_size, input_len);
    ASSERT_OK(stream->Write(input, nbytes));
    input += nbytes;
    input_len -= nbytes;
    if (do_flush) {
      ASSERT_OK(stream->Flush());
    }
  }
  ASSERT_OK_AND_EQ(static_cast<int64_t>(data.size()), stream->Tell());
  ASSERT_OK(stream->Close());

  // Get compressed data and decompress it
  ASSERT_OK_AND_ASSIGN(auto compressed, buffer_writer->Finish());
  std::vector<uint8_t> decompressed(data.size());
  ASSERT_OK(codec->Decompress(compressed->size(), compressed->data(), decompressed.size(),
                              decompressed.data()));
  ASSERT_EQ(decompressed, data);
}

class CompressedInputStreamTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() { return *Codec::Create(GetCompression()); }
};

class CompressedOutputStreamTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() { return *Codec::Create(GetCompression()); }
};

TEST_P(CompressedInputStreamTest, CompressibleData) {
  auto codec = MakeCodec();
  auto data = MakeCompressibleData(COMPRESSIBLE_DATA_SIZE);

  CheckCompressedInputStream(codec.get(), data);
}

TEST_P(CompressedInputStreamTest, RandomData) {
  auto codec = MakeCodec();
  auto data = MakeRandomData(RANDOM_DATA_SIZE);

  CheckCompressedInputStream(codec.get(), data);
}

TEST_P(CompressedInputStreamTest, TruncatedData) {
  auto codec = MakeCodec();
  auto data = MakeRandomData(10000);
  auto compressed = CompressDataOneShot(codec.get(), data);
  auto truncated = SliceBuffer(compressed, 0, compressed->size() - 3);

  std::vector<uint8_t> decompressed;
  ASSERT_RAISES(IOError, RunCompressedInputStream(codec.get(), truncated, &decompressed));
}

TEST_P(CompressedInputStreamTest, InvalidData) {
  auto codec = MakeCodec();
  auto compressed_data = MakeRandomData(100);

  auto buffer_reader = std::make_shared<BufferReader>(Buffer::Wrap(compressed_data));
  ASSERT_OK_AND_ASSIGN(auto stream,
                       CompressedInputStream::Make(codec.get(), buffer_reader));
  ASSERT_RAISES(IOError, stream->Read(1024));
}

TEST_P(CompressedInputStreamTest, ConcatenatedStreams) {
  // ARROW-5974: just like the "gunzip", "bzip2" and "xz" commands,
  // decompressing concatenated compressed streams should yield the entire
  // original data.
  auto codec = MakeCodec();
  auto data1 = MakeCompressibleData(100);
  auto data2 = MakeCompressibleData(200);
  auto compressed1 = CompressDataOneShot(codec.get(), data1);
  auto compressed2 = CompressDataOneShot(codec.get(), data2);
  std::vector<uint8_t> expected;
  std::copy(data1.begin(), data1.end(), std::back_inserter(expected));
  std::copy(data2.begin(), data2.end(), std::back_inserter(expected));

  ASSERT_OK_AND_ASSIGN(auto concatenated, ConcatenateBuffers({compressed1, compressed2}));
  std::vector<uint8_t> decompressed;
  ASSERT_OK(RunCompressedInputStream(codec.get(), concatenated, &decompressed));
  ASSERT_EQ(decompressed.size(), expected.size());
  ASSERT_EQ(decompressed, expected);

  // Same, but with an empty decompressed stream in the middle
  auto compressed_empty = CompressDataOneShot(codec.get(), {});
  ASSERT_OK_AND_ASSIGN(concatenated,
                       ConcatenateBuffers({compressed1, compressed_empty, compressed2}));
  ASSERT_OK(RunCompressedInputStream(codec.get(), concatenated, &decompressed));
  ASSERT_EQ(decompressed.size(), expected.size());
  ASSERT_EQ(decompressed, expected);

  // Same, but with an empty decompressed stream at the end
  ASSERT_OK_AND_ASSIGN(concatenated,
                       ConcatenateBuffers({compressed1, compressed2, compressed_empty}));
  ASSERT_OK(RunCompressedInputStream(codec.get(), concatenated, &decompressed));
  ASSERT_EQ(decompressed.size(), expected.size());
  ASSERT_EQ(decompressed, expected);
}

TEST_P(CompressedOutputStreamTest, CompressibleData) {
  auto codec = MakeCodec();
  auto data = MakeCompressibleData(COMPRESSIBLE_DATA_SIZE);

  CheckCompressedOutputStream(codec.get(), data, false /* do_flush */);
  CheckCompressedOutputStream(codec.get(), data, true /* do_flush */);
}

TEST_P(CompressedOutputStreamTest, RandomData) {
  auto codec = MakeCodec();
  auto data = MakeRandomData(RANDOM_DATA_SIZE);

  CheckCompressedOutputStream(codec.get(), data, false /* do_flush */);
  CheckCompressedOutputStream(codec.get(), data, true /* do_flush */);
}

// NOTES:
// - Snappy doesn't support streaming decompression
// - BZ2 doesn't support one-shot compression
// - LZ4 raw format doesn't support streaming decompression

#ifdef ARROW_WITH_SNAPPY
TEST(TestSnappyInputStream, NotImplemented) {
  std::unique_ptr<Codec> codec;
  ASSERT_OK_AND_ASSIGN(codec, Codec::Create(Compression::SNAPPY));
  std::shared_ptr<InputStream> stream = std::make_shared<BufferReader>("");
  ASSERT_RAISES(NotImplemented, CompressedInputStream::Make(codec.get(), stream));
}

TEST(TestSnappyOutputStream, NotImplemented) {
  std::unique_ptr<Codec> codec;
  ASSERT_OK_AND_ASSIGN(codec, Codec::Create(Compression::SNAPPY));
  std::shared_ptr<OutputStream> stream = std::make_shared<MockOutputStream>();
  ASSERT_RAISES(NotImplemented, CompressedOutputStream::Make(codec.get(), stream));
}
#endif

#if !defined ARROW_WITH_ZLIB && !defined ARROW_WITH_BROTLI && !defined ARROW_WITH_LZ4 && \
    !defined ARROW_WITH_ZSTD
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(CompressedInputStreamTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(CompressedOutputStreamTest);
#endif

#ifdef ARROW_WITH_ZLIB
INSTANTIATE_TEST_SUITE_P(TestGZipInputStream, CompressedInputStreamTest,
                         ::testing::Values(Compression::GZIP));
INSTANTIATE_TEST_SUITE_P(TestGZipOutputStream, CompressedOutputStreamTest,
                         ::testing::Values(Compression::GZIP));
#endif

#ifdef ARROW_WITH_BROTLI
INSTANTIATE_TEST_SUITE_P(TestBrotliInputStream, CompressedInputStreamTest,
                         ::testing::Values(Compression::BROTLI));
INSTANTIATE_TEST_SUITE_P(TestBrotliOutputStream, CompressedOutputStreamTest,
                         ::testing::Values(Compression::BROTLI));
#endif

#ifdef ARROW_WITH_LZ4
INSTANTIATE_TEST_SUITE_P(TestLZ4InputStream, CompressedInputStreamTest,
                         ::testing::Values(Compression::LZ4_FRAME));
INSTANTIATE_TEST_SUITE_P(TestLZ4OutputStream, CompressedOutputStreamTest,
                         ::testing::Values(Compression::LZ4_FRAME));
#endif

#ifdef ARROW_WITH_ZSTD
INSTANTIATE_TEST_SUITE_P(TestZSTDInputStream, CompressedInputStreamTest,
                         ::testing::Values(Compression::ZSTD));
INSTANTIATE_TEST_SUITE_P(TestZSTDOutputStream, CompressedOutputStreamTest,
                         ::testing::Values(Compression::ZSTD));
#endif

}  // namespace io
}  // namespace arrow

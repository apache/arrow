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
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/compression.h"

namespace arrow {
namespace util {

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

// Check roundtrip of one-shot compression and decompression functions.
void CheckCodecRoundtrip(std::unique_ptr<Codec>& c1, std::unique_ptr<Codec>& c2,
                         const std::vector<uint8_t>& data, bool check_reverse = true) {
  int max_compressed_len =
      static_cast<int>(c1->MaxCompressedLen(data.size(), data.data()));
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size());

  // compress with c1
  int64_t actual_size;
  ASSERT_OK_AND_ASSIGN(actual_size, c1->Compress(data.size(), data.data(),
                                                 max_compressed_len, compressed.data()));
  compressed.resize(actual_size);

  // decompress with c2
  int64_t actual_decompressed_size;
  ASSERT_OK_AND_ASSIGN(actual_decompressed_size,
                       c2->Decompress(compressed.size(), compressed.data(),
                                      decompressed.size(), decompressed.data()));

  ASSERT_EQ(data, decompressed);
  ASSERT_EQ(data.size(), actual_decompressed_size);

  if (check_reverse) {
    // compress with c2
    ASSERT_EQ(max_compressed_len,
              static_cast<int>(c2->MaxCompressedLen(data.size(), data.data())));
    // Resize to prevent ASAN from detecting container overflow.
    compressed.resize(max_compressed_len);

    int64_t actual_size2;
    ASSERT_OK_AND_ASSIGN(
        actual_size2,
        c2->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
    ASSERT_EQ(actual_size2, actual_size);
    compressed.resize(actual_size2);

    // decompress with c1
    int64_t actual_decompressed_size2;
    ASSERT_OK_AND_ASSIGN(actual_decompressed_size2,
                         c1->Decompress(compressed.size(), compressed.data(),
                                        decompressed.size(), decompressed.data()));

    ASSERT_EQ(data, decompressed);
    ASSERT_EQ(data.size(), actual_decompressed_size2);
  }
}

// Check the streaming compressor against one-shot decompression

void CheckStreamingCompressor(Codec* codec, const std::vector<uint8_t>& data) {
  std::shared_ptr<Compressor> compressor;
  ASSERT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());

  ASSERT_OK_AND_ASSIGN(auto compressed_result,
                       compressor->Compress(data.size(), data.data()));

  ASSERT_EQ(compressed_result.bytes_read, data.size());
  ASSERT_LE(compressed_result.bytes_written, data.size());

  ASSERT_OK_AND_ASSIGN(auto compressed, compressor->Finish());

  // Check decompressing the compressed data
  std::vector<uint8_t> decompressed(data.size());
  ASSERT_OK(codec->Decompress(compressed->size(), compressed->data(), decompressed.size(),
                              decompressed.data()));

  ASSERT_EQ(data, decompressed);
}

// Check the streaming decompressor against one-shot compression

void CheckStreamingDecompressor(Codec* codec, const std::vector<uint8_t>& data) {
  // Create compressed data
  int64_t max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  std::vector<uint8_t> compressed(max_compressed_len);
  int64_t compressed_size;
  ASSERT_OK_AND_ASSIGN(
      compressed_size,
      codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
  compressed.resize(compressed_size);

  // Run streaming decompression
  std::shared_ptr<Decompressor> decompressor;
  ASSERT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

  ASSERT_OK_AND_ASSIGN(auto decompress_result,
                       decompressor->Decompress(compressed.size(), compressed.data()));
  ASSERT_EQ(decompress_result.bytes_read, compressed.size());
  ASSERT_GE(decompress_result.bytes_written, compressed.size());

  ASSERT_OK_AND_ASSIGN(auto decompressed, decompressor->Finish());

  // Check the decompressed data
  auto data_ = Buffer::FromVector(data);
  ASSERT_EQ(data_->size(), decompressed->size());
  ASSERT_EQ(data_, decompressed);
}

// Check the streaming compressor and decompressor together

void CheckStreamingRoundtrip(std::shared_ptr<Compressor> compressor,
                             std::shared_ptr<Decompressor> decompressor,
                             const std::vector<uint8_t>& data) {
  // Compress...
  ASSERT_OK(compressor->Compress(data.size(), data.data()));
  ASSERT_OK_AND_ASSIGN(auto compressed, compressor->Finish());

  // Then decompress...
  ASSERT_OK(decompressor->Decompress(compressed->size(), compressed->data()));
  ASSERT_OK_AND_ASSIGN(auto decompressed, decompressor->Finish());

  auto data_ = Buffer::FromVector(data);
  ASSERT_EQ(data_->size(), decompressed->size());
  ASSERT_EQ(data_, decompressed);
}

void CheckStreamingRoundtrip(Codec* codec, const std::vector<uint8_t>& data) {
  std::shared_ptr<Compressor> compressor;
  std::shared_ptr<Decompressor> decompressor;
  ASSERT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
  ASSERT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

  CheckStreamingRoundtrip(compressor, decompressor, data);
}

class CodecTest : public ::testing::TestWithParam<Compression::type> {
 protected:
  Compression::type GetCompression() { return GetParam(); }

  std::unique_ptr<Codec> MakeCodec() { return *Codec::Create(GetCompression()); }
};

TEST(TestCodecMisc, GetCodecAsString) {
  EXPECT_EQ(Codec::GetCodecAsString(Compression::UNCOMPRESSED), "uncompressed");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::SNAPPY), "snappy");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::GZIP), "gzip");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::LZO), "lzo");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::BROTLI), "brotli");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::LZ4), "lz4_raw");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::LZ4_FRAME), "lz4");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::ZSTD), "zstd");
  EXPECT_EQ(Codec::GetCodecAsString(Compression::BZ2), "bz2");
}

TEST(TestCodecMisc, GetCompressionType) {
  ASSERT_OK_AND_EQ(Compression::UNCOMPRESSED, Codec::GetCompressionType("uncompressed"));
  ASSERT_OK_AND_EQ(Compression::SNAPPY, Codec::GetCompressionType("snappy"));
  ASSERT_OK_AND_EQ(Compression::GZIP, Codec::GetCompressionType("gzip"));
  ASSERT_OK_AND_EQ(Compression::LZO, Codec::GetCompressionType("lzo"));
  ASSERT_OK_AND_EQ(Compression::BROTLI, Codec::GetCompressionType("brotli"));
  ASSERT_OK_AND_EQ(Compression::LZ4, Codec::GetCompressionType("lz4_raw"));
  ASSERT_OK_AND_EQ(Compression::LZ4_FRAME, Codec::GetCompressionType("lz4"));
  ASSERT_OK_AND_EQ(Compression::ZSTD, Codec::GetCompressionType("zstd"));
  ASSERT_OK_AND_EQ(Compression::BZ2, Codec::GetCompressionType("bz2"));

  ASSERT_RAISES(Invalid, Codec::GetCompressionType("unk"));
  ASSERT_RAISES(Invalid, Codec::GetCompressionType("SNAPPY"));
}

TEST_P(CodecTest, CodecRoundtrip) {
  const auto compression = GetCompression();
  if (compression == Compression::BZ2) {
    GTEST_SKIP() << "BZ2 does not support one-shot compression";
  }

  int sizes[] = {0, 10000, 100000};

  // create multiple compressors to try to break them
  std::unique_ptr<Codec> c1, c2;
  ASSERT_OK_AND_ASSIGN(c1, Codec::Create(compression));
  ASSERT_OK_AND_ASSIGN(c2, Codec::Create(compression));

  for (int data_size : sizes) {
    std::vector<uint8_t> data = MakeRandomData(data_size);
    CheckCodecRoundtrip(c1, c2, data);

    data = MakeCompressibleData(data_size);
    CheckCodecRoundtrip(c1, c2, data);
  }
}

TEST(TestCodecMisc, SpecifyCompressionLevel) {
  struct CombinationOption {
    Compression::type codec;
    int level;
    bool expect_success;
  };
  constexpr CombinationOption combinations[] = {
      {Compression::GZIP, 2, true},     {Compression::BROTLI, 10, true},
      {Compression::ZSTD, 4, true},     {Compression::LZ4, 10, true},
      {Compression::LZO, -22, false},   {Compression::UNCOMPRESSED, 10, false},
      {Compression::SNAPPY, 16, false}, {Compression::GZIP, -992, false},
      {Compression::LZ4_FRAME, 9, true}};

  std::vector<uint8_t> data = MakeRandomData(2000);
  for (const auto& combination : combinations) {
    const auto compression = combination.codec;
    if (!Codec::IsAvailable(compression)) {
      // Support for this codec hasn't been built
      continue;
    }
    const auto level = combination.level;
    const auto codec_options = arrow::util::CodecOptions(level);
    const auto expect_success = combination.expect_success;
    auto result1 = Codec::Create(compression, codec_options);
    auto result2 = Codec::Create(compression, codec_options);
    ASSERT_EQ(expect_success, result1.ok());
    ASSERT_EQ(expect_success, result2.ok());
    if (expect_success) {
      CheckCodecRoundtrip(*result1, *result2, data);
    }
  }
}

TEST(TestCodecMisc, SpecifyCodecOptionsGZip) {
  // for now only GZIP & Brotli codec options supported, since it has specific parameters
  // to be customized, other codecs could directly go with CodecOptions, could add more
  // specific codec options if needed.
  struct CombinationOption {
    int level;
    GZipFormat format;
    int window_bits;
    bool expect_success;
  };
  constexpr CombinationOption combinations[] = {{2, GZipFormat::ZLIB, 12, true},
                                                {9, GZipFormat::GZIP, 9, true},
                                                {9, GZipFormat::GZIP, 20, false},
                                                {5, GZipFormat::DEFLATE, -12, false},
                                                {-992, GZipFormat::GZIP, 15, false}};

  std::vector<uint8_t> data = MakeRandomData(2000);
  for (const auto& combination : combinations) {
    const auto compression = Compression::GZIP;
    if (!Codec::IsAvailable(compression)) {
      // Support for this codec hasn't been built
      continue;
    }
    auto codec_options = arrow::util::GZipCodecOptions();
    codec_options.compression_level = combination.level;
    codec_options.gzip_format = combination.format;
    codec_options.window_bits = combination.window_bits;
    const auto expect_success = combination.expect_success;
    auto result1 = Codec::Create(compression, codec_options);
    auto result2 = Codec::Create(compression, codec_options);
    ASSERT_EQ(expect_success, result1.ok());
    ASSERT_EQ(expect_success, result2.ok());
    if (expect_success) {
      CheckCodecRoundtrip(*result1, *result2, data);
    }
  }
}

TEST(TestCodecMisc, SpecifyCodecOptionsBrotli) {
  // for now only GZIP & Brotli codec options supported, since it has specific parameters
  // to be customized, other codecs could directly go with CodecOptions, could add more
  // specific codec options if needed.
  struct CombinationOption {
    int level;
    int window_bits;
    bool expect_success;
  };
  constexpr CombinationOption combinations[] = {
      {8, 22, true}, {11, 10, true}, {1, 24, true}, {5, -12, false}, {-992, 25, false}};

  std::vector<uint8_t> data = MakeRandomData(2000);
  for (const auto& combination : combinations) {
    const auto compression = Compression::BROTLI;
    if (!Codec::IsAvailable(compression)) {
      // Support for this codec hasn't been built
      continue;
    }
    auto codec_options = arrow::util::BrotliCodecOptions();
    codec_options.compression_level = combination.level;
    codec_options.window_bits = combination.window_bits;
    const auto expect_success = combination.expect_success;
    auto result1 = Codec::Create(compression, codec_options);
    auto result2 = Codec::Create(compression, codec_options);
    ASSERT_EQ(expect_success, result1.ok());
    ASSERT_EQ(expect_success, result2.ok());
    if (expect_success) {
      CheckCodecRoundtrip(*result1, *result2, data);
    }
  }
}

TEST_P(CodecTest, MinMaxCompressionLevel) {
  auto type = GetCompression();
  ASSERT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

  if (Codec::SupportsCompressionLevel(type)) {
    ASSERT_OK_AND_ASSIGN(auto min_level, Codec::MinimumCompressionLevel(type));
    ASSERT_OK_AND_ASSIGN(auto max_level, Codec::MaximumCompressionLevel(type));
    ASSERT_OK_AND_ASSIGN(auto default_level, Codec::DefaultCompressionLevel(type));
    ASSERT_NE(min_level, Codec::UseDefaultCompressionLevel());
    ASSERT_NE(max_level, Codec::UseDefaultCompressionLevel());
    ASSERT_NE(default_level, Codec::UseDefaultCompressionLevel());
    ASSERT_LT(min_level, max_level);
    ASSERT_EQ(min_level, codec->minimum_compression_level());
    ASSERT_EQ(max_level, codec->maximum_compression_level());
    ASSERT_GE(default_level, min_level);
    ASSERT_LE(default_level, max_level);
  } else {
    ASSERT_RAISES(Invalid, Codec::MinimumCompressionLevel(type));
    ASSERT_RAISES(Invalid, Codec::MaximumCompressionLevel(type));
    ASSERT_RAISES(Invalid, Codec::DefaultCompressionLevel(type));
    ASSERT_EQ(codec->minimum_compression_level(), Codec::UseDefaultCompressionLevel());
    ASSERT_EQ(codec->maximum_compression_level(), Codec::UseDefaultCompressionLevel());
    ASSERT_EQ(codec->default_compression_level(), Codec::UseDefaultCompressionLevel());
  }
}

TEST_P(CodecTest, OutputBufferIsSmall) {
  auto type = GetCompression();
  if (type != Compression::SNAPPY) {
    return;
  }

  ASSERT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

  std::vector<uint8_t> data = MakeRandomData(10);
  auto max_compressed_len = codec->MaxCompressedLen(data.size(), data.data());
  std::vector<uint8_t> compressed(max_compressed_len);
  std::vector<uint8_t> decompressed(data.size() - 1);

  int64_t actual_size;
  ASSERT_OK_AND_ASSIGN(
      actual_size,
      codec->Compress(data.size(), data.data(), max_compressed_len, compressed.data()));
  compressed.resize(actual_size);

  std::stringstream ss;
  ss << "Invalid: Output buffer size (" << decompressed.size() << ") must be "
     << data.size() << " or larger.";
  ASSERT_RAISES_WITH_MESSAGE(Invalid, ss.str(),
                             codec->Decompress(compressed.size(), compressed.data(),
                                               decompressed.size(), decompressed.data()));
}

TEST_P(CodecTest, StreamingCompressor) {
  if (GetCompression() == Compression::SNAPPY) {
    GTEST_SKIP() << "snappy doesn't support streaming compression";
  }
  if (GetCompression() == Compression::BZ2) {
    GTEST_SKIP() << "Z2 doesn't support one-shot decompression";
  }
  if (GetCompression() == Compression::LZ4 ||
      GetCompression() == Compression::LZ4_HADOOP) {
    GTEST_SKIP() << "LZ4 raw format doesn't support streaming compression.";
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    std::vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingCompressor(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingCompressor(codec.get(), data);
  }
}

TEST_P(CodecTest, StreamingDecompressor) {
  if (GetCompression() == Compression::SNAPPY) {
    GTEST_SKIP() << "snappy doesn't support streaming decompression.";
  }
  if (GetCompression() == Compression::BZ2) {
    GTEST_SKIP() << "Z2 doesn't support one-shot compression";
  }
  if (GetCompression() == Compression::LZ4 ||
      GetCompression() == Compression::LZ4_HADOOP) {
    GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    std::vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingDecompressor(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingDecompressor(codec.get(), data);
  }
}

TEST_P(CodecTest, StreamingRoundtrip) {
  if (GetCompression() == Compression::SNAPPY) {
    GTEST_SKIP() << "snappy doesn't support streaming decompression";
  }
  if (GetCompression() == Compression::LZ4 ||
      GetCompression() == Compression::LZ4_HADOOP) {
    GTEST_SKIP() << "LZ4 raw format doesn't support streaming compression.";
  }

  int sizes[] = {0, 10, 100000};
  for (int data_size : sizes) {
    auto codec = MakeCodec();

    std::vector<uint8_t> data = MakeRandomData(data_size);
    CheckStreamingRoundtrip(codec.get(), data);

    data = MakeCompressibleData(data_size);
    CheckStreamingRoundtrip(codec.get(), data);
  }
}

TEST_P(CodecTest, StreamingDecompressorReuse) {
  if (GetCompression() == Compression::SNAPPY) {
    GTEST_SKIP() << "snappy doesn't support streaming decompression";
  }
  if (GetCompression() == Compression::LZ4 ||
      GetCompression() == Compression::LZ4_HADOOP) {
    GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
  }

  auto codec = MakeCodec();
  std::shared_ptr<Compressor> compressor;
  std::shared_ptr<Decompressor> decompressor;
  ASSERT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
  ASSERT_OK_AND_ASSIGN(decompressor, codec->MakeDecompressor());

  std::vector<uint8_t> data = MakeRandomData(100);
  CheckStreamingRoundtrip(compressor, decompressor, data);
  // Decompressor::Reset() should allow reusing decompressor for a new stream
  ASSERT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());
  ASSERT_OK(decompressor->Reset());
  data = MakeRandomData(200);
  CheckStreamingRoundtrip(compressor, decompressor, data);
}

TEST_P(CodecTest, StreamingMultiFlush) {
  // Regression test for ARROW-11937
  if (GetCompression() == Compression::SNAPPY) {
    GTEST_SKIP() << "snappy doesn't support streaming decompression";
  }
  if (GetCompression() == Compression::LZ4 ||
      GetCompression() == Compression::LZ4_HADOOP) {
    GTEST_SKIP() << "LZ4 raw format doesn't support streaming decompression.";
  }
  auto type = GetCompression();
  ASSERT_OK_AND_ASSIGN(auto codec, Codec::Create(type));

  std::shared_ptr<Compressor> compressor;
  ASSERT_OK_AND_ASSIGN(compressor, codec->MakeCompressor());

  // Grow the buffer and flush again while requested (up to a bounded number of times)
  std::vector<uint8_t> compressed(1024);
  Compressor::FlushResult result;
  int attempts = 0;
  int64_t actual_size = 0;
  int64_t output_len = 0;
  uint8_t* output = compressed.data();
  do {
    compressed.resize(compressed.capacity() * 2);
    output_len = compressed.size() - actual_size;
    output = compressed.data() + actual_size;
    ASSERT_OK_AND_ASSIGN(result, compressor->Flush());
    actual_size += result.bytes_written;
    attempts++;
  } while (attempts < 8 && result.should_retry);
  // The LZ4 codec actually needs this many attempts to settle

  // Flush again having done nothing - should not require retry
  output_len = compressed.size() - actual_size;
  output = compressed.data() + actual_size;
  ASSERT_OK_AND_ASSIGN(result, compressor->Flush());
  ASSERT_FALSE(result.should_retry);
}

#if !defined ARROW_WITH_ZLIB && !defined ARROW_WITH_SNAPPY && !defined ARROW_WITH_LZ4 && \
    !defined ARROW_WITH_BROTLI && !defined ARROW_WITH_BZ2 && !defined ARROW_WITH_ZSTD
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(CodecTest);
#endif

#ifdef ARROW_WITH_ZLIB
INSTANTIATE_TEST_SUITE_P(TestGZip, CodecTest, ::testing::Values(Compression::GZIP));
#endif

#ifdef ARROW_WITH_SNAPPY
INSTANTIATE_TEST_SUITE_P(TestSnappy, CodecTest, ::testing::Values(Compression::SNAPPY));
#endif

#ifdef ARROW_WITH_LZ4
INSTANTIATE_TEST_SUITE_P(TestLZ4, CodecTest, ::testing::Values(Compression::LZ4));
INSTANTIATE_TEST_SUITE_P(TestLZ4Hadoop, CodecTest,
                         ::testing::Values(Compression::LZ4_HADOOP));
#endif

#ifdef ARROW_WITH_LZ4
INSTANTIATE_TEST_SUITE_P(TestLZ4Frame, CodecTest,
                         ::testing::Values(Compression::LZ4_FRAME));
#endif

#ifdef ARROW_WITH_BROTLI
INSTANTIATE_TEST_SUITE_P(TestBrotli, CodecTest, ::testing::Values(Compression::BROTLI));
#endif

#if ARROW_WITH_BZ2
INSTANTIATE_TEST_SUITE_P(TestBZ2, CodecTest, ::testing::Values(Compression::BZ2));
#endif

#ifdef ARROW_WITH_ZSTD
INSTANTIATE_TEST_SUITE_P(TestZSTD, CodecTest, ::testing::Values(Compression::ZSTD));
#endif

#ifdef ARROW_WITH_LZ4
TEST(TestCodecLZ4Hadoop, Compatibility) {
  // LZ4 Hadoop codec should be able to read back LZ4 raw blocks
  ASSERT_OK_AND_ASSIGN(auto c1, Codec::Create(Compression::LZ4));
  ASSERT_OK_AND_ASSIGN(auto c2, Codec::Create(Compression::LZ4_HADOOP));

  std::vector<uint8_t> data = MakeRandomData(100);
  CheckCodecRoundtrip(c1, c2, data, /*check_reverse=*/false);
}
#endif

}  // namespace util
}  // namespace arrow

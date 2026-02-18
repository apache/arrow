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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/file.h"
#include "arrow/util/secure_string.h"
#include "arrow/util/span.h"

#include "parquet/bloom_filter.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/test_util.h"
#include "parquet/thrift_internal.h"
#include "parquet/types.h"

namespace parquet::encryption::test {
namespace {

using ::parquet::BlockSplitBloomFilter;
using ::parquet::CreateOutputStream;
using ::parquet::Decryptor;
using ::parquet::ReaderProperties;

struct EncryptedBloomFilterPayload {
  std::shared_ptr<Buffer> buffer;
  std::unique_ptr<Decryptor> header_decryptor;
  std::unique_ptr<Decryptor> bitset_decryptor;
  std::vector<uint8_t> bitset;
  int64_t total_cipher_len = 0;
};

EncryptedBloomFilterPayload BuildEncryptedBloomFilterPayload(int32_t bitset_size) {
  // Build an encrypted payload in memory to exercise the reader path without relying
  // on writer-side encryption support.
  EncryptedBloomFilterPayload payload;
  payload.bitset.resize(bitset_size);
  for (int32_t i = 0; i < bitset_size; ++i) {
    payload.bitset[static_cast<size_t>(i)] = static_cast<uint8_t>(i);
  }

  // Prepare a valid Bloom filter header for the given bitset size.
  format::BloomFilterHeader header;
  header.algorithm.__set_BLOCK(format::SplitBlockAlgorithm());
  header.hash.__set_XXHASH(format::XxHash());
  header.compression.__set_UNCOMPRESSED(format::Uncompressed());
  header.__set_numBytes(bitset_size);

  auto header_sink = CreateOutputStream();
  ThriftSerializer serializer;
  serializer.Serialize(&header, header_sink.get());
  PARQUET_ASSIGN_OR_THROW(auto header_buf, header_sink->Finish());

  const std::string file_aad = "file_aad";
  const int16_t row_group_ordinal = 0;
  const int16_t column_ordinal = 0;
  // AAD must match the module type, row group, and column ordinals.
  const std::string header_aad =
      encryption::CreateModuleAad(file_aad, encryption::kBloomFilterHeader,
                                  row_group_ordinal, column_ordinal, kNonPageOrdinal);
  const std::string bitset_aad =
      encryption::CreateModuleAad(file_aad, encryption::kBloomFilterBitset,
                                  row_group_ordinal, column_ordinal, kNonPageOrdinal);

  // Use fixed keys for deterministic test data.
  const ::arrow::util::SecureString header_key("0123456789abcdef");
  const ::arrow::util::SecureString bitset_key("abcdef0123456789");
  auto header_encryptor = encryption::AesEncryptor::Make(
      ParquetCipher::AES_GCM_V1, static_cast<int32_t>(header_key.size()), true);
  auto bitset_encryptor = encryption::AesEncryptor::Make(
      ParquetCipher::AES_GCM_V1, static_cast<int32_t>(bitset_key.size()), false);

  // Encrypt header with the metadata cipher (AES-GCM).
  const int32_t header_cipher_len =
      header_encryptor->CiphertextLength(static_cast<int64_t>(header_buf->size()));
  std::shared_ptr<Buffer> header_cipher_buf =
      AllocateBuffer(::arrow::default_memory_pool(), header_cipher_len);
  const int32_t header_written = header_encryptor->Encrypt(
      header_buf->span_as<const uint8_t>(), header_key.as_span(), str2span(header_aad),
      header_cipher_buf->mutable_span_as<uint8_t>());
  if (header_written != header_cipher_len) {
    throw ParquetException("Header encryption length mismatch");
  }

  // Encrypt bitset with the data cipher (AES-GCM).
  const int32_t bitset_cipher_len =
      bitset_encryptor->CiphertextLength(static_cast<int64_t>(payload.bitset.size()));
  std::shared_ptr<Buffer> bitset_cipher_buf =
      AllocateBuffer(::arrow::default_memory_pool(), bitset_cipher_len);
  const int32_t bitset_written = bitset_encryptor->Encrypt(
      ::arrow::util::span<const uint8_t>(payload.bitset.data(), payload.bitset.size()),
      bitset_key.as_span(), str2span(bitset_aad),
      bitset_cipher_buf->mutable_span_as<uint8_t>());
  if (bitset_written != bitset_cipher_len) {
    throw ParquetException("Bitset encryption length mismatch");
  }

  // Concatenate encrypted header and encrypted bitset.
  payload.total_cipher_len = static_cast<int64_t>(header_cipher_len) + bitset_cipher_len;
  std::shared_ptr<Buffer> total_cipher_buf =
      AllocateBuffer(::arrow::default_memory_pool(), payload.total_cipher_len);
  std::memcpy(total_cipher_buf->mutable_data(), header_cipher_buf->data(),
              header_cipher_len);
  std::memcpy(total_cipher_buf->mutable_data() + header_cipher_len,
              bitset_cipher_buf->data(), bitset_cipher_len);
  payload.buffer = std::move(total_cipher_buf);

  payload.header_decryptor = std::make_unique<Decryptor>(
      encryption::AesDecryptor::Make(ParquetCipher::AES_GCM_V1,
                                     static_cast<int32_t>(header_key.size()), true),
      header_key, file_aad, header_aad, ::arrow::default_memory_pool());
  payload.bitset_decryptor = std::make_unique<Decryptor>(
      encryption::AesDecryptor::Make(ParquetCipher::AES_GCM_V1,
                                     static_cast<int32_t>(bitset_key.size()), false),
      bitset_key, file_aad, bitset_aad, ::arrow::default_memory_pool());

  return payload;
}

}  // namespace

TEST(EncryptedDeserializeTest, TestBloomFilter) {
  // It decrypts the header and bitset and validates Bloom filter behavior.
  const int32_t bitset_size = 128;
  auto payload = BuildEncryptedBloomFilterPayload(bitset_size);
  ReaderProperties reader_properties;
  ::arrow::io::BufferReader source(payload.buffer);
  BlockSplitBloomFilter decrypted = BlockSplitBloomFilter::Deserialize(
      reader_properties, &source, payload.total_cipher_len,
      payload.header_decryptor.get(), payload.bitset_decryptor.get());

  BlockSplitBloomFilter expected;
  expected.Init(payload.bitset.data(), bitset_size);
  for (int value : {1, 2, 3, 4, 5, 42, 99}) {
    EXPECT_EQ(expected.FindHash(expected.Hash(value)),
              decrypted.FindHash(decrypted.Hash(value)));
  }
}

TEST(EncryptedDeserializeTest, TestBloomFilterInvalidLength) {
  // It throws when the reported total length does not match the payload.
  const int32_t bitset_size = 128;
  auto payload = BuildEncryptedBloomFilterPayload(bitset_size);

  ReaderProperties reader_properties;
  ::arrow::io::BufferReader source(payload.buffer);
  EXPECT_THROW_THAT(
      [&]() {
        BlockSplitBloomFilter::Deserialize(
            reader_properties, &source, payload.total_cipher_len + 1,
            payload.header_decryptor.get(), payload.bitset_decryptor.get());
      },
      ParquetException,
      ::testing::Property(&ParquetException::what,
                          ::testing::HasSubstr("Bloom filter length")));
}

TEST(EncryptedDeserializeTest, TestBloomFilterMissingDecryptor) {
  // It throws when only one decryptor is provided.
  const int32_t bitset_size = 128;
  auto payload = BuildEncryptedBloomFilterPayload(bitset_size);
  ReaderProperties reader_properties;

  {
    ::arrow::io::BufferReader source(payload.buffer);
    EXPECT_THROW_THAT(
        [&]() {
          BlockSplitBloomFilter::Deserialize(reader_properties, &source,
                                             payload.total_cipher_len,
                                             payload.header_decryptor.get(), nullptr);
        },
        ParquetException,
        ::testing::Property(
            &ParquetException::what,
            ::testing::HasSubstr("Bloom filter decryptors must be both provided")));
  }
  {
    ::arrow::io::BufferReader source(payload.buffer);
    EXPECT_THROW_THAT(
        [&]() {
          BlockSplitBloomFilter::Deserialize(reader_properties, &source,
                                             payload.total_cipher_len, nullptr,
                                             payload.bitset_decryptor.get());
        },
        ParquetException,
        ::testing::Property(
            &ParquetException::what,
            ::testing::HasSubstr("Bloom filter decryptors must be both provided")));
  }
}

TEST(EncryptedDeserializeTest, TestBloomFilterWithoutLength) {
  // It decrypts the payload when bloom_filter_length is not provided.
  const int32_t bitset_size = 128;
  auto payload = BuildEncryptedBloomFilterPayload(bitset_size);
  ReaderProperties reader_properties;
  ::arrow::io::BufferReader source(payload.buffer);
  BlockSplitBloomFilter decrypted = BlockSplitBloomFilter::Deserialize(
      reader_properties, &source, std::nullopt, payload.header_decryptor.get(),
      payload.bitset_decryptor.get());

  BlockSplitBloomFilter expected;
  expected.Init(payload.bitset.data(), bitset_size);
  for (int value : {1, 2, 3, 4, 5, 42, 99}) {
    EXPECT_EQ(expected.FindHash(expected.Hash(value)),
              decrypted.FindHash(decrypted.Hash(value)));
  }
}

}  // namespace parquet::encryption::test

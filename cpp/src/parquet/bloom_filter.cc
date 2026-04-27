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

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>

#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"

#include "generated/parquet_types.h"

#include "parquet/bloom_filter.h"
#include "parquet/encryption/encryption_internal.h"
#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/exception.h"
#include "parquet/thrift_internal.h"
#include "parquet/xxhasher.h"

namespace parquet {
namespace {

constexpr int32_t kCiphertextLengthSize = 4;

/// Parse the 4-byte little-endian length prefix and return the total ciphertext size,
/// including the 4-byte length field itself.
int64_t ParseCiphertextTotalLength(const uint8_t* data, int64_t length) {
  if (length < kCiphertextLengthSize) {
    throw ParquetException("Ciphertext length buffer is too small");
  }
  uint32_t buffer_size =
      (static_cast<uint32_t>(data[3]) << 24) | (static_cast<uint32_t>(data[2]) << 16) |
      (static_cast<uint32_t>(data[1]) << 8) | (static_cast<uint32_t>(data[0]));
  return static_cast<int64_t>(buffer_size) + kCiphertextLengthSize;
}

void CheckBloomFilterShortRead(int64_t expected, int64_t actual,
                               std::string_view context) {
  if (ARROW_PREDICT_FALSE(actual < expected)) {
    std::stringstream ss;
    ss << context << " read failed: expected ";
    ss << expected << " bytes, got " << actual;
    throw ParquetException(ss.str());
  }
}

}  // namespace

constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

BlockSplitBloomFilter::BlockSplitBloomFilter(::arrow::MemoryPool* pool)
    : pool_(pool),
      hash_strategy_(HashStrategy::XXHASH),
      algorithm_(Algorithm::BLOCK),
      compression_strategy_(CompressionStrategy::UNCOMPRESSED) {}

void BlockSplitBloomFilter::Init(uint32_t num_bytes) {
  if (num_bytes < kMinimumBloomFilterBytes) {
    num_bytes = kMinimumBloomFilterBytes;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(::arrow::bit_util::NextPower2(num_bytes));
  }

  if (num_bytes > kMaximumBloomFilterBytes) {
    num_bytes = kMaximumBloomFilterBytes;
  }

  num_bytes_ = num_bytes;
  PARQUET_ASSIGN_OR_THROW(data_, ::arrow::AllocateBuffer(num_bytes_, pool_));
  memset(data_->mutable_data(), 0, num_bytes_);

  this->hasher_ = std::make_unique<XxHasher>();
}

void BlockSplitBloomFilter::Init(const uint8_t* bitset, uint32_t num_bytes) {
  DCHECK(bitset != nullptr);

  if (num_bytes < kMinimumBloomFilterBytes || num_bytes > kMaximumBloomFilterBytes ||
      (num_bytes & (num_bytes - 1)) != 0) {
    throw ParquetException("Given length of bitset is illegal");
  }

  num_bytes_ = num_bytes;
  PARQUET_ASSIGN_OR_THROW(data_, ::arrow::AllocateBuffer(num_bytes_, pool_));
  std::memcpy(data_->mutable_data(), bitset, num_bytes_);

  this->hasher_ = std::make_unique<XxHasher>();
}

namespace {

constexpr uint32_t kBloomFilterHeaderSizeGuess = 256;

::arrow::Status ValidateBloomFilterHeader(const format::BloomFilterHeader& header) {
  if (!header.algorithm.__isset.BLOCK) {
    return ::arrow::Status::Invalid(
        "Unsupported Bloom filter algorithm: ", header.algorithm, ".");
  }

  if (!header.hash.__isset.XXHASH) {
    return ::arrow::Status::Invalid("Unsupported Bloom filter hash: ", header.hash, ".");
  }

  if (!header.compression.__isset.UNCOMPRESSED) {
    return ::arrow::Status::Invalid(
        "Unsupported Bloom filter compression: ", header.compression, ".");
  }

  if (header.numBytes <= 0 ||
      static_cast<uint32_t>(header.numBytes) > BloomFilter::kMaximumBloomFilterBytes) {
    std::stringstream ss;
    ss << "Bloom filter size is incorrect: " << header.numBytes << ". Must be in range ("
       << 0 << ", " << BloomFilter::kMaximumBloomFilterBytes << "].";
    return ::arrow::Status::Invalid(ss.str());
  }

  return ::arrow::Status::OK();
}

BlockSplitBloomFilter DeserializeEncryptedFromStream(
    const ReaderProperties& properties, ArrowInputStream* input,
    std::optional<int64_t> bloom_filter_length, Decryptor* decryptor,
    int16_t row_group_ordinal, int16_t column_ordinal) {
  ThriftDeserializer deserializer(properties);
  format::BloomFilterHeader header;

  // Read the length-prefixed ciphertext for the header.
  PARQUET_ASSIGN_OR_THROW(auto length_buf, input->Read(kCiphertextLengthSize));
  CheckBloomFilterShortRead(kCiphertextLengthSize, length_buf->size(),
                            "Bloom filter header length");

  const int64_t header_cipher_total_len =
      ParseCiphertextTotalLength(length_buf->data(), length_buf->size());
  if (ARROW_PREDICT_FALSE(header_cipher_total_len >
                          std::numeric_limits<int32_t>::max())) {
    throw ParquetException("Bloom filter header ciphertext length overflows int32");
  }
  if (bloom_filter_length && header_cipher_total_len > *bloom_filter_length) {
    throw ParquetException(
        "Bloom filter length less than encrypted bloom filter header length");
  }

  // Read the full header ciphertext and decrypt the Thrift header.
  auto header_cipher_buf =
      AllocateBuffer(properties.memory_pool(), header_cipher_total_len);
  std::memcpy(header_cipher_buf->mutable_data(), length_buf->data(),
              kCiphertextLengthSize);
  const int64_t header_cipher_remaining = header_cipher_total_len - kCiphertextLengthSize;
  PARQUET_ASSIGN_OR_THROW(auto read_size, input->Read(header_cipher_remaining,
                                                      header_cipher_buf->mutable_data() +
                                                          kCiphertextLengthSize));
  CheckBloomFilterShortRead(header_cipher_remaining, read_size, "Bloom filter header");

  // Bloom filter header and bitset are separate encrypted modules with different AADs.
  UpdateDecryptor(decryptor, row_group_ordinal, column_ordinal,
                  encryption::kBloomFilterHeader);
  auto header_cipher_len = static_cast<uint32_t>(header_cipher_total_len);
  try {
    deserializer.DeserializeMessage(header_cipher_buf->data(), &header_cipher_len,
                                    &header, decryptor);
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Deserializing bloom filter header failed.\n" << e.what();
    throw ParquetException(ss.str());
  }
  if (ARROW_PREDICT_FALSE(header_cipher_len != header_cipher_total_len)) {
    std::stringstream ss;
    ss << "Encrypted bloom filter header length mismatch: expected "
       << header_cipher_total_len << " bytes, got " << header_cipher_len;
    throw ParquetException(ss.str());
  }
  PARQUET_THROW_NOT_OK(ValidateBloomFilterHeader(header));

  const int32_t bloom_filter_size = header.numBytes;
  UpdateDecryptor(decryptor, row_group_ordinal, column_ordinal,
                  encryption::kBloomFilterBitset);
  const int32_t bitset_cipher_len = decryptor->CiphertextLength(bloom_filter_size);
  const int64_t total_cipher_len =
      header_cipher_total_len + static_cast<int64_t>(bitset_cipher_len);
  if (bloom_filter_length && *bloom_filter_length != total_cipher_len) {
    std::stringstream ss;
    ss << "Bloom filter length (" << bloom_filter_length.value()
       << ") does not match the actual bloom filter (size: " << total_cipher_len << ").";
    throw ParquetException(ss.str());
  }

  // Read and decrypt the bitset bytes.
  PARQUET_ASSIGN_OR_THROW(auto bitset_cipher_buf, input->Read(bitset_cipher_len));
  CheckBloomFilterShortRead(bitset_cipher_len, bitset_cipher_buf->size(),
                            "Bloom filter bitset");

  const int32_t bitset_plain_len =
      decryptor->PlaintextLength(static_cast<int32_t>(bitset_cipher_len));
  if (ARROW_PREDICT_FALSE(bitset_plain_len != bloom_filter_size)) {
    throw ParquetException("Bloom filter bitset size does not match header");
  }

  auto bitset_plain_buf = AllocateBuffer(properties.memory_pool(), bitset_plain_len);
  int32_t decrypted_len =
      decryptor->Decrypt(bitset_cipher_buf->span_as<const uint8_t>(),
                         bitset_plain_buf->mutable_span_as<uint8_t>());
  if (ARROW_PREDICT_FALSE(decrypted_len != bitset_plain_len)) {
    throw ParquetException("Bloom filter bitset decryption failed");
  }

  // Initialize the bloom filter from the decrypted bitset.
  BlockSplitBloomFilter bloom_filter(properties.memory_pool());
  bloom_filter.Init(bitset_plain_buf->data(), bloom_filter_size);
  return bloom_filter;
}

}  // namespace

BlockSplitBloomFilter BlockSplitBloomFilter::DeserializeEncrypted(
    const ReaderProperties& properties, ArrowInputStream* input,
    std::optional<int64_t> bloom_filter_length, Decryptor* decryptor,
    int16_t row_group_ordinal, int16_t column_ordinal) {
  if (decryptor == nullptr) {
    throw ParquetException("Bloom filter decryptor must be provided");
  }

  // Read the full Bloom filter payload up front when the total length is known.
  if (bloom_filter_length.has_value()) {
    PARQUET_ASSIGN_OR_THROW(auto bloom_filter_buf, input->Read(*bloom_filter_length));
    CheckBloomFilterShortRead(*bloom_filter_length, bloom_filter_buf->size(),
                              "Bloom filter");
    ::arrow::io::BufferReader reader(bloom_filter_buf);
    return DeserializeEncryptedFromStream(properties, &reader, bloom_filter_length,
                                          decryptor, row_group_ordinal, column_ordinal);
  }

  return DeserializeEncryptedFromStream(properties, input, bloom_filter_length, decryptor,
                                        row_group_ordinal, column_ordinal);
}

BlockSplitBloomFilter BlockSplitBloomFilter::Deserialize(
    const ReaderProperties& properties, ArrowInputStream* input,
    std::optional<int64_t> bloom_filter_length) {
  ThriftDeserializer deserializer(properties);
  format::BloomFilterHeader header;
  int64_t bloom_filter_header_read_size = 0;
  if (bloom_filter_length.has_value()) {
    bloom_filter_header_read_size = bloom_filter_length.value();
  } else {
    // NOTE: we don't know the bloom filter header size upfront without
    // bloom_filter_length, and we can't rely on InputStream::Peek() which isn't always
    // implemented. Therefore, we must first Read() with an upper bound estimate of the
    // header size, then once we know the bloom filter data size, we can Read() the exact
    // number of remaining data bytes.
    bloom_filter_header_read_size = kBloomFilterHeaderSizeGuess;
  }

  // Read and deserialize bloom filter header
  PARQUET_ASSIGN_OR_THROW(auto header_buf, input->Read(bloom_filter_header_read_size));
  // This gets used, then set by DeserializeThriftMsg
  uint32_t header_size = static_cast<uint32_t>(header_buf->size());
  try {
    deserializer.DeserializeMessage(header_buf->data(), &header_size, &header);
    DCHECK_LE(header_size, header_buf->size());
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Deserializing bloom filter header failed.\n" << e.what();
    throw ParquetException(ss.str());
  }
  PARQUET_THROW_NOT_OK(ValidateBloomFilterHeader(header));

  const int32_t bloom_filter_size = header.numBytes;
  if (bloom_filter_size + header_size <= header_buf->size()) {
    // The bloom filter data is entirely contained in the buffer we just read
    // => just return it.
    BlockSplitBloomFilter bloom_filter(properties.memory_pool());
    bloom_filter.Init(header_buf->data() + header_size, bloom_filter_size);
    return bloom_filter;
  }
  if (bloom_filter_length && *bloom_filter_length != bloom_filter_size + header_size) {
    // We know the bloom filter data size, but the real size is different.
    std::stringstream ss;
    ss << "Bloom filter length (" << bloom_filter_length.value()
       << ") does not match the actual bloom filter (size: "
       << bloom_filter_size + header_size << ").";
    throw ParquetException(ss.str());
  }
  // We have read a part of the bloom filter already, copy it to the target buffer
  // and read the remaining part from the InputStream.
  auto buffer = AllocateBuffer(properties.memory_pool(), bloom_filter_size);

  const auto bloom_filter_bytes_in_header = header_buf->size() - header_size;
  SafeMemcpy(buffer->mutable_data(), header_buf->data() + header_size,
             static_cast<size_t>(bloom_filter_bytes_in_header));

  const auto required_read_size = bloom_filter_size - bloom_filter_bytes_in_header;
  PARQUET_ASSIGN_OR_THROW(
      auto read_size, input->Read(required_read_size,
                                  buffer->mutable_data() + bloom_filter_bytes_in_header));
  CheckBloomFilterShortRead(required_read_size, read_size, "Bloom filter");
  BlockSplitBloomFilter bloom_filter(properties.memory_pool());
  bloom_filter.Init(buffer->data(), bloom_filter_size);
  return bloom_filter;
}

void BlockSplitBloomFilter::WriteTo(ArrowOutputStream* sink) const {
  DCHECK(sink != nullptr);

  format::BloomFilterHeader header;
  if (ARROW_PREDICT_FALSE(algorithm_ != BloomFilter::Algorithm::BLOCK)) {
    throw ParquetException("BloomFilter does not support Algorithm other than BLOCK");
  }
  header.algorithm.__set_BLOCK(format::SplitBlockAlgorithm());
  if (ARROW_PREDICT_FALSE(hash_strategy_ != HashStrategy::XXHASH)) {
    throw ParquetException("BloomFilter does not support Hash other than XXHASH");
  }
  header.hash.__set_XXHASH(format::XxHash());
  if (ARROW_PREDICT_FALSE(compression_strategy_ != CompressionStrategy::UNCOMPRESSED)) {
    throw ParquetException(
        "BloomFilter does not support Compression other than UNCOMPRESSED");
  }
  header.compression.__set_UNCOMPRESSED(format::Uncompressed());
  header.__set_numBytes(num_bytes_);

  ThriftSerializer serializer;
  serializer.Serialize(&header, sink);

  PARQUET_THROW_NOT_OK(sink->Write(data_->data(), num_bytes_));
}

bool BlockSplitBloomFilter::FindHash(uint64_t hash) const {
  const uint32_t bucket_index =
      static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  const uint32_t* bitset32 = reinterpret_cast<const uint32_t*>(data_->data());

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    if (ARROW_PREDICT_FALSE(0 ==
                            (bitset32[kBitsSetPerBlock * bucket_index + i] & mask))) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::InsertHashImpl(uint64_t hash) {
  const uint32_t bucket_index =
      static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  const uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

  for (int i = 0; i < kBitsSetPerBlock; i++) {
    // Calculate mask for key in the given bitset.
    const uint32_t mask = UINT32_C(0x1) << ((key * SALT[i]) >> 27);
    bitset32[bucket_index * kBitsSetPerBlock + i] |= mask;
  }
}

void BlockSplitBloomFilter::InsertHash(uint64_t hash) { InsertHashImpl(hash); }

void BlockSplitBloomFilter::InsertHashes(const uint64_t* hashes, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    InsertHashImpl(hashes[i]);
  }
}

}  // namespace parquet

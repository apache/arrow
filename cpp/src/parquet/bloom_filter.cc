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
#include <memory>

#include "arrow/result.h"
#include "arrow/util/logging.h"
#include "generated/parquet_types.h"
#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/thrift_internal.h"
#include "parquet/xxhash.h"

namespace parquet {
constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

BlockSplitBloomFilter::BlockSplitBloomFilter()
    : pool_(::arrow::default_memory_pool()),
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
  memcpy(data_->mutable_data(), bitset, num_bytes_);

  this->hasher_ = std::make_unique<XxHasher>();
}

static constexpr uint32_t HEADER_SIZE_GUESS = 32;
static constexpr uint32_t MAX_BLOOM_FILTER_HEADER_SIZE = 1024;

static ::arrow::Status validateBloomFilterHeader(
    const format::BloomFilterHeader& header) {
  if (!header.algorithm.__isset.BLOCK) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter algorithm: " << header.algorithm << ".";
    return ::arrow::Status::Invalid(ss.str());
  }

  if (!header.hash.__isset.XXHASH) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter hash: " << header.hash << ".";
    return ::arrow::Status::Invalid(ss.str());
  }

  if (!header.compression.__isset.UNCOMPRESSED) {
    std::stringstream ss;
    ss << "Unsupported Bloom filter compression: " << header.compression << ".";
    return ::arrow::Status::Invalid(ss.str());
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

BlockSplitBloomFilter BlockSplitBloomFilter::Deserialize(ArrowInputStream* input) {
  ReaderProperties readerProperties;
  uint32_t length = HEADER_SIZE_GUESS;

  uint32_t headerSize = 0;

  ThriftDeserializer deserializer(readerProperties);
  format::BloomFilterHeader header;

  // Read and deserialize bloom filter header
  while (true) {
    PARQUET_ASSIGN_OR_THROW(auto sv, input->Peek(length));
    // This gets used, then set by DeserializeThriftMsg
    headerSize = static_cast<uint32_t>(sv.size());
    try {
      deserializer.DeserializeMessage(reinterpret_cast<const uint8_t*>(sv.data()),
                                      &headerSize, &header);
      break;
    } catch (std::exception& e) {
      // Failed to deserialize. Double the allowed page header size and try again
      length *= 2;
      if (length > MAX_BLOOM_FILTER_HEADER_SIZE) {
        std::stringstream ss;
        ss << "Deserializing bloom filter header failed.\n" << e.what();
        throw ParquetException(ss.str());
      }
    }
  }

  // Throw if the header is invalid
  auto status = validateBloomFilterHeader(header);
  if (!status.ok()) {
    throw ParquetException(status.ToString());
  }

  // Read remaining data of bitset
  PARQUET_THROW_NOT_OK(input->Advance(headerSize));
  PARQUET_ASSIGN_OR_THROW(auto buffer, input->Read(header.numBytes));

  BlockSplitBloomFilter bloomFilter;
  bloomFilter.Init(buffer->data(), header.numBytes);
  return bloomFilter;
}

void BlockSplitBloomFilter::WriteTo(ArrowOutputStream* sink) const {
  DCHECK(sink != nullptr);

  format::BloomFilterHeader header;
  if (ARROW_PREDICT_FALSE(algorithm_ != BloomFilter::Algorithm::BLOCK)) {
    throw ParquetException("BloomFilter not support Algorithm other than BLOCK");
  }
  header.algorithm.__set_BLOCK(format::SplitBlockAlgorithm());
  if (ARROW_PREDICT_FALSE(hash_strategy_ != HashStrategy::XXHASH)) {
    throw ParquetException("BloomFilter not support Hash other than XXHASH");
  }
  header.hash.__set_XXHASH(format::XxHash());
  if (ARROW_PREDICT_FALSE(compression_strategy_ != CompressionStrategy::UNCOMPRESSED)) {
    throw ParquetException("BloomFilter not support Compression other than UNCOMPRESSED");
  }
  header.compression.__set_UNCOMPRESSED(format::Uncompressed());
  header.__set_numBytes(num_bytes_);

  ThriftSerializer serializer;
  serializer.Serialize(&header, sink);

  PARQUET_THROW_NOT_OK(sink->Write(data_->data(), num_bytes_));
}

void BlockSplitBloomFilter::SetMask(uint32_t key, BlockMask& block_mask) const {
  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = key * SALT[i];
  }

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = block_mask.item[i] >> 27;
  }

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    block_mask.item[i] = UINT32_C(0x1) << block_mask.item[i];
  }
}

bool BlockSplitBloomFilter::FindHash(uint64_t hash) const {
  const uint32_t bucket_index =
      static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < kBitsSetPerBlock; ++i) {
    if (0 == (bitset32[kBitsSetPerBlock * bucket_index + i] & block_mask.item[i])) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::InsertHash(uint64_t hash) {
  const uint32_t bucket_index =
      static_cast<uint32_t>(((hash >> 32) * (num_bytes_ / kBytesPerFilterBlock)) >> 32);
  uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < kBitsSetPerBlock; i++) {
    bitset32[bucket_index * kBitsSetPerBlock + i] |= block_mask.item[i];
  }
}

}  // namespace parquet

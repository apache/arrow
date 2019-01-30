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

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/murmur3.h"

namespace parquet {
constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

BlockSplitBloomFilter::BlockSplitBloomFilter()
    : pool_(::arrow::default_memory_pool()),
      hash_strategy_(HashStrategy::MURMUR3_X64_128),
      algorithm_(Algorithm::BLOCK) {}

void BlockSplitBloomFilter::Init(uint32_t num_bytes) {
  if (num_bytes < kMinimumBloomFilterBytes) {
    num_bytes = kMinimumBloomFilterBytes;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bytes));
  }

  if (num_bytes > kMaximumBloomFilterBytes) {
    num_bytes = kMaximumBloomFilterBytes;
  }

  num_bytes_ = num_bytes;
  PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(pool_, num_bytes_, &data_));
  memset(data_->mutable_data(), 0, num_bytes_);

  this->hasher_.reset(new MurmurHash3());
}

void BlockSplitBloomFilter::Init(const uint8_t* bitset, uint32_t num_bytes) {
  DCHECK(bitset != nullptr);

  if (num_bytes < kMinimumBloomFilterBytes || num_bytes > kMaximumBloomFilterBytes ||
      (num_bytes & (num_bytes - 1)) != 0) {
    throw ParquetException("Given length of bitset is illegal");
  }

  num_bytes_ = num_bytes;
  PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(pool_, num_bytes_, &data_));
  memcpy(data_->mutable_data(), bitset, num_bytes_);

  this->hasher_.reset(new MurmurHash3());
}

BlockSplitBloomFilter BlockSplitBloomFilter::Deserialize(InputStream* input) {
  int64_t bytes_available;

  const uint8_t* read_buffer = NULL;
  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  uint32_t len;
  memcpy(&len, read_buffer, sizeof(uint32_t));

  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  uint32_t hash;
  memcpy(&hash, read_buffer, sizeof(uint32_t));
  if (static_cast<HashStrategy>(hash) != HashStrategy::MURMUR3_X64_128) {
    throw ParquetException("Unsupported hash strategy");
  }

  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  uint32_t algorithm;
  memcpy(&algorithm, read_buffer, sizeof(uint32_t));
  if (static_cast<Algorithm>(algorithm) != BloomFilter::Algorithm::BLOCK) {
    throw ParquetException("Unsupported Bloom filter algorithm");
  }

  BlockSplitBloomFilter bloom_filter;
  bloom_filter.Init(input->Read(len, &bytes_available), len);
  return bloom_filter;
}

void BlockSplitBloomFilter::WriteTo(OutputStream* sink) const {
  DCHECK(sink != nullptr);

  sink->Write(reinterpret_cast<const uint8_t*>(&num_bytes_), sizeof(num_bytes_));
  sink->Write(reinterpret_cast<const uint8_t*>(&hash_strategy_), sizeof(hash_strategy_));
  sink->Write(reinterpret_cast<const uint8_t*>(&algorithm_), sizeof(algorithm_));
  sink->Write(data_->mutable_data(), num_bytes_);
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
      static_cast<uint32_t>((hash >> 32) & (num_bytes_ / kBytesPerFilterBlock - 1));
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
      static_cast<uint32_t>(hash >> 32) & (num_bytes_ / kBytesPerFilterBlock - 1);
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

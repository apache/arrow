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

#include "parquet/bloom_filter_reader.h"
#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/metadata.h"

namespace parquet {

class RowGroupBloomFilterReaderImpl final : public RowGroupBloomFilterReader {
 public:
  RowGroupBloomFilterReaderImpl(std::shared_ptr<::arrow::io::RandomAccessFile> input,
                                std::shared_ptr<RowGroupMetaData> row_group_metadata,
                                const ReaderProperties& properties)
      : input_(std::move(input)),
        row_group_metadata_(std::move(row_group_metadata)),
        properties_(properties) {}

  std::unique_ptr<BloomFilter> GetColumnBloomFilter(int i) override;

 private:
  /// The input stream that can perform random access read.
  std::shared_ptr<::arrow::io::RandomAccessFile> input_;

  /// The row group metadata to get column chunk metadata.
  std::shared_ptr<RowGroupMetaData> row_group_metadata_;

  /// Reader properties used to deserialize thrift object.
  const ReaderProperties& properties_;
};

std::unique_ptr<BloomFilter> RowGroupBloomFilterReaderImpl::GetColumnBloomFilter(int i) {
  if (i < 0 || i >= row_group_metadata_->num_columns()) {
    throw ParquetException("Invalid column index at column ordinal ", i);
  }

  auto col_chunk = row_group_metadata_->ColumnChunk(i);
  std::unique_ptr<ColumnCryptoMetaData> crypto_metadata = col_chunk->crypto_metadata();
  if (crypto_metadata != nullptr) {
    ParquetException::NYI("Cannot read encrypted bloom filter yet");
  }

  auto bloom_filter_offset = col_chunk->bloom_filter_offset();
  if (!bloom_filter_offset.has_value()) {
    return nullptr;
  }
  PARQUET_ASSIGN_OR_THROW(auto file_size, input_->GetSize());
  if (file_size <= *bloom_filter_offset) {
    throw ParquetException("file size less or equal than bloom offset");
  }
  auto stream = ::arrow::io::RandomAccessFile::GetStream(
      input_, *bloom_filter_offset, file_size - *bloom_filter_offset);
  auto bloom_filter = BlockSplitBloomFilter::Deserialize(properties_, stream->get());
  return std::make_unique<BlockSplitBloomFilter>(std::move(bloom_filter));
}

class BloomFilterReaderImpl final : public BloomFilterReader {
 public:
  BloomFilterReaderImpl(std::shared_ptr<::arrow::io::RandomAccessFile> input,
                        std::shared_ptr<FileMetaData> file_metadata,
                        const ReaderProperties& properties,
                        std::shared_ptr<InternalFileDecryptor> file_decryptor)
      : input_(std::move(input)),
        file_metadata_(std::move(file_metadata)),
        properties_(properties) {
    if (file_decryptor != nullptr) {
      ParquetException::NYI("BloomFilter decryption is not yet supported");
    }
  }

  std::shared_ptr<RowGroupBloomFilterReader> RowGroup(int i) {
    if (i < 0 || i >= file_metadata_->num_row_groups()) {
      throw ParquetException("Invalid row group ordinal: ", i);
    }

    auto row_group_metadata = file_metadata_->RowGroup(i);
    return std::make_shared<RowGroupBloomFilterReaderImpl>(
        input_, std::move(row_group_metadata), properties_);
  }

 private:
  /// The input stream that can perform random read.
  std::shared_ptr<::arrow::io::RandomAccessFile> input_;

  /// The file metadata to get row group metadata.
  std::shared_ptr<FileMetaData> file_metadata_;

  /// Reader properties used to deserialize thrift object.
  const ReaderProperties& properties_;
};

std::unique_ptr<BloomFilterReader> BloomFilterReader::Make(
    std::shared_ptr<::arrow::io::RandomAccessFile> input,
    std::shared_ptr<FileMetaData> file_metadata, const ReaderProperties& properties,
    std::shared_ptr<InternalFileDecryptor> file_decryptor) {
  return std::make_unique<BloomFilterReaderImpl>(std::move(input), file_metadata,
                                                 properties, std::move(file_decryptor));
}

}  // namespace parquet

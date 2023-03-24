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

#pragma once

#include "parquet/bloom_filter.h"
#include "parquet/encryption/internal_file_decryptor.h"

namespace parquet {

class PARQUET_EXPORT RowGroupBloomFilterReader {
 public:
  virtual ~RowGroupBloomFilterReader() = default;

  virtual std::unique_ptr<BloomFilter> GetColumnBloomFilter(int i) = 0;
};

/// \brief Interface for reading the bloom filter for a Parquet file.
class PARQUET_EXPORT BloomFilterReader {
 public:
  virtual ~BloomFilterReader() = default;

  /// \brief Create a BloomFilterReader instance.
  /// \returns a BloomFilterReader instance.
  /// WARNING: The returned BloomFilterReader references to all the input parameters, so
  /// it must not outlive all of the input parameters. Usually these input parameters
  /// come from the same ParquetFileReader object, so it must not outlive the reader
  /// that creates this BloomFilterReader.
  static std::shared_ptr<BloomFilterReader> Make(
      std::shared_ptr<::arrow::io::RandomAccessFile> input,
      std::shared_ptr<FileMetaData> file_metadata, const ReaderProperties& properties,
      std::shared_ptr<InternalFileDecryptor> file_decryptor = NULLPTR);

  virtual std::shared_ptr<RowGroupBloomFilterReader> RowGroup(int i) = 0;
};

}  // namespace parquet
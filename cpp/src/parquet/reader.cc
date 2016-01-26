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

#include "parquet/reader.h"

#include <cstdio>
#include <vector>

#include "parquet/exception.h"
#include "parquet/thrift/util.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// LocalFile methods

LocalFile::~LocalFile() {
  // You must explicitly call Close
}

void LocalFile::Open(const std::string& path) {
  path_ = path;
  file_ = fopen(path_.c_str(), "r");
  is_open_ = true;
}

void LocalFile::Close() {
  if (is_open_) {
    fclose(file_);
    is_open_ = false;
  }
}

size_t LocalFile::Size() {
  fseek(file_, 0L, SEEK_END);
  return Tell();
}

void LocalFile::Seek(size_t pos) {
  fseek(file_, pos, SEEK_SET);
}

size_t LocalFile::Tell() {
  return ftell(file_);
}

void LocalFile::Read(size_t nbytes, uint8_t* buffer,
    size_t* bytes_read) {
  *bytes_read = fread(buffer, 1, nbytes, file_);
}

// ----------------------------------------------------------------------
// ParquetFileReader

// 4 byte constant + 4 byte metadata len
static constexpr uint32_t FOOTER_SIZE = 8;
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

void ParquetFileReader::Open(FileLike* buffer) {
  buffer_ = buffer;
}

void ParquetFileReader::Close() {
  buffer_->Close();
}

void ParquetFileReader::ParseMetaData() {
  size_t filesize = buffer_->Size();

  if (filesize < FOOTER_SIZE) {
    throw ParquetException("Corrupted file, smaller than file footer");
  }

  size_t bytes_read;
  uint8_t footer_buffer[FOOTER_SIZE];

  buffer_->Seek(filesize - FOOTER_SIZE);
  buffer_->Read(FOOTER_SIZE, footer_buffer, &bytes_read);

  if (bytes_read != FOOTER_SIZE) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }
  if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    throw ParquetException("Invalid parquet file. Corrupt footer.");
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  size_t metadata_start = filesize - FOOTER_SIZE - metadata_len;
  if (metadata_start < 0) {
    throw ParquetException("Invalid parquet file. File is less than file metadata size.");
  }

  buffer_->Seek(metadata_start);

  std::vector<uint8_t> metadata_buffer(metadata_len);
  buffer_->Read(metadata_len, &metadata_buffer[0], &bytes_read);
  if (bytes_read != metadata_len) {
    throw ParquetException("Invalid parquet file. Could not read metadata bytes.");
  }
  DeserializeThriftMsg(&metadata_buffer[0], &metadata_len, &metadata_);
}

} // namespace parquet_cpp

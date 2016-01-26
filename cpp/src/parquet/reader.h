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

#ifndef PARQUET_FILE_READER_H
#define PARQUET_FILE_READER_H

#include <cstdint>
#include <string>
#include <stdio.h>

#include "parquet/thrift/parquet_types.h"
#include "parquet/parquet.h"

namespace parquet_cpp {

class FileLike {
 public:
  virtual ~FileLike() {}

  virtual void Close() = 0;
  virtual size_t Size() = 0;
  virtual size_t Tell() = 0;
  virtual void Seek(size_t pos) = 0;
  virtual void Read(size_t nbytes, uint8_t* out, size_t* bytes_read) = 0;
};


class LocalFile : public FileLike {
 public:
  LocalFile() : file_(nullptr), is_open_(false) {}
  virtual ~LocalFile();

  void Open(const std::string& path);

  virtual void Close();
  virtual size_t Size();
  virtual size_t Tell();
  virtual void Seek(size_t pos);
  virtual void Read(size_t nbytes, uint8_t* out, size_t* bytes_read);

  bool is_open() const { return is_open_;}
  const std::string& path() const { return path_;}

 private:
  std::string path_;
  FILE* file_;
  bool is_open_;
};


class ParquetFileReader {
 public:
  ParquetFileReader() : buffer_(nullptr) {}
  ~ParquetFileReader() {}

  // The class takes ownership of the passed file-like object
  void Open(FileLike* buffer);

  void Close();

  void ParseMetaData();

  const parquet::FileMetaData& metadata() const {
    return metadata_;
  }

 private:
  parquet::FileMetaData metadata_;
  FileLike* buffer_;
};


} // namespace parquet_cpp

#endif // PARQUET_FILE_READER_H

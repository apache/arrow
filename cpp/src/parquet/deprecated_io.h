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

// DEPRECATED IO INTERFACES: We have transitioned to using the Apache
// Arrow file input and output abstract interfaces defined in
// arrow/io/interfaces.h. These legacy interfaces are being preserved
// through a wrapper layer for one to two releases

#pragma once

#include <cstdint>
#include <memory>

#include "parquet/platform.h"

namespace parquet {

class PARQUET_EXPORT FileInterface {
 public:
  virtual ~FileInterface() = default;

  // Close the file
  virtual void Close() = 0;

  // Return the current position in the file relative to the start
  virtual int64_t Tell() = 0;
};

/// It is the responsibility of implementations to mind threadsafety of shared
/// resources
class PARQUET_EXPORT RandomAccessSource : virtual public FileInterface {
 public:
  virtual ~RandomAccessSource() = default;

  virtual int64_t Size() const = 0;

  // Returns bytes read
  virtual int64_t Read(int64_t nbytes, uint8_t* out) = 0;

  virtual std::shared_ptr<Buffer> Read(int64_t nbytes) = 0;

  virtual std::shared_ptr<Buffer> ReadAt(int64_t position, int64_t nbytes) = 0;

  /// Returns bytes read
  virtual int64_t ReadAt(int64_t position, int64_t nbytes, uint8_t* out) = 0;
};

class PARQUET_EXPORT OutputStream : virtual public FileInterface {
 public:
  virtual ~OutputStream() = default;

  // Copy bytes into the output stream
  virtual void Write(const uint8_t* data, int64_t length) = 0;
};

// ----------------------------------------------------------------------
// Wrapper classes

class PARQUET_EXPORT ParquetInputWrapper : public ::arrow::io::RandomAccessFile {
 public:
  explicit ParquetInputWrapper(std::unique_ptr<RandomAccessSource> source);
  explicit ParquetInputWrapper(RandomAccessSource* source);

  ~ParquetInputWrapper() override;

  // FileInterface
  ::arrow::Status Close() override;
  ::arrow::Result<int64_t> Tell() const override;
  bool closed() const override;

  // Seekable
  ::arrow::Status Seek(int64_t position) override;

  // InputStream / RandomAccessFile
  ::arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  ::arrow::Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override;
  ::arrow::Result<std::shared_ptr<Buffer>> ReadAt(int64_t position,
                                                  int64_t nbytes) override;
  ::arrow::Result<int64_t> GetSize() override;

 private:
  std::unique_ptr<RandomAccessSource> owned_source_;
  RandomAccessSource* source_;
  bool closed_;
};

class PARQUET_EXPORT ParquetOutputWrapper : public ::arrow::io::OutputStream {
 public:
  explicit ParquetOutputWrapper(std::shared_ptr<::parquet::OutputStream> sink);
  explicit ParquetOutputWrapper(std::unique_ptr<::parquet::OutputStream> sink);
  explicit ParquetOutputWrapper(::parquet::OutputStream* sink);

  ~ParquetOutputWrapper() override;

  // FileInterface
  ::arrow::Status Close() override;
  ::arrow::Result<int64_t> Tell() const override;
  bool closed() const override;

  // Writable
  ::arrow::Status Write(const void* data, int64_t nbytes) override;

 private:
  std::unique_ptr<::parquet::OutputStream> owned_sink_;
  std::shared_ptr<::parquet::OutputStream> shared_sink_;
  ::parquet::OutputStream* sink_;
  bool closed_;
};

}  // namespace parquet

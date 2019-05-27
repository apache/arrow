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

#include "parquet/deprecated_io.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>

#include "arrow/status.h"

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

ParquetInputWrapper::ParquetInputWrapper(std::unique_ptr<RandomAccessSource> source,
                    ::arrow::MemoryPool* pool)
    : ParquetInputWrapper(source.get(), pool),
      owned_source_(std::move(source)) {}

ParquetInputWrapper::ParquetInputWrapper(RandomAccessSource* source,
                                         ::arrow::MemoryPool* pool)
    : source_(source), pool_(pool), closed_(false) {}

ParquetInputWrapper::~ParquetInputWrapper() {
  if (!closed_) {
    source_->Close();
    closed_ = true;
  }
}

::arrow::Status ParquetInputWrapper::Close() {
  PARQUET_CATCH_NOT_OK(source_->Close());
  closed_ = true;
  return ::arrow::Status::OK();
}

::arrow::Status ParquetInputWrapper::Tell(int64_t* position) const {
  PARQUET_CATCH_NOT_OK(*position = source_->Tell());
  return ::arrow::Status::OK();
}

bool ParquetInputWrapper::closed() const {
  return closed_;
}

::arrow::Status ParquetInputWrapper::Seek(int64_t position) {
  PARQUET_CATCH_NOT_OK(source_->Seek(position));
  return ::arrow::Status::OK();
}

::arrow::Status ParquetInputWrapper::Read(int64_t nbytes, int64_t* bytes_read, void* out) {
  PARQUET_CATCH_NOT_OK(*bytes_read = source_->Read(nbytes, out));
  return ::arrow::Status::OK();
}

::arrow::Status ParquetInputWrapper::Read(int64_t nbytes, std::shared_ptr<Buffer>* out) {
  PARQUET_CATCH_NOT_OK(*out = source_->Read(nbytes));
  return ::arrow::Status::OK();
}

::arrow::Status ParquetInputWrapper::ReadAt(int64_t position, int64_t nbytes,
                                            std::shared_ptr<Buffer>* out) {
  PARQUET_CATCH_NOT_OK(*out = source_->ReadAt(position, nbytes));
  return ::arrow::Status::OK();
}

ParquetOutputWrapper::ParquetOutputWrapper(std::unique_ptr<OutputStream> sink)
    : ParquetOutputWrapper(sink.get()),
      owned_sink_(std::move(sink)) {}

ParquetOutputWrapper::ParquetOutputWrapper(OutputStream* sink)
    : sink_(sink), closed_(false) {}

ParquetOutputWrapper::~ParquetOutputWrapper() {
  if (!closed_) {
    sink_->Close();
    closed_ = true;
  }
}

::arrow::Status ParquetOutputWrapper::Close() {
  PARQUET_CATCH_NOT_OK(sink_->Close());
  closed_ = true;
  return ::arrow::Status::OK();
}

::arrow::Status ParquetOutputWrapper::Tell(int64_t* position) const {
  PARQUET_CATCH_NOT_OK(*position = source_->Tell());
  return ::arrow::Status::OK();
}

bool ParquetOutputWrapper::closed() const {
  return closed_;
}

::arrow::Status ParquetOutputWrapper::Write(const void* data, int64_t nbytes) {
  PARQUET_CATCH_NOT_OK(sink_->Write(reinterpret_cast<const uint8_t*>(data), nbytes));
  return ::arrow::Status::OK();
}

}  // namespace parquet

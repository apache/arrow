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

#include "arrow_types.h"

using namespace Rcpp;

// ------ arrow::io::Readable

// [[Rcpp::export]]
std::shared_ptr<arrow::Buffer> io___Readable__Read(
    const std::shared_ptr<arrow::io::Readable>& x, int64_t nbytes) {
  std::shared_ptr<arrow::Buffer> buf;
  R_ERROR_NOT_OK(x->Read(nbytes, &buf));
  return buf;
}

// ------ arrow::io::InputStream

// [[Rcpp::export]]
void io___InputStream__Close(const std::shared_ptr<arrow::io::InputStream>& x) {
  R_ERROR_NOT_OK(x->Close());
}

// ------ arrow::io::RandomAccessFile

// [[Rcpp::export]]
int64_t io___RandomAccessFile__GetSize(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  int64_t out;
  R_ERROR_NOT_OK(x->GetSize(&out));
  return out;
}

// [[Rcpp::export]]
bool io___RandomAccessFile__supports_zero_copy(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  return x->supports_zero_copy();
}

// [[Rcpp::export]]
void io___RandomAccessFile__Seek(const std::shared_ptr<arrow::io::RandomAccessFile>& x,
                                 int64_t position) {
  R_ERROR_NOT_OK(x->Seek(position));
}

// [[Rcpp::export]]
int64_t io___RandomAccessFile__Tell(
    const std::shared_ptr<arrow::io::RandomAccessFile>& x) {
  int64_t out;
  R_ERROR_NOT_OK(x->Tell(&out));
  return out;
}

// ------ arrow::io::MemoryMappedFile

// [[Rcpp::export]]
std::shared_ptr<arrow::io::MemoryMappedFile> io___MemoryMappedFile__Create(
    const std::string& path, int64_t size) {
  std::shared_ptr<arrow::io::MemoryMappedFile> out;
  R_ERROR_NOT_OK(arrow::io::MemoryMappedFile::Create(path, size, &out));
  return out;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::io::MemoryMappedFile> io___MemoryMappedFile__Open(
    const std::string& path, arrow::io::FileMode::type mode) {
  std::shared_ptr<arrow::io::MemoryMappedFile> out;
  R_ERROR_NOT_OK(arrow::io::MemoryMappedFile::Open(path, mode, &out));
  return out;
}

// [[Rcpp::export]]
void io___MemoryMappedFile__Resize(const std::shared_ptr<arrow::io::MemoryMappedFile>& x,
                                   int64_t size) {
  R_ERROR_NOT_OK(x->Resize(size));
}

// ------ arrow::io::ReadableFile

// [[Rcpp::export]]
std::shared_ptr<arrow::io::ReadableFile> io___ReadableFile__Open(
    const std::string& path) {
  std::shared_ptr<arrow::io::ReadableFile> out;
  R_ERROR_NOT_OK(arrow::io::ReadableFile::Open(path, &out));
  return out;
}

// ------ arrow::io::BufferReader

// [[Rcpp::export]]
std::shared_ptr<arrow::io::BufferReader> io___BufferReader__initialize(
    const std::shared_ptr<arrow::Buffer>& buffer) {
  return std::make_shared<arrow::io::BufferReader>(buffer);
}

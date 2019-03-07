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

#include "./arrow_types.h"

// [[Rcpp::export]]
std::unique_ptr<arrow::util::Codec> util___Codec__Create(arrow::Compression::type codec) {
  std::unique_ptr<arrow::util::Codec> out;
  STOP_IF_NOT_OK(arrow::util::Codec::Create(codec, &out));
  return out;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::io::CompressedOutputStream> io___CompressedOutputStream__Make(
    const std::unique_ptr<arrow::util::Codec>& codec,
    const std::shared_ptr<arrow::io::OutputStream>& raw) {
  std::shared_ptr<arrow::io::CompressedOutputStream> stream;
  STOP_IF_NOT_OK(arrow::io::CompressedOutputStream::Make(codec.get(), raw, &stream));
  return stream;
}

// [[Rcpp::export]]
std::shared_ptr<arrow::io::CompressedInputStream> io___CompressedInputStream__Make(
    const std::unique_ptr<arrow::util::Codec>& codec,
    const std::shared_ptr<arrow::io::InputStream>& raw) {
  std::shared_ptr<arrow::io::CompressedInputStream> stream;
  STOP_IF_NOT_OK(arrow::io::CompressedInputStream::Make(codec.get(), raw, &stream));
  return stream;
}

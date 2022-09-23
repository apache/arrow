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

#include "./safe-call-into-r.h"

#include <arrow/ipc/feather.h>
#include <arrow/type.h>

// ---------- WriteFeather

// [[arrow::export]]
void ipc___WriteFeather__Table(const std::shared_ptr<arrow::io::OutputStream>& stream,
                               const std::shared_ptr<arrow::Table>& table, int version,
                               int chunk_size, arrow::Compression::type compression,
                               int compression_level) {
  auto properties = arrow::ipc::feather::WriteProperties::Defaults();
  properties.version = version;
  properties.chunksize = chunk_size;
  properties.compression = compression;
  if (compression_level != -1) {
    properties.compression_level = compression_level;
  }
  StopIfNotOk(arrow::ipc::feather::WriteTable(*table, stream.get(), properties));
}

// ----------- Reader

// [[arrow::export]]
int ipc___feather___Reader__version(
    const std::shared_ptr<arrow::ipc::feather::Reader>& reader) {
  return reader->version();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> ipc___feather___Reader__Read(
    const std::shared_ptr<arrow::ipc::feather::Reader>& reader, cpp11::sexp columns) {
  bool use_names = columns != R_NilValue;
  std::vector<std::string> names;
  if (use_names) {
    cpp11::strings columns_chr(columns);
    names.reserve(columns_chr.size());
    for (const auto& name : columns_chr) {
      names.push_back(name);
    }
  }

  auto result = RunWithCapturedRIfPossible<std::shared_ptr<arrow::Table>>([&]() {
    std::shared_ptr<arrow::Table> table;
    arrow::Status read_result;
    if (use_names) {
      read_result = reader->Read(names, &table);
    } else {
      read_result = reader->Read(&table);
    }

    if (read_result.ok()) {
      return arrow::Result<std::shared_ptr<arrow::Table>>(table);
    } else {
      return arrow::Result<std::shared_ptr<arrow::Table>>(read_result);
    }
  });

  return ValueOrStop(result);
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::feather::Reader> ipc___feather___Reader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& stream) {
  auto result = RunWithCapturedRIfPossible<std::shared_ptr<arrow::ipc::feather::Reader>>(
      [&]() { return arrow::ipc::feather::Reader::Open(stream); });
  return ValueOrStop(result);
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ipc___feather___Reader__schema(
    const std::shared_ptr<arrow::ipc::feather::Reader>& reader) {
  return reader->schema();
}

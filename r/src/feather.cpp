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

#if defined(ARROW_R_WITH_ARROW)
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
    const std::shared_ptr<arrow::ipc::feather::Reader>& reader, SEXP columns) {
  std::shared_ptr<arrow::Table> table;

  switch (TYPEOF(columns)) {
    case STRSXP: {
      R_xlen_t n = XLENGTH(columns);
      std::vector<std::string> names(n);
      for (R_xlen_t i = 0; i < n; i++) {
        names[i] = CHAR(STRING_ELT(columns, i));
      }
      StopIfNotOk(reader->Read(names, &table));
      break;
    }
    case NILSXP:
      StopIfNotOk(reader->Read(&table));
      break;
    default:
      cpp11::stop("incompatible column specification");
      break;
  }

  return table;
}

// [[arrow::export]]
std::shared_ptr<arrow::ipc::feather::Reader> ipc___feather___Reader__Open(
    const std::shared_ptr<arrow::io::RandomAccessFile>& stream) {
  return ValueOrStop(arrow::ipc::feather::Reader::Open(stream));
}

// [[arrow::export]]
std::shared_ptr<arrow::Schema> ipc___feather___Reader__schema(
    const std::shared_ptr<arrow::ipc::feather::Reader>& reader) {
  return reader->schema();
}

#endif

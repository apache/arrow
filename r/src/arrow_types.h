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

#include <limits>
#include <memory>
#include <vector>

#include <RcppCommon.h>
#undef Free

#include "./symbols.h"
#include "./Rcpp_arrow_forward.h"
#include <Rcpp.h>
#include "./Rcpp_arrow_definitions.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/compressed.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>

RCPP_EXPOSED_ENUM_NODECL(arrow::Type::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::DateUnit)
RCPP_EXPOSED_ENUM_NODECL(arrow::TimeUnit::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::StatusCode)
RCPP_EXPOSED_ENUM_NODECL(arrow::io::FileMode::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::ipc::Message::Type)
RCPP_EXPOSED_ENUM_NODECL(arrow::Compression::type)

SEXP ChunkedArray__as_vector(const std::shared_ptr<arrow::ChunkedArray>& chunked_array);
SEXP Array__as_vector(const std::shared_ptr<arrow::Array>& array);
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x, SEXP type);
std::shared_ptr<arrow::RecordBatch> RecordBatch__from_arrays(SEXP, SEXP);

namespace arrow {
namespace r {

void inspect(SEXP obj);

// the integer64 sentinel
constexpr int64_t NA_INT64 = std::numeric_limits<int64_t>::min();

template <int RTYPE, typename Vec = Rcpp::Vector<RTYPE>>
class RBuffer : public MutableBuffer {
public:
  explicit RBuffer(Vec vec)
    : MutableBuffer(reinterpret_cast<uint8_t*>(vec.begin()),
      vec.size() * sizeof(typename Vec::stored_type)),
      vec_(vec) {}

private:
  // vec_ holds the memory
  Vec vec_;
};

}
}

#endif

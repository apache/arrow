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

// This gets included in arrowExports.cpp

#pragma once

#include "./arrow_rcpp.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/csv/reader.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/compressed.h>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/json/reader.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

RCPP_EXPOSED_ENUM_NODECL(arrow::Type::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::DateUnit)
RCPP_EXPOSED_ENUM_NODECL(arrow::TimeUnit::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::StatusCode)
RCPP_EXPOSED_ENUM_NODECL(arrow::io::FileMode::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::ipc::Message::Type)
RCPP_EXPOSED_ENUM_NODECL(arrow::Compression::type)
RCPP_EXPOSED_ENUM_NODECL(arrow::fs::FileType)
RCPP_EXPOSED_ENUM_NODECL(parquet::ParquetVersion::type)

namespace ds = ::arrow::dataset;
namespace fs = ::arrow::fs;

#endif

#if defined(ARROW_R_WITH_S3)
#include <arrow/filesystem/s3fs.h>
#endif

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

#include <memory>
#include <vector>

#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/type_fwd.h"

namespace parquet {

class PARQUET_EXPORT ParquetFileRewriter {
 public:
  struct PARQUET_EXPORT Contents {
    virtual ~Contents() = default;
    virtual void Close() = 0;
    virtual void Rewrite() = 0;
  };

  ParquetFileRewriter();
  ~ParquetFileRewriter();

  static std::unique_ptr<ParquetFileRewriter> Open(
      std::vector<std::vector<std::shared_ptr<ArrowInputFile>>> sources,
      std::shared_ptr<ArrowOutputStream> sink,
      std::vector<std::vector<std::shared_ptr<FileMetaData>>> sources_metadata,
      std::shared_ptr<const ::arrow::KeyValueMetadata> sink_metadata = NULLPTR,
      std::shared_ptr<RewriterProperties> props = default_rewriter_properties());

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  /// Rewrite all Parquet files.
  ///
  /// This method may throw.
  void Rewrite();

 private:
  std::unique_ptr<Contents> contents_;
};

}  // namespace parquet

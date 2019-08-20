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

#include "arrow/dataset/file_base.h"

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"

namespace arrow {
namespace dataset {

Status FileSource::Open(std::shared_ptr<arrow::io::RandomAccessFile>* out) const {
  switch (type_) {
    case PATH:
      return filesystem_->OpenInputFile(path_, out);
    case BUFFER:
      *out = std::make_shared<::arrow::io::BufferReader>(buffer_);
      return Status::OK();
  }

  return Status::OK();
}

Status FileBasedDataFragment::Scan(std::shared_ptr<ScanContext> scan_context,
                                   std::unique_ptr<ScanTaskIterator>* out) {
  return format_->ScanFile(source_, scan_options_, scan_context, out);
}

}  // namespace dataset
}  // namespace arrow

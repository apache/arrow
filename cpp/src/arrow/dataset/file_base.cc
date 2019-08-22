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

#include <algorithm>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/stl.h"

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

FileSystemBasedDataSource::FileSystemBasedDataSource(
    fs::FileSystem* filesystem, const fs::Selector& selector,
    std::shared_ptr<FileFormat> format, std::shared_ptr<ScanOptions> scan_options,
    std::vector<fs::FileStats> stats)
    : filesystem_(filesystem),
      selector_(std::move(selector)),
      format_(std::move(format)),
      scan_options_(std::move(scan_options)),
      stats_(std::move(stats)) {}

Status FileSystemBasedDataSource::Make(fs::FileSystem* filesystem,
                                       const fs::Selector& selector,
                                       std::shared_ptr<FileFormat> format,
                                       std::shared_ptr<ScanOptions> scan_options,
                                       std::unique_ptr<FileSystemBasedDataSource>* out) {
  std::vector<fs::FileStats> stats;
  RETURN_NOT_OK(filesystem->GetTargetStats(selector, &stats));

  auto new_end =
      std::remove_if(stats.begin(), stats.end(), [&](const fs::FileStats& stats) {
        return stats.type() != fs::FileType::File ||
               !format->IsKnownExtension(stats.extension());
      });
  stats.resize(new_end - stats.begin());

  out->reset(new FileSystemBasedDataSource(filesystem, selector, std::move(format),
                                           std::move(scan_options), std::move(stats)));
  return Status::OK();
}

std::unique_ptr<DataFragmentIterator> FileSystemBasedDataSource::GetFragments(
    std::shared_ptr<ScanOptions> options) {
  struct Impl : DataFragmentIterator {
    Impl(fs::FileSystem* filesystem, std::shared_ptr<FileFormat> format,
         std::shared_ptr<ScanOptions> scan_options, std::vector<fs::FileStats> stats)
        : filesystem_(filesystem),
          format_(std::move(format)),
          scan_options_(std::move(scan_options)),
          stats_(std::move(stats)) {}

    Status Next(std::shared_ptr<DataFragment>* out) {
      if (i_ == stats_.size()) {
        *out = nullptr;
        return Status::OK();
      }
      FileSource src(stats_[i_++].path(), filesystem_);

      std::unique_ptr<DataFragment> fragment;
      RETURN_NOT_OK(format_->MakeFragment(src, scan_options_, &fragment));
      *out = std::move(fragment);
      return Status::OK();
    }

    size_t i_ = 0;
    fs::FileSystem* filesystem_;
    std::shared_ptr<FileFormat> format_;
    std::shared_ptr<ScanOptions> scan_options_;
    std::vector<fs::FileStats> stats_;
  };

  return internal::make_unique<Impl>(filesystem_, format_, options, stats_);
}

}  // namespace dataset
}  // namespace arrow

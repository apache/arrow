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

#include "arrow/dataset/dataset_writer.h"

#include <memory>
#include <utility>

#include "arrow/compute/expression.h"
#include "arrow/dataset/partition.h"
#include "arrow/util/async_generator.h"

namespace arrow::dataset::internal {

Future<> DatasetWriteCleanup::Initialize(const FileSystemDatasetWriteOptions& options,
                                         const std::string& directory,
                                         const std::string& prefix) {
  const auto& filesystem = options.filesystem;
  auto create_directory = [filesystem, directory] {
    return DeferNotOk(filesystem->io_context().executor()->Submit(
        [filesystem, directory] { return filesystem->CreateDir(directory); }));
  };
  if (options.existing_data_behavior != ExistingDataBehavior::kDeleteMatchingPartitions) {
    return create_directory();
  }
  if (directory != options.base_dir ||
      dynamic_cast<const FilenamePartitioning*>(options.partitioning.get()) == nullptr) {
    return filesystem->DeleteDirContentsAsync(directory, /*missing_dir_ok=*/true)
        .Then(create_directory);
  }
  return DeleteFilenamePartition(options, directory, prefix).Then(create_directory);
}

Future<> DatasetWriteCleanup::DeleteFilenamePartition(
    const FileSystemDatasetWriteOptions& options, const std::string& directory,
    const std::string& prefix) {
  const auto& filesystem = options.filesystem;
  std::call_once(list_files_, [&] {
    // Filename partitions share a directory. Read the old file list before
    // releasing any partition to write new files into it.
    fs::FileSelector selector;
    selector.base_dir = directory;
    selector.allow_not_found = true;
    auto files = std::make_shared<FilesByPrefix>();
    auto collect = [files, partitioning =
                               options.partitioning](const fs::FileInfoVector& entries) {
      for (const auto& info : entries) {
        if (info.type() != fs::FileType::File) continue;
        auto expression = partitioning->Parse(info.path());
        if (!expression.ok() || (partitioning->schema()->num_fields() > 0 &&
                                 *expression == compute::literal(true))) {
          continue;
        }
        auto path = partitioning->Format(*expression);
        if (path.ok()) {
          (*files)[path->filename].push_back(info);
        }
      }
      return Status::OK();
    };
    existing_files_ = VisitAsyncGenerator(filesystem->GetFileInfoGenerator(selector),
                                          std::move(collect))
                          .Then([files] { return std::move(*files); });
  });

  return existing_files_.Then(
      [filesystem, format = options.format(), prefix](const FilesByPrefix& files) {
        auto matching = files.find(prefix);
        if (matching == files.end()) {
          return Future<>::MakeFinished();
        }
        return DeferNotOk(filesystem->io_context().executor()->Submit(
            [filesystem, format, entries = matching->second] {
              std::vector<std::string> paths;
              for (const auto& info : entries) {
                ARROW_ASSIGN_OR_RAISE(bool supported,
                                      format->IsSupported(FileSource(info, filesystem)));
                if (supported) paths.push_back(info.path());
              }
              return filesystem->DeleteFiles(paths);
            }));
      });
}

}  // namespace arrow::dataset::internal

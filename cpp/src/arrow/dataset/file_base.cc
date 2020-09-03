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

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<io::RandomAccessFile>> FileSource::Open() const {
  if (filesystem_) {
    return filesystem_->OpenInputFile(file_info_);
  }

  if (buffer_) {
    return std::make_shared<io::BufferReader>(buffer_);
  }

  return custom_open_();
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Schema> physical_schema) {
  return MakeFragment(std::move(source), scalar(true), std::move(physical_schema));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return MakeFragment(std::move(source), std::move(partition_expression), nullptr);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<Schema> physical_schema) {
  return std::shared_ptr<FileFragment>(
      new FileFragment(std::move(source), shared_from_this(),
                       std::move(partition_expression), std::move(physical_schema)));
}

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchemaImpl() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                            std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(std::move(options), std::move(context), this);
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::shared_ptr<fs::FileSystem> filesystem,
                                     std::vector<std::shared_ptr<FileFragment>> fragments)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      filesystem_(std::move(filesystem)),
      fragments_(std::move(fragments)) {}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format, std::shared_ptr<fs::FileSystem> filesystem,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  return std::shared_ptr<FileSystemDataset>(new FileSystemDataset(
      std::move(schema), std::move(root_partition), std::move(format),
      std::move(filesystem), std::move(fragments)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(new FileSystemDataset(
      std::move(schema), partition_expression_, format_, filesystem_, fragments_));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  for (const auto& fragment : fragments_) {
    files.push_back(fragment->source().path());
  }

  return files;
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (fragments_.empty()) {
    return repr + " []";
  }

  for (const auto& fragment : fragments_) {
    repr += "\n" + fragment->source().path();

    const auto& partition = fragment->partition_expression();
    if (!partition->Equals(true)) {
      repr += ": " + partition->ToString();
    }
  }

  return repr;
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<Expression> predicate) {
  FragmentVector fragments;

  for (const auto& fragment : fragments_) {
    if (predicate->IsSatisfiableWith(fragment->partition_expression())) {
      fragments.push_back(fragment);
    }
  }

  return MakeVectorIterator(std::move(fragments));
}

struct WriteTask {
  Status Execute();

  /// The basename of files written by this WriteTask. Extensions
  /// are derived from format
  std::string basename;

  /// The partitioning with which paths will be generated
  std::shared_ptr<Partitioning> partitioning;

  /// The format in which fragments will be written
  std::shared_ptr<FileFormat> format;

  /// The FileSystem and base directory into which fragments will be written
  std::shared_ptr<fs::FileSystem> filesystem;
  std::string base_dir;

  /// Batches to be written
  std::shared_ptr<RecordBatchReader> batches;

  /// An Expression already satisfied by every batch to be written
  std::shared_ptr<Expression> partition_expression;
};

Status WriteTask::Execute() {
  std::unordered_map<std::string, RecordBatchVector> path_to_batches;

  // TODO(bkietz) these calls to Partition() should be scattered across a TaskGroup
  for (auto maybe_batch : IteratorFromReader(batches)) {
    ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
    ARROW_ASSIGN_OR_RAISE(auto partitioned_batches, partitioning->Partition(batch));
    for (auto&& partitioned_batch : partitioned_batches) {
      AndExpression expr(std::move(partitioned_batch.partition_expression),
                         partition_expression);
      ARROW_ASSIGN_OR_RAISE(std::string path, partitioning->Format(expr));
      path = fs::internal::EnsureLeadingSlash(path);
      path_to_batches[path].push_back(std::move(partitioned_batch.batch));
    }
  }

  for (auto&& path_batches : path_to_batches) {
    auto dir = base_dir + path_batches.first;
    RETURN_NOT_OK(filesystem->CreateDir(dir, /*recursive=*/true));

    auto path = fs::internal::ConcatAbstractPath(dir, basename);
    ARROW_ASSIGN_OR_RAISE(auto destination, filesystem->OpenOutputStream(path));

    DCHECK(!path_batches.second.empty());
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          RecordBatchReader::Make(std::move(path_batches.second)));
    RETURN_NOT_OK(format->WriteFragment(reader.get(), destination.get()));
  }

  return Status::OK();
}

Status FileSystemDataset::Write(std::shared_ptr<Schema> schema,
                                std::shared_ptr<FileFormat> format,
                                std::shared_ptr<fs::FileSystem> filesystem,
                                std::string base_dir,
                                std::shared_ptr<Partitioning> partitioning,
                                std::shared_ptr<ScanContext> scan_context,
                                FragmentIterator fragment_it) {
  auto task_group = scan_context->TaskGroup();

  base_dir = fs::internal::RemoveTrailingSlash(base_dir).to_string();

  for (const auto& f : partitioning->schema()->fields()) {
    if (f->type()->id() == Type::DICTIONARY) {
      return Status::NotImplemented("writing with dictionary partitions");
    }
  }

  int i = 0;
  for (auto maybe_fragment : fragment_it) {
    ARROW_ASSIGN_OR_RAISE(auto fragment, std::move(maybe_fragment));
    auto task = std::make_shared<WriteTask>();

    task->basename = "dat_" + std::to_string(i++) + "." + format->type_name();
    task->partition_expression = fragment->partition_expression();
    task->format = format;
    task->filesystem = filesystem;
    task->base_dir = base_dir;
    task->partitioning = partitioning;

    // make a record batch reader which yields from a fragment
    ARROW_ASSIGN_OR_RAISE(task->batches, FragmentRecordBatchReader::Make(
                                             std::move(fragment), schema, scan_context));
    task_group->Append([task] { return task->Execute(); });
  }

  return task_group->Finish();
}

}  // namespace dataset
}  // namespace arrow

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

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  if (filesystem_) {
    return filesystem_->OpenInputFile(file_info_);
  }

  if (buffer_) {
    return std::make_shared<::arrow::io::BufferReader>(buffer_);
  }

  return custom_open_();
}

Result<std::shared_ptr<arrow::io::OutputStream>> WritableFileSource::Open() const {
  if (filesystem_) {
    auto parent = fs::internal::GetAbstractPathParent(path_).first;
    RETURN_NOT_OK(filesystem_->CreateDir(parent, /* recursive = */ true));
    return filesystem_->OpenOutputStream(path_);
  }

  return std::make_shared<::arrow::io::BufferOutputStream>(buffer_);
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

Result<std::shared_ptr<FileFragment>> FileFormat::WriteFragment(
    WritableFileSource destination, std::shared_ptr<Expression> partition_expression,
    std::shared_ptr<RecordBatchReader> batches) {
  return Status::NotImplemented("writing fragment of format ", type_name());
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
                                     std::vector<std::shared_ptr<FileFragment>> fragments)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      fragments_(std::move(fragments)) {}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  return std::shared_ptr<FileSystemDataset>(
      new FileSystemDataset(std::move(schema), std::move(root_partition),
                            std::move(format), std::move(fragments)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(new FileSystemDataset(
      std::move(schema), partition_expression_, format_, fragments_));
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

Result<std::vector<std::shared_ptr<FileFragment>>> WriteTask::Execute() {
  // to_write = {}
  // to_write_exprs = {}
  //
  // for batch in batches:
  //   for sub in partitioning.partition(batch):
  //     sub_path = partitioning.format(sub.partition_expression)
  //
  //     if sub_path not in to_write:
  //       to_write[sub_path] = []
  //       to_write_exprs[sub_path] = sub.partition_expression
  //     to_write[sub_path] += [sub.batch]
  //
  // partition_dir = partitioning.format(partition_expression)
  //
  // fragments = []
  // for sub_path, batches in to_write.items():
  //   file = 'dat.' + format.type_name()
  //   path = '/'.join([base_dir, partition_dir, sub_path, file])
  //   expr = to_write_exprs[sub_path] and partition_expression
  //   fragments += [format.write({path, filesystem}, expr, batches)]
  //
  // return fragments

  struct DoWrite {
    RecordBatchVector batches;
    std::shared_ptr<Expression> partition_expression;
  };

  std::unordered_map<std::string, DoWrite> do_writes;

  for (auto maybe_batch : IteratorFromReader(batches)) {
    ARROW_ASSIGN_OR_RAISE(auto batch, std::move(maybe_batch));
    ARROW_ASSIGN_OR_RAISE(auto partitioned_batches, partitioning->Partition(batch));
    for (auto&& partitioned_batch : partitioned_batches) {
      auto expr =
          and_(std::move(partitioned_batch.partition_expression), partition_expression);
      ARROW_ASSIGN_OR_RAISE(std::string path, partitioning->Format(*expr));

      auto do_write = do_writes.emplace(path, DoWrite{{}, std::move(expr)}).first;
      do_write->second.batches.push_back(std::move(partitioned_batch.batch));
    }
  }

  std::string file = "dat." + format->type_name();

  auto filesystem = this->filesystem;
  if (filesystem == nullptr) {
    filesystem = std::make_shared<fs::LocalFileSystem>();
  }

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (auto&& path_write : do_writes) {
    auto path = fs::internal::JoinAbstractPath(
        std::vector<std::string>{base_dir, path_write.first, file});

    DCHECK(!path_write.second.batches.empty());

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          RecordBatchReader::Make(std::move(path_write.second.batches)));

    ARROW_ASSIGN_OR_RAISE(
        auto fragment,
        format->WriteFragment({std::move(path), filesystem},
                              std::move(path_write.second.partition_expression),
                              std::move(reader)));

    fragments.push_back(std::move(fragment));
  }

  return fragments;
}

Result<std::shared_ptr<FileSystemDataset>> WritePlan::Execute() {
  if (task_group == nullptr) {
    task_group = internal::TaskGroup::MakeSerial();
  }

  if (tasks.empty()) {
    return Status::Invalid("no WriteTasks specified");
  }

  auto format = tasks[0]->format;

  std::vector<std::vector<std::shared_ptr<FileFragment>>> written_fragments(tasks.size());
  for (size_t i = 0; i < tasks.size(); ++i) {
    task_group->Append([&, i] {
      ARROW_ASSIGN_OR_RAISE(written_fragments[i], tasks[i]->Execute());
      return Status::OK();
    });
  }

  RETURN_NOT_OK(task_group->Finish());

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (auto&& fs : written_fragments) {
    for (auto&& f : fs) {
      fragments.push_back(std::move(f));
    }
  }

  return FileSystemDataset::Make(schema, scalar(true), std::move(format),
                                 std::move(fragments));
}

}  // namespace dataset
}  // namespace arrow

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

Result<std::shared_ptr<WriteTask>> FileFormat::WriteFragment(
    WritableFileSource destination, std::shared_ptr<Fragment> fragment,
    std::shared_ptr<ScanOptions> scan_options,
    std::shared_ptr<ScanContext> scan_context) {
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

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Write(
    const WritePlan& plan, std::shared_ptr<ScanOptions> scan_options,
    std::shared_ptr<ScanContext> scan_context) {
  auto filesystem = plan.filesystem;
  if (filesystem == nullptr) {
    filesystem = std::make_shared<fs::LocalFileSystem>();
  }

  auto task_group = scan_context->TaskGroup();
  auto partition_base_dir = fs::internal::EnsureTrailingSlash(plan.partition_base_dir);
  auto extension = "." + plan.format->type_name();

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (size_t i = 0; i < plan.paths.size(); ++i) {
    const auto& op = plan.fragment_or_partition_expressions[i];
    if (op.kind() == WritePlan::FragmentOrPartitionExpression::FRAGMENT) {
      auto path = partition_base_dir + plan.paths[i] + extension;

      const auto& input_fragment = op.fragment();
      FileSource dest(path, filesystem);

      ARROW_ASSIGN_OR_RAISE(auto write_task,
                            plan.format->WriteFragment({path, filesystem}, input_fragment,
                                                       scan_options, scan_context));
      task_group->Append([write_task] { return write_task->Execute(); });

      ARROW_ASSIGN_OR_RAISE(
          auto fragment, plan.format->MakeFragment(
                             {path, filesystem}, input_fragment->partition_expression()));
      fragments.push_back(std::move(fragment));
    }
  }

  RETURN_NOT_OK(task_group->Finish());

  return Make(plan.schema, scalar(true), plan.format, fragments);
}

Status WriteTask::CreateDestinationParentDir() const {
  if (auto filesystem = destination_.filesystem()) {
    auto parent = fs::internal::GetAbstractPathParent(destination_.path()).first;
    return filesystem->CreateDir(parent, /* recursive = */ true);
  }

  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
